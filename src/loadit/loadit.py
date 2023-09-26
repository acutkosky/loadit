from .sharded_dataset import ShardedDataset
from .dict_cache import DictCache
from .writer import WriterPool
from .util import size_estimator
from typing import Any, Union, Optional, Iterable, Callable, List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import logging

logger = logging.getLogger("randomacces")

PreloadType = Callable[[Any, int], List[int]]

def preload_next_shard(loader: Any, idx: int) -> List[int]:
    return [idx + i * loader.shards.max_shard_length for i in range(1, loader.max_workers)]


def preload_next_shard_in_indices(indices: List[int], loader: Any, idx: int) -> List[int]:
    return [indices[max(len(indices) - 1, idx + 1)] + loader.shards.max_shard_length]


class View:
    def __init__(self, loader, indices):
        self.loader = loader
        self.indices = indices
        self.preload_fn = lambda loader, idx: preload_next_shard_in_indices(
            self.indices, loader, idx
        )

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            return View(self.loader, self.indices[idx])

        return self.loader.get(self.indices[idx], self.preload_fn)

    def __len__(self):
        return len(self.indices)

    def __iter__(self):
        idx = 0
        while True:
            try:
                yield self[idx]
                idx += 1
            except IndexError:
                return


class LoadIt:
    def __init__(
        self,
        create_it: Optional[Callable[None, Iterable]],
        root_dir: Union[str, Path] = "cache/",
        max_shard_length: Optional[Union[str, int]] = 512,
        max_cache_size: int = 128,
        max_workers: int = 3,
        memory_limit: Optional[int] = None,
        preload_fn: Optional[PreloadType] = preload_next_shard,
    ):

        if create_it is None:
            max_shard_length = None

        if isinstance(max_shard_length, str):
            assert max_shard_length[-2:].lower() == 'mb'
            length_mb = float(max_shard_length[:-2])
            max_shard_length = int(length_mb * (2**20) / size_estimator(create_it()))
            
        
        if create_it is not None:
            self.writer_pool = WriterPool(
                create_it=create_it,
                num_workers=max_workers,
            )
            shard_load_fn = self.writer_pool.load_fn
        else:
            self.writer_pool = None
            shard_load_fn = None
            
        self.shards = ShardedDataset(
            max_size_bytes=memory_limit,
            root_dir=root_dir,
            max_shard_length=max_shard_length,
            load_fn=shard_load_fn,
        )
        self.max_workers = max_workers
        if self.max_workers > 1:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers - 1)
        else:
            self.executor = None

        def load_from_disk(d, k):
            shard = self.shards[k]
            d.cache[k] = shard
            return shard

        self.memory_cache = DictCache(max_size=max_cache_size, load_fn=load_from_disk)

        self.preload_fn = preload_fn

    def get_start_idx(self, idx: int) -> int:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        return start_idx

    def __getitem__(self, idx: int) -> Any:
        return self.get(idx)

    def get(self, idx: int, preload_fn: Optional[PreloadType] = None) -> Any:
        if isinstance(idx, slice):
            step = idx.step or 1
            start = idx.start or 0
            stop = idx.stop or len(self)

            if start < 0:
                start = start + len(self)

            if stop < 0:
                stop = stop + len(self)

            indices = list(range(start, stop, step))
            return View(self, indices)
        if idx < 0:
            idx = len(self) + idx
        start_idx = self.get_start_idx(idx)

        # schedule a preload if enabled
        preload_fn = preload_fn or self.preload_fn
        if self.max_workers > 1 and preload_fn is not None:
            for n_idx in preload_fn(self, idx):
                self.load_async(n_idx)

        # actually fetch the requested data        
        try:
            shard = self.memory_cache[start_idx]
        except KeyError:
            raise IndexError(f"index {idx} out of range!")


        return shard[idx - start_idx]

    def __len__(self):
        optional_length = self.shards.length()
        if optional_length is not None:
            return optional_length
        guess = 100
        while optional_length is None:
            try:
                self[guess]
            except IndexError:
                pass
            guess *= 10
            optional_length = self.shards.length()
        return optional_length

    def load_async(self, idx) -> None:
        if self.executor is None:
            return
        start_idx = self.get_start_idx(idx)
        self.memory_cache.load_async(start_idx, self.executor)

    def __iter__(self):
        idx = 0
        while True:
            try:
                yield self[idx]
                idx += 1
            except IndexError:
                return
