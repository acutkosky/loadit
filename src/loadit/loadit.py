from .sharded_dataset import ShardedDataset, dataset_metadata
from .dict_cache import DictCache
from .writer import WriterPool
from .util import size_estimator, SequenceView, is_sequence
from typing import Any, Union, Optional, Iterable, Callable, List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import time
import re
from collections.abc import Sequence

import logging

logger = logging.getLogger("loadit")

PreloadType = Callable[[Any, int], List[int]]


def preload_next_shard(loader: Any, idx: int) -> Iterable[List[int]]:
    return [
        [idx + i * loader.shards.max_shard_length]
        for i in range(1, loader.max_workers // 2)
    ]


class LoaditView(SequenceView):
    def __init__(self, loader, indices: SequenceView):
        self.loader = loader
        self.indices = indices

    def preload_fn(self, loader, idx: int) -> Sequence[List[int]]:
        assert loader == self.loader
        num_to_preload = self.loader.max_workers // 2
        next_indices = list(
            set(
                [
                    loader.get_start_idx(
                        self.indices[min(len(self.indices) - 1, idx + i)]
                        + loader.shards.max_shard_length
                    )
                    for i in range(1, num_to_preload)
                ]
            )
        )
        result = [[idx] for idx in next_indices]
        return result

    def __len__(self):
        return len(self.indices)

    def __getitem__(self, *idx):
        if len(idx) == 1:
            idx = idx[0]
        if isinstance(idx, slice):
            return LoaditView(self.loader, self.indices[idx])
        if is_sequence(idx):
            return LoaditView(self.loader, self.indices[idx])

        return self.loader.get(self.indices[idx], preload_fn=self.preload_fn)


class LoadIt(SequenceView):
    def __init__(
        self,
        create_it: Optional[Callable[None, Iterable]],
        root_dir: Union[str, Path] = "cache/",
        max_shard_length: Optional[Union[str, int]] = "64mb",
        max_cache_size: int = 128,
        max_workers: int = 3,
        memory_limit: Optional[int] = None,
        compression: Optional[str] = None,
        preload_fn: Optional[PreloadType] = preload_next_shard,
        preload_all_async=False,
        info=None,
    ):
        if create_it is None:
            max_shard_length = None

        if isinstance(max_shard_length, str):
            match = re.fullmatch("([0-9]+)mb", max_shard_length.lower())
            if not match:
                raise ValueError(
                    "Invalid max_shard_length! Must be an integer or match ([0-9]+)mb."
                )
            length_mb = float(match.group(1))

            metadata = dataset_metadata(root_dir)
            if metadata is None:
                max_shard_length = int(
                    length_mb
                    * (2**20)
                    / size_estimator(create_it(), compression=compression)
                )
            else:
                max_shard_length = metadata.max_shard_length
                logger.warn(
                    "User-specified max_shard_length in mb for an already-existing ShardedDataset! Ignoring user specification"
                )

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
            compression=compression,
            info=info,
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

        if preload_all_async:
            assert self.max_workers > 1
            if self.shards.length() is None:

                def iterate_all():
                    for x in self:
                        pass

                self.executor.submit(iterate_all)

    def info(self):
        return self.shards.info()

    def set_info(self, info: Any):
        self.shards.set_info(info)

    def all_cached_to_disk(self) -> bool:
        return self.shards.all_present()

    def wait_until_all_cached(self):
        # Warning: this function may wait forever unless there is some
        # thread that is actively writing all the shards to disk (e.g.
        # as spawned by providing preload_all_async argument).
        while not self.all_cached_to_disk():
            time.sleep(1)

    def get_start_idx(self, idx: int) -> int:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        return int(start_idx)

    def __getitem__(self, *idx: int) -> Any:
        return self.get(*idx)

    def get(self, *idx: int, preload_fn: Optional[PreloadType] = None) -> Any:
        if len(idx) == 1:
            idx = idx[0]
        if is_sequence(idx):
            return LoaditView(self, SequenceView(idx))

        if isinstance(idx, slice):
            step = idx.step or 1
            start = idx.start or 0
            stop = idx.stop or len(self)

            if start < 0:
                start = start + len(self)

            if stop < 0:
                stop = stop + len(self)
            return self[range(start, stop, step)]

        if idx < 0:
            idx = len(self) + idx
        start_idx = self.get_start_idx(idx)

        # schedule a preload if enabled
        preload_fn = preload_fn or self.preload_fn
        if self.max_workers > 1 and preload_fn is not None:
            for idx_list in preload_fn(self, idx):
                self.load_async(idx_list)

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
        guess = 1000000
        while optional_length is None:
            try:
                self[guess]
            except IndexError:
                pass
            guess *= 10
            optional_length = self.shards.length()
        return optional_length

    def load_async(self, idx_list: List[int]) -> None:
        if self.executor is None:
            return
        start_idx_list = [self.get_start_idx(idx) for idx in idx_list]
        self.memory_cache.load_async(start_idx_list, self.executor)

    def finalize(self) -> None:
        self.shards.set_length()
        self.shards.finalize_length(True)
