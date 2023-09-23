from .sharded_dataset import ShardedDataset, ShardInfo
from .dict_cache import DictCache
from .writer import WriterPool, Writer
from typing import Any, Union, List, Optional, Iterable, Dict, Callable
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from filelock import FileLock
import json

import logging

logger = logging.getLogger("randomacces")


def preload_next_shard(loaded, idx):
    return idx + loaded.shards.max_shard_length


def preload_next_shard_in_indices(indices, loader, idx):
    return indices[max(len(indices) - 1, idx + 1)] + loader.shards.max_shard_length


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
        create_it: Union[Iterable, Callable[None, Iterable]],
        root_dir: Union[str, Path] = "cache/",
        max_shard_length: int = 4096,
        max_cache_size: int = 10,
        max_workers: int = 1,
        memory_limit: Optional[int] = None,
        preload_fn: Optional[Callable] = preload_next_shard,
    ):
        self.writer_pool = WriterPool(
            create_it=create_it,
            num_workers=max_workers,
        )
        self.shards = ShardedDataset(
            max_size_bytes=memory_limit,
            root_dir=root_dir,
            max_shard_length=max_shard_length,
            load_fn=self.writer_pool.load_fn,
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

        if preload_fn is not None:
            self.preload_fn = preload_fn
        else:
            self.preload_fn = None

    def get_start_idx(self, idx: int) -> int:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        return start_idx

    def __getitem__(self, idx: int) -> Any:
        return self.get(idx)

    def get(self, idx: int, preload_fn: Optional[Callable] = None) -> Any:
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
        try:
            shard = self.memory_cache[start_idx]
            return shard[idx - start_idx]
        except KeyError:
            raise IndexError(f"index {idx} out of range!")

        preload_fn = preload_fn or self.preload_fn
        if self.max_workers > 1 and preload_fn is not None:
            self.load_async(preload_fn(self, idx))

    def __len__(self):
        l = self.shards.length()
        if l is not None:
            return l
        guess = 100
        while l is None:
            try:
                self[guess]
            except IndexError:
                pass
            guess *= 10
            l = self.shards.length()
        return l

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
