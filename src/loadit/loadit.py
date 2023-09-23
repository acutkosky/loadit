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
            self.preload_fn = lambda idx: preload_fn(self, idx)
        else:
            self.preload_fn = None

    def get_start_idx(self, idx: int) -> int:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        return start_idx

    def __getitem__(self, idx: int) -> Any:
        start_idx = self.get_start_idx(idx)
        try:
            shard = self.memory_cache[start_idx]
            return shard[idx - start_idx]
        except KeyError:
            raise IndexError(f"index {idx} out of range!")

        if self.max_workers > 1 and self.preload_fn is not None:
            self.load_async(self.preload_fn(idx))

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
