from .sharded_dataset import ShardedDataset, ShardInfo
from .cache_base import DictCache
from .writer import WriterPool, Writer
from typing import Any, Union, List, Optional, Iterator, Dict
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from filelock import FileLock
import json

import logging

logger = logging.getLogger("randomacces")


class RandomAccess:
    def __init__(
        self,
        it: Iterator,
        root_dir: Union[str, Path],
        iterator_generator=None,
        max_shard_length: int = 4096,
        max_cache_size: int = 10,
        max_workers: int = 1,
        memory_limit: Optional[int] = None,
        extend: bool = False,
    ):

        self.writer_pool = WriterPool(
            iterator=it,
            iterator_generator=iterator_generator,
            num_workers=1,
        )
        self.shards = ShardedDataset(
            max_size_bytes=memory_limit,
            root_dir=root_dir,
            max_shard_length=max_shard_length,
            load_fn = self.writer_pool.load_fn,
        )
        self.max_workers = max_workers
        if self.max_workers > 1:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers-1)
        def load_from_disk(d, k):
            shard = self.shards[k]
            d.cache[k] = shard
            return shard

        self.memory_cache = DictCache(max_size=max_cache_size, load_fn = load_from_disk)

    def __getitem__(self, idx: int) -> Any:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        try:
            shard = self.memory_cache[start_idx]
            return shard[shard_offset]
        except KeyError:
            raise IndexError(f"index {idx} out of range!")

        if self.max_workers > 1:
            self.memory_cache.touch(start_idx + self.shards.max_shard_length, self.executor)
        
    def __iter__(self):
        idx = 0
        while True:
            try:
                yield self[idx]
                idx += 1
            except IndexError:
                return
