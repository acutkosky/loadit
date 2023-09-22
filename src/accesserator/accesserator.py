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

        self.memory_cache = DictCache(max_size=max_cache_size, load_fn = lambda d, k: self.shards[k])

    def __getitem__(self, idx: int) -> Any:
        shard_offset = idx % self.shards.max_shard_length
        start_idx = idx - shard_offset
        try:
            shard = self.memory_cache[start_idx]
            return shard[shard_offset]
        except KeyError:
            raise IndexError(f"index {idx} out of range!")
        
        
        

    #     self.shard_access_times = self.shards.root_dir / "access_times.json"
    #     self.access_times_lock = FileLock(self.shards.root_dir / "access_times.lock")

    #     self.cache = SimpleCache(max_cache_size)
    #     executor = ThreadPoolExecutor(max_workers=max_workers)
    #     self.writer_pool = WriterPool(
    #         self.shards,
    #         self.cache,
    #         iterator=it,
    #         executor=executor,
    #         iterator_generator=iterator_generator,
    #         num_workers=1,
    #     )

    #     self.access_times = {}
    #     self.last_access_time_sync = time.time()
    #     self.access_time_sync_delay = 0.1
    #     if extend:
    #         self.shards.set_length_final(False)

    #     self.memory_limit = memory_limit

    # def request_shard(self, start_idx: int) -> List[Any]:
    #     result = None
    #     do_shard_evict = False
    #     # also load the next shard
    #     next_idx = start_idx + self.shards.max_shard_length
    #     if self.memory_limit is not None:
    #         self.update_access_times(next_idx, start_idx)
    #     if start_idx in self.cache:
    #         result = self.cache[start_idx]
    #     else:
    #         print("woah")
    #         do_shard_evict = self.writer_pool.write_shard_async(start_idx)

    #     if next_idx not in self.cache:
    #         do_shard_evict = do_shard_evict or self.writer_pool.write_shard_async(
    #             next_idx
    #         )

    #     while result is None:
    #         print("waiting")
    #         result = self.cache.wait_for(start_idx)

    #     if do_shard_evict:
    #         self.evict_shards()
    #     return result

    # def maybe_sync_access_times(self):
    #     curtime = time.time()
    #     if curtime < self.last_access_time_sync + self.access_time_sync_delay:
    #         return
    #     print("hi: ", curtime)
    #     with self.access_times_lock:
    #         try:
    #             with open(self.shard_access_times, "r") as fp:
    #                 stored_access_times = json.load(fp)
    #         except FileNotFoundError:
    #             stored_access_times = {}

    #         for k, v in self.access_times.items():
    #             stored_access_times[k] = max(v, stored_access_times.get(k, 0))

    #         self.access_times = stored_access_times

    #         with open(self.shard_access_times, "w") as fp:
    #             json.dump(stored_access_times, fp)
    #     self.last_access_time_sync = time.time()

    # def update_access_times(self, *values):
    #     if self.memory_limit is None:
    #         return
    #     timestamp = time.time()

    #     for value in values:
    #         self.access_times[value] = timestamp
    #     self.maybe_sync_access_times()

    #     # with self.access_times_lock:
    #     #     try:
    #     #         with open(self.shard_access_times, "r") as fp:
    #     #             # print(fp)
    #     #             access_times = json.load(fp)
    #     #     except FileNotFoundError:
    #     #         access_times = {}
    #     #     for value in values:
    #     #         access_times[value] = timestamp
    #     #     with open(self.shard_access_times, "w") as fp:
    #     #         json.dump(access_times, fp)#, access_times, fp)

    # def evict_shards(self):
    #     # print("yo")
    #     if self.memory_limit is None:
    #         return
    #     if self.shards.memory_usage() <= self.memory_limit:
    #         return

    #     with self.access_times_lock:
    #         # access_times = self.get_access_times()
    #         all_shard_info = self.shards.get_all_shards()
    #         # print("all shard info: ",all_shard_info)
    #         # print("access_time: ", access_times.keys())
    #         # evict all shards that we have never accessed
    #         for info in all_shard_info:
    #             if str(info.start) not in self.access_times:
    #                 # print("start missing: ",info.start)
    #                 self.shards.delete_shard(info.start)

    #         # iterate through the shards, and evict shards until we aren't too much memory
    #         eviction_order = sorted(self.access_times.items(), key=lambda x: int(x[1]))
    #         # print("eviction order: ",eviction_order)
    #         count = 0
    #         while self.shards.memory_usage() > self.memory_limit:
    #             # print("memory usage: ",self.shards.memory_usage())
    #             (start, _) = eviction_order[count]
    #             self.shards.delete_shard(int(start))
    #             self.access_times[start] = time.time()
    #             count += 1
    #         self.maybe_sync_access_times()

    # def set_access_time(self, start_idx: int, value: int) -> None:
    #     print("yo sef")
    #     with self.access_times_lock:
    #         try:
    #             with open(self.shard_access_times, "r") as fp:
    #                 access_times = json.load(fp)
    #         except FileNotFoundError:
    #             access_times = {}

    #         access_times[start_idx] = value
    #         self.write_access_times(access_times)

    # def write_access_times(self, access_times: Dict) -> None:
    #     print("yo write")
    #     with self.access_times_lock:
    #         with open(self.shard_access_times, "w") as fp:
    #             json.dump(access_times, fp)

    # def get_access_times(self) -> Dict:
    #     print("yo get")
    #     with self.access_times_lock:
    #         try:
    #             with open(self.shard_access_times, "r") as fp:
    #                 access_times = json.load(fp)
    #         except FileNotFoundError:
    #             access_times = {}
    #     return access_times

    # def __getitem__(self, idx):
    #     start_idx = idx - (idx % self.shards.max_shard_length)
    #     shard = self.request_shard(start_idx)
    #     return shard[idx - start_idx]

    # def __len__(self):
    #     while not self.shards.metadata().length_final:
    #         time.sleep(1)
    #     return self.shards.metadata().length

    def __iter__(self):
        idx = 0
        while True:
            try:
                yield self[idx]
                idx += 1
            except IndexError:
                raise StopIteration
