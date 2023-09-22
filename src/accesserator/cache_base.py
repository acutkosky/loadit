import time
from typing import List, Any, Optional, Dict, Callable
from threading import Condition
import logging
from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import os

logger = logging.getLogger("randomacces")


class CacheMiss:
    pass

cache_miss = CacheMiss()

class NotFound:
    pass
not_found = NotFound()

def is_cache_miss(value):
    return isinstance(value, CacheMiss)

def is_not_found(value):
    return isinstance(value, NotFound)

def always_unavailable(key: Any):
    raise KeyError(f"key {key} does not exist yet! Please provide a method for loading new keys!")

# special return values
cache_miss = ("cache_miss", object())  # the requested object cannot be loaded
not_available = ("not_present", object())  # the requested object is not in the cache currently.


class AsyncCacheBase:
    def __init__(self, max_size: Optional[int], load_fn: Optional[Callable[Any, bool]] = None):
        self.max_size = max_size
        self.modification_cv = Condition()
        if load_fn is None:
            load_fn = always_unavailable
        self.load_fn = load_fn

    def evict(self) -> None:
        if self.max_size is None:
            print("no evict...")
            return
        while self.size() > self.max_size:
            sorted_keys = self.get_keys_sorted_by_timestamp()
            # print(sorted_keys)
            self.delete(sorted_keys[0])

    def __getitem__(self, key: Any) -> Any:
        # first try to get the value without explicitly
        # asking for a lock.
        try:
            value = self.get(key)
        except KeyError:
            value = self.load(key)

        print("returning value!", type(self))
        return value
            
        # print("entryL ",entry)
        # print("is miss: ",is_cache_miss(entry))

        # if is_cache_miss:
            
            # with self.modification_cv:
            #     entry, is_available = self.load(key)
            #     if not is_available:
            #         raise KeyError
            #     entry, is_cache_miss = self.get(key)
            #     while is_cache_miss(entry):
            #         self.modification_cv.wait()
            #         entry, is_cache_miss = self.load(key)
            #         entry, is_cache_miss = self.get(key)


        # return entry

    def touch(self, key: Any, executor: ThreadPoolExecutor) -> None:
        if key not in self:
            executor.submit(self.load, key)

    def set_load_fn(self, load_fn: Callable[Any, Any]) -> None:
        self.load_fn = load_fn

    def load(self, key: Any) -> Any:
        result = self.load_fn(self, key)
        self.set_timestamp(key, time.time())
        self.evict()
        return result

    def get(self, key: Any) -> Any:
        data = self.get_(key)
        self.set_timestamp(key, time.time())
        return data

    def delete(self, key: Any) -> None:
        raise NotImplementedError

    def get_(self, key: Any) -> Any:
        raise NotImplementedError

    def set_timestamp(self, key: Any, timestamp: int):
        raise NotImplementedError

    def get_keys_sorted_by_timestamp(self) -> List[Any]:
        raise NotImplementedError

    def size(self) -> int:
        raise NotImplementedError

    def __contains__(self, key: Any) -> bool:
        raise NotImplementedError


class DictCache(AsyncCacheBase):
    def __init__(self, max_size: Optional[int], load_fn: Optional[Callable]=None):
        super().__init__(max_size, load_fn)
        self.cache = {}
        self.timestamps = {}

    def delete(self, key: Any):
        del self.cache[key]
        del self.timestamps[key]

    def get_(self, key: Any) -> Any:
        print("hit: ", key in self.cache)
        return self.cache[key]

    def set_timestamp(self, key: Any, timestamp: int):
        self.timestamps[key] = timestamp

    def get_keys_sorted_by_timestamp(self) -> List[Any]:
        return [k for k, t in sorted(self.timestamps.items(), key=lambda k, t: t)]

    def size(self) -> int:
        return len(self.cache)

    def __contains__(self, key: Any) -> bool:
        return key in self.cache


# class TimestampHandler(PatternMatchingEventHandler):
#     def __init__(self, file_cache, patterns, *args, **kwargs):
#         super().__init__(patterns, *args, **kwargs)
#         self.file_cache = file_cache

#     def on_created(self, event):
#         path = Path(event.src_path)
#         key = self.file_cache.get_key(path)
#         self.file_cache.set_timestamp(key, time.time())


# class FileCache(AsyncCacheBase):
#     def __init__(
#         self, max_size_bytes: Optional[int], dir: Path, pattern: str, open_mode: str="rb", load_fn: Optional[Callable]=None
#     ):
#         super().__init__(max_size_bytes, load_fn)
#         self.pattern = pattern
#         self.dir = dir
#         self.open_mode = open_mode
#         self.timestamps = defaultdict(time.time)
#         self.file_notification_cv = Condition()
#         self.observer = Observer()
#         self.handler = TimestampHandler(self, self.pattern)
#         self.observer.schedule(self.handler, path=dir)
#         self.observer.start()

#     def get_key(self, path):
#         return path.relative_to(self.dir)

#     def get_path_for_key(self, key):
#         path = self.dir / key
#         print("path: ",path)
#         return path

#     def delete(self, key: Any):
#         path = self.cache[key]
#         os.unlink(self.path)
#         del self.cache[key]
#         del self.timestamps[key]

#     def get_(self, key: Any) -> Any:
#         print("hello")
#         path = self.get_path_for_key(key)
#         try:
#             fp = open(self.get_path_for_key(key), mode=self.open_mode)
#         except FileNotFoundError:
#             return cache_miss
#         print(" now fp: ",fp)
#         return fp

#     def set_timestamp(self, key: Any, timestamp: int):
#         self.timestamps[key] = timestamp

#     def get_keys_sorted_by_timestamp(self) -> List[Any]:
#         return [k for k, t in sorted(self.timestamps.items(), key=lambda k, t: t)]

#     def size(self) -> int:
#         usage = 0
#         for path in self.dir.glob(self.pattern):
#             usage += os.path.getsize(path)
#         return usage

#     def __contains__(self, key: Any) -> bool:
#         return self.get_path_for_key(key) in set(self.dir.glob(self.pattern))
