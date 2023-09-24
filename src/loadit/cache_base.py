import time
from typing import List, Any, Optional, Callable
from threading import Condition
import logging
from concurrent.futures import ThreadPoolExecutor

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
    raise KeyError(
        f"key {key} does not exist yet! Please provide a method for loading new keys!"
    )


# special return values
cache_miss = ("cache_miss", object())  # the requested object cannot be loaded
not_available = (
    "not_present",
    object(),
)  # the requested object is not in the cache currently.


class AsyncCacheBase:
    def __init__(
        self, max_size: Optional[int], load_fn: Optional[Callable[Any, bool]] = None
    ):
        self.max_size = max_size
        self.modification_cv = Condition()
        if load_fn is None:
            load_fn = always_unavailable
        self.load_fn = load_fn

        self._cache_miss_count = 0

    def evict(self) -> None:
        if self.max_size is None:
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
            self._cache_miss_count += 1
            value = self.load(key)

        return value

    def load_async(self, key: Any, executor: ThreadPoolExecutor) -> None:
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
