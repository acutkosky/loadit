from .cache_base import AsyncCacheBase
from .util import deep_getsizeof
from threading import Lock
from typing import Optional, Callable, Any, List


class DictCache(AsyncCacheBase):
    def __init__(
        self,
        max_size: Optional[int],
        load_fn: Optional[Callable] = None,
        size_mode="count",
    ):
        self.cache = {}
        self.timestamps = {}
        self.size_mode = size_mode
        super().__init__(max_size, load_fn)

    def delete(self, key: Any):
        # tricky: this is a bit race-y: we delete the cache first and then the timestamp.
        # This makes it so that if a insertion is interleaved with a deletion, it is possible
        # that there will be some cache entries that do not have timestamps.
        # We give these an "effective" timestamp of 0 in get_keys_sorted_by_timestamp.
        # There might also be some timestamps that don't have cache entries.
        # For now we'll just ignore this. It's a potential memory leak, but hopefully not that signifanct.
        del self.cache[key]
        del self.timestamps[key]

    def get_(self, key: Any) -> Any:
        return self.cache[key]

    def set_timestamp(self, key: Any, timestamp: int):
        self.timestamps[key] = timestamp

    def get_keys_sorted_by_timestamp(self) -> List[Any]:
        return [
            k
            for k in sorted(self.cache.keys(), key=lambda k: self.timestamps.get(k, 0))
        ]

    def size(self) -> int:
        return len(self.cache)

    def __contains__(self, key: Any) -> bool:
        return key in self.cache
