from .cache_base import AsyncCacheBase
from typing import Optional, Callable, Any, List


class DictCache(AsyncCacheBase):
    def __init__(self, max_size: Optional[int], load_fn: Optional[Callable] = None):
        super().__init__(max_size, load_fn)
        self.cache = {}
        self.timestamps = {}

    def delete(self, key: Any):
        del self.cache[key]
        del self.timestamps[key]

    def get_(self, key: Any) -> Any:
        return self.cache[key]

    def set_timestamp(self, key: Any, timestamp: int):
        self.timestamps[key] = timestamp

    def get_keys_sorted_by_timestamp(self) -> List[Any]:
        return [k for k, t in sorted(self.timestamps.items(), key=lambda kt: kt[1])]

    def size(self) -> int:
        return len(self.cache)

    def __contains__(self, key: Any) -> bool:
        return key in self.cache
