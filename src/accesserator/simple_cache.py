import time
from typing import List, Any, Optional
from threading import Condition
import logging
logger = logging.getLogger("randomacces")

class SimpleCache:
    def __init__(self, max_size: int):
        self.cache = {}
        self.max_size = max_size
        self.modification_cv = Condition()

    def insert(self, key: Any, item: Any) -> None:
        timestamp = time.time()
        if key in self.cache:
            if len(self.cache[key][1]) > len(item):
                self.cache[key][0] = timestamp
                return

        self.cache[key] = (timestamp, item)
        self.evict()
        with self.modification_cv:
            self.modification_cv.notify_all()

    def evict(self) -> None:
        if len(self.cache) <= self.max_size:
            return

        old_keys = sorted(self.cache.keys(), key=lambda k: self.cache[k][0])
        for k in old_keys[: len(self.cache) - self.max_size]:
            del self.cache[k]

    def wait_for(self, key: Any):
        with self.modification_cv:
            while key not in self.cache:
                self.modification_cv.wait()
            return self.get(key)

    def __setitem__(self, key: Any, value: Any):
        self.insert(key, value)

    def __getitem__(self, key: Any) -> Optional[Any]:
        return self.get(key)

    def get(self, key: Any) -> Optional[Any]:
        if key in self.cache:
            value  = self.cache[key][1]
            self.cache[key] = (time.time(), value)
            return value
        return None

    def __contains__(self, key):
        return key in self.cache
