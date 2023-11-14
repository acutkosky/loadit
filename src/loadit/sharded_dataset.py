import pickle
import os
import time
from pathlib import Path
import json
from collections import namedtuple
from typing import Any, List, Optional, Callable, NamedTuple, Dict
from filelock import FileLock
from .cache_base import AsyncCacheBase
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer
import logging
import fsspec

logger = logging.getLogger("loadit")


class ShardInfo(NamedTuple):
    path: Path
    start: int
    end: int
    size: int


class Metadata(NamedTuple):
    max_shard_length: int
    length: int
    length_final: bool
    compression: Optional[str]
    version: int
    info: Any


def is_consistent_metadata(m1: Metadata, m2: Metadata) -> bool:
    m1 = m1._asdict()
    m2 = m2._asdict()
    for key in ["max_shard_length", "compression", "version"]:
        if m1[key] is None or m2[key] is None:
            continue
        if m1[key] != m2[key]:
            return False
    return True


def assert_valid_metadata(m: Metadata) -> bool:
    assert m.max_shard_length > 0
    assert m.length >= 0
    assert isinstance(m.length_final, bool)
    assert m.compression is None or m.compression in fsspec.available_compressions()


def merge_metadata(prev: Metadata, m: Metadata) -> Metadata:
    m = m._asdict()
    prev = prev._asdict()
    for k, v in m.items():
        if v is None:
            m[k] = prev[k]
    if prev["length_final"]:
        m["length_final"] = True
        m["length"] = prev["length"]
    return Metadata(**m)


class TimestampHandler(PatternMatchingEventHandler):
    def __init__(self, file_cache, patterns, *args, **kwargs):
        super().__init__(patterns, *args, **kwargs)
        self.file_cache = file_cache

    def on_created(self, event):
        path = Path(event.src_path)
        key = self.file_cache.get_key(path)
        self.file_cache.set_timestamp(key, time.time())


def dataset_metadata(root_dir: str):
    metadata_path = Path(root_dir) / "metadata.json"
    file_lock = Path(root_dir) / "locks" / "writer_lock.lock"
    if not metadata_path.exists() or not file_lock.exists():
        return None

    flock = FileLock(file_lock)
    with flock:
        with open(metadata_path, "r") as fp:
            metadata = Metadata(**json.load(fp))

    return metadata


class ShardedDataset(AsyncCacheBase):
    def __init__(
        self,
        max_size_bytes: Optional[int],
        root_dir: str,
        max_shard_length: int = 4096,
        load_fn: Optional[Callable] = None,
        compression: Optional[str] = None,
        info: Optional[Dict[str, Any]] = None,
    ):
        self.max_shard_length = max_shard_length
        self.root_dir = Path(root_dir)
        self.shard_dir = self.root_dir / "shards"
        self.lock_dir = self.root_dir / "locks"
        self.scratch_dir = self.root_dir / "scratch"

        self.shard_dir.mkdir(parents=True, exist_ok=True)
        self.lock_dir.mkdir(exist_ok=True)
        self.scratch_dir.mkdir(exist_ok=True)
        self.scratch_path = self.scratch_dir / "shard.pickle"

        self.metadata_path = self.root_dir / "metadata.json"

        metadata = Metadata(
            max_shard_length=max_shard_length,
            length=0,
            length_final=False,
            compression=compression,
            version=4,
            info=info,
        )
        self.writer_file_lock = FileLock(self.lock_dir / "writer_lock.lock")
        self.fs = fsspec.filesystem("file")

        with self.writer_file_lock:
            if self.metadata_path.exists():
                with self.fs.open(self.metadata_path, "r") as fp:
                    prev_metadata = Metadata(**json.load(fp))
                assert is_consistent_metadata(
                    metadata, prev_metadata
                ), f"requested metadata {metadata} for existing sharded dataset with incompatable metadata {prev_metadata}!"
                metadata = merge_metadata(prev_metadata, metadata)
        self.write_metadata(metadata)
        assert_valid_metadata(metadata)

        self.max_shard_length = metadata.max_shard_length
        self.compression = metadata.compression
        self.suffix = "shard.pickle"
        if self.compression is not None:
            self.suffix += "." + self.compression

        self.pattern = "*.*.shard.pickle"
        self.timestamps = {}
        self.observer = Observer()
        self.handler = TimestampHandler(self, [self.pattern])
        self.observer.schedule(self.handler, path=self.shard_dir, recursive=False)
        self.observer.start()

        self.initialize_timestamps()
        super().__init__(max_size_bytes, load_fn)

    def initialize_timestamps(self):
        with self.writer_file_lock:
            all_shard_info = self.get_all_shards()
            for info in all_shard_info:
                if info not in self.timestamps:
                    self.timestamps[info.start] = time.time()

    def length(self):
        metadata = self.metadata()
        if not metadata.length_final:
            return None
        return metadata.length

    def __len__(self) -> int:
        length = self.length()
        if length is None:
            raise RuntimeError("Length is currently unknown for this ShardedDataset!")
        return length

    def metadata(self):
        with self.writer_file_lock:
            with self.fs.open(self.metadata_path, "r") as fp:
                metadata_json = json.load(fp)
                result = Metadata(**metadata_json)
        return result

    def write_metadata(self, metadata):
        if isinstance(metadata, Metadata):
            metadata = metadata._asdict()

        with self.writer_file_lock:
            with self.fs.open(self.metadata_path, "w") as fp:
                json.dump(metadata, fp)

    def set_metadata_entry(self, key, value):
        metadata = self.metadata()._asdict()
        metadata[key] = value
        self.write_metadata(metadata)

    def info(self):
        return self.metadata().info

    def set_info(self, info):
        return self.set_metadata_entry("info", info)

    def get_key(self, path):
        return int(path.stem.split(".")[0])

    def get_path_for_key(self, key):
        assert key % self.max_shard_length == 0, f"non-aligned key: {key}"
        eligible_paths = sorted(
            self.shard_dir.glob(f"{key}.*.{self.suffix}"),
            key=lambda x: int(x.stem.split(".")[1]),
        )
        if len(eligible_paths) == 0:
            raise FileNotFoundError

        return eligible_paths[-1]

    def delete(self, key: Any):
        with self.writer_file_lock:
            try:
                path = self.get_path_for_key(key)
                os.unlink(path)
                del self.timestamps[key]
            except FileNotFoundError:
                raise KeyError

    def get_(self, start_idx: int) -> List[Any]:
        try:
            with self.fs.open(
                self.get_path_for_key(start_idx), "rb", compression=self.compression
            ) as fp:
                data = pickle.load(fp)
        except FileNotFoundError:
            raise KeyError

        return data

    def set_timestamp(self, key: Any, timestamp: int):
        self.timestamps[key] = timestamp

    def get_keys_sorted_by_timestamp(self) -> List[Any]:
        return [k for k, t in sorted(self.timestamps.items(), key=lambda item: item[1])]

    def size(self) -> int:
        with self.writer_file_lock:
            usage = 0
            for path in self.shard_dir.glob(self.pattern):
                usage += os.path.getsize(path)
        return usage

    def __contains__(self, key: Any) -> bool:
        return self.get_path_for_key(key) in self.shard_dir.glob(self.pattern)

    def set_length(self, value: Optional[int] = None) -> None:
        if value is None:
            value = self.get_highest_saved_index_plus_one()
        self.set_metadata_entry("length", value)

    def get_highest_saved_index_plus_one(self) -> int:
        all_shard_info = self.get_all_shards()
        if len(all_shard_info) == 0:
            return 0
        length = max([info.end for info in all_shard_info])
        return length

    def finalize_length(self, value: bool) -> None:
        self.set_metadata_entry("length_final", value)

    def write_shard(self, start: int, data: List[Any]) -> int:
        if len(data) == 0:
            return 0
        # check if this shard already exists
        with self.writer_file_lock:
            prev_shard_info = self.get_shard_info(start)

            assert len(data) <= self.max_shard_length

            if prev_shard_info is not None and prev_shard_info.end >= start + len(data):
                # this shard already exists and is at least as big as the one
                # we are trying to write. No need to write anything.
                return 0

            shard_path = self.get_shard_path(start, data)

            with self.fs.open(
                self.scratch_path, "wb", compression=self.compression
            ) as temp_shard:
                pickle.dump(data, temp_shard)
            os.rename(self.scratch_path, shard_path)
            if len(data) < self.max_shard_length:
                self.set_length(start + len(data))
                self.finalize_length(True)
            self.cleanup_overlapping_shards(start)
        if prev_shard_info is not None:
            return start + len(data) - prev_shard_info.end
        return len(data)

    def shard_exists(self, start_idx: int) -> bool:
        return len(list(self.shard_dir.glob(f"{start_idx}.*.{self.suffix}"))) > 0

    def get_shard_info(self, idx: int) -> Optional[ShardInfo]:
        with self.writer_file_lock:
            max_shard_len = self.max_shard_length
            shard_num = idx // max_shard_len
            start = shard_num * max_shard_len

            path = self.cleanup_overlapping_shards(start)
            if path is None:
                return None

            end = int(path.stem.split(".")[1])

            size = os.path.getsize(path)

            return ShardInfo(path=path, start=start, end=end, size=size)

    def cleanup_overlapping_shards(self, start):
        paths = sorted(
            self.shard_dir.glob(f"{start}.*.{self.suffix}"),
            key=lambda x: int(x.stem.split(".")[1]),
        )
        if len(paths) == 0:
            return None
        final_path = paths[-1]
        if len(paths) > 1:
            with self.writer_file_lock:
                # grab the paths again in case someone else has just added more...
                paths = sorted(
                    self.shard_dir.glob(f"{start}.*.{self.suffix}"),
                    key=lambda x: int(x.stem.split(".")[1]),
                )
                if len(paths) == 0:
                    return None
                final_path = paths[-1]
                if len(paths) > 1:
                    for path in paths[:-1]:
                        os.unlink(path)

        return final_path

    def get_shard_path(self, start: int, data: List[Any]) -> Path:
        end = start + len(data)
        return self.shard_dir / f"{start}.{end}.{self.suffix}"

    def all_present(self) -> bool:
        l = self.length()
        if l is None:
            return False
        all_shards = self.get_all_shards()

        all_shards = sorted(all_shards, key=lambda s: s.start)

        if all_shards[-1].end != l and all_shards[0].start == 0:
            return False

        for i in range(len(all_shards) - 1):
            if all_shards[i].end != all_shards[i + 1].start:
                return False

        return True

    def get_all_shards(self) -> List[ShardInfo]:
        with self.writer_file_lock:

            def shard_info_from_path(path):
                stem = path.stem.split(".")
                start = int(stem[0])
                end = int(stem[1])
                size = os.path.getsize(path)
                return ShardInfo(path, start, end, size)

            return [
                shard_info_from_path(p)
                for p in self.shard_dir.glob(f"*.*.{self.suffix}")
            ]
