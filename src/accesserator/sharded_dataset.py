import filelock
import pickle
import os
import threading
import time
from pathlib import Path
import json
from collections import namedtuple
from typing import Any, List, Optional, Generator
from filelock import FileLock
import logging
logger = logging.getLogger("randomacces")
ShardInfo = namedtuple(
    "ShardInfo",
    [
        "path",
        "start",
        "end",
        "size",
    ],
)

Metadata = namedtuple(
    "Metadata",
    [
        "max_shard_length",
        "length",
        "length_final",
    ],
)


def is_consistent_metadata(m1, m2):
    m1 = m1._asdict()
    m2 = m2._asdict()
    for key in ["max_shard_length"]:
        if m1[key] != m2[key]:
            return False
    return True


class ShardedDataset:
    def __init__(
        self,
        root_dir: str,
        max_shard_length: int = 4096,
    ):
        self.root_dir = Path(root_dir)

        self.root_dir.mkdir(parents=True, exist_ok=True)

        self.shard_dir = self.root_dir / "shards"
        self.shard_dir.mkdir(exist_ok=True)
        self.lock_dir = self.root_dir / "locks"
        self.lock_dir.mkdir(exist_ok=True)

        self.scratch_path = self.shard_dir / "scratch.shard.pickle"

        self.metadata_path = self.root_dir / "metadata.json"

        metadata = Metadata(max_shard_length, 0, False)
        self.max_shard_length = max_shard_length
        self.writer_file_lock = FileLock(self.lock_dir / "writer_lock.lock")

        with self.writer_file_lock:
            if self.metadata_path.exists():
                with open(self.metadata_path, "r") as fp:
                    prev_metadata = Metadata(**json.load(fp))
                assert is_consistent_metadata(
                    metadata, prev_metadata
                ), f"requested metadata {metadata} for existing sharded dataset with incompatable metadata {prev_metadata}!"
            else:
                with open(self.metadata_path, "w") as fp:
                    json.dump(metadata._asdict(), fp)


    def __len__(self):
        return self.metadata().length

    def metadata(self):
        with self.writer_file_lock:
            with open(self.metadata_path, "r") as fp:
                return Metadata(**json.load(fp))

    def write_metadata(self, metadata):
        if isinstance(metadata, Metadata):
            metadata = metadata._asdict()

        with self.writer_file_lock:
            with open(self.metadata_path, "w") as fp:
                json.dump(metadata, fp)

    def set_metadata_entry(self, key, value):
        metadata = self.metadata()._asdict()
        metadata[key] = value
        self.write_metadata(metadata)

    def get_shard_path(self, start: int, data: List[Any]) -> Path:
        end = start + len(data)
        return self.shard_dir / f"{start}.{end}.shard.pickle"

    def set_length_final(self, value: bool) -> None:
        self.set_metadata_entry("length_final", value)

    def write_shard(self, start: int, data: List[Any]) -> int:
        if len(data) == 0:
            return 0
        # check if this shard already exists
        prev_shard_info = self.get_shard_info(start)

        assert len(data) <= self.max_shard_length

        if prev_shard_info is not None and prev_shard_info.end >= start + len(data):
            # this shard already exists and is at least as big as the one
            # we are trying to write. No need to write anything.
            return 0

        shard_path = self.get_shard_path(start, data)

        with self.writer_file_lock:
            with open(self.scratch_path, "wb") as temp_shard:
                pickle.dump(data, temp_shard)
            os.rename(self.scratch_path, shard_path)
            if len(data) < self.max_shard_length:
                self.set_length_final(True)
            self.cleanup_overlapping_shards(start)
        if prev_shard_info is not None:
            return start + len(data) - prev_shard_info.end
        return len(data)

    def read_shard(self, shard_info: ShardInfo) -> List[Any]:
        with open(shard_info.path, "rb") as fp:
            data = pickle.load(fp)

        return data

    def read_shard_from_handle(self, fp: Any) -> List[Any]:
        data = pickle.load(fp)
        fp.close()
        return data

    def cleanup_overlapping_shards(self, start):
        paths = sorted(
            self.shard_dir.glob(f"{start}.*.shard.pickle"),
            key=lambda x: int(x.stem.split(".")[1]),
        )
        if len(paths) == 0:
            return None
        final_path = paths[-1]
        if len(paths) > 0:
            with self.writer_file_lock:
                # grab the paths again in case someone else has just added more...
                paths = sorted(
                    self.shard_dir.glob(f"{start}.*.shard.pickle"),
                    key=lambda x: int(x.stem.split(".")[1]),
                )
                if len(paths) == 0:
                    return None
                final_path = paths[-1]
                # for path in paths[:-1]:
                #     os.unlink(path)

        return final_path

    def get_shard_handle(self, idx: int) -> Any:
        info = self.get_shard_info(idx)
        if info is None:
            return None
        try:
            fp = open(info.path, "rb")
        except FileNotFoundError:
            fp = None

        return fp

    def shard_exists(self, start_idx: int) -> bool:
        return len(list(self.shard_dir.glob(f"{start_idx}.*.shard.pickle"))) > 0

    def get_shard_info(self, idx: int) -> Optional[ShardInfo]:
        max_shard_len = self.max_shard_length
        shard_num = idx // max_shard_len
        start = shard_num * max_shard_len

        path = self.cleanup_overlapping_shards(start)
        if path is None:
            return None

        end = int(path.stem.split(".")[1])

        return ShardInfo(path=path, start=start, end=end, size=os.path.getsize(path))

    def get_all_shards(self) -> List[ShardInfo]:
        def shard_info_from_path(path):
            stem = path.stem.split(".")
            start = int(stem[0])
            end = int(stem[1])
            size = os.path.getsize(path)
            return ShardInfo(path, start, end, size)

        return [
            shard_info_from_path(p) for p in self.shard_dir.glob("*.*.shard.pickle")
        ]

    def delete_shard(self, start_idx: int) -> None:
        with self.writer_file_lock:
            # with self.get_shard_lock(start_idx):
            shard_info = self.get_shard_info(start_idx)
            if shard_info is not None:
                os.unlink(shard_info.path)

    def memory_usage(self) -> int:
        usage = 0
        for info in self.get_all_shards():
            usage += os.path.getsize(info.path)
        return usage
