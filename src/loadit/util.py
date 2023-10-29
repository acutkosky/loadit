from typing import Iterable
import fsspec
import pickle
import tempfile
import os

def size_estimator(it: Iterable, num_samples: int = 128, compression = None) -> int:
    buffer = []
    for i, x in enumerate(it):
        buffer.append(x)
        if i == num_samples:
            break
    fd, name = tempfile.mkstemp()
    with fsspec.open(name, mode="wb", compression=compression) as f:
        pickle.dump(buffer, f)
    os.close(fd)
    size = os.path.getsize(name)
    os.unlink(name)
    return size/num_samples

