from typing import Iterable
import pickle

def size_estimator(it: Iterable, num_samples: int = 128) -> int:
    buffer = []
    for i, x in enumerate(it):
        buffer.append(x)
        if i == num_samples:
            break
    data = pickle.dumps(buffer)
    return len(data)/num_samples

