from typing import Iterable, Optional, List, Callable, Union
import fsspec
import pickle
import tempfile
import os
import sys
from itertools import chain
from collections import deque
from collections.abc import Sequence
import numpy as np
import json


def is_sequence(s):
    seq_attrs = ["__getitem__", "__len__", "__iter__"]
    for at in seq_attrs:
        if not hasattr(s, at):
            return False
    return True


def size_estimator(it: Iterable, num_samples: int = 16, compression=None) -> int:
    buffer = []
    for count, x in enumerate(it):
        buffer.append(x)
        if count == num_samples - 1:
            break
    fd, name = tempfile.mkstemp()
    with fsspec.open(name, mode="wb", compression=compression) as f:
        pickle.dump(buffer, f)
    os.close(fd)
    size = os.path.getsize(name)
    os.unlink(name)
    return size / count


def deep_getsizeof(*obs, seen_ids=set()):
    children = []
    for o in obs:
        for iter_fn in ["__iter__", "values"]:
            if hasattr(o, iter_fn):
                new_children = [
                    c for c in getattr(o, iter_fn)() if id(c) not in seen_ids
                ]
                seen_ids.extend([id(c) for c in new_children])
                children.extend(new_children)
    return sum([sys.getsizeof(o) for o in obs]) + deep_getsizeof(*children, seen_ids)


class SequenceView(Sequence):
    def __init__(
        self,
        seq: Sequence,
        indices: Optional[Union[Sequence, Callable]] = None,
        length: Optional[int] = None,
    ):
        self.seq = seq
        self.indices = indices
        self.length = None
        if is_sequence(indices):
            self.index_map = indices.__getitem__
        elif isinstance(indices, Callable):
            if length is None:
                raise ValueError(
                    "cannot specify a callable indices argument without manually providing the length!"
                )
            self.index_map = indices
            self.length = length
        else:
            self.index_map = lambda idx: idx

    def __getitem__(self, *idx: Union[int, Sequence]):
        if len(idx) == 1:
            idx = idx[0]
        if is_sequence(idx):
            return SequenceView(self, idx)
        else:
            return self.seq[self.index_map(idx)]

    def __len__(self):
        if self.length is not None:
            return self.length
        if self.indices is None:
            self.length = len(self.seq)
            return self.length
        else:
            self.length = len(self.indices)
            return self.length


class InterleaveSequences(Sequence):
    def __init__(self, *seqs: List[Sequence]):
        self.seqs = seqs
        self.lengths = [len(seq) for seq in seqs]
        self.len = sum(self.lengths)

        self.cutoffs = []
        self.offsets = []
        self.seq_indices = [list(range(len(seqs)))]

        temp_lengths = list(self.lengths)
        cum_sum = 0
        offset = 0
        while len(temp_lengths) > 0:
            min_value = min(temp_lengths)
            self.offsets.append(offset)
            self.cutoffs.append(cum_sum + min_value * len(temp_lengths))
            offset += min_value
            cum_sum += min_value * len(temp_lengths)
            temp_lengths = [x for x in temp_lengths if x != min_value]
            self.seq_indices.append(
                [i for i, l in enumerate(self.lengths) if l > min_value]
            )

    def __getitem__(self, idx: int):
        for c, offset in zip(self.cutoffs, self.offsets):
            if idx < c:
                idx = idx - c
                current_indices = self.seq_indices[idx]
                seq_idx = idx % len(current_indices)
                seq_offset = idx // len(current_indices)
                return self.seqs[current_indices[seq_idx]][seq_offset + offset]

        raise IndexError

    def __len__(self):
        return self.len


class ConcatableSequence(Sequence):
    def __init__(self, *seqs: List[Sequence]):
        self.seqs = seqs
        self.lengths = [float("inf") for s in seqs]

    def __getitem__(self, idx: int):
        # we go to great lengths to avoid computing len(self.seqs[i]) for any i
        # unless absolutely necessary.
        for seq_idx in range(len(self.seqs)):
            l = self.lengths[seq_idx]
            if l > idx:
                try:
                    return self.seqs[seq_idx][idx]
                except IndexError:
                    self.lengths[seq_idx] = len(self.seqs[seq_idx])
                    l = self.lengths[seq_idx]
                idx -= l
                if idx < 0:
                    raise IndexError

        raise IndexError

    def __len__(self):
        self.lengths = [len(seq) for seq in self.seqs]
        return sum(self.lengths)

    def __add__(self, other: Sequence):
        return ConcatableSequence(self.seqs + [other])


class RepeatSequence(Sequence):
    def __init__(self, seq: Sequence, repeats: Optional[int] = None):
        self.seq = seq
        self.repeats = repeats

    def __getitem__(self, idx):
        try:
            return self.seq[idx]
        except IndexError:
            if self.repeats is not None:
                if idx // len(self.seq) >= self.repeats:
                    raise IndexError
            idx = idx % len(self.seq)
            return self.seq[idx]

    def __len__(self):
        return len(self.seq)


def chunk_shuffle_idx(chunk_size: int, length: int, seed: Optional = None):
    num_chunks = (chunk_size + length - 1) // chunk_size

    rng = np.random.default_rng(seed)

    permutations = np.concatenate(
        [i * chunk_size + rng.permutation(chunk_size) for i in range(num_chunks)]
    )
    return permutations


def chunk_shuffle(
    seq: Sequence,
    chunk_size: Optional[int],
    length: Optional[int],
    seed: Optional = None,
):
    if length is None:
        length = len(seq)

    if chunk_size is None:
        chunk_size = len(seq)

    shuffle_idx = chunk_shuffle_idx(chunk_size, length, seed)

    if isinstance(seq, SequenceView):
        return seq[shuffle_idx]
    else:
        return SequenceView(seq, shuffle_idx)
