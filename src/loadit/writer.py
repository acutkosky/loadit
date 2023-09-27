from typing import Iterable, Any, Optional, List, Callable, Union
from .sharded_dataset import ShardedDataset
from threading import Lock, Condition
import logging

logger = logging.getLogger("loadit")


class Writer:
    def __init__(
        self,
        create_it: Optional[Union[Iterable, Callable[None, Iterable]]] = None,
    ):
        assert is_iterator_creator(
            create_it
        ), "you must supply a function that creates iterables!"
        self.it = enumerate(create_it())
        self.current_idx = -1
        self.finished = False
        self.create_it = create_it

    def reset_iterator(self):
        self.it = enumerate(self.create_it())

        self.finished = False
        self.current_idx = -1

    def iterate_and_write_shard(
        self, start_idx: int, shards: ShardedDataset
    ) -> List[Any]:
        # Shards must be aligned with self.shards.max_shard_length
        assert start_idx % shards.max_shard_length == 0

        # self.current_idx is current index of iterator.
        # If we've already passed this index, we need
        # to return to the beginning. reset_iterator should
        # take care of throwing some error if that's not possible.
        if start_idx < self.current_idx:
            self.reset_iterator()

        # read in up to self.shards.max_shard_length iterations.
        # this should update self.current_idx as well.
        shard_data = self.buffer_iterations(
            start_idx, start_idx + shards.max_shard_length
        )
        logger.debug(f"buffered shard data for idx: {start_idx}")
        shards.write_shard(start_idx, shard_data)
        if self.finished:
            shards.set_length(self.current_idx + 1)
            shards.finalize_length(True)
        return shard_data

    def buffer_iterations(self, start_idx: int, end_idx: int) -> List[Any]:
        buffer = []
        while self.current_idx < start_idx - 1:
            try:
                # we'll wrap the iterator in an enumerate in __init__
                self.current_idx, datum = next(self.it)
            except StopIteration:
                self.finished = True
                return []

        while self.current_idx < end_idx - 1:
            try:
                self.current_idx, datum = next(self.it)
                buffer.append(datum)
            except StopIteration:
                self.finished = True
                return buffer

        return buffer


def exactly_one_not_none(*items):
    result = False
    for item in items:
        if item is not None:
            if result:
                return False
            else:
                result = True
    return result


def is_iterator(it: Any) -> bool:
    try:
        it = enumerate(it)
        return True
    except TypeError:
        return False


def is_iterator_creator(create_it: Any) -> bool:
    try:
        return is_iterator(create_it())
    except TypeError:
        return False


class QueueItem:
    def __init__(self, start_idx: int, writer: Writer):
        self.start_idx = start_idx
        self.writer = writer
        self.cv = Condition()
        self.result = None
        self.finished = False


class WriterPool:
    def __init__(
        self,
        writers: Optional[List[Writer]] = None,
        create_it: Optional[Callable[None, Iterable]] = None,
        num_workers: int = 1,
    ):
        assert exactly_one_not_none(writers, create_it)
        if writers is not None:
            self.writers = writers
        if create_it is not None:
            self.writers = [Writer(create_it=create_it) for _ in range(num_workers)]
        self.queue_lock = Lock()
        # self.queue_cv = Condition()
        self.queues = {writer: [] for writer in self.writers}

    def add_to_queue(self, start_idx: int, shards: ShardedDataset) -> Optional[Writer]:
        # returns None if the selected writer does not need to be scheduled.
        writer_to_append = None
        smallest_gap = float("inf")
        with self.queue_lock:
            for writer in self.writers:
                queue = self.queues[writer]
                if len(queue) == 0:
                    # this writer's current_idx is correct
                    if start_idx >= writer.current_idx:
                        gap = start_idx - writer.current_idx
                    else:
                        gap = writer.current_idx
                    if gap < smallest_gap:
                        smallest_gap = gap
                        writer_to_append = writer
                    continue
                # at this point, the queue must be non-empty.
                if queue[0].start_idx <= start_idx and queue[-1].start_idx >= start_idx:
                    # this writer will pass through the desired shard
                    for i in range(len(queue)):
                        if queue[i].start_idx == start_idx:
                            return queue[i]
                        if queue[i].start_idx > start_idx:
                            queue_item = QueueItem(start_idx, writer)
                            queue.insert(i, queue_item)
                            return queue_item

                # if the writer will end before start_idx,
                # then we record how much further it needs to go.
                # Otherwise, we want to choose the earliest writer.
                if start_idx >= queue[-1].start_idx + shards.max_shard_length:
                    gap = start_idx - (queue[-1].start_idx + shards.max_shard_length)
                else:
                    gap = queue[-1].start_idx + shards.max_shard_length
                if gap < smallest_gap:
                    smallest_gap = gap
                    writer_to_append = writer

            # no drive-by writing possible, so let's just pick
            # the one with smallest gap
            queue_item = QueueItem(start_idx, writer_to_append)
            self.queues[writer_to_append].append(queue_item)
            return queue_item

    def block_until_write(
        self, queue_item: QueueItem, shards: ShardedDataset
    ) -> List[Any]:
        writer = queue_item.writer
        wi = self.writers.index(writer)
        start_idx = queue_item.start_idx
        queue = self.queues[writer]
        next_cv = None
        # must get cv before queue_lock
        with queue_item.cv:
            while not queue_item.finished:
                with self.queue_lock:
                    if queue[0] == queue_item:
                        data = writer.iterate_and_write_shard(start_idx, shards)
                        queue.pop(0)
                        queue_item.result = data
                        queue_item.finished = True
                        queue_item.cv.notify_all()
                        break
                queue_item.cv.wait()
            with self.queue_lock:
                if len(queue) > 0:
                    next_cv = queue[0].cv
        if next_cv:
            # wake up threads that are waiting for their turn to read data.
            with next_cv:
                next_cv.notify()

        if len(queue_item.result) == 0:
            raise KeyError(f"start_idx {start_idx} is out of range!")
        return queue_item.result

    def load_fn(self, shards: ShardedDataset, start_idx: int) -> List[Any]:
        queue_item = self.add_to_queue(start_idx, shards)
        result = self.block_until_write(queue_item, shards)
        return result
