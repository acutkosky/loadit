from typing import Iterator, Any, Optional, List, Callable
from .sharded_dataset import ShardedDataset, ShardInfo
from collections import deque
from threading import Lock, Condition
import traceback
import logging
logger = logging.getLogger("randomacces")

class Writer:
    def __init__(
        self,
        it: Iterator,
        iterator_generator: Optional[Callable[None, Iterator]] = None,
    ):
        self.it = enumerate(it)
        self.current_idx = -1
        self.pending_max_idx = -1
        self.finished = False
        self.iterator_generator = iterator_generator

    def reset_iterator(self):
        logger.info("reseting....")
        assert self.iterator_generator is not None, "must supply an iterator_generator!"

        self.it = enumerate(self.iterator_generator())
        self.finished = False
        self.current_idx = -1

    def update_pending_max_idx(self, start_idx: int, shards: ShardedDataset) -> None:
        self.pending_max_idx = start_idx + shards.max_shard_length

    def iterate_and_write_shard(self, start_idx: int, shards: ShardedDataset) -> List[Any]:
        # Shards must be aligned with self.shards.max_shard_length
        assert start_idx % shards.max_shard_length == 0

        # self.current_idx is current index of iterator.
        # If we've already passed this index, we need
        # to return to the beginning. reset_iterator should
        # take care of throwing some error if that's not possible.
        if start_idx < self.current_idx:
            self.reset_iterator()
            logging.info(f"reset, starting up... {start_idx}")

        # read in up to self.shards.max_shard_length iterations.
        # this should update self.current_idx as well.
        shard_data = self.buffer_iterations(
            start_idx, start_idx + shards.max_shard_length
        )
        logger.debug(f"buffered: {shard_data}")
        written = shards.write_shard(start_idx, shard_data)
        logger.debug("wrote...")
        if self.finished:
            shards.set_length_final(self.finished)
        return shard_data

    def buffer_iterations(self, start_idx: int, end_idx: int) -> List[Any]:
        buffer = []
        logger.debug(f"start idx: {start_idx}  end: {end_idx} current_idx: {self.current_idx}")
        while self.current_idx  < start_idx - 1:
            logger.info("shouldn't be here...")
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

class QueueItem:
    def __init__(self, start_idx: int, writer: Writer):
        self.start_idx = start_idx
        self.writer = writer
        self.cv = Condition()
        self.result = None

class WriterPool:
    def __init__(
        self,
        writers: Optional[List[Writer]] = None,
        iterator_generator: Optional[Callable[None, Iterator]] = None,
        iterator: Optional[Iterator] = None,
        num_workers: int = 1,
    ):
        assert exactly_one_not_none(writers, iterator_generator, iterator)
        if writers is not None:
            self.writers = writers
        if iterator is not None:
            self.writers = [Writer(iterator)]
        if iterator_generator is not None:
            self.writers = [
                Writer(iterator_generator(), iterator_generator)
                for _ in range(num_workers)
            ]
        self.queue_lock = Lock()
        # self.queue_cv = Condition()
        self.queues = {writer: [] for writer in self.writers}

    def add_to_queue(self, start_idx: int) -> Optional[Writer]:
        # returns None if the selected writer does not need to be scheduled.
        writer_to_append = None
        smallest_gap = float('inf')
        with self.queue_lock:
            for writer in self.writers:
                queue = self.queues[writer]
                if len(queue) == 0:
                    gap = start_idx
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
                            queue.insert(i-1, queue_item)
                            return queue_item
                        
                # if the writer will end before start_idx, 
                # then we record how much further it needs to go.
                # Otherwise, we want to choose the earliest writer.
                gap = max(queue[-1].start_idx, start_idx - queue[-1].start_idx)
                if gap < smallest_gap:
                    smallest_gap = gap
                    writer_to_append = writer

            # no drive-by writing possible, so let's just pick 
            # the one with smallest gap
            queue_item = QueueItem(start_idx, writer_to_append)
            self.queues[writer_to_append].append(queue_item)
            return queue_item

    def block_until_write(self, queue_item: QueueItem, shards: ShardedDataset) -> List[Any]:
        writer = queue_item.writer
        start_idx = queue_item.start_idx
        queue = self.queues[writer]
        while queue_item.result is None:
            with self.queue_lock:
                if queue[0] == queue_item:
                    data = writer.iterate_and_write_shard(start_idx, shards)
                    queue.pop(0)
                    with queue_item.cv:
                        queue_item.result = data
                        queue_item.cv.notify_all()
                        break
            with queue_item.cv:
                queue_item.cv.wait()

        if len(queue_item.result) == 0:
            raise KeyError(f"start_idx {start_idx} is out of range!")
        return queue_item.result

    def load_fn(self, shards: ShardedDataset, start_idx: int) -> List[Any]:
        queue_item = self.add_to_queue(start_idx)
        result = self.block_until_write(queue_item, shards)
        return result

