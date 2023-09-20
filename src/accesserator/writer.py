from typing import Iterator, Any, Optional, List, Callable
from .sharded_dataset import ShardedDataset, ShardInfo
from concurrent.futures import ThreadPoolExecutor
from .simple_cache import SimpleCache
from collections import deque
from threading import Lock
import traceback
import logging
logger = logging.getLogger("randomacces")

class Writer:
    def __init__(
        self,
        it: Iterator,
        shards: ShardedDataset,
        iterator_generator: Optional[Callable[None, Iterator]] = None,
    ):
        self.it = enumerate(it)
        self.shards = shards
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

    def update_pending_max_idx(self, start_idx: int) -> None:
        self.pending_max_idx = start_idx + self.shards.max_shard_length

    def iterate_and_write_shard(self, start_idx: int) -> List[Any]:
        # Shards must be aligned with self.shards.max_shard_length
        assert start_idx % self.shards.max_shard_length == 0

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
            start_idx, start_idx + self.shards.max_shard_length
        )
        logger.debug(f"buffered: {shard_data}")
        self.shards.write_shard(start_idx, shard_data)
        logger.debug("wrote...")
        if self.finished:
            self.shards.set_length_final(self.finished)
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


class WriterPool:
    def __init__(
        self,
        shards: ShardedDataset,
        cache: SimpleCache,
        writers: Optional[List[Writer]] = None,
        iterator_generator: Optional[Callable[None, Iterator]] = None,
        num_workers: Optional[int] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        iterator: Optional[Iterator] = None,
    ):
        assert exactly_one_not_none(writers, iterator_generator, iterator)
        if writers is not None:
            self.writers = writers
        if iterator is not None:
            self.writers = [Writer(iterator, shards)]
        if iterator_generator is not None:
            self.writers = [
                Writer(iterator_generator(), shards, iterator_generator)
                for _ in range(num_workers)
            ]
        self.executor = executor
        self.cache = cache
        self.shards = shards
        self.queue_lock = Lock()
        self.queues = {writer: [] for writer in self.writers}

    def select_writer_for_shard(self, start_idx: int) -> Writer:
        chosen_writer = None

        # at worse we can start from 0...
        best_gap = start_idx
        for writer in self.writers:
            gap = start_idx - writer.pending_max_idx
            if gap > 0 and gap <= best_gap:
                chosen_writer = writer
                best_gap = gap
        if chosen_writer is not None:
            return chosen_writer

        # all the writers are too big! we need to select one to reset
        ordered_writers = sorted(self.writers, key=lambda w: w.pending_max_idx)

        # if we have a "finished" writer, then choose that one...
        last_writer = ordered_writers[-1]
        if last_writer.finished:
            return last_writer

        # otherwise, choose a the writer with the smallest gap...
        prev_idx = 0
        writer_to_reset = ordered_writers[0]
        smallest_gap = writer_to_reset.pending_max_idx
        for writer in ordered_writers[1:]:
            gap = writer.pending_max_idx - prev_idx
            prev_idx = writer.pendinf_max_idx
            if gap < smallest_gap:
                smallest_gap = gap
                writer_to_reset = writer

        return writer_to_reset

    def write_shard_async(
        self, start_idx: int, executor: Optional[ThreadPoolExecutor] = None
    ) -> bool:
        fp = self.shards.get_shard_handle(start_idx)
        # fp will go out of scope at the end of this function unless we submit
        # a read request using it.
        if fp is not None:
            shard_info = self.shards.get_shard_info(start_idx)
            if shard_info.end == shard_info.start + self.shards.max_shard_length:
                # this shard already exists and is full: we don't need to write
                # anything.
                if executor is None:
                    executor = self.executor
                executor.submit(read_and_cache_shard, start_idx, fp, self.shards, self.cache)
                return False
        writer = self.add_to_queue(start_idx)
        if writer is not None:
            self.flush_queue_async(writer, executor)
        return True

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
                if queue[0] <= start_idx and queue[-1] >= start_idx:
                    # this writer will pass through the desired shard
                    for i in range(len(queue)):
                        if queue[i] == start_idx:
                            return
                        if queue[i] > start_idx:
                            queue.insert(i-1, start_idx)
                            return
                        
                # if the writer will end before start_idx, 
                # then we record how much further it needs to go.
                # Otherwise, we want to choose the earliest writer.
                gap = max(queue[-1], start_idx - queue[-1])
                if gap < smallest_gap:
                    smallest_gap = gap
                    writer_to_append = writer

            # no drive-by writing possible, so let's just pick 
            # the one with smallest gap
            self.queues[writer_to_append].append(start_idx)
            if len(self.queues[writer_to_append]) == 1:
                return writer_to_append


    def flush_queue(self, writer: Writer):
        try:
            queue = self.queues[writer]
            while len(queue) > 0:
                with self.queue_lock:
                    start_idx = queue.pop(0)
                shard = writer.iterate_and_write_shard(start_idx)
                if len(shard) > 0:
                    self.cache[start_idx] = shard
            writer.pending_max_idx = writer.current_idx
        except Exception as e:
            # logger.debug(e)
            print(traceback.format_exc())

    def flush_queue_async(
        self, writer: Writer, executor: Optional[ThreadPoolExecutor] = None
    ) -> None:
        if executor is None:
            executor = self.executor
        executor.submit(self.flush_queue, writer)

def read_and_cache_shard(start_idx: int, fp: Any, shards: ShardedDataset, cache: SimpleCache) -> None:
    try:
        cache[start_idx] = shards.read_shard_from_handle(fp)
    except Exception as e:
        print(traceback.format_exc())
    
    
