# loadit: random access to iterables in python

-----

**Table of Contents**
- [Installation](#installation)
- [Usage](#usage)
- [Storage Format](#storage-format)
- [License](#license)

## Installation

```console
pip install loadit
```

## Usage

```python
from loadit import LoadIt

def my_iterator():
    ...
    yield item

loader = LoadIt(my_iterator, root_dir="cache/")

# same output as "for x in my_iterator():"
for x in loader:
    print(x)
    
# but we can also do this:
for i in range(10):
    print(loader[i])
    
# or this:
for i in [2,4,9,10,32,4,6]:
    print(loader[i])

# this might be slow to start, because we have to
# calculate the length (and if the iterator never terminates
# then it will wait forever):
loader = LoadIt(another_iterator, root_dir="cache/")
for i in range(len(loader)):
    print(loader[i])

# similarly, this might be slow:
print(loader[-2])
# once the above has run, this will be fast:
print(loader[-10])
```

### What if I cannot store a copy of my data on disk??
We got you:
```python
loader = LoadIt(
    fn_that_creates_new_iterators,
    root_dir="cache/",
    memory_limit=16 * 2**(30)) # 16 GB on-disk cache

# ~ as fast as normal iteration:
for x in loader:
    print(x)

# possibly a bit slow:
print(loader[11030])

# probably pretty fast (after running the previous line)
print(loader[11193])
print(loader[10500])
```

### What if I want to make sure that my data is cached to disk?
```python
loader = LoadIt(
    lambda : iterate_over_cloud_dataset(bucket),
    root_dir = "cache/"
    memory_limit = None,
    max_workers = 2, # needs to be > 1 for preload_all_async=True
    preload_all_async=True)

# you can access items as usual. All data is being loaded
# asynchronously
x = loader[5]

# is the data cached yet?
if loader.all_cached_to_disk():
    print("not all cached yet!")

# if you must wait until all the data is on disk:
loader.wait_until_all_cached()

# Note that the above call will not return until all the data is cached.
# If you do not allow enough space on disk to cache it all, or if you don't
# provide  preload_all_async = True, (either in this process, or in some other process)
# then it will never return. Use with caution!
```

## Other goodies
The following utilities operate on any python objects that have a  `len` and a `__getitem__`:
```python
view = loadit.SequenceView(a, [2,3,1,2,3,5,7,8,9,10,1])
assert view[1] == a[3]

new_view = loadit.SequenceView(view, lambda idx: -2*idx, length=len(view)//2)
assert new_view[1] == a[10]

newer_view = new_view[0,3,4]
assert newer_view[1] == a[5]

a_b_c = loadit.ConcatableSequence(a,b) + loadit.ConcatableSequence(b)
# a_b_c is equivalent to a + b + c if a,b,c were all lists.
# like itertools.chain, but for random access.

a_b_c = loadit.InterleaveSequence(a,b,c)
# a_b_c will interleave a, b, c in round-robin fashion.

repeat = loadit.RepeatSequence(a, repeats=3)
# like itertools.repeat. repeats=None will repeat infinitely

shuffled = chunk_shuffle(seq, chunk_size=128, length=1024)
# bins indices into groups of 128 and returns a view of the
# first 1024 elements of seq that is shuffled within each bin.
# if chunk_size of length is unspecified, they default to len(seq).
```

### Features
* Aims to provide the same user-interface as `loader = list(my_iterator)`.
* Allows for iterators that do not fit in memory by caching iterations in the file system.
* Previously cached data can be re-used: if the entire iterator can be cached, then you only need the cache.
* If we don't have the disk space to cache all iterations, we'll automatically regenerate iterations on-demand.
* Safe to use with multithreading or multiprocessing.
* Also provides some helpful utilities for manipulating list-like objects.


### Restrictions/Caveats
* The objects returned by the iterator must be pickleable.
* The ordering of the iterator must be deterministic.
* If your iterator is small, then you're definitely going to be better off with `loader = list(my_iterator)`. This module is for LARGE iterators.
* If you are just going to be making in-order linear passes over the data, it is definitely going to be faster and simpler to just do `for x in my_iterator:`. This is for workloads that involve jumping around in the indices a bit.
* The default settings store data in "shard" files of about 64 MB and cache at most 128 such shards in a dictionary. On some systems this might exceed available memory. In this case, you should adjust one or both settings to reduce memory usage.

### Detailed Options

The `LoadIt` initialization signature is:
```python
class LoadIt
    def __init__(
        self,
        create_it: Optional[Callable[None, Iterable]],
        root_dir: Union[str, Path],
        max_shard_length: Union[str,int] = "64mb",
        max_cache_size: int = 128,
        max_workers: int = 3,
        memory_limit: Optional[int] = None,
        compression: Optional[str] = None,
        preload_fn: Optional[Callable[[Self, int], Iterable[List[int]]]] = preload_next_shard,
        preload_all_async: False,
    ):
```
The arguments are:
* `create_it`: this is a function that takes no arguments and returns a new iterable (that is, it is possible to do `for x in create_it():`). If `create_it` is `None`, then we cannot load new iterates from scratch. However, if all of the data has already been cached on disk at `root_dir`, then things will still work fine - we don't need the iterator anymore.
* `root_dir`: this is where we will stash iterations on the file system. If you instantiate a new `LoadIt` instance
with the same `root_dir`, then either `create_it` should return the same iterator, or you can set `create_it` to `None`
and simply use the cached data directly.
* `max_shard_length`: Each file (a "shard") stored in the `root_dir` directory will contain at most this many iterations.
You can also specify a string ending in `mb`, such as `32mb`. This will automatically set the length so that the size of the shards will be approximately the given number of megabytes.
Note that this approximation is based on the size of the first 128 iterations, and so may be poor if there is high variation in iteration size.
* `max_cache_size`: We will keep at most this shards in main memory (i.e. loaded in from disk) at once.
* `max_workers`: This is the number of worker threads that will be spawned to write shards. CAUTION: if `max_workers` is >1, then the function `create_it` must be safe to run in multiple threads.
* `memory_limit`: The total size of all shard files stored on disk in `root_dir` will be at most this many bytes.
* `compression`: You can provide an optional string representing a compression format to use when caching data on disk. Compression uses `fsspec`, so anything in `fsspec.available_compressions()` is valid.
* `preload_fn`: This function will be called every time you request an iterate to schedule pre-fetching of further iterates. By default it 
fetches the next `max_workers/2` shards. Iterating over `preload_fn(loader, idx)` should yield lists of indices. For each list, a seperate thread
will go in order over the list and make sure that each index is in memory.
* `preload_all_async`: if True, then we will iterate through the iterator in the background and cache all iterations to disk. If False, then we will only iterate to cache iterations that are actually requested (plus a few speculative extra iterations specified by `preload_fn`).



## Storage format

The goal of the storage format is to store the data to disk in a way that:
* is very simple.
* enables reasonably fast access to the ith element.
* can be quickly appended to.
* is easily understandable by looking at the directory structure (see first point)
* is easily modifiable by custom code written "from scratch" without using this module.


```
root_dir/
    metadata.json
    shards/
        # <start>.<end>.shard.pickle
        0.30.shard.pickle # first 30 items
        30.60.shard.pickle # next 30 items
        60.90.shard.pickle # items 60 - 89
        ...
        3030.3050.shard.pickle 
        # last shard might contain less than 30 items.
    # the following directories are used when writing or reading the data.
    # if you want to copy the data somewhere else, it is ok to omit these directories.
    locks/
        # contains locking files for synchronization.
    scratch/
        # place to store temporary files when writing data.
```

The file `metadata.json` contains the following object:
```
{
    "max_shard_length": int # max number of entries in each shard. Each shard will store exactly this many items, except possibly the last shard.
    "length": int # a *lower bound* on the length of the iterator.
    "compression": Optional[str] # what compression is being used.
    "length_final": bool # whether "length" is correct.
    "version": int # a version number incremented whenever a breaking change is made.
}
```
Each shard file will be loaded by:
```python
with open(f"{start}.{end}.shard.pickle", "rb") as fp:
    data = pickle.load(fp)
```
Then `data` will be a list for which `data[i]` is the `start + i`th element in the iterator.


## License

`loadit` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
