# loadit: random access to iterables in python

-----

**Table of Contents**
- [Usage](#Usage)
- [Installation](#installation)
- [License](#license)

## Usage

```python
from loadit import LoadIt

def my_iterator():
    ...
    yield item

loader = LoadIt(my_iterator)

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
loader = LoadIt(another_iterator)
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
    memory_limit=16 * 2**(30)) # 16 GB cache

# ~ as fast as normal iteration:
for x in loader:
    print(x)

# possibly a bit slow:
print(loader[11030])

# probably pretty fast (after running the previous line)
print(loader[11193])
print(loader[10500])

```


### Features
* Should provide the same user-interface as `loader = list(my_iterator)`.
* Allows for iterators that do not fit in memory by caching iterations in the file system.
* Previously cached data can be re-used: if the entire iterator can be cached then you only need the cache.
* If we don't have the disk space to cache all iterations, we'll automatically regenerate iterations on-demand.
* Safe to use with multithreading or multiprocessing.


### Restrictions/Caveats
* The objects returned by the iterator must be pickleable.
* The ordering of the iterator must be deterministic.
* If your iterator is small, then you're definitely going to be better off with `loader = list(my_iterator)`. This module is for LARGE iterators.
* If you are just going to be making in-order linear passes over the data, it is definitely going to be faster and simpler to just do `for x in my_iterator:`. This is for workloads that involve jumping around in the indices a bit.

### Detailed Options

The `LoadIt` initialization signature is:
```python
class LoadIt
    def __init__(
        self,
        create_it: Callable[None, Iterable],
        root_dir: Union[str, Path] = "cache/",
        max_shard_length: Union[str,int] = 512,
        max_cache_size: int = 128,
        max_workers: int = 3,
        memory_limit: Optional[int] = None,
        preload_fn: Optional[Callable[[Self, int], Iterable[List[int]]]] = preload_next_shard,
    ):
```
The arguments are:
* `create_it`: this is a function that takes no arguments and returns a new iterable (that is, it is possible to do `for x in create_it():`).
* `root_dir`: this is where we will stash iterations on the file system. If you instantiate a new `LoadIt` instance
with the same `root_dir`, then either `create_it` should return the same iterator, or you can set `create_it` to `None`
and simply use the cached data directly.
* `max_shard_length`: Each file (a "shard") stored in the `root_dir` directory will contain at most this many iterations.
You can also specify a string ending in `mb`, such as `32mb`. Then, the size of the shards will be approximately the given number of megabytes.
Note that this approximation is based on the size of the first 128 iterations, and so may be poor if there is high variation in iteration size.
* `max_cache_size`: We will keep at most this shards in RAM at once.
* `max_workers`: This is the number of worker threads that will be spawned to write shards.
* `memory_limit`: The total size of all shard files stored in `root_dir` will be at most this many bytes.
* `preload_fn`: This function will be called every time you request an iterate to schedule pre-fetching of further iterates. By default it 
fetches the next `max_workers-1` shards. Iterating over `preload_fn(loader, idx)` should yield lists of indices. For each list, a seperate thread
will go in order over the list and make sure that each index is in memory.


## Installation

```console
pip install git+https://github.com/acutkosky/loadit.git@main
```

## License

`loadit` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html) license.
