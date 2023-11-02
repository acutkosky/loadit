import loadit
import pytest
import time
import numpy as np
import pickle
import random
import os
from concurrent.futures import ThreadPoolExecutor


def get_array(n):
    return (np.reshape(np.arange(0, (n + 1) * 10000, n + 1), (100, 100)),)


def create_it(N: int = 100000, delay=0):
    for n in range(N):
        result = {
            "array": get_array(n),
            "label": f"item_{n}",
            "index": n,
        }
        if delay > 0:
            time.sleep(delay)
        yield result


def validate_data(data, n):
    assert np.array_equal(data["array"], get_array(n))
    assert data["label"] == f"item_{n}"
    assert data["index"] == n


def create_fixture(name, values):
    return pytest.fixture(lambda request: request.param, params=values, name=name)


# load_from_it = create_fixture("load_from_it", None)
it_size = create_fixture("it_size", [100, 2000])
delay = create_fixture("delay", [0, 0.0001])
max_shard_length = create_fixture("max_shard_length", [64, 512])
max_cache_size = create_fixture("max_cache_size", [5, 100])
max_workers = create_fixture("max_workers", [1, 10])


@pytest.fixture
def verify_sizes(max_shard_length, max_cache_size, memory_limit):
    def verify(loader):
        if memory_limit is not None:
            if loader.shards.size() > memory_limit:
                # it's possible that an eviction is in progress:
                # give it time to complete.
                time.sleep(0.1)
            assert loader.shards.size() <= memory_limit
        assert loader.memory_cache.size() <= max_cache_size

    return verify


@pytest.fixture
def random_indices():
    return [random.randint(0, 1000000) for _ in range(100000)]


@pytest.fixture(params=[10, None])
def memory_limit(request, max_shard_length):
    if request.param is None:
        return None
    return request.param * max_shard_length * one_item_memory_size()


def one_item_memory_size():
    it = create_it(N=10)
    minishard = list(it)
    s = pickle.dumps(minishard)
    return len(s) / 10


@pytest.fixture
def small_cache_loader(tmp_path):
    loader = loadit.LoadIt(
        create_it=lambda: create_it(),
        root_dir=tmp_path,
        max_shard_length=16,
        max_cache_size=10,
        max_workers=5,
        memory_limit=20 * 16 * one_item_memory_size(),
    )
    return loader


def nowriter_loader(tmp_path):
    loader = loadit.LoadIt(
        create_it=None,
        root_dir=tmp_path,
        max_shard_length=16,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    return loader


@pytest.fixture
def full_save_loader(tmp_path):
    loader = loadit.LoadIt(
        create_it=lambda: create_it(N=16 * 100),
        root_dir=tmp_path,
        max_shard_length=16,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    return loader


@pytest.fixture
def loader(
    tmp_path,
    it_size,
    delay,
    max_shard_length,
    max_cache_size,
    memory_limit,
    max_workers,
):
    create_it_fn = lambda: create_it(it_size, delay)
    loader = loadit.LoadIt(
        create_it=create_it_fn,
        root_dir=tmp_path,
        max_shard_length=max_shard_length,
        max_cache_size=max_cache_size,
        memory_limit=memory_limit,
        max_workers=max_workers,
    )

    yield loader

    loader.shards.observer.unschedule_all()
    loader.shards.observer.stop()
    loader.shards.observer.join()


def test_shard_size_mb(tmp_path):
    create_it = lambda: [np.arange(i, i + 128) for i in range(2024)]
    loader = loadit.LoadIt(
        create_it=create_it,
        root_dir=tmp_path,
        max_shard_length="1mb",
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )

    for x in loader:
        pass

    cached_loader = loadit.LoadIt(
        create_it=None,
        root_dir=tmp_path,
        max_shard_length=None,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    for i, x in enumerate(loader):
        assert np.array_equal(np.arange(i, i + 128), x)

    assert len(loader.shards.get_all_shards()) == 3

    for p in sorted(loader.shards.get_all_shards(), key=lambda p: p.start)[:-1]:
        assert p.size <= 2**20
        assert p.size >= 0.9 * 2**20


def test_parallel_loaders(tmp_path, delay):
    create_it_fn = lambda: create_it(2000, delay)

    def make_loader(create_it_fn):
        loader = loadit.LoadIt(
            create_it=create_it_fn,
            root_dir=tmp_path,
            max_shard_length=16,
            max_cache_size=32,
            memory_limit=None,
            max_workers=10,
        )
        return loader

    def iterate():
        l = make_loader(create_it_fn)
        for i, x in enumerate(l):
            validate_data(x, i)
        l.shards.observer.unschedule_all()
        l.shards.observer.stop()
        l.shards.observer.join()
        return 0

    N = 10
    executor = ThreadPoolExecutor(max_workers=N)
    futures = [executor.submit(iterate) for _ in range(N)]
    for future in futures:
        r = future.result()
    executor.shutdown(wait=True)


def test_loader_can_iterate(loader, it_size, verify_sizes):
    for i, x in enumerate(loader):
        validate_data(x, i)
        verify_sizes(loader)
        if i == 100:
            break
    assert i == min(100, it_size - 1)


def test_loader_can_compress(tmp_path):
    loader = loadit.LoadIt(
        create_it=lambda: create_it(N=1000),
        root_dir=tmp_path,
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
        preload_all_async=True,
        compression="zstd",
    )
    for i, x in enumerate(loader):
        validate_data(x, i)
        if i == 100:
            break

    try:
        with open(loader.shards.get_all_shards()[0].path, "rb") as fp:
            d = pickle.load(fp)
        assert False, "should not be able to load a compressed file..."
    except pickle.PickleError:
        pass


def test_preload_when_infinite_memory(tmp_path):
    loader = loadit.LoadIt(
        create_it=lambda: create_it(N=1000),
        root_dir=tmp_path,
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
        preload_all_async=True,
    )

    assert not loader.all_cached_to_disk()

    loader.wait_until_all_cached()

    shard_cache_misses = loader.shards._cache_miss_count

    for i in range(1000):
        x = loader[i]

    assert loader.all_cached_to_disk()

    assert loader.shards._cache_miss_count == shard_cache_misses


def test_caching(small_cache_loader):
    loader = small_cache_loader
    loader.preload_fn = None
    indices = list(
        range(0, loader.shards.max_shard_length * 21, loader.shards.max_shard_length)
    )
    for i in indices:
        x = loader[i]

    expected_memory_miss = 21
    expected_shard_miss = 21

    assert loader.memory_cache._cache_miss_count == expected_memory_miss
    assert loader.shards._cache_miss_count == expected_shard_miss

    next_indices = indices[::-1]
    for i in next_indices:
        x = loader[i]

    expected_memory_miss += 11
    expected_shard_miss += 1

    assert loader.memory_cache._cache_miss_count == expected_memory_miss
    assert loader.shards._cache_miss_count == expected_shard_miss


def test_preload(small_cache_loader):
    loader = small_cache_loader

    x = small_cache_loader[0]
    assert loader.memory_cache._cache_miss_count == 1

    # sleep for 1 second to give the next shard time to preload
    time.sleep(1)
    # disable preloading:
    loader.preload_fn = None
    for i in range(1, loader.max_workers // 2 + 2):
        x = small_cache_loader[i * loader.shards.max_shard_length]

    assert loader.memory_cache._cache_miss_count == 3


def test_reuse_data(full_save_loader, tmp_path):
    # write all the data
    for x in full_save_loader:
        pass

    new_loader = nowriter_loader(tmp_path)
    # read without writers
    for i, x in enumerate(new_loader):
        validate_data(x, i)
        pass


def test_set_length_from_shards(full_save_loader, tmp_path):
    full_save_loader.preload_fn = None
    shard_length = full_save_loader.shards.max_shard_length
    # write all the data
    for i in range(16):
        x = full_save_loader[i]
    full_save_loader.finalize()

    new_loader = nowriter_loader(tmp_path)
    assert len(new_loader) == 16


def test_uses_multiple_writers(small_cache_loader):
    loader = small_cache_loader
    loader.preload_fn = None
    x = small_cache_loader[11 * loader.shards.max_shard_length]

    assert (
        loader.writer_pool.writers[0].current_idx
        == 12 * loader.shards.max_shard_length - 1
    )
    assert loader.writer_pool.writers[1].current_idx == -1

    x = small_cache_loader[loader.shards.max_shard_length]

    assert (
        loader.writer_pool.writers[0].current_idx
        == 12 * loader.shards.max_shard_length - 1
    )
    assert (
        loader.writer_pool.writers[1].current_idx
        == 2 * loader.shards.max_shard_length - 1
    )


def test_loader_random_access(loader, it_size, random_indices, verify_sizes):
    for i in range(100):
        n = random_indices[i] % it_size
        x = loader[n]
        validate_data(x, n)
        verify_sizes(loader)


def test_negative_indices(loader, it_size):
    validate_data(loader[-10], it_size - 10)
    assert len(loader) == it_size


def test_slicing(loader, it_size):
    for i, x in enumerate(loader[11:21][::-5]):
        validate_data(x, 20 - i * 5)
    assert i == 1

    for i, x in enumerate(loader[-10 : it_size - 4 : 2]):
        validate_data(x, it_size - 10 + i * 2)
    assert i == 2


def test_view(loader, it_size):
    indices1 = [2, 4, 5, 9]
    indices2 = [2, 3]
    for i, x in enumerate(loader[indices1][2, 3]):
        validate_data(x, indices1[indices2[i]])


def test_concat(loader, it_size):
    concat = loadit.ConcatableSequence(loader, loader[3, 4])
    validate_data(concat[it_size + 1], 4)


def test_shuffle(loader, it_size):
    chunk_size = 70
    shuffled = loadit.chunk_shuffle(loader, chunk_size=chunk_size, length=it_size)
    seen_indices = set()
    for i, x in enumerate(shuffled):
        idx = x["index"]
        assert idx not in seen_indices
        assert idx // chunk_size == i // chunk_size
        seen_indices.add(idx)


def test_repeat(tmp_path):
    loader1 = loadit.LoadIt(
        create_it=lambda: create_it(N=100),
        root_dir=os.path.join(tmp_path, "l1"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )

    repeat3 = loadit.RepeatSequence(loader1, 3)
    assert repeat3[130]["index"] == 30
    assert repeat3[299]["index"] == 99

    try:
        _ = repeat3[300]
    except IndexError:
        pass


def test_interleave(tmp_path):
    loader1 = loadit.LoadIt(
        create_it=lambda: create_it(N=100),
        root_dir=os.path.join(tmp_path, "l1"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    loader2 = loadit.LoadIt(
        create_it=lambda: create_it(N=10),
        root_dir=os.path.join(tmp_path, "l2"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    loader3 = loadit.LoadIt(
        create_it=lambda: create_it(N=100),
        root_dir=os.path.join(tmp_path, "l3"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    loader4 = loadit.LoadIt(
        create_it=lambda: create_it(N=10),
        root_dir=os.path.join(tmp_path, "l4"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )
    loader5 = loadit.LoadIt(
        create_it=lambda: create_it(N=22),
        root_dir=os.path.join(tmp_path, "l5"),
        max_shard_length=100,
        max_cache_size=5,
        max_workers=10,
        memory_limit=None,
    )

    interleave = loadit.InterleaveSequences(
        *[
            loader1,
            loader2,
            loader3,
            loader4,
            loader5,
        ]
    )

    test_lists = [
        list(range(100)),
        list(range(10)),
        list(range(100)),
        list(range(10)),
        list(range(22)),
    ]

    idx = 0

    for x in interleave:
        check_value = test_lists[idx].pop(0)
        if len(test_lists[idx]) == 0:
            test_lists.pop(idx)
        else:
            idx += 1
        idx = idx % len(test_lists)

        assert x["index"] == check_value


def test_info(tmp_path):
    loader1 = loadit.LoadIt(
        create_it=lambda: create_it(N=300),
        root_dir=tmp_path,
        max_shard_length=100,
        max_cache_size=5,
        max_workers=3,
        memory_limit=None,
        info={"hello": "there"},
    )
    loader2 = loadit.LoadIt(
        create_it=None,
        root_dir=tmp_path,
        max_shard_length=None,
        max_workers=3,
        memory_limit=None,
    )

    info2 = loader2.info()

    assert len(info2) == 1
    assert info2["hello"] == "there"

    for i in range(4):
        loader1[i * 50]

    loader1.set_info({"hello": "goodbye", "a": [{'1': 2}, "b"]})

    info2 = loader2.info()

    assert len(info2) == 2
    assert info2["hello"] == "goodbye"
    assert len(info2["a"]) == 2
    assert len(info2["a"][0]) == 1
    assert info2["a"][0]['1'] == 2
    assert info2["a"][1] == "b"
