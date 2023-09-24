import loadit
import pytest
import time
import numpy as np
import pickle
import random


def get_array(n):
    return (np.reshape(np.arange(0, (n + 1) * 10000, n + 1), (100, 100)),)


def create_it(N: int = 100000, delay=0):
    for n in range(N):
        result = {
            "array": get_array(n),
            "label": f"item_{n}",
        }
        if delay > 0:
            time.sleep(delay)
        yield result


def validate_data(data, n):
    assert np.array_equal(data["array"], get_array(n))
    assert data["label"] == f"item_{n}"


def create_fixture(name, values):
    return pytest.fixture(lambda request: request.param, params=values, name=name)


load_from_it = create_fixture("load_from_it", None)
it_size = create_fixture("it_size", [100, 2000])
delay = create_fixture("delay", [0, 0.0001])
max_shard_length = create_fixture("max_shard_length", [64, 512])
max_cache_size = create_fixture("max_cache_size", [5, 100])
max_workers = create_fixture("max_workers", [1, 5])


@pytest.fixture
def verify_sizes(max_shard_length, max_cache_size, memory_limit):
    def verify(loader):
        if memory_limit is not None:
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


@pytest.fixture
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
    load_from_it,
    it_size,
    delay,
    max_shard_length,
    max_cache_size,
    memory_limit,
):
    create_it_fn = lambda: create_it(it_size, delay)
    if load_from_it:
        create_it_arg = create_it_fn()
    else:
        create_it_arg = create_it_fn
    loader = loadit.LoadIt(
        create_it=create_it_arg,
        root_dir=tmp_path,
        max_shard_length=max_shard_length,
        max_cache_size=max_cache_size,
        memory_limit=memory_limit,
    )

    yield loader

    loader.shards.observer.unschedule_all()
    loader.shards.observer.stop()
    loader.shards.observer.join()


@pytest.fixture
def load_from_creator(tmp_path):
    return loader


def pytest_generate_tests(metafunc):
    if metafunc.function in [
        test_loader_random_access,
        test_negative_indices,
        test_slicing,
    ]:
        metafunc.parametrize("load_from_it", [False])
    elif "load_from_it" in metafunc.fixturenames:
        metafunc.parametrize("load_from_it", [True, False])


def test_loader_can_iterate(loader, it_size, verify_sizes):
    for i, x in enumerate(loader):
        validate_data(x, i)
        verify_sizes(loader)
        if i == 100:
            break
    assert i == min(100, it_size - 1)


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
    for i in range(1, loader.max_workers + 1):
        x = small_cache_loader[i * loader.shards.max_shard_length]

    assert loader.memory_cache._cache_miss_count == 2


def test_reuse_data(full_save_loader, nowriter_loader):
    # write all the data
    for x in full_save_loader:
        pass

    # read without writers
    for i, x in enumerate(nowriter_loader):
        validate_data(x, i)
        pass


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

    for i, x in enumerate(loader[-10 : it_size - 4 : 2]):
        validate_data(x, it_size - 10 + i * 2)