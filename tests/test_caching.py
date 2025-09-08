from collections.abc import Iterator

import obstore
import pytest
from diskcache import Cache as DiskCache

from simple_httpfs._caching import CachedStore, CacheMonitor, LRUCache


class TestLRUCache:
    """Test cases for LRUCache implementation."""

    def test_init_default_capacity(self):
        cache = LRUCache()
        assert cache.capacity == 128
        assert len(cache) == 0

    def test_init_custom_capacity(self):
        cache = LRUCache(capacity=64)
        assert cache.capacity == 64
        assert len(cache) == 0

    def test_basic_setitem_getitem(self):
        cache = LRUCache(capacity=3)
        cache["key1"] = "value1"
        cache["key2"] = "value2"

        assert cache["key1"] == "value1"
        assert cache["key2"] == "value2"
        assert len(cache) == 2

    def test_getitem_keyerror(self):
        cache = LRUCache()
        with pytest.raises(KeyError):
            _ = cache["nonexistent"]

    def test_delitem(self):
        cache = LRUCache()
        cache["key"] = "value"
        assert "key" in cache

        del cache["key"]
        assert "key" not in cache
        assert len(cache) == 0

    def test_delitem_keyerror(self):
        cache = LRUCache()
        with pytest.raises(KeyError):
            del cache["nonexistent"]

    def test_update_existing_key(self):
        cache = LRUCache(capacity=3)
        cache["key"] = "value1"
        cache["other"] = "other_value"

        # Update existing key - should move to end
        cache["key"] = "value2"

        assert cache["key"] == "value2"
        assert len(cache) == 2

    def test_lru_eviction(self):
        cache = LRUCache(capacity=3)

        # Fill to capacity
        cache["a"] = "value_a"
        cache["b"] = "value_b"
        cache["c"] = "value_c"
        assert len(cache) == 3

        # Add one more - should evict 'a' (least recently used)
        cache["d"] = "value_d"
        assert len(cache) == 3
        assert "a" not in cache
        assert "b" in cache
        assert "c" in cache
        assert "d" in cache

    def test_lru_access_updates_order(self):
        cache = LRUCache(capacity=3)

        # Fill to capacity
        cache["a"] = "value_a"
        cache["b"] = "value_b"
        cache["c"] = "value_c"

        # Access 'a' to make it most recently used
        _ = cache["a"]

        # Add one more - should evict 'b' (now least recently used)
        cache["d"] = "value_d"
        assert "a" in cache  # Should still be there
        assert "b" not in cache  # Should be evicted
        assert "c" in cache
        assert "d" in cache

    def test_lru_update_existing_moves_to_end(self):
        cache = LRUCache(capacity=3)

        # Fill to capacity
        cache["a"] = "value_a"
        cache["b"] = "value_b"
        cache["c"] = "value_c"

        # Update 'a' - should move it to end
        cache["a"] = "new_value_a"

        # Add one more - should evict 'b' (now least recently used)
        cache["d"] = "value_d"
        assert cache["a"] == "new_value_a"  # Should still be there
        assert "b" not in cache  # Should be evicted
        assert "c" in cache
        assert "d" in cache

    def test_get_method_with_default(self):
        cache = LRUCache()
        cache["exists"] = "value"

        assert cache.get("exists") == "value"
        assert cache.get("nonexistent") is None
        assert cache.get("nonexistent", "default") == "default"

    def test_get_method_updates_order(self):
        cache = LRUCache(capacity=3)

        # Fill to capacity
        cache["a"] = "value_a"
        cache["b"] = "value_b"
        cache["c"] = "value_c"

        # Use get() to access 'a'
        assert cache.get("a") == "value_a"

        # Add one more - should evict 'b' (now least recently used)
        cache["d"] = "value_d"
        assert "a" in cache  # Should still be there
        assert "b" not in cache  # Should be evicted

    def test_pop_method(self):
        cache = LRUCache()
        cache["key"] = "value"

        result = cache.pop("key")
        assert result == "value"
        assert "key" not in cache

    def test_pop_method_with_default(self):
        cache = LRUCache()

        result = cache.pop("nonexistent", "default")
        assert result == "default"

    def test_pop_method_keyerror(self):
        cache = LRUCache()
        with pytest.raises(KeyError):
            cache.pop("nonexistent")

    def test_clear_method(self):
        cache = LRUCache()
        cache["a"] = "value_a"
        cache["b"] = "value_b"

        assert len(cache) == 2
        cache.clear()
        assert len(cache) == 0
        assert "a" not in cache
        assert "b" not in cache

    def test_iter(self):
        cache = LRUCache()
        cache["a"] = "value_a"
        cache["b"] = "value_b"
        cache["c"] = "value_c"

        keys = list(cache)
        assert set(keys) == {"a", "b", "c"}
        assert len(keys) == 3

    def test_iter_is_iterator(self):
        cache = LRUCache()
        cache["a"] = "value_a"

        assert isinstance(iter(cache), Iterator)

    def test_contains(self):
        cache = LRUCache()
        cache["exists"] = "value"

        assert "exists" in cache
        assert "nonexistent" not in cache

    def test_repr(self):
        cache = LRUCache(capacity=64)
        cache["a"] = 1
        cache["b"] = 2

        repr_str = repr(cache)
        assert "LRUCache" in repr_str
        assert "capacity=64" in repr_str
        # Should contain the dict contents
        assert "'a': 1" in repr_str or "'b': 2" in repr_str

    def test_capacity_zero_behavior(self):
        cache = LRUCache(capacity=0)

        # Should immediately evict anything added
        cache["key"] = "value"
        assert len(cache) == 0
        assert "key" not in cache

    def test_capacity_one_behavior(self):
        cache = LRUCache(capacity=1)

        cache["a"] = "value_a"
        assert len(cache) == 1
        assert "a" in cache

        # Adding another should evict the first
        cache["b"] = "value_b"
        assert len(cache) == 1
        assert "a" not in cache
        assert "b" in cache

    def test_large_capacity(self):
        cache = LRUCache(capacity=1000)

        # Add many items
        for i in range(500):
            cache[f"key_{i}"] = f"value_{i}"

        assert len(cache) == 500
        # All should still be there
        for i in range(500):
            assert f"key_{i}" in cache

    def test_mixed_key_types(self):
        cache = LRUCache()

        cache["string_key"] = "string_value"
        cache[42] = "int_key_value"
        cache[("tuple", "key")] = "tuple_key_value"

        assert cache["string_key"] == "string_value"
        assert cache[42] == "int_key_value"
        assert cache[("tuple", "key")] == "tuple_key_value"

    def test_complex_eviction_scenario(self):
        cache = LRUCache(capacity=4)

        # Fill cache
        for i in range(4):
            cache[f"key_{i}"] = f"value_{i}"

        # Access keys in different order to change LRU order
        _ = cache["key_1"]  # key_1 becomes most recent
        _ = cache["key_3"]  # key_3 becomes most recent
        cache["key_0"] = "updated_value_0"  # key_0 becomes most recent

        # LRU order should now be: key_2, key_1, key_3, key_0
        # Adding new item should evict key_2
        cache["new_key"] = "new_value"

        assert "key_2" not in cache  # Should be evicted
        assert "key_1" in cache
        assert "key_3" in cache
        assert cache["key_0"] == "updated_value_0"
        assert cache["new_key"] == "new_value"


class TestCachedStore:
    """Test cases for CachedStore wrapper."""

    @pytest.fixture
    def memory_store(self):
        store = obstore.store.MemoryStore()

        # Add test data at empty path (what CachedStore expects for block access)
        file1_data = (
            b"Hello, World! This is file1 content for testing caching behavior."
        )
        store.put("", file1_data)

        # Also add named files for list operations
        store.put("file1.txt", file1_data)
        store.put(
            "file2.txt", b"This is file2 with different content for cache testing."
        )

        return store

    @pytest.fixture
    def caches(self):
        meta_cache = LRUCache(capacity=10)
        mem_cache = LRUCache(capacity=10)
        disk_cache = DiskCache()
        return meta_cache, mem_cache, disk_cache

    @pytest.fixture
    def cached_store(self, memory_store, caches):
        meta_cache, mem_cache, disk_cache = caches
        return CachedStore(
            store=memory_store,
            base_url="memory://test",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=32,
            cache_monitor=CacheMonitor(),
        )

    def test_init(self, memory_store, caches):
        meta_cache, mem_cache, disk_cache = caches
        cached_store = CachedStore(
            store=memory_store,
            base_url="http://example.com/data",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=1024,
            cache_monitor=CacheMonitor(),
        )

        assert cached_store.store is memory_store
        assert cached_store.scheme == "http"
        assert cached_store.base_path == "example.com/data"
        assert cached_store.meta_cache is meta_cache
        assert cached_store.mem_cache is mem_cache
        assert cached_store.disk_cache is disk_cache
        assert cached_store.block_size == 1024

    def test_head_cache(self, cached_store):
        cache_key = "memory://test/file1.txt"
        assert cache_key not in cached_store.meta_cache

        # First call should miss cache and hit the underlying store
        result = cached_store.head("file1.txt")
        assert result["path"] == "file1.txt"
        assert result["size"] == 65
        assert result["e_tag"] is not None  # MemoryStore generates e_tags

        # Result should be cached
        assert cache_key in cached_store.meta_cache
        assert cached_store.meta_cache[cache_key] == result
        assert cached_store.head("file1.txt") == result

    def test_head_file_not_found(self, cached_store):
        with pytest.raises(FileNotFoundError):
            cached_store.head("nonexistent.txt")

    def test_get_range_single_block(self, cached_store):
        # Request data fits in one 32-byte block
        data = cached_store.get_range("file1.txt", start=0, length=20)
        expected = b"Hello, World! This i"
        assert data == expected

    def test_get_range_multiple_blocks(self, cached_store):
        # Request spans multiple 32-byte blocks
        data = cached_store.get_range("file1.txt", start=0, length=50)
        expected = b"Hello, World! This is file1 content for testing ca"
        assert data == expected

    def test_get_range_partial_block(self, cached_store):
        # Request data starting in the middle of a block
        data = cached_store.get_range("file1.txt", start=10, length=10)
        expected = b"ld! This i"
        assert data == expected

    def test_get_range_with_end_parameter(self, cached_store):
        data = cached_store.get_range("file1.txt", start=0, end=15)
        expected = b"Hello, World! T"
        assert data == expected

    def test_get_range_length_required_error(self, cached_store):
        with pytest.raises(ValueError, match="Either end or length must be provided"):
            cached_store.get_range("file1.txt", start=0)

    def test_block_caching_mem_cache(self, cached_store):
        # First request should load blocks into cache
        data1 = cached_store.get_range("file1.txt", start=0, length=32)

        # Check that block is in memory cache
        cache_key = "memory://test/file1.txt.32.0"
        assert cache_key in cached_store.mem_cache

        # Second request for same block should hit memory cache
        data2 = cached_store.get_range("file1.txt", start=0, length=32)

        assert data1 == data2

    def test_block_caching_disk_cache(self, cached_store):
        # Load a block
        data1 = cached_store.get_range("file1.txt", start=0, length=32)
        cache_key = "memory://test/file1.txt.32.0"

        # Block should be in both memory and disk cache
        assert cache_key in cached_store.mem_cache
        assert cache_key in cached_store.disk_cache

        # Remove from memory cache to test disk cache hit
        del cached_store.mem_cache[cache_key]
        assert cache_key not in cached_store.mem_cache

        # Request same block - should hit disk cache
        data2 = cached_store.get_range("file1.txt", start=0, length=32)
        assert data1 == data2

        # Block should be back in memory cache
        assert cache_key in cached_store.mem_cache

    def test_cache_key_generation(self, cached_store):
        # Test that cache keys are generated correctly for different operations
        data1 = cached_store.get_range("file1.txt", start=0, length=32)
        data2 = cached_store.get_range("file1.txt", start=32, length=32)

        # Should have separate cache entries for different blocks
        cache_key1 = "memory://test/file1.txt.32.0"  # Block 0
        cache_key2 = "memory://test/file1.txt.32.1"  # Block 1

        assert cache_key1 in cached_store.mem_cache
        assert cache_key2 in cached_store.mem_cache

        # Different blocks should have different content
        assert data1 != data2

    def test_list_with_delimiter_returns_correct_structure(self, cached_store):
        result = cached_store.list_with_delimiter()
        file_paths = {obj["path"] for obj in result["objects"]}
        assert len(file_paths) == 2
        assert "file1.txt" in file_paths
        assert "file2.txt" in file_paths

        # Verify result structure matches expected format
        assert "objects" in result
        assert "common_prefixes" in result
        assert isinstance(result["objects"], list)
        assert isinstance(result["common_prefixes"], list)

    def test_path_prefix_handling(self, memory_store, caches):
        meta_cache, mem_cache, disk_cache = caches
        cached_store = CachedStore(
            store=memory_store,
            base_url="memory://example.com/data/files/",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=32,
            cache_monitor=CacheMonitor(),
        )
        _ = cached_store.head("file1.txt")

        # Cache key should be normalized
        cache_key = "memory://example.com/data/files/file1.txt"
        assert cache_key in cached_store.meta_cache

    def test_block_size_configuration(self, memory_store, caches):
        meta_cache, mem_cache, disk_cache = caches
        cached_store = CachedStore(
            store=memory_store,
            base_url="memory://test",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=16,  # Smaller block size
            cache_monitor=CacheMonitor(),
        )
        _ = cached_store.get_range("file1.txt", start=0, length=20)

        # Should create cache keys with the correct block size
        cache_key = "memory://test/file1.txt.16.0"
        assert (
            cache_key in cached_store.mem_cache or cache_key in cached_store.disk_cache
        )

    def test_scheme_based_cache_isolation(self, memory_store, caches):
        meta_cache, mem_cache, disk_cache = caches

        # Create two CachedStore instances with different schemes
        http_store = CachedStore(
            store=memory_store,
            base_url="http://example.com/test",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=32,
            cache_monitor=CacheMonitor(),
        )

        s3_store = CachedStore(
            store=memory_store,
            base_url="s3://example.com/test",
            meta_cache=meta_cache,
            mem_cache=mem_cache,
            disk_cache=disk_cache,
            block_size=32,
            cache_monitor=CacheMonitor(),
        )

        # Access the same file through both stores
        http_store.get_range("file1.txt", start=0, length=32)
        s3_store.get_range("file1.txt", start=0, length=32)

        # Both stores should have separate cache entries
        http_cache_key = "http://example.com/test/file1.txt.32.0"
        s3_cache_key = "s3://example.com/test/file1.txt.32.0"

        assert http_cache_key in mem_cache
        assert s3_cache_key in mem_cache
        assert http_cache_key != s3_cache_key

        # Same for metadata cache
        http_meta_key = "http://example.com/test/file1.txt"
        s3_meta_key = "s3://example.com/test/file1.txt"

        http_store.head("file1.txt")
        s3_store.head("file1.txt")

        assert http_meta_key in meta_cache
        assert s3_meta_key in meta_cache
        assert http_meta_key != s3_meta_key
