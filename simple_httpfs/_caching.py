from __future__ import annotations

import posixpath as pp
import threading
from collections import OrderedDict
from collections.abc import Iterator, MutableMapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar
from weakref import WeakValueDictionary

from diskcache import Cache as DiskCache
from obspec import GetRange, Head, ListResult, ListWithDelimiter, ObjectMeta

if TYPE_CHECKING:
    try:
        from collections.abc import Buffer
    except ImportError:
        from typing_extensions import Buffer

    from obstore import Bytes


class Store(GetRange, Head, ListWithDelimiter, Protocol): ...


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class CacheMonitor:
    total_requests: int = 0
    total_blocks: int = 0
    lru_hits: int = 0
    lru_misses: int = 0
    disk_hits: int = 0
    disk_misses: int = 0

    @property
    def lru_hit_rate(self) -> float:
        total = self.lru_hits + self.lru_misses
        return self.lru_hits / total if total > 0 else 0.0

    @property
    def disk_hit_rate(self) -> float:
        total = self.disk_hits + self.disk_misses
        return self.disk_hits / total if total > 0 else 0.0

    def reset(self) -> None:
        self.total_requests = 0
        self.total_blocks = 0
        self.lru_hits = 0
        self.lru_misses = 0
        self.disk_hits = 0
        self.disk_misses = 0


class LRUCache(MutableMapping[K, V], Generic[K, V]):
    def __init__(self, capacity: int = 128):
        self.capacity = capacity
        self.cache: OrderedDict[K, V] = OrderedDict()

    def __getitem__(self, key: K) -> V:
        """Like dict.__getitem__, but updates usage if key exists."""
        if key not in self.cache:
            raise KeyError(key)
        self.cache.move_to_end(key)
        return self.cache[key]

    def __setitem__(self, key: K, value: V) -> None:
        # If cache hit, move to end before updating
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        # Evict least recently used item if capacity exceeded
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

    def __delitem__(self, key: K) -> None:
        del self.cache[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self.cache)

    def __len__(self) -> int:
        return len(self.cache)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({dict(self.cache)}, capacity={self.capacity})"
        )

    def get(self, key: K, default: Any = None) -> Any:
        """Like dict.get, but updates usage if key exists."""
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return default

    def pop(self, key: K, *args: Any) -> Any:
        return self.cache.pop(key, *args)

    def clear(self) -> None:
        self.cache.clear()


class CachedStore(Store):
    def __init__(
        self,
        store: Store,
        *,
        base_url: str,
        meta_cache: LRUCache[str, ObjectMeta],
        mem_cache: LRUCache[str, Buffer],
        disk_cache: DiskCache,
        block_size: int = 1024 * 1024,
        cache_monitor: CacheMonitor | None = None,
    ):
        self.store = store
        scheme, path = base_url.split("://", 1)
        self.scheme = scheme
        self.base_path = path.lstrip("/")
        self.meta_cache = meta_cache
        self.mem_cache = mem_cache
        self.disk_cache = disk_cache
        self.block_size = block_size
        self.monitor = cache_monitor or CacheMonitor()
        self._block_locks: WeakValueDictionary[str, threading.Lock] = (
            WeakValueDictionary()
        )
        self._lock: threading.Lock = threading.Lock()

    def _meta_cache_key(self, path: str) -> str:
        full_path = pp.normpath(pp.join(self.base_path, path)).lstrip("/")
        return f"{self.scheme}://{full_path}"

    def _block_cache_key(self, path: str, block_num: int) -> str:
        full_path = pp.normpath(pp.join(self.base_path, path)).lstrip("/")
        return f"{self.scheme}://{full_path}.{self.block_size}.{block_num}"

    def head(self, path: str) -> ObjectMeta:
        cache_key = self._meta_cache_key(path)
        if cache_key in self.meta_cache:
            return self.meta_cache[cache_key]
        meta = self.store.head(path)
        self.meta_cache[cache_key] = meta
        return meta

    def get_range(
        self,
        path: str,
        *,
        start: int,
        end: int | None = None,
        length: int | None = None,
    ) -> Bytes | bytes:
        pos = start
        if length is not None:
            end = start + length
        elif end is None:
            raise ValueError("Either end or length must be provided")

        output = b""
        while pos < end:
            block_num = pos // self.block_size
            remaining = end - pos
            data_start = pos % self.block_size
            data_size = min(self.block_size - data_start, remaining)

            # Read the block from cache or from the obstore and write to cache.
            # This is thread-safe.
            block = self._get_block(path, block_num)
            if not block:
                break

            if not hasattr(block, "__getitem__"):
                block = bytes(block)

            # Extract only the portion we need from this block
            output += block[data_start : data_start + data_size : 1]  # type: ignore[index]
            pos += data_size

        self.monitor.total_requests += 1
        return output

    def _get_lock(self, cache_key: str) -> threading.Lock:
        with self._lock:
            if cache_key not in self._block_locks:
                block_lock = threading.Lock()
                self._block_locks[cache_key] = block_lock
            else:
                block_lock = self._block_locks[cache_key]
            return block_lock

    def _get_block(self, path: str, block_num: int) -> Buffer:
        cache_key = self._block_cache_key(path, block_num)

        self.monitor.total_blocks += 1
        with self._get_lock(cache_key):
            if cache_key in self.mem_cache:
                self.monitor.lru_hits += 1
                return self.mem_cache[cache_key]
            self.monitor.lru_misses += 1

            if cache_key in self.disk_cache:
                block = self.disk_cache[cache_key]
                self.monitor.disk_hits += 1
                self.mem_cache[cache_key] = block
                return block  # type: ignore[no-any-return]
            self.monitor.disk_misses += 1

            block = self.store.get_range(
                "", start=block_num * self.block_size, length=self.block_size
            )
            self.mem_cache[cache_key] = block
            self.disk_cache[cache_key] = block  # Writes to disk

        return block

    def list_with_delimiter(
        self, prefix: str | None = None
    ) -> ListResult[Sequence[ObjectMeta]]:
        return self.store.list_with_delimiter(prefix)
