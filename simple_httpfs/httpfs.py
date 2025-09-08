from __future__ import annotations

import logging
import posixpath as pp
from errno import EACCES, EIO, ENOENT
from stat import S_IFDIR, S_IFREG
from time import time
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import urlparse

import obspec.exceptions
import obstore
from diskcache import Cache as DiskCache
from fuse import FuseOSError, LoggingMixIn, Operations
from obspec.exceptions import map_exception

from ._caching import CachedStore, CacheMonitor, LRUCache, Store
from ._ftp import FTPStore

if TYPE_CHECKING:
    try:
        from collections.abc import Buffer
    except ImportError:
        from typing_extensions import Buffer

    from obstore.store import (
        AzureConfig,
        AzureCredentialProvider,
        ClientConfig,
        GCSConfig,
        GCSCredentialProvider,
        RetryConfig,
        S3Config,
        S3CredentialProvider,
    )


class StoreConfigDict(TypedDict, total=False):
    s3: S3Config
    gcs: GCSConfig
    azure: AzureConfig


class CredentialProviderDict(TypedDict, total=False):
    s3: S3CredentialProvider
    gcs: GCSCredentialProvider
    azure: AzureCredentialProvider


def path_to_url(path: str, sentinel: str) -> str | None:
    if path == "/" or not path.endswith(sentinel):
        return None

    # Repeated slashes were already removed
    scheme, path = path.split(":/", 1)
    scheme = scheme.lstrip("/")
    path = (
        path.replace(
            f"{sentinel}/", "/"
        ).rstrip(  # Remove sentinels from any "parent dirs"
            sentinel
        )  # Remove trailing sentinel
    )
    path = pp.normpath(path)

    return f"{scheme}://{path}"


def load_store(
    url: str,
    *,
    configs: StoreConfigDict | None = None,
    credential_providers: CredentialProviderDict | None = None,
    client_options: ClientConfig | None = None,
    retry_config: RetryConfig | None = None,
) -> Store:
    try:
        scheme: str = obstore.parse_scheme(url)
    except Exception:
        scheme = urlparse(url).scheme

    config: S3Config | GCSConfig | AzureConfig | None
    match scheme:
        case "ftp":
            return FTPStore(url)
        case "http" | "https":
            client_options = (client_options or {}).copy()
            client_options.setdefault("allow_http", True)
            return obstore.store.HTTPStore(
                url, client_options=client_options, retry_config=retry_config
            )
        case "s3" | "gcs" | "azure":
            if configs and scheme in configs:
                config = configs[scheme]
            else:
                config = {"skip_signature": True}
            if credential_providers and scheme in credential_providers:
                credential_provider = credential_providers[scheme]
            else:
                credential_provider = None
            return obstore.store.from_url(
                url,
                config=config,  # type: ignore
                client_options=client_options,
                retry_config=retry_config,
                credential_provider=credential_provider,  # type: ignore
            )
        case _:
            return obstore.store.from_url(url)


class HttpFs(LoggingMixIn, Operations):  # type: ignore[misc]
    """
    A read-only http(s)/ftp/object storage filesystem for FUSE.
    """

    def __init__(
        self,
        sentinel: str = "...",
        block_size: int = 2**20,
        disk_cache_size: int = 2**30,
        disk_cache_dir: str = "/tmp/xx",
        lru_capacity: int = 400,
        store_configs: StoreConfigDict | None = None,
        credential_providers: CredentialProviderDict | None = None,
        client_options: ClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize a filesystem.

        Parameters
        ----------
        sentinel : str
            The terminal sentinel string to identify paths as URLs. See notes.
        block_size : int
            The block size to use for reads and writes.
        disk_cache_size : int
            The size of the disk cache to use.
        disk_cache_dir : str
            The directory to use for the disk cache.
        lru_capacity : int
            The capacity of the LRU cache.
        store_configs : dict[str, dict] | None
            Configuration options for different object store schemes. Each key
            is the scheme name, and the value is a typed dict of configuration
            options for that scheme. See the obstore documentation for details.
        credential_provider : dict[str, Callable] | None
            Credential providers for different object store schemes. Each key
            is the scheme name, and the value is a callback that returns
            credentials. See the obstore documentation for details.
        client_options : dict | None
            Configuration options for the HTTP client if supported.
        retry_config : dict | None
            Configuration options for the retry mechanism if supported.
        logger : logging.Logger | None
            The logger to use for logging.

        Notes
        -----
        For a given path, the kernel will traverse its ``/``-based hierarchy
        and call ``getattr`` on each component. If any component of the path is
        not detected by the filesystem, the entire lookup will fail. Therefore,
        for this filesystem to properly recognize a URI, each component of its
        path needs to be interpreted as a directory, but the full URI needs to
        be interpreted as a file. To accomplish this, the end of a qualified
        URI is signalled by the presence of a trailing *sentinel* string.
        """
        self.logger: logging.Logger
        if not logger:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.logger.info(f"Starting with disk_cache_size: {disk_cache_size}")

        self.sentinel: str = sentinel
        self.meta_cache: LRUCache[str, Any] = LRUCache(capacity=lru_capacity)
        self.mem_cache: LRUCache[str, Any] = LRUCache(capacity=lru_capacity)
        self.disk_cache: DiskCache = DiskCache(
            disk_cache_dir, size_limit=disk_cache_size
        )
        self.block_size: int = block_size
        self.cache_monitor: CacheMonitor = CacheMonitor()

        self.store_configs: StoreConfigDict | None = store_configs or {}
        self.credential_providers: CredentialProviderDict | None = (
            credential_providers or {}
        )
        self.client_options: ClientConfig | None = client_options
        self.retry_config: RetryConfig | None = retry_config

    def _load_cached_store(self, url: str) -> CachedStore:
        """
        Load a store from a URL.
        """
        if "://" not in url:
            raise FuseOSError(ENOENT)

        store = load_store(
            url,
            configs=self.store_configs,
            credential_providers=self.credential_providers,
            client_options=self.client_options,
            retry_config=self.retry_config,
        )

        return CachedStore(
            store,
            base_url=url,
            meta_cache=self.meta_cache,
            mem_cache=self.mem_cache,
            disk_cache=self.disk_cache,
            block_size=self.block_size,
            cache_monitor=self.cache_monitor,
        )

    def getattr(self, path: str, fh: Any = None) -> dict[str, Any]:
        """
        Return an attribute dictionary for the given path.

        If the sentinel string is missing, the path is interpreted as an
        empty directory.

        Parameters
        ----------
        path : str
            The URI, unix-normalized as a path. If the sentinel string is
            missing, the path is interpreted as an empty directory.
        fh : Any
            File handle (not used).

        Returns
        -------
        dict[str, Any]
            The attribute dictionary for the given path. It has keys identical
            to the stat C structure of stat(2).

        Notes
        -----
        ``st_atime``, ``st_mtime`` and ``st_ctime`` should be unix timestamps
        (floats).
        """
        url = path_to_url(path, self.sentinel)
        if url is None:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        store = self._load_cached_store(url)

        self.logger.info(f"getattr: HEAD: {url}")
        try:
            metadata = store.head("")
        except Exception as e:
            mapped_exc = map_exception(e)
            if isinstance(mapped_exc, obspec.exceptions.NotFoundError):
                return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

            self.logger.error(f"getattr: HEAD: {e}")
            if isinstance(
                mapped_exc,
                obspec.exceptions.PermissionDeniedError
                | obspec.exceptions.UnauthenticatedError,
            ):
                raise FuseOSError(EACCES) from e
            elif isinstance(
                mapped_exc,
                obspec.exceptions.InvalidPathError
                | obspec.exceptions.NotSupportedError,
            ):
                raise FuseOSError(ENOENT) from e
            else:
                raise FuseOSError(EIO) from e

        # Cached directories marked as None
        if metadata is None:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

        now = time()
        timestamp = (
            metadata["last_modified"].timestamp()
            if "last_modified" in metadata
            else now
        )
        return dict(
            st_mode=(S_IFREG | 0o644),
            st_nlink=1,
            st_size=metadata["size"],
            st_ctime=timestamp,
            st_mtime=timestamp,
            st_atime=now,
        )

    def readdir(self, path: str, fh: Any = None) -> list[str]:
        """
        Return a list of files in the directory.

        Parameters
        ----------
        path : str
            The URI, unix-normalized as a path. If the sentinel string is
            missing, the path is interpreted as an empty directory.
        fh : Any
            File handle (not used).

        Returns
        -------
        list[str]
            A list of contents of the "directory", including sentinel strings.
        """
        url = path_to_url(path, self.sentinel)
        if url is None:
            return [".", ".."]

        store = self._load_cached_store(url)

        self.logger.info(f"readdir: LIST_WITH_DELIMITER: {url}")
        try:
            result = store.list_with_delimiter()
        except Exception as e:
            self.logger.error(f"readdir: LIST_WITH_DELIMITER: {e}")
            mapped_exc = map_exception(e)
            if isinstance(mapped_exc, obspec.exceptions.NotFoundError):
                return [".", ".."]  # Return empty directory for not found
            elif isinstance(
                mapped_exc,
                obspec.exceptions.PermissionDeniedError
                | obspec.exceptions.UnauthenticatedError,
            ):
                raise FuseOSError(EACCES) from e
            else:
                raise FuseOSError(EIO) from e

        dirs = [(item + self.sentinel) for item in result["common_prefixes"]]
        # Mark directories as seen by caching None
        for dir in result["common_prefixes"]:
            cache_key = store._meta_cache_key(dir)
            if cache_key not in self.meta_cache:
                self.meta_cache[cache_key] = None

        files = [(item["path"] + self.sentinel) for item in result["objects"]]
        # Cache file metadata for all objects seen in listing
        for item in result["objects"]:
            cache_key = store._meta_cache_key(item["path"])
            if cache_key not in self.meta_cache:
                self.meta_cache[cache_key] = item

        return [".", "..", *dirs, *files]

    def read(self, path: str, size: int, offset: int, fh: Any = None) -> Buffer | bytes:
        """
        Return a byte string containing the data requested.

        Parameters
        ----------
        path : str
            The URI, unix-normalized as a path. If the sentinel string is
            missing, the path is interpreted as an empty directory.
        size : int
            The number of bytes to read.
        offset : int
            The offset to start reading from.
        fh : Any
            File handle (not used).

        Returns
        -------
        Buffer | bytes
            The requested data.
        """
        url = path_to_url(path, self.sentinel)
        if url is None:
            return b""

        store = self._load_cached_store(url)

        self.logger.debug(
            f"read: GET_RANGE: {url}\n"
            f"range: {offset} - {offset + size - 1}\n"
            f"request_size (KB): {size / 1024:.2f}\n"
        )
        try:
            return store.get_range(url, start=offset, length=size)
        except Exception as e:
            self.logger.error(f"read: GET_RANGE: {e}")
            mapped_exc = map_exception(e)
            if isinstance(mapped_exc, obspec.exceptions.NotFoundError):
                raise FuseOSError(ENOENT) from e
            elif isinstance(
                mapped_exc,
                obspec.exceptions.PermissionDeniedError
                | obspec.exceptions.UnauthenticatedError,
            ):
                raise FuseOSError(EACCES) from e
            else:
                raise FuseOSError(EIO) from e

    def link(self, target: str, source: str) -> None: ...

    def symlink(self, target: str, source: str) -> None: ...

    def unlink(self, path: str) -> None: ...

    def write(self, path: str, buf: bytes, size: int, offset: int, fip: Any) -> int:
        return 0

    def statfs(self, path: str) -> dict[str, int]:
        """
        Some fake facts about the filesystem.
        """
        fs_block_size = 128 * 1024
        return dict(
            f_frsize=fs_block_size,  # fundamental block size
            f_bsize=fs_block_size,  # preferred block size
            f_blocks=1024 * 1024,  # pretend total capacity (in blocks)
            f_bfree=512 * 1024,  # half free for root
            f_bavail=512 * 1024,  # same for non-root
            f_namemax=8192,  # maximum filename length (in chars)
        )

    def destroy(self, path: str) -> None:
        """
        Called on filesystem destruction. Path is always `/`.
        """
        self.disk_cache.close()
