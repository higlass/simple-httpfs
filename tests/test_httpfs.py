from datetime import datetime
from stat import S_IFDIR, S_IFREG
from unittest.mock import Mock, patch

import obstore
import pytest
from fuse import FuseOSError

from simple_httpfs._ftp import FTPStore
from simple_httpfs.httpfs import HttpFs, load_store, path_to_url


class TestHelperFunctions:
    """Test cases for helper functions."""

    def test_path_to_url_root_path(self):
        result = path_to_url("/", "...")
        assert result is None

    def test_path_to_url_no_sentinel(self):
        result = path_to_url("/some/path", "...")
        assert result is None

    def test_path_to_url_simple_http(self):
        result = path_to_url("/http:/example.com/file...", "...")
        assert result == "http://example.com/file"

    def test_path_to_url_nested_path(self):
        result = path_to_url("/http:/example.com.../data/file.txt...", "...")
        assert result == "http://example.com/data/file.txt"

    def test_path_to_url_s3_with_bucket(self):
        result = path_to_url("/s3:/my-bucket.../path/to/file.txt...", "...")
        assert result == "s3://my-bucket/path/to/file.txt"

    def test_path_to_url_removes_sentinels_from_parent_dirs(self):
        result = path_to_url(
            "/http:/example.com.../dir.../subdir.../file.txt...", "..."
        )
        assert result == "http://example.com/dir/subdir/file.txt"

    def test_load_store_http(self):
        store = load_store("http://example.com/file.txt")
        assert isinstance(store, obstore.store.HTTPStore)

    def test_load_store_https(self):
        store = load_store("https://example.com/file.txt")
        assert isinstance(store, obstore.store.HTTPStore)

    def test_load_store_ftp(self):
        store = load_store("ftp://example.com/file.txt")
        assert isinstance(store, FTPStore)

    def test_load_store_with_client_options(self):
        client_options = {"timeout": "30s"}  # obstore expects string format
        store = load_store("http://example.com/file.txt", client_options=client_options)
        assert isinstance(store, obstore.store.HTTPStore)

    @patch("obstore.store.from_url")
    def test_load_store_other_schemes(self, mock_from_url):
        mock_store = Mock()
        mock_from_url.return_value = mock_store

        store = load_store("s3://bucket/file.txt")
        assert store == mock_store
        mock_from_url.assert_called_once_with(
            "s3://bucket/file.txt",
            config={"skip_signature": True},
            client_options=None,
            retry_config=None,
            credential_provider=None,
        )


class TestHttpFs:
    """Test cases for HttpFs FUSE operations."""

    @pytest.fixture
    def memory_store(self):
        store = obstore.store.MemoryStore()

        # Add test files
        file1_data = b"Hello, World! This is test file content for HttpFs testing."
        file2_data = b"Another test file with different content for verification."

        store.put("file1.txt", file1_data)
        store.put("file2.txt", file2_data)
        store.put("subdir/nested.txt", b"Nested file content")

        return store

    @pytest.fixture
    def httpfs(self, tmp_path):
        return HttpFs(
            sentinel="...",
            block_size=64,  # Small block size for testing
            disk_cache_size=1024 * 1024,  # 1MB cache
            disk_cache_dir=str(tmp_path / "cache"),
            lru_capacity=10,
        )

    @patch("simple_httpfs.httpfs.load_store")
    def test_getattr_root_directory(self, mock_load_store, httpfs):
        # Root path should return directory attributes, store not used
        attrs = httpfs.getattr("/")

        assert attrs["st_mode"] == (S_IFDIR | 0o555)
        assert attrs["st_nlink"] == 2
        mock_load_store.assert_not_called()

    @patch("simple_httpfs.httpfs.load_store")
    def test_getattr_directory_without_sentinel(self, mock_load_store, httpfs):
        # Path without sentinel should return directory attrs, store not used
        attrs = httpfs.getattr("/http:/example.com/some/path")

        assert attrs["st_mode"] == (S_IFDIR | 0o555)
        assert attrs["st_nlink"] == 2
        mock_load_store.assert_not_called()

    def test_getattr_file_success(self, httpfs):
        # Mock store with file metadata
        mock_cached_store = Mock()
        mock_cached_store.head.return_value = {
            "e_tag": "abc123",
            "last_modified": datetime(2023, 1, 1, 12, 0, 0),
            "size": 59,
            "path": "file1.txt",
        }
        with patch.object(httpfs, "_load_cached_store", return_value=mock_cached_store):
            attrs = httpfs.getattr("/http:/file1.txt...")

        assert attrs["st_mode"] == (S_IFREG | 0o644)
        assert attrs["st_nlink"] == 1
        assert attrs["st_size"] == 59
        assert "st_atime" in attrs
        assert "st_mtime" in attrs
        assert "st_ctime" in attrs

    @patch("simple_httpfs.httpfs.load_store")
    def test_getattr_file_not_found(self, mock_load_store, httpfs):
        # Mock store that raises FileNotFoundError
        mock_cached_store = Mock()
        mock_cached_store.head.side_effect = FileNotFoundError()

        with patch.object(httpfs, "_load_cached_store", return_value=mock_cached_store):
            attrs = httpfs.getattr("/http:/nonexistent.txt...")

        # Should return directory attributes on file not found
        assert attrs["st_mode"] == (S_IFDIR | 0o555)
        assert attrs["st_nlink"] == 2

    @patch("simple_httpfs.httpfs.load_store")
    def test_readdir_root_directory(self, mock_load_store, httpfs):
        # Root directory should return minimal listing
        contents = httpfs.readdir("/")

        assert contents == [".", ".."]
        mock_load_store.assert_not_called()

    @patch("simple_httpfs.httpfs.load_store")
    def test_readdir_directory_without_sentinel(self, mock_load_store, httpfs):
        # Directory without sentinel should return minimal listing
        contents = httpfs.readdir("/http:/example.com/some/path")

        assert contents == [".", ".."]
        mock_load_store.assert_not_called()

    @patch("simple_httpfs.httpfs.load_store")
    def test_readdir_with_files_and_dirs(self, mock_load_store, httpfs):
        # Mock store with files and directories
        mock_cached_store = Mock()
        mock_cached_store.list_with_delimiter.return_value = {
            "common_prefixes": ["subdir/", "another_dir/"],
            "objects": [{"path": "file1.txt"}, {"path": "file2.txt"}],
        }

        with patch.object(httpfs, "_load_cached_store", return_value=mock_cached_store):
            contents = httpfs.readdir("/s3:/mybucket/...")

        expected = [
            ".",
            "..",
            "subdir/...",
            "another_dir/...",  # Directories with sentinel
            "file1.txt...",
            "file2.txt...",  # Files with sentinel
        ]
        assert contents == expected

    @patch("simple_httpfs.httpfs.load_store")
    def test_read_root_directory(self, mock_load_store, httpfs):
        # Reading root should return empty
        data = httpfs.read("/", 100, 0)

        assert data == b""
        mock_load_store.assert_not_called()

    @patch("simple_httpfs.httpfs.load_store")
    def test_read_directory_without_sentinel(self, mock_load_store, httpfs):
        # Reading directory without sentinel should return empty
        data = httpfs.read("/http:/example.com/some/path", 100, 0)

        assert data == b""
        mock_load_store.assert_not_called()

    @patch("simple_httpfs.httpfs.load_store")
    def test_read_file_success(self, mock_load_store, httpfs):
        # Mock cached store with file data
        mock_cached_store = Mock()
        test_data = b"Hello, World! This is test content."
        mock_cached_store.get_range.return_value = test_data[:20]  # First 20 bytes

        with patch.object(httpfs, "_load_cached_store", return_value=mock_cached_store):
            data = httpfs.read("/s3:/bucket/file1.txt...", 20, 0)

        assert data == test_data[:20]
        mock_cached_store.get_range.assert_called_once_with(
            "s3://bucket/file1.txt", start=0, length=20
        )

    @patch("simple_httpfs.httpfs.load_store")
    def test_read_file_with_offset(self, mock_load_store, httpfs):
        # Test reading with offset
        mock_cached_store = Mock()
        test_data = b"0123456789abcdefghij"
        mock_cached_store.get_range.return_value = test_data[5:15]  # Bytes 5-14

        with patch.object(httpfs, "_load_cached_store", return_value=mock_cached_store):
            data = httpfs.read("/http:/example.com/file1.txt...", 10, 5)

        assert data == test_data[5:15]
        mock_cached_store.get_range.assert_called_once_with(
            "http://example.com/file1.txt", start=5, length=10
        )

    def test_init_with_custom_parameters(self, tmp_path):
        cache_dir = str(tmp_path / "custom_cache")

        fs = HttpFs(
            sentinel="EOL",
            block_size=128,
            disk_cache_size=2 * 1024 * 1024,
            disk_cache_dir=cache_dir,
            lru_capacity=50,
            store_configs={"s3": {"region": "us-west-2"}},
            client_options={"timeout": "60s"},
            retry_config={"max_retries": 3},
        )

        assert fs.sentinel == "EOL"
        assert fs.block_size == 128
        assert fs.meta_cache.capacity == 50
        assert fs.mem_cache.capacity == 50
        assert fs.store_configs == {"s3": {"region": "us-west-2"}}
        assert fs.client_options == {"timeout": "60s"}
        assert fs.retry_config == {"max_retries": 3}

    def test_load_cached_store_creates_correct_store(self, httpfs):
        with patch("simple_httpfs.httpfs.load_store") as mock_load_store:
            mock_store = Mock()
            mock_load_store.return_value = mock_store

            result = httpfs._load_cached_store("http://example.com/file.txt")

            # Should create a CachedStore with correct parameters
            assert hasattr(result, "store")
            assert hasattr(result, "scheme")
            assert hasattr(result, "base_path")

            mock_load_store.assert_called_once_with(
                "http://example.com/file.txt",
                configs={},
                credential_providers={},
                client_options=None,
                retry_config=None,
            )

    def test_load_cached_store_invalid_url(self, httpfs):
        with pytest.raises(FuseOSError):
            httpfs._load_cached_store("invalid-url-without-scheme")

    def test_statfs_returns_fake_filesystem_stats(self, httpfs):
        stats = httpfs.statfs("/any/path")

        expected_block_size = 128 * 1024
        assert stats["f_frsize"] == expected_block_size
        assert stats["f_bsize"] == expected_block_size
        assert stats["f_blocks"] == 1024 * 1024
        assert stats["f_bfree"] == 512 * 1024
        assert stats["f_bavail"] == 512 * 1024
        assert stats["f_namemax"] == 8192

    def test_write_operations_return_zero(self, httpfs):
        # All write operations should return 0 (read-only filesystem)
        result = httpfs.write("/any/path", b"data", 4, 0, None)
        assert result == 0

    def test_unimplemented_operations_are_no_ops(self, httpfs):
        # These should not raise exceptions
        httpfs.link("target", "source")
        httpfs.symlink("target", "source")
        httpfs.unlink("/path")

    @patch("simple_httpfs.httpfs.load_store")
    def test_caching_behavior_with_same_url(self, mock_load_store, httpfs):
        # Test that repeated access to same URL uses caching effectively
        mock_store = Mock()
        mock_load_store.return_value = mock_store

        # Mock the cached store behavior
        mock_cached_store = Mock()
        mock_cached_store.head.return_value = {
            "path": "file.txt",
            "size": 100,
            "last_modified": datetime.now(),
            "e_tag": "test-etag",
        }

        with patch.object(
            httpfs, "_load_cached_store", return_value=mock_cached_store
        ) as mock_load_cached:
            # First call
            attrs1 = httpfs.getattr("/http:/example.com/file.txt...")
            # Second call to same URL
            attrs2 = httpfs.getattr("/http:/example.com/file.txt...")

            # Both should succeed
            assert attrs1["st_mode"] == (S_IFREG | 0o644)
            assert attrs2["st_mode"] == (S_IFREG | 0o644)

            # _load_cached_store should be called for each operation
            # (caching happens at the CachedStore level, not HttpFs level)
            assert mock_load_cached.call_count == 2

    def test_destroy_closes_disk_cache(self, httpfs):
        # Mock the disk cache to verify close is called
        with patch.object(httpfs.disk_cache, "close") as mock_close:
            httpfs.destroy("/")
            mock_close.assert_called_once()
