import ftplib
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from simple_httpfs._ftp import (
    FTPStore,
    _modify_time_to_datetime,
    _resolve_path,
    _resolve_search_dir,
)


class MockFTP:
    """Mock FTP server for testing."""

    def __init__(self, host):
        self.host = host
        self.files = {
            "/test/file.txt": b"Hello, World! This is a test file content.",
            "/test/large.txt": b"A" * 10000,  # 10KB file
            "/test/empty.txt": b"",
            "/test/subdir/foo.txt": b"foo content",
            "/test/subdir/bar.txt": b"bar content",
        }
        # Mock MLSD responses for different directories
        self.directories = {
            "/test": [
                (
                    "file.txt",
                    {"size": "42", "modify": "20231201123000", "type": "file"},
                ),
                (
                    "large.txt",
                    {"size": "10000", "modify": "20231202143000", "type": "file"},
                ),
                (
                    "empty.txt",
                    {"size": "0", "modify": "20231203153000", "type": "file"},
                ),
                ("subdir", {"type": "dir"}),
            ],
            "/test/subdir": [
                ("foo.txt", {"size": "11", "modify": "20231204123000", "type": "file"}),
                ("bar.txt", {"size": "11", "modify": "20231205123000", "type": "file"}),
            ],
        }

    def login(self):
        pass

    def voidcmd(self, cmd):
        if cmd == "TYPE I":
            return "200 Type set to I"
        return "200 OK"

    def size(self, path):
        if path in self.files:
            return len(self.files[path])
        raise ftplib.error_perm("550 File not found")

    def sendcmd(self, cmd):
        if cmd.startswith("MDTM "):
            path = cmd[5:]
            if path in self.files:
                return "213 20231201123000"
            raise ftplib.error_perm("550 File not found")
        return "200 OK"

    def transfercmd(self, cmd, rest=0):
        if cmd.startswith("RETR "):
            path = cmd[5:]
            if path in self.files:
                data = self.files[path][rest:]
                mock_conn = Mock()
                mock_conn.recv = Mock(side_effect=self._create_recv_func(data))
                return mock_conn
            raise ftplib.error_perm("550 File not found")
        raise ftplib.error_perm("502 Command not implemented")

    def _create_recv_func(self, data):
        chunks = [data[i : i + 1024] for i in range(0, len(data), 1024)]
        chunks.append(b"")  # EOF
        return lambda size: chunks.pop(0) if chunks else b""

    def mlsd(self, path):
        # Normalize path for lookup (handle both /test and /test/)
        normalized_path = path.rstrip("/")
        if normalized_path in self.directories:
            return self.directories[normalized_path]
        # Also try with trailing slash
        if path in self.directories:
            return self.directories[path]
        raise ftplib.error_perm("550 Directory not found")

    def close(self):
        pass


@pytest.fixture
def mock_ftp():
    with patch("simple_httpfs._ftp.FTP") as mock_ftp_class:
        mock_instance = MockFTP("ftp.example.com")
        mock_ftp_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def ftp_store():
    return FTPStore("ftp://ftp.example.com/test/")


class TestFTPStore:
    """Test cases for FTPStore implementation."""

    def test_init(self):
        store = FTPStore("ftp://ftp.example.com/path/to/files/")
        assert store.host == "ftp.example.com"
        assert store.path == "/path/to/files/"
        assert store.chunk_size == 32 * 1024

    def test_init_custom_chunk_size(self):
        store = FTPStore("ftp://ftp.example.com/", chunk_size=8192)
        assert store.chunk_size == 8192

    def test_head_success(self, ftp_store, mock_ftp):
        metadata = ftp_store.head("file.txt")

        assert metadata["path"] == "/test/file.txt"
        assert metadata["size"] == 42
        assert isinstance(metadata["last_modified"], datetime)
        assert metadata["e_tag"] is None
        assert metadata["version"] is None

    def test_head_file_not_found(self, ftp_store, mock_ftp):
        mock_ftp.size = Mock(side_effect=ftplib.error_perm("550 File not found"))

        with pytest.raises(FileNotFoundError):
            ftp_store.head("nonexistent.txt")

    def test_get_range_full_file(self, ftp_store, mock_ftp):
        data = ftp_store.get_range("file.txt", start=0, length=42)
        assert data == b"Hello, World! This is a test file content."

    def test_get_range_partial(self, ftp_store, mock_ftp):
        data = ftp_store.get_range("file.txt", start=7, length=5)
        assert data == b"World"

    def test_get_range_with_end(self, ftp_store, mock_ftp):
        data = ftp_store.get_range("file.txt", start=0, end=12)
        assert data == b"Hello, World"

    def test_get_range_beyond_file_size(self, ftp_store, mock_ftp):
        # When start > file size, should return empty data
        mock_ftp.size = Mock(return_value=10)
        data = ftp_store.get_range("file.txt", start=20, length=10)
        assert data == b""

    def test_list_with_delimiter_no_prefix(self, ftp_store, mock_ftp):
        """Test listing root directory without prefix."""
        result = ftp_store.list_with_delimiter(None)

        assert "common_prefixes" in result
        assert "objects" in result

        # Should have one directory (subdir/)
        assert "subdir/" in result["common_prefixes"]

        # Should have three files
        file_names = [obj["path"] for obj in result["objects"]]
        assert "file.txt" in file_names
        assert "large.txt" in file_names
        assert "empty.txt" in file_names

        # Check file metadata
        file_obj = next(obj for obj in result["objects"] if obj["path"] == "file.txt")
        assert file_obj["size"] == 42
        assert isinstance(file_obj["last_modified"], datetime)

    def test_list_with_delimiter_with_prefix(self, ftp_store, mock_ftp):
        """Test listing with prefix filter."""
        result = ftp_store.list_with_delimiter("sub")

        # Should filter to items starting with "sub"
        assert len(result["common_prefixes"]) == 1
        assert "subdir/" in result["common_prefixes"]
        assert len(result["objects"]) == 0  # No files start with "sub"

    def test_list_with_delimiter_subdir(self, ftp_store, mock_ftp):
        """Test listing subdirectory."""
        result = ftp_store.list_with_delimiter("subdir/")

        # Should list contents of subdir
        assert len(result["common_prefixes"]) == 0  # No subdirectories in subdir
        assert len(result["objects"]) == 2  # Two files in subdir

        file_names = [obj["path"] for obj in result["objects"]]
        assert "subdir/foo.txt" in file_names
        assert "subdir/bar.txt" in file_names

    def test_list_with_delimiter_mlsd_not_supported(self, ftp_store, mock_ftp):
        """Test fallback when MLSD is not supported."""
        mock_ftp.mlsd = Mock(side_effect=ftplib.error_perm("500 MLSD not supported"))

        result = ftp_store.list_with_delimiter(None)

        # Should return empty results when MLSD fails
        assert result["common_prefixes"] == []
        assert result["objects"] == []

    def test_list_with_delimiter_timestamp_parsing(self, ftp_store, mock_ftp):
        """Test parsing of MLSD timestamp with fractional seconds."""
        mock_ftp.directories["/test"] = [
            (
                "test.txt",
                {"size": "100", "modify": "20231201123000.123", "type": "file"},
            )
        ]

        result = ftp_store.list_with_delimiter(None)
        file_obj = result["objects"][0]

        # Should parse correctly, ignoring fractional seconds
        expected_time = datetime(2023, 12, 1, 12, 30, 0)
        assert file_obj["last_modified"] == expected_time

    def test_list_with_delimiter_no_modify_time(self, ftp_store, mock_ftp):
        """Test when modify time is not available."""
        mock_ftp.directories["/test"] = [
            ("test.txt", {"size": "100", "type": "file"})  # No modify field
        ]

        with patch("simple_httpfs._ftp.datetime") as mock_datetime:
            mock_now = datetime(2023, 12, 1, 15, 0, 0)
            mock_datetime.now.return_value = mock_now
            mock_datetime.strptime = datetime.strptime

            result = ftp_store.list_with_delimiter(None)
            file_obj = result["objects"][0]
            assert file_obj["last_modified"] == mock_now

    def test_list_with_delimiter_skips_dot_entries(self, ftp_store, mock_ftp):
        """Test that . and .. entries are filtered out."""
        mock_ftp.directories["/test"] = [
            (".", {"type": "dir"}),
            ("..", {"type": "dir"}),
            ("file.txt", {"size": "42", "modify": "20231201123000", "type": "file"}),
        ]

        result = ftp_store.list_with_delimiter(None)

        # Should only have the actual file, not . or ..
        assert len(result["objects"]) == 1
        assert result["objects"][0]["path"] == "file.txt"
        assert len(result["common_prefixes"]) == 0


class TestHelperFunctions:
    """Test the helper functions."""

    def test_resolve_search_dir_no_prefix(self):
        result = _resolve_search_dir("/base/path", None)
        assert result == "/base/path"

    def test_resolve_search_dir_with_directory_prefix(self):
        result = _resolve_search_dir("/base/path", "subdir/")
        assert result == "/base/path/subdir"

    def test_resolve_search_dir_with_file_prefix(self):
        result = _resolve_search_dir("/base/path", "file.txt")
        assert result == "/base/path"

    def test_resolve_search_dir_with_nested_file_prefix(self):
        result = _resolve_search_dir("/base/path", "subdir/file.txt")
        assert result == "/base/path/subdir"

    def test_resolve_path_no_prefix(self):
        result = _resolve_path("file.txt", None)
        assert result == "file.txt"

    def test_resolve_path_with_directory_prefix(self):
        result = _resolve_path("file.txt", "subdir/")
        assert result == "subdir/file.txt"

        result = _resolve_path("file.txt", "sub/dir/")
        assert result == "sub/dir/file.txt"

    def test_resolve_path_with_incomplete_prefix(self):
        result = _resolve_path("file.txt", "fi")
        assert result == "file.txt"

        result = _resolve_path("file.txt", "subdir/fi")
        assert result == "subdir/file.txt"

        result = _resolve_path("file.txt", "sub/dir/file.txt")
        assert result == "sub/dir/file.txt"

    def test_resolve_path_with_bad_prefix(self):
        result = _resolve_path("file.txt", "bad")
        assert result is None

        result = _resolve_path("file.txt", "sub/dir/bad")
        assert result is None

    def test_modify_time_to_datetime_basic(self):
        result = _modify_time_to_datetime("20231201123000")
        expected = datetime(2023, 12, 1, 12, 30, 0)
        assert result == expected

    def test_modify_time_to_datetime_with_fractional_seconds(self):
        result = _modify_time_to_datetime("20231201123000.123")
        expected = datetime(2023, 12, 1, 12, 30, 0)
        assert result == expected

    def test_path_joining(self, mock_ftp):
        # Test various URL path scenarios
        store1 = FTPStore("ftp://ftp.example.com/")
        assert store1.path == "/"

        store2 = FTPStore("ftp://ftp.example.com/data/files/")
        assert store2.path == "/data/files/"

        store3 = FTPStore("ftp://ftp.example.com/data/files")
        assert store3.path == "/data/files"
