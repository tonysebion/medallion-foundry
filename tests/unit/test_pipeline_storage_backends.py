"""Tests for pipelines/lib/storage/ - storage backend abstraction."""

from pipelines.lib.storage import (
    get_storage,
    parse_uri,
    LocalStorage,
    S3Storage,
    ADLSStorage,
)
from pipelines.lib.storage.base import StorageResult, FileInfo


class TestParseUri:
    """Tests for URI parsing."""

    def test_local_path(self):
        """Local paths return 'local' scheme."""
        assert parse_uri("./data/bronze/") == ("local", "./data/bronze/")
        assert parse_uri("/abs/path/") == ("local", "/abs/path/")
        assert parse_uri("C:\\data\\bronze") == ("local", "C:\\data\\bronze")

    def test_s3_path(self):
        """S3 URIs return 's3' scheme."""
        assert parse_uri("s3://bucket/path/") == ("s3", "bucket/path/")
        assert parse_uri("s3://my-bucket/bronze/data/") == (
            "s3",
            "my-bucket/bronze/data/",
        )

    def test_adls_abfss_path(self):
        """ADLS abfss:// URIs return 'abfs' scheme."""
        result = parse_uri("abfss://container@account.dfs.core.windows.net/bronze/")
        assert result == ("abfs", "container@account.dfs.core.windows.net/bronze/")

    def test_adls_wasbs_path(self):
        """ADLS wasbs:// URIs return 'abfs' scheme."""
        result = parse_uri("wasbs://container@account.blob.core.windows.net/bronze/")
        assert result == ("abfs", "container@account.blob.core.windows.net/bronze/")

    def test_adls_az_path(self):
        """ADLS az:// URIs return 'abfs' scheme."""
        result = parse_uri("az://container/bronze/")
        assert result == ("abfs", "container/bronze/")


class TestGetStorage:
    """Tests for get_storage factory function."""

    def test_local_storage(self, tmp_path):
        """Local paths return LocalStorage."""
        storage = get_storage(str(tmp_path))
        assert isinstance(storage, LocalStorage)
        assert storage.scheme == "local"

    def test_s3_storage(self):
        """S3 URIs return S3Storage."""
        storage = get_storage("s3://my-bucket/bronze/")
        assert isinstance(storage, S3Storage)
        assert storage.scheme == "s3"

    def test_adls_storage(self):
        """ADLS URIs return ADLSStorage."""
        storage = get_storage("abfss://container@account.dfs.core.windows.net/bronze/")
        assert isinstance(storage, ADLSStorage)
        assert storage.scheme == "abfs"


class TestLocalStorage:
    """Tests for LocalStorage backend."""

    def test_exists_file(self, tmp_path):
        """exists() returns True for existing files."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        storage = LocalStorage(str(tmp_path))
        assert storage.exists("test.txt") is True

    def test_exists_missing(self, tmp_path):
        """exists() returns False for missing files."""
        storage = LocalStorage(str(tmp_path))
        assert storage.exists("nonexistent.txt") is False

    def test_exists_glob_pattern(self, tmp_path):
        """exists() handles glob patterns."""
        (tmp_path / "data1.parquet").write_bytes(b"data")
        (tmp_path / "data2.parquet").write_bytes(b"data")

        storage = LocalStorage(str(tmp_path))
        assert storage.exists("*.parquet") is True
        assert storage.exists("*.csv") is False

    def test_list_files(self, tmp_path):
        """list_files() returns file info."""
        (tmp_path / "file1.txt").write_text("a")
        (tmp_path / "file2.txt").write_text("bb")

        storage = LocalStorage(str(tmp_path))
        files = storage.list_files()

        assert len(files) == 2
        assert all(isinstance(f, FileInfo) for f in files)
        paths = [f.path for f in files]
        assert "file1.txt" in paths
        assert "file2.txt" in paths

    def test_list_files_with_pattern(self, tmp_path):
        """list_files() filters by pattern."""
        (tmp_path / "data.parquet").write_bytes(b"data")
        (tmp_path / "other.txt").write_text("text")

        storage = LocalStorage(str(tmp_path))
        files = storage.list_files(pattern="*.parquet")

        assert len(files) == 1
        assert files[0].path == "data.parquet"

    def test_list_files_recursive(self, tmp_path):
        """list_files() with recursive=True lists nested files."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "root.txt").write_text("root")
        (subdir / "nested.txt").write_text("nested")

        storage = LocalStorage(str(tmp_path))
        files = storage.list_files(recursive=True)

        paths = [f.path for f in files]
        assert any("root.txt" in p for p in paths)
        assert any("nested.txt" in p for p in paths)

    def test_read_bytes(self, tmp_path):
        """read_bytes() returns file contents."""
        test_file = tmp_path / "test.bin"
        test_file.write_bytes(b"\x00\x01\x02\x03")

        storage = LocalStorage(str(tmp_path))
        data = storage.read_bytes("test.bin")

        assert data == b"\x00\x01\x02\x03"

    def test_read_text(self, tmp_path):
        """read_text() returns decoded text."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")

        storage = LocalStorage(str(tmp_path))
        text = storage.read_text("test.txt")

        assert text == "hello world"

    def test_write_bytes(self, tmp_path):
        """write_bytes() creates file with content."""
        storage = LocalStorage(str(tmp_path))
        result = storage.write_bytes("output.bin", b"\x00\x01\x02")

        assert result.success is True
        assert result.bytes_written == 3
        assert (tmp_path / "output.bin").read_bytes() == b"\x00\x01\x02"

    def test_write_bytes_creates_directories(self, tmp_path):
        """write_bytes() creates parent directories."""
        storage = LocalStorage(str(tmp_path))
        result = storage.write_bytes("nested/dir/output.bin", b"data")

        assert result.success is True
        assert (tmp_path / "nested" / "dir" / "output.bin").exists()

    def test_write_text(self, tmp_path):
        """write_text() creates file with text."""
        storage = LocalStorage(str(tmp_path))
        result = storage.write_text("output.txt", "hello")

        assert result.success is True
        assert (tmp_path / "output.txt").read_text() == "hello"

    def test_delete_file(self, tmp_path):
        """delete() removes file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("delete me")

        storage = LocalStorage(str(tmp_path))
        result = storage.delete("test.txt")

        assert result is True
        assert not test_file.exists()

    def test_delete_directory(self, tmp_path):
        """delete() removes directory recursively."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file.txt").write_text("nested")

        storage = LocalStorage(str(tmp_path))
        result = storage.delete("subdir")

        assert result is True
        assert not subdir.exists()

    def test_delete_missing_returns_false(self, tmp_path):
        """delete() returns False for missing files."""
        storage = LocalStorage(str(tmp_path))
        result = storage.delete("nonexistent.txt")

        assert result is False

    def test_makedirs(self, tmp_path):
        """makedirs() creates directories."""
        storage = LocalStorage(str(tmp_path))
        storage.makedirs("a/b/c")

        assert (tmp_path / "a" / "b" / "c").is_dir()

    def test_copy(self, tmp_path):
        """copy() copies file."""
        (tmp_path / "source.txt").write_text("content")

        storage = LocalStorage(str(tmp_path))
        result = storage.copy("source.txt", "dest.txt")

        assert result.success is True
        assert (tmp_path / "dest.txt").read_text() == "content"

    def test_get_full_path(self, tmp_path):
        """get_full_path() combines base and relative paths."""
        storage = LocalStorage(str(tmp_path))
        full = storage.get_full_path("subdir/file.txt")

        assert str(tmp_path) in full
        assert "subdir/file.txt" in full

    def test_get_file_info(self, tmp_path):
        """get_file_info() returns file metadata."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        storage = LocalStorage(str(tmp_path))
        info = storage.get_file_info("test.txt")

        assert info is not None
        assert info.size == 7
        assert info.modified is not None


class TestStorageResult:
    """Tests for StorageResult dataclass."""

    def test_to_dict(self):
        """to_dict() returns all fields."""
        result = StorageResult(
            success=True,
            path="/path/to/file",
            files_written=["file1.txt", "file2.txt"],
            bytes_written=100,
            metadata={"key": "value"},
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["path"] == "/path/to/file"
        assert d["files_written"] == ["file1.txt", "file2.txt"]
        assert d["bytes_written"] == 100
        assert d["metadata"] == {"key": "value"}

    def test_error_result(self):
        """Error results capture error message."""
        result = StorageResult(
            success=False,
            path="/path/to/file",
            error="Permission denied",
        )

        assert result.success is False
        assert result.error == "Permission denied"


class TestS3StorageInit:
    """Tests for S3Storage initialization (without actual S3 connection)."""

    def test_parses_bucket_and_prefix(self):
        """S3Storage parses bucket and prefix from URI."""
        storage = S3Storage("s3://my-bucket/bronze/data/")

        assert storage._bucket == "my-bucket"
        assert storage._prefix == "bronze/data/"

    def test_bucket_only(self):
        """S3Storage handles bucket-only URI."""
        storage = S3Storage("s3://my-bucket")

        assert storage._bucket == "my-bucket"
        assert storage._prefix == ""


class TestADLSStorageInit:
    """Tests for ADLSStorage initialization (without actual ADLS connection)."""

    def test_parses_abfss_uri(self):
        """ADLSStorage parses abfss:// URI."""
        storage = ADLSStorage("abfss://container@account.dfs.core.windows.net/bronze/")

        assert storage._container == "container"
        assert storage._account == "account"
        assert storage._prefix == "bronze/"

    def test_parses_wasbs_uri(self):
        """ADLSStorage parses wasbs:// URI."""
        storage = ADLSStorage("wasbs://container@account.blob.core.windows.net/bronze/")

        assert storage._container == "container"
        assert storage._account == "account"
        assert storage._prefix == "bronze/"

    def test_container_only(self):
        """ADLSStorage handles container-only path."""
        storage = ADLSStorage("abfss://container@account.dfs.core.windows.net")

        assert storage._container == "container"
        assert storage._account == "account"
        assert storage._prefix == ""
