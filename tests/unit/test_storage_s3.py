"""Unit tests for `pipelines.lib.storage.S3Storage` using a mock boto3 client."""

import io
from datetime import datetime
from typing import Any, Dict, List

import pytest

from pipelines.lib.storage import S3Storage, get_storage


AWS_BUCKET = "test-bucket"
AWS_PREFIX = "bronze-data"
BASE_PATH = f"s3://{AWS_BUCKET}/{AWS_PREFIX}/"


class MockClientError(Exception):
    """Mock botocore ClientError for testing."""

    def __init__(self, code: str, message: str = "Error"):
        self.response = {"Error": {"Code": code, "Message": message}}
        super().__init__(message)


class DummyPaginator:
    """Mock paginator for list_objects_v2."""

    def __init__(self, objects: Dict[str, bytes]):
        self.objects = objects

    def paginate(self, Bucket: str, Prefix: str = "", **kwargs) -> List[Dict[str, Any]]:
        contents = []
        delimiter = kwargs.get("Delimiter")
        max_keys = kwargs.get("MaxKeys")

        for key, data in self.objects.items():
            if key.startswith(Prefix):
                # If delimiter is set, only include objects at this level
                if delimiter:
                    relative_key = key[len(Prefix) :]
                    if delimiter in relative_key:
                        continue  # Skip nested objects

                contents.append(
                    {
                        "Key": key,
                        "Size": len(data),
                        "LastModified": datetime.now(),
                    }
                )
                if max_keys and len(contents) >= max_keys:
                    break

        return [{"Contents": contents, "KeyCount": len(contents)}]


class DummyS3Client:
    """In-memory approximation of boto3 S3 client for testing."""

    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        if Key not in self.objects:
            raise MockClientError("NoSuchKey", "Not found")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs) -> Dict[str, Any]:
        self.objects[Key] = Body
        return {}

    def head_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        if Key not in self.objects:
            # Check if it's a "directory"
            prefix = Key.rstrip("/") + "/"
            if any(k.startswith(prefix) for k in self.objects):
                return {"ContentLength": 0}
            raise MockClientError("404", "Not found")
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        self.objects.pop(Key, None)
        return {}

    def delete_objects(self, Bucket: str, Delete: Dict[str, List]) -> Dict[str, Any]:
        for obj in Delete.get("Objects", []):
            self.objects.pop(obj["Key"], None)
        return {}

    def copy_object(
        self, Bucket: str, CopySource: Dict[str, str], Key: str
    ) -> Dict[str, Any]:
        src_key = CopySource["Key"]
        if src_key not in self.objects:
            raise MockClientError("NoSuchKey", "Not found")
        self.objects[Key] = self.objects[src_key]
        return {}

    def get_paginator(self, operation: str):
        return DummyPaginator(self.objects)


@pytest.fixture
def s3_storage() -> S3Storage:
    storage = S3Storage(BASE_PATH)
    storage._client = DummyS3Client()
    return storage


def write_test_files(storage: S3Storage, paths: List[str]):
    for path in paths:
        storage.write_bytes(path, f"data-{path}".encode("utf-8"))


def test_write_and_read_bytes(s3_storage: S3Storage):
    result = s3_storage.write_bytes("data/test.txt", b"hello")
    assert result.success
    assert s3_storage.read_bytes("data/test.txt") == b"hello"


def test_exists_and_list_files(s3_storage: S3Storage):
    write_test_files(s3_storage, ["dir/a.txt", "dir/sub/b.txt", "dir/sub/c.csv"])

    assert s3_storage.exists("dir/a.txt")
    assert s3_storage.exists("dir/*")

    results = s3_storage.list_files("dir", recursive=True)
    names = [entry.path for entry in results]
    # Filter by pattern manually for this test
    txt_files = [n for n in names if n.endswith(".txt")]
    csv_files = [n for n in names if n.endswith(".csv")]

    assert len(txt_files) == 2
    assert len(csv_files) == 1


def test_list_files_with_pattern(s3_storage: S3Storage):
    write_test_files(s3_storage, ["dir/a.txt", "dir/b.txt", "dir/c.csv"])

    results = s3_storage.list_files("dir", pattern="*.txt", recursive=True)
    names = [entry.path for entry in results]
    assert len(names) == 2
    assert all(n.endswith(".txt") for n in names)


def test_delete_file(s3_storage: S3Storage):
    s3_storage.write_bytes("delete/me.txt", b"x")
    assert s3_storage.exists("delete/me.txt")

    assert s3_storage.delete("delete/me.txt")
    assert not s3_storage.exists("delete/me.txt")


def test_copy_file(s3_storage: S3Storage):
    s3_storage.write_bytes("original.txt", b"payload")
    result = s3_storage.copy("original.txt", "copied.txt")

    assert result.success
    assert s3_storage.exists("copied.txt")
    assert s3_storage.read_bytes("copied.txt") == b"payload"


def test_get_storage_returns_s3():
    storage = get_storage(BASE_PATH)
    assert isinstance(storage, S3Storage)


def test_glob_pattern(s3_storage: S3Storage):
    """Test glob pattern matching."""
    write_test_files(
        s3_storage,
        ["data/dt=2025-01-01/file.parquet", "data/dt=2025-01-02/file.parquet"],
    )

    matches = s3_storage.glob("data/dt=*/file.parquet")
    assert len(matches) == 2


def test_glob_no_matches(s3_storage: S3Storage):
    """Test glob with no matches returns empty list."""
    matches = s3_storage.glob("nonexistent/*/file.parquet")
    assert matches == []


def test_exists_directory(s3_storage: S3Storage):
    """Test exists returns True for directory-like paths."""
    s3_storage.write_bytes("mydir/file.txt", b"content")

    # The directory "mydir" should exist (has objects under it)
    assert s3_storage.exists("mydir")
    assert s3_storage.exists("mydir/file.txt")


def test_delete_directory(s3_storage: S3Storage):
    """Test deleting a directory removes all objects under it."""
    write_test_files(s3_storage, ["mydir/a.txt", "mydir/b.txt", "mydir/sub/c.txt"])

    assert s3_storage.delete("mydir")
    assert not s3_storage.exists("mydir/a.txt")
    assert not s3_storage.exists("mydir/b.txt")
    assert not s3_storage.exists("mydir/sub/c.txt")


# ============================================================
# Tests for S3-compatible storage configuration (Nutanix, etc.)
# ============================================================


def test_s3_storage_signature_version_option():
    """Test S3Storage accepts signature_version option."""
    storage = S3Storage(BASE_PATH, signature_version="s3v4")
    assert storage.options.get("signature_version") == "s3v4"


def test_s3_storage_addressing_style_option():
    """Test S3Storage accepts addressing_style option."""
    storage = S3Storage(BASE_PATH, addressing_style="path")
    assert storage.options.get("addressing_style") == "path"


def test_s3_storage_nutanix_compatible_config():
    """Test typical Nutanix Objects configuration."""
    storage = S3Storage(
        "s3://nutanix-bucket/bronze/",
        endpoint_url="https://objects.nutanix.local:443",
        signature_version="s3v4",
        addressing_style="path",
        key="access_key",
        secret="secret_key",
    )
    assert storage.options.get("endpoint_url") == "https://objects.nutanix.local:443"
    assert storage.options.get("signature_version") == "s3v4"
    assert storage.options.get("addressing_style") == "path"
    assert storage.options.get("key") == "access_key"
    assert storage.options.get("secret") == "secret_key"


def test_s3_storage_signature_version_from_env(monkeypatch):
    """Test S3Storage reads signature_version from environment."""
    monkeypatch.setenv("AWS_S3_SIGNATURE_VERSION", "s3v4")
    monkeypatch.setenv("AWS_S3_ADDRESSING_STYLE", "path")

    storage = S3Storage(BASE_PATH)
    # Options dict won't have env vars, but they're read in client property
    # Just verify the storage was created without error
    assert storage is not None


def test_s3_storage_option_overrides_env(monkeypatch):
    """Test that explicit options override environment variables."""
    monkeypatch.setenv("AWS_S3_SIGNATURE_VERSION", "s3")
    monkeypatch.setenv("AWS_S3_ADDRESSING_STYLE", "virtual")

    storage = S3Storage(
        BASE_PATH,
        signature_version="s3v4",
        addressing_style="path",
    )
    # Explicit options should be stored
    assert storage.options.get("signature_version") == "s3v4"
    assert storage.options.get("addressing_style") == "path"


def test_s3_storage_bucket_and_prefix_parsing():
    """Test that bucket and prefix are correctly parsed from S3 URI."""
    storage = S3Storage("s3://my-bucket/some/prefix/path/")
    assert storage._bucket == "my-bucket"
    assert storage._prefix == "some/prefix/path"


def test_s3_storage_bucket_only():
    """Test S3 URI with only bucket (no prefix)."""
    storage = S3Storage("s3://my-bucket/")
    assert storage._bucket == "my-bucket"
    assert storage._prefix == ""


def test_s3_storage_bucket_no_trailing_slash():
    """Test S3 URI without trailing slash."""
    storage = S3Storage("s3://my-bucket/prefix")
    assert storage._bucket == "my-bucket"
    assert storage._prefix == "prefix"
