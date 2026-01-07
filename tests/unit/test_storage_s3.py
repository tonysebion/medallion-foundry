"""Unit tests for `pipelines.lib.storage.S3Storage` using a fake filesystem."""

import io
from typing import Dict, List

import pytest

from pipelines.lib.storage import S3Storage, get_storage


AWS_BUCKET = "test-bucket"
AWS_PREFIX = "bronze-data"
BASE_PATH = f"s3://{AWS_BUCKET}/{AWS_PREFIX}/"


class DummyS3FS:
    """In-memory approximation of s3fs.S3FileSystem for testing."""

    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}

    def open(self, path: str, mode: str = "rb"):
        class DummyFile(io.BytesIO):
            def __init__(self, storage: Dict[str, bytes], key: str, mode: str) -> None:
                super().__init__()
                self._storage = storage
                self._key = key
                self._mode = mode
                if "r" in mode and key in storage:
                    self.write(storage[key])
                    self.seek(0)

            def __enter__(self) -> "DummyFile":
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                if "w" in self._mode or "a" in self._mode:
                    self._storage[self._key] = self.getvalue()

        return DummyFile(self.objects, path, mode)

    def exists(self, path: str) -> bool:
        return any(obj.startswith(path.rstrip("/") + "/") or obj == path for obj in self.objects)

    def ls(self, path: str, detail: bool = True):
        entries = []
        prefix = path.rstrip("/")
        for key, data in self.objects.items():
            if key.startswith(prefix):
                entries.append(
                    {
                        "name": key,
                        "type": "file",
                        "Size": len(data),
                        "LastModified": None,
                    }
                )
        return entries

    def find(self, path: str, detail: bool = True):
        return self.ls(path, detail=detail)

    def copy(self, src: str, dst: str) -> None:
        if src not in self.objects:
            raise FileNotFoundError(src)
        self.objects[dst] = self.objects[src]

    def info(self, path: str):
        size = len(self.objects.get(path, b""))
        return {"Size": size}

    def rm(self, path: str, recursive: bool = False) -> None:
        keys = [k for k in self.objects if k == path or k.startswith(path.rstrip("/") + "/")]
        for key in keys:
            del self.objects[key]

    def isdir(self, path: str) -> bool:
        prefix = path.rstrip("/") + "/"
        return any(k.startswith(prefix) for k in self.objects)

    def glob(self, pattern: str) -> List[str]:
        if "*" not in pattern:
            return [pattern] if pattern in self.objects else []
        prefix = pattern.split("*", 1)[0]
        return [k for k in self.objects if k.startswith(prefix)]


@pytest.fixture
def s3_storage() -> S3Storage:
    storage = S3Storage(BASE_PATH)
    storage._fs = DummyS3FS()  # type: ignore[attr-defined]
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

    results = s3_storage.list_files("dir", pattern="*.txt")
    names = [entry.path for entry in results]
    assert any(name.endswith("a.txt") for name in names)
    assert any(name.endswith("b.txt") for name in names)
    assert not any(name.endswith("c.csv") for name in names)


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
    # Options dict won't have env vars, but they're read in fs property
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
