"""Unit tests covering storage backend registry and LocalStorage."""

from pathlib import Path

import pytest

from core.infrastructure.storage import backend as backend_mod
from core.infrastructure.storage.backend import (
    StorageBackend,
    _STORAGE_BACKEND_CACHE,
    BACKEND_REGISTRY,
    register_backend,
    resolve_backend_type,
    get_storage_backend,
)
from core.infrastructure.storage.local import LocalStorage


class DummyBackend(StorageBackend):
    def __init__(self, config):
        self.config = config

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        return True

    def download_file(self, remote_path: str, local_path: str) -> bool:
        return True

    def list_files(self, prefix: str) -> list[str]:
        return []

    def delete_file(self, remote_path: str) -> bool:
        return True

    def get_backend_type(self) -> str:
        return "dummy"


def test_backend_registration_and_cache(monkeypatch):
    original_registry = BACKEND_REGISTRY.copy()
    original_cache = _STORAGE_BACKEND_CACHE.copy()
    try:
        @register_backend("dummy-test")
        def dummy_factory(config):
            return DummyBackend(config)

        assert "dummy-test" in backend_mod.list_backends()
        cfg = {"bronze": {"storage_backend": "dummy-test"}}
        assert resolve_backend_type(cfg) == "dummy-test"

        instance_a = get_storage_backend(cfg)
        instance_b = get_storage_backend(cfg)
        assert instance_a is instance_b

        instance_c = get_storage_backend(cfg, use_cache=False)
        assert instance_c is not instance_a
    finally:
        BACKEND_REGISTRY.clear()
        BACKEND_REGISTRY.update(original_registry)
        _STORAGE_BACKEND_CACHE.clear()
        _STORAGE_BACKEND_CACHE.update(original_cache)


def test_local_storage_file_operations(tmp_path):
    config = {"bronze": {"local_root": str(tmp_path)}}
    storage = LocalStorage(config)

    source = tmp_path / "source.txt"
    source.write_text("payload")
    assert storage.upload_file(str(source), "dest/hello.txt")

    target = tmp_path / "download.txt"
    storage.download_file("dest/hello.txt", str(target))
    assert target.read_text() == "payload"

    listed = storage.list_files("dest")
    assert any("hello.txt" in Path(path).name for path in listed)

    assert storage.delete_file("dest/hello.txt")
    assert not (tmp_path / "dest" / "hello.txt").exists()
