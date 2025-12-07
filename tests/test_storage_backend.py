"""Unit tests covering storage backend registry and LocalStorage."""

from pathlib import Path

import pytest

from core.infrastructure.storage import backend as backend_mod
from core.infrastructure.storage.backend import (
    HealthCheckResult,
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


class TestHealthCheckResult:
    """Tests for HealthCheckResult dataclass."""

    def test_healthy_result(self) -> None:
        """Healthy result should have is_healthy=True."""
        result = HealthCheckResult(
            is_healthy=True,
            capabilities={"versioning": True},
            errors=[],
            latency_ms=42.5,
            checked_permissions={"read": True, "write": True},
        )
        assert result.is_healthy
        assert result.capabilities["versioning"]
        assert result.latency_ms == 42.5
        assert "HEALTHY" in str(result)

    def test_unhealthy_result(self) -> None:
        """Unhealthy result should have is_healthy=False and errors."""
        result = HealthCheckResult(
            is_healthy=False,
            errors=["Connection failed", "Permission denied"],
            checked_permissions={"read": False, "write": False},
        )
        assert not result.is_healthy
        assert len(result.errors) == 2
        assert "UNHEALTHY" in str(result)

    def test_to_dict_roundtrip(self) -> None:
        """to_dict and from_dict should preserve data."""
        original = HealthCheckResult(
            is_healthy=True,
            capabilities={"versioning": True, "multipart_upload": True},
            errors=[],
            latency_ms=123.4,
            checked_permissions={"read": True, "write": True, "list": True, "delete": True},
        )
        data = original.to_dict()
        restored = HealthCheckResult.from_dict(data)

        assert restored.is_healthy == original.is_healthy
        assert restored.capabilities == original.capabilities
        assert restored.errors == original.errors
        assert restored.latency_ms == original.latency_ms
        assert restored.checked_permissions == original.checked_permissions


class TestLocalStorageHealthCheck:
    """Tests for LocalStorage.health_check()."""

    def test_health_check_succeeds_on_writable_directory(self, tmp_path) -> None:
        """Health check should succeed on a writable temp directory."""
        config = {"bronze": {"local_root": str(tmp_path)}}
        storage = LocalStorage(config)

        result = storage.health_check()

        assert result.is_healthy
        assert result.checked_permissions["read"]
        assert result.checked_permissions["write"]
        assert result.checked_permissions["list"]
        assert result.checked_permissions["delete"]
        assert result.latency_ms is not None
        assert result.latency_ms >= 0
        assert len(result.errors) == 0

    def test_health_check_creates_base_directory_if_missing(self, tmp_path) -> None:
        """Health check should create base directory if it doesn't exist."""
        new_dir = tmp_path / "new_storage_dir"
        assert not new_dir.exists()

        config = {"bronze": {"local_root": str(new_dir)}}
        storage = LocalStorage(config)

        result = storage.health_check()

        assert result.is_healthy
        assert new_dir.exists()

    def test_health_check_cleans_up_test_file(self, tmp_path) -> None:
        """Health check should not leave test files behind."""
        config = {"bronze": {"local_root": str(tmp_path)}}
        storage = LocalStorage(config)

        # Get initial file count
        initial_files = list(tmp_path.iterdir())

        result = storage.health_check()

        # Should not have added any files
        assert result.is_healthy
        final_files = list(tmp_path.iterdir())
        assert len(final_files) == len(initial_files)


class TestBaseStorageBackendHealthCheck:
    """Tests for base StorageBackend.health_check()."""

    def test_base_health_check_returns_not_implemented(self) -> None:
        """Base StorageBackend.health_check() should return not implemented error."""
        backend = DummyBackend({})
        result = backend.health_check()

        assert not result.is_healthy
        assert "not implemented" in result.errors[0].lower()
