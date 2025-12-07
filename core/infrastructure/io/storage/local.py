"""Local filesystem storage backend."""

from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List

from .base import HealthCheckResult, StorageBackend

logger = logging.getLogger(__name__)


class LocalStorage(StorageBackend):
    """Simple filesystem backend used for tests and local samples."""

    def __init__(self, config: Dict[str, Any]) -> None:
        bronze_cfg = config.get("bronze", {})
        root = bronze_cfg.get("local_root")
        self.base_dir = (
            Path(root).expanduser().resolve() if root else Path(".").resolve()
        )

    def _resolve_path(self, remote_path: str) -> Path:
        candidate = Path(remote_path)
        if not candidate.is_absolute():
            candidate = self.base_dir / candidate
        return candidate.resolve()

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        dest = self._resolve_path(remote_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(local_path, dest)
        return True

    def download_file(self, remote_path: str, local_path: str) -> bool:
        src = self._resolve_path(remote_path)
        if not src.exists():
            raise FileNotFoundError(f"Local file {src} not found")
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, local_path)
        return True

    def list_files(self, prefix: str) -> List[str]:
        resolved = self._resolve_path(prefix)
        if resolved.is_file():
            return [str(resolved)]
        files: List[str] = []
        if resolved.exists():
            for path in resolved.rglob("*"):
                if path.is_file():
                    files.append(str(path))
        return files

    def delete_file(self, remote_path: str) -> bool:
        target = self._resolve_path(remote_path)
        if target.exists():
            if target.is_file():
                target.unlink()
                return True
            shutil.rmtree(target)
            return True
        return False

    def get_backend_type(self) -> str:
        return "local"

    def health_check(self) -> HealthCheckResult:
        """Verify local filesystem connectivity and permissions.

        Checks:
        - Base directory exists or can be created
        - Write permission (create test file)
        - Read permission (read test file)
        - List permission (list directory)
        - Delete permission (delete test file)

        Returns:
            HealthCheckResult with permission checks and any errors
        """
        errors: List[str] = []
        permissions: Dict[str, bool] = {
            "read": False,
            "write": False,
            "list": False,
            "delete": False,
        }
        capabilities: Dict[str, bool] = {
            "versioning": False,
            "multipart_upload": False,
        }

        start_time = time.monotonic()
        test_file = None

        try:
            # Check/create base directory
            if not self.base_dir.exists():
                try:
                    self.base_dir.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    errors.append(f"Cannot create base directory: {e}")
                    return HealthCheckResult(
                        is_healthy=False,
                        errors=errors,
                        capabilities=capabilities,
                        checked_permissions=permissions,
                    )

            # Create a unique test file name
            test_file = self.base_dir / f"_health_check_{os.getpid()}_{time.time_ns()}.tmp"

            # Test write permission
            try:
                test_file.write_text("health_check_test")
                permissions["write"] = True
            except OSError as e:
                errors.append(f"Write permission failed: {e}")

            # Test read permission
            if permissions["write"]:
                try:
                    content = test_file.read_text()
                    permissions["read"] = content == "health_check_test"
                    if not permissions["read"]:
                        errors.append("Read content mismatch")
                except OSError as e:
                    errors.append(f"Read permission failed: {e}")

            # Test list permission
            try:
                list(self.base_dir.iterdir())
                permissions["list"] = True
            except OSError as e:
                errors.append(f"List permission failed: {e}")

            # Test delete permission
            if test_file.exists():
                try:
                    test_file.unlink()
                    permissions["delete"] = True
                    test_file = None  # Mark as cleaned up
                except OSError as e:
                    errors.append(f"Delete permission failed: {e}")

        except Exception as e:
            errors.append(f"Unexpected error during health check: {e}")
        finally:
            # Clean up test file if it still exists
            if test_file is not None and test_file.exists():
                try:
                    test_file.unlink()
                except OSError:
                    pass

        latency_ms = (time.monotonic() - start_time) * 1000
        is_healthy = all(permissions.values()) and len(errors) == 0

        return HealthCheckResult(
            is_healthy=is_healthy,
            capabilities=capabilities,
            errors=errors,
            latency_ms=latency_ms,
            checked_permissions=permissions,
        )
