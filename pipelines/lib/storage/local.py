"""Local filesystem storage backend."""

from __future__ import annotations

import fnmatch
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from pipelines.lib.storage.base import FileInfo, StorageBackend, StorageResult

logger = logging.getLogger(__name__)

__all__ = ["LocalStorage"]


class LocalStorage(StorageBackend):
    """Local filesystem storage backend.

    Provides storage operations on the local filesystem.

    Example:
        >>> storage = LocalStorage("./data/bronze/")
        >>> storage.exists("2025-01-15/data.parquet")
        True
        >>> files = storage.list_files(pattern="*.parquet")
        >>> for f in files:
        ...     print(f.path, f.size)
    """

    @property
    def scheme(self) -> str:
        return "local"

    def _resolve_path(self, path: str) -> Path:
        """Resolve a path to an absolute Path object."""
        full_path = self.get_full_path(path)
        return Path(full_path).resolve()

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        resolved = self._resolve_path(path)

        # Handle glob patterns
        if "*" in str(resolved) or "?" in str(resolved):
            import glob
            return len(glob.glob(str(resolved))) > 0

        return resolved.exists()

    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path."""
        resolved = self._resolve_path(path)

        if not resolved.exists():
            return []

        files: List[FileInfo] = []

        if recursive:
            iterator = resolved.rglob("*")
        else:
            iterator = resolved.iterdir()

        for item in iterator:
            if item.is_file():
                # Apply pattern filter if specified
                if pattern and not fnmatch.fnmatch(item.name, pattern):
                    continue

                stat = item.stat()
                files.append(
                    FileInfo(
                        path=str(item.relative_to(resolved)),
                        size=stat.st_size,
                        modified=datetime.fromtimestamp(stat.st_mtime),
                    )
                )

        return sorted(files, key=lambda f: f.path)

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes."""
        resolved = self._resolve_path(path)
        return resolved.read_bytes()

    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file."""
        resolved = self._resolve_path(path)

        try:
            # Ensure parent directory exists
            resolved.parent.mkdir(parents=True, exist_ok=True)

            # Write data
            resolved.write_bytes(data)

            return StorageResult(
                success=True,
                path=str(resolved),
                files_written=[str(resolved)],
                bytes_written=len(data),
            )
        except Exception as e:
            logger.error("Failed to write %s: %s", resolved, e)
            return StorageResult(
                success=False,
                path=str(resolved),
                error=str(e),
            )

    def delete(self, path: str) -> bool:
        """Delete a file or directory."""
        resolved = self._resolve_path(path)

        if not resolved.exists():
            return False

        try:
            if resolved.is_dir():
                shutil.rmtree(resolved)
            else:
                resolved.unlink()
            return True
        except Exception as e:
            logger.error("Failed to delete %s: %s", resolved, e)
            return False

    def makedirs(self, path: str) -> None:
        """Create directories recursively."""
        resolved = self._resolve_path(path)
        resolved.mkdir(parents=True, exist_ok=True)

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file (uses shutil for efficiency)."""
        src_resolved = self._resolve_path(src)
        dst_resolved = self._resolve_path(dst)

        try:
            # Ensure parent directory exists
            dst_resolved.parent.mkdir(parents=True, exist_ok=True)

            # Copy file
            shutil.copy2(src_resolved, dst_resolved)
            size = dst_resolved.stat().st_size

            return StorageResult(
                success=True,
                path=str(dst_resolved),
                files_written=[str(dst_resolved)],
                bytes_written=size,
            )
        except Exception as e:
            logger.error("Failed to copy %s to %s: %s", src_resolved, dst_resolved, e)
            return StorageResult(
                success=False,
                path=str(dst_resolved),
                error=str(e),
            )

    def get_file_info(self, path: str) -> Optional[FileInfo]:
        """Get information about a specific file."""
        resolved = self._resolve_path(path)

        if not resolved.exists() or not resolved.is_file():
            return None

        stat = resolved.stat()
        return FileInfo(
            path=str(resolved),
            size=stat.st_size,
            modified=datetime.fromtimestamp(stat.st_mtime),
        )
