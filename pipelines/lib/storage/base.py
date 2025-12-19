"""Abstract base class for storage backends.

Defines the interface that all storage backends must implement.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

__all__ = ["StorageBackend", "StorageResult", "FileInfo"]


@dataclass
class FileInfo:
    """Information about a file in storage."""

    path: str
    size: int
    modified: Optional[datetime] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StorageResult:
    """Result of a storage operation."""

    success: bool
    path: str
    files_written: List[str] = field(default_factory=list)
    bytes_written: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "path": self.path,
            "files_written": self.files_written,
            "bytes_written": self.bytes_written,
            "error": self.error,
            "metadata": self.metadata,
        }


class StorageBackend(ABC):
    """Abstract base class for storage backends.

    Provides a unified interface for reading and writing data to different
    storage systems (local filesystem, S3, ADLS, etc.).

    Subclasses must implement all abstract methods.
    """

    def __init__(self, base_path: str, **options: Any) -> None:
        """Initialize the storage backend.

        Args:
            base_path: Base path for this storage backend
            **options: Backend-specific options
        """
        self.base_path = base_path
        self.options = options

    @property
    @abstractmethod
    def scheme(self) -> str:
        """Return the URI scheme for this backend (e.g., 'local', 's3', 'abfs')."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists.

        Args:
            path: Path to check (relative to base_path or absolute)

        Returns:
            True if path exists, False otherwise
        """
        pass

    @abstractmethod
    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path.

        Args:
            path: Path to list (relative to base_path)
            pattern: Optional glob pattern to filter files
            recursive: If True, list files recursively

        Returns:
            List of FileInfo objects
        """
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes.

        Args:
            path: Path to read (relative to base_path or absolute)

        Returns:
            File contents as bytes
        """
        pass

    @abstractmethod
    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file.

        Args:
            path: Path to write (relative to base_path or absolute)
            data: Bytes to write

        Returns:
            StorageResult with operation details
        """
        pass

    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete a file or directory.

        Args:
            path: Path to delete

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    def makedirs(self, path: str) -> None:
        """Create directories recursively.

        Args:
            path: Directory path to create
        """
        pass

    # Convenience methods (can be overridden for efficiency)

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read file contents as text.

        Args:
            path: Path to read
            encoding: Text encoding (default: utf-8)

        Returns:
            File contents as string
        """
        return self.read_bytes(path).decode(encoding)

    def write_text(
        self, path: str, data: str, encoding: str = "utf-8"
    ) -> StorageResult:
        """Write text to a file.

        Args:
            path: Path to write
            data: Text to write
            encoding: Text encoding (default: utf-8)

        Returns:
            StorageResult with operation details
        """
        return self.write_bytes(path, data.encode(encoding))

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file.

        Default implementation reads and writes. Subclasses may override
        for more efficient server-side copy.

        Args:
            src: Source path
            dst: Destination path

        Returns:
            StorageResult with operation details
        """
        data = self.read_bytes(src)
        return self.write_bytes(dst, data)

    def get_full_path(self, path: str) -> str:
        """Get the full path including base_path.

        Args:
            path: Relative path

        Returns:
            Full path with base_path prefix
        """
        if not path:
            return self.base_path

        # Handle absolute paths
        if path.startswith(("s3://", "abfss://", "wasbs://", "az://", "/")):
            return path

        # Join with base path
        base = self.base_path.rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(base_path={self.base_path!r})"
