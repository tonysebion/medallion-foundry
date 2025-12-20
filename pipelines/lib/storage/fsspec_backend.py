"""Universal fsspec-based storage backend.

Provides a single storage backend that works with any fsspec-compatible
filesystem: local, S3, Azure, GCS, HDFS, SFTP, and 40+ others.

This is the preferred backend for new integrations as it provides a
unified interface without custom implementations per cloud provider.
"""

from __future__ import annotations

import fnmatch
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import fsspec
from fsspec.spec import AbstractFileSystem

from pipelines.lib.storage.base import FileInfo, StorageBackend, StorageResult

logger = logging.getLogger(__name__)

__all__ = ["FsspecStorage", "get_fsspec_filesystem"]


def get_fsspec_filesystem(path: str, **storage_options: Any) -> AbstractFileSystem:
    """Get an fsspec filesystem for the given path.

    This is a convenience function that creates the appropriate filesystem
    based on the path protocol.

    Args:
        path: Storage path with protocol prefix (s3://, gs://, abfs://, etc.)
        **storage_options: Protocol-specific options (credentials, etc.)

    Returns:
        Configured fsspec filesystem instance

    Example:
        >>> fs = get_fsspec_filesystem("s3://my-bucket/data/")
        >>> fs.ls("s3://my-bucket/data/")

        >>> fs = get_fsspec_filesystem(
        ...     "gs://my-bucket/data/",
        ...     token="/path/to/credentials.json"
        ... )
    """
    # Detect protocol from path
    if "://" in path:
        protocol = path.split("://")[0]
    else:
        protocol = "file"

    return fsspec.filesystem(protocol, **storage_options)


class FsspecStorage(StorageBackend):
    """Universal storage backend using fsspec.

    Works with any fsspec-compatible filesystem:
    - Local: file://
    - AWS S3: s3://
    - Google Cloud Storage: gs://
    - Azure Blob: az://, abfs://, abfss://
    - HDFS: hdfs://
    - SFTP: sftp://
    - HTTP/HTTPS: http://, https://
    - And 40+ more...

    This backend leverages fsspec's unified API to provide consistent
    file operations across all supported storage systems.

    Example:
        >>> # Local filesystem
        >>> storage = FsspecStorage("./data/bronze/")
        >>> storage.write_text("test.txt", "Hello World")

        >>> # S3 with explicit credentials
        >>> storage = FsspecStorage(
        ...     "s3://my-bucket/bronze/",
        ...     key="AKIAIOSFODNN7EXAMPLE",
        ...     secret="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ... )

        >>> # GCS with service account
        >>> storage = FsspecStorage(
        ...     "gs://my-bucket/bronze/",
        ...     token="/path/to/service-account.json",
        ... )

        >>> # Azure with connection string
        >>> storage = FsspecStorage(
        ...     "az://container/bronze/",
        ...     connection_string="DefaultEndpointsProtocol=https;...",
        ... )

    Environment Variables:
        S3:
            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

        GCS:
            GOOGLE_APPLICATION_CREDENTIALS

        Azure:
            AZURE_STORAGE_CONNECTION_STRING or
            AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY
    """

    def __init__(self, base_path: str, **options: Any) -> None:
        """Initialize the fsspec storage backend.

        Args:
            base_path: Base path with protocol (e.g., s3://bucket/prefix/)
            **options: Filesystem-specific options passed to fsspec
        """
        super().__init__(base_path, **options)
        self._fs: Optional[AbstractFileSystem] = None
        self._protocol = self._detect_protocol()

    def _detect_protocol(self) -> str:
        """Detect the protocol from the base path."""
        if "://" in self.base_path:
            return self.base_path.split("://")[0]
        return "file"

    @property
    def scheme(self) -> str:
        """Return the URI scheme for this backend."""
        return self._protocol

    @property
    def fs(self) -> AbstractFileSystem:
        """Lazy-load the filesystem."""
        if self._fs is None:
            self._fs = fsspec.filesystem(self._protocol, **self.options)
        return self._fs

    def _normalize_path(self, path: str) -> str:
        """Normalize a path, handling both relative and absolute paths.

        Args:
            path: Path to normalize (relative or absolute)

        Returns:
            Full path ready for fsspec operations
        """
        if not path:
            return self.base_path.rstrip("/")

        # Already absolute with protocol
        if "://" in path:
            return path

        # For file protocol, handle relative vs absolute
        if self._protocol == "file":
            # If path starts with /, it's already absolute
            if path.startswith("/"):
                return path
            # Otherwise, join with base_path
            base = self.base_path.rstrip("/")
            return f"{base}/{path.lstrip('/')}"

        # For cloud protocols, join with base_path
        base = self.base_path.rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        full_path = self._normalize_path(path)

        # Handle glob patterns
        if "*" in full_path or "?" in full_path:
            try:
                matches = self.fs.glob(full_path)
                return len(matches) > 0
            except Exception:
                return False

        try:
            result: bool = self.fs.exists(full_path)
            return result
        except Exception as e:
            logger.debug("Error checking existence of %s: %s", full_path, e)
            return False

    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path."""
        full_path = self._normalize_path(path)

        try:
            if recursive:
                # Use find for recursive listing
                items = self.fs.find(full_path, detail=True)
            else:
                # Use ls for non-recursive listing
                items = self.fs.ls(full_path, detail=True)

            files: List[FileInfo] = []

            # Handle both dict and list responses
            file_items: List[tuple[str, Dict[str, Any]]] = []
            if isinstance(items, dict):
                file_items = list(items.items())
            else:
                # Convert list to (name, info) pairs
                for item in items:
                    if isinstance(item, dict):
                        item_name: str = item.get("name", item.get("Key", ""))
                        file_items.append((item_name, item))
                    else:
                        file_items.append((str(item), {"name": str(item), "type": "file", "size": 0}))

            for name, info in file_items:
                # Skip directories
                if info.get("type") == "directory":
                    continue

                # Apply pattern filter
                basename = name.split("/")[-1]
                if pattern and not fnmatch.fnmatch(basename, pattern):
                    continue

                # Extract file info
                size = info.get("size", info.get("Size", 0))
                modified = info.get("mtime", info.get("LastModified"))

                # Convert mtime to datetime if it's a timestamp
                if isinstance(modified, (int, float)):
                    modified = datetime.fromtimestamp(modified)

                files.append(
                    FileInfo(
                        path=name,
                        size=size,
                        modified=modified,
                    )
                )

            return sorted(files, key=lambda f: f.path)

        except Exception as e:
            logger.warning("Error listing %s: %s", full_path, e)
            return []

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes."""
        full_path = self._normalize_path(path)
        with self.fs.open(full_path, "rb") as f:
            result: bytes = f.read()
            return result

    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file."""
        full_path = self._normalize_path(path)

        try:
            # Ensure parent directory exists for local filesystem
            if self._protocol == "file":
                self.fs.makedirs(self.fs._parent(full_path), exist_ok=True)

            with self.fs.open(full_path, "wb") as f:
                f.write(data)

            return StorageResult(
                success=True,
                path=full_path,
                files_written=[full_path],
                bytes_written=len(data),
            )
        except Exception as e:
            logger.error("Failed to write %s: %s", full_path, e)
            return StorageResult(
                success=False,
                path=full_path,
                error=str(e),
            )

    def delete(self, path: str) -> bool:
        """Delete a file or directory."""
        full_path = self._normalize_path(path)

        try:
            if self.fs.isdir(full_path):
                self.fs.rm(full_path, recursive=True)
            else:
                self.fs.rm(full_path)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error("Failed to delete %s: %s", full_path, e)
            return False

    def makedirs(self, path: str) -> None:
        """Create directories recursively."""
        full_path = self._normalize_path(path)

        # Some filesystems (like S3) don't have real directories
        if self._protocol in ("s3", "gs", "az", "abfs", "abfss"):
            return

        try:
            self.fs.makedirs(full_path, exist_ok=True)
        except Exception as e:
            logger.warning("Failed to create directory %s: %s", full_path, e)

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file (uses server-side copy when available)."""
        src_path = self._normalize_path(src)
        dst_path = self._normalize_path(dst)

        try:
            self.fs.copy(src_path, dst_path)
            info = self.fs.info(dst_path)
            size = info.get("size", info.get("Size", 0))

            return StorageResult(
                success=True,
                path=dst_path,
                files_written=[dst_path],
                bytes_written=size,
            )
        except Exception as e:
            logger.error("Failed to copy %s to %s: %s", src_path, dst_path, e)
            return StorageResult(
                success=False,
                path=dst_path,
                error=str(e),
            )

    def get_cache_path(self, path: str) -> str:
        """Get a local cached version of a remote file.

        Uses fsspec's caching layer to download remote files locally
        for efficient repeated access.

        Args:
            path: Path to the file

        Returns:
            Local path to cached file
        """
        full_path = self._normalize_path(path)

        # For local files, just return the path
        if self._protocol == "file":
            return full_path

        # Use fsspec's simplecache for remote files
        cached_fs = fsspec.filesystem(
            "simplecache",
            target_protocol=self._protocol,
            target_options=self.options,
        )

        # This triggers the download and returns the local path
        with cached_fs.open(full_path, "rb") as f:
            name: str = f.name
            return name

    def glob(self, pattern: str) -> List[str]:
        """Find files matching a glob pattern.

        Args:
            pattern: Glob pattern (e.g., "*.parquet", "**/*.csv")

        Returns:
            List of matching file paths
        """
        full_pattern = self._normalize_path(pattern)
        try:
            result: List[str] = self.fs.glob(full_pattern)
            return result
        except Exception as e:
            logger.warning("Error globbing %s: %s", full_pattern, e)
            return []

    def open(self, path: str, mode: str = "rb", **kwargs: Any):
        """Open a file and return a file-like object.

        Args:
            path: Path to the file
            mode: File mode ('rb', 'wb', 'r', 'w', etc.)
            **kwargs: Additional arguments passed to fsspec.open

        Returns:
            File-like object
        """
        full_path = self._normalize_path(path)
        return self.fs.open(full_path, mode, **kwargs)

    def info(self, path: str) -> Dict[str, Any]:
        """Get file information.

        Args:
            path: Path to the file

        Returns:
            Dictionary with file metadata (size, mtime, type, etc.)
        """
        full_path = self._normalize_path(path)
        result: Dict[str, Any] = self.fs.info(full_path)
        return result
