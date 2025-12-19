"""AWS S3 storage backend."""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import Any, List, Optional

from pipelines.lib.storage.base import FileInfo, StorageBackend, StorageResult

logger = logging.getLogger(__name__)

__all__ = ["S3Storage"]


class S3Storage(StorageBackend):
    """AWS S3 storage backend.

    Provides storage operations on AWS S3 using fsspec/s3fs.

    Example:
        >>> storage = S3Storage("s3://my-bucket/bronze/")
        >>> storage.exists("2025-01-15/data.parquet")
        True
        >>> files = storage.list_files(pattern="*.parquet")
        >>> for f in files:
        ...     print(f.path, f.size)

    Environment Variables:
        AWS_ACCESS_KEY_ID: AWS access key
        AWS_SECRET_ACCESS_KEY: AWS secret key
        AWS_REGION: AWS region (default: us-east-1)
        AWS_ENDPOINT_URL: Custom S3 endpoint (for MinIO, LocalStack, etc.)

    Options:
        key: AWS access key (overrides env var)
        secret: AWS secret key (overrides env var)
        region: AWS region (overrides env var)
        endpoint_url: Custom S3 endpoint
        anon: If True, use anonymous access (for public buckets)
    """

    def __init__(self, base_path: str, **options: Any) -> None:
        super().__init__(base_path, **options)
        self._fs = None
        self._bucket = None
        self._prefix = None
        self._parse_path()

    def _parse_path(self) -> None:
        """Parse S3 URI into bucket and prefix."""
        path = self.base_path
        if path.startswith("s3://"):
            path = path[5:]

        parts = path.split("/", 1)
        self._bucket = parts[0]
        self._prefix = parts[1] if len(parts) > 1 else ""

    @property
    def scheme(self) -> str:
        return "s3"

    @property
    def fs(self):
        """Lazy-load the S3 filesystem."""
        if self._fs is None:
            try:
                import s3fs
            except ImportError:
                raise ImportError(
                    "s3fs is required for S3 storage. "
                    "Install with: pip install s3fs"
                )

            # Build options from environment and passed options
            fs_options = {}

            # Credentials
            key = self.options.get("key") or os.environ.get("AWS_ACCESS_KEY_ID")
            secret = self.options.get("secret") or os.environ.get("AWS_SECRET_ACCESS_KEY")
            if key and secret:
                fs_options["key"] = key
                fs_options["secret"] = secret

            # Region
            region = self.options.get("region") or os.environ.get("AWS_REGION")
            if region:
                fs_options["client_kwargs"] = {"region_name": region}

            # Custom endpoint (MinIO, LocalStack, etc.)
            endpoint_url = self.options.get("endpoint_url") or os.environ.get("AWS_ENDPOINT_URL")
            if endpoint_url:
                fs_options.setdefault("client_kwargs", {})
                fs_options["client_kwargs"]["endpoint_url"] = endpoint_url

            # Anonymous access
            if self.options.get("anon"):
                fs_options["anon"] = True

            self._fs = s3fs.S3FileSystem(**fs_options)

        return self._fs

    def _get_s3_path(self, path: str) -> str:
        """Get the full S3 path (bucket/prefix/path)."""
        if path.startswith("s3://"):
            return path[5:]

        if not path:
            return f"{self._bucket}/{self._prefix}".rstrip("/")

        prefix = self._prefix.rstrip("/") if self._prefix else ""
        if prefix:
            return f"{self._bucket}/{prefix}/{path.lstrip('/')}"
        else:
            return f"{self._bucket}/{path.lstrip('/')}"

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        s3_path = self._get_s3_path(path)

        # Handle glob patterns
        if "*" in s3_path or "?" in s3_path:
            try:
                matches = self.fs.glob(s3_path)
                return len(matches) > 0
            except Exception:
                return False

        try:
            return self.fs.exists(s3_path)
        except Exception as e:
            logger.warning("Error checking existence of s3://%s: %s", s3_path, e)
            return False

    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path."""
        s3_path = self._get_s3_path(path)

        try:
            if recursive:
                items = self.fs.find(s3_path, detail=True)
            else:
                items = self.fs.ls(s3_path, detail=True)
                # Filter to files only
                items = {k: v for k, v in items.items() if v.get("type") == "file"} if isinstance(items, dict) else items

            files: List[FileInfo] = []

            # Handle both dict and list responses
            if isinstance(items, dict):
                for key, info in items.items():
                    if info.get("type") == "file":
                        # Apply pattern filter
                        name = key.split("/")[-1]
                        if pattern and not fnmatch.fnmatch(name, pattern):
                            continue

                        files.append(
                            FileInfo(
                                path=key,
                                size=info.get("Size", info.get("size", 0)),
                                modified=info.get("LastModified"),
                            )
                        )
            else:
                for info in items:
                    if isinstance(info, str):
                        # Simple path list
                        name = info.split("/")[-1]
                        if pattern and not fnmatch.fnmatch(name, pattern):
                            continue
                        files.append(FileInfo(path=info, size=0))
                    elif isinstance(info, dict) and info.get("type") == "file":
                        name = info.get("name", info.get("Key", "")).split("/")[-1]
                        if pattern and not fnmatch.fnmatch(name, pattern):
                            continue
                        files.append(
                            FileInfo(
                                path=info.get("name", info.get("Key", "")),
                                size=info.get("Size", info.get("size", 0)),
                                modified=info.get("LastModified"),
                            )
                        )

            return sorted(files, key=lambda f: f.path)

        except Exception as e:
            logger.warning("Error listing s3://%s: %s", s3_path, e)
            return []

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes."""
        s3_path = self._get_s3_path(path)
        with self.fs.open(s3_path, "rb") as f:
            return f.read()

    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file."""
        s3_path = self._get_s3_path(path)

        try:
            with self.fs.open(s3_path, "wb") as f:
                f.write(data)

            return StorageResult(
                success=True,
                path=f"s3://{s3_path}",
                files_written=[f"s3://{s3_path}"],
                bytes_written=len(data),
            )
        except Exception as e:
            logger.error("Failed to write s3://%s: %s", s3_path, e)
            return StorageResult(
                success=False,
                path=f"s3://{s3_path}",
                error=str(e),
            )

    def delete(self, path: str) -> bool:
        """Delete a file or directory."""
        s3_path = self._get_s3_path(path)

        try:
            if self.fs.isdir(s3_path):
                self.fs.rm(s3_path, recursive=True)
            else:
                self.fs.rm(s3_path)
            return True
        except Exception as e:
            logger.error("Failed to delete s3://%s: %s", s3_path, e)
            return False

    def makedirs(self, path: str) -> None:
        """Create directories (no-op for S3 as directories don't exist)."""
        # S3 doesn't have real directories - they're implied by object keys
        pass

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file (uses server-side copy for efficiency)."""
        src_path = self._get_s3_path(src)
        dst_path = self._get_s3_path(dst)

        try:
            self.fs.copy(src_path, dst_path)
            info = self.fs.info(dst_path)
            size = info.get("Size", info.get("size", 0))

            return StorageResult(
                success=True,
                path=f"s3://{dst_path}",
                files_written=[f"s3://{dst_path}"],
                bytes_written=size,
            )
        except Exception as e:
            logger.error("Failed to copy s3://%s to s3://%s: %s", src_path, dst_path, e)
            return StorageResult(
                success=False,
                path=f"s3://{dst_path}",
                error=str(e),
            )
