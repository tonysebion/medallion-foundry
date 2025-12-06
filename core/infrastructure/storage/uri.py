"""Storage URI parsing and resolution for multi-backend storage support.

This module provides utilities for parsing storage URIs that can reference
local filesystem, S3, Azure Blob Storage, and other backends.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Optional

if TYPE_CHECKING:
    from core.infrastructure.config.environment import EnvironmentConfig

StorageBackendType = Literal["local", "s3", "azure"]


@dataclass
class StorageURI:
    """Parsed storage URI with backend type and path components.

    Attributes:
        backend: Storage backend type ("local", "s3", "azure")
        bucket: Bucket/container name for cloud storage (None for local)
        key: Object key for cloud storage or file path for local
        original: Original unparsed URI string
    """

    backend: StorageBackendType
    bucket: Optional[str]
    key: str
    original: str

    @staticmethod
    def parse(uri: str) -> "StorageURI":
        """Parse a storage URI into components.

        Supported formats:
        - S3: s3://bucket/path/to/file
        - Azure: azblob://container/path/to/file
        - Local absolute: /path/to/file
        - Local relative: ./path/to/file or path/to/file

        Args:
            uri: URI string to parse

        Returns:
            StorageURI instance with parsed components

        Example:
            >>> StorageURI.parse("s3://my-bucket/data/file.csv")
            StorageURI(backend='s3', bucket='my-bucket', key='data/file.csv', ...)

            >>> StorageURI.parse("./local/file.csv")
            StorageURI(backend='local', bucket=None, key='./local/file.csv', ...)
        """
        uri = uri.strip()

        # S3 URI: s3://bucket/key
        s3_match = re.match(r"^s3://([^/]+)/(.+)$", uri)
        if s3_match:
            return StorageURI(
                backend="s3",
                bucket=s3_match.group(1),
                key=s3_match.group(2),
                original=uri,
            )

        # S3 URI without trailing path (just bucket)
        s3_bucket_match = re.match(r"^s3://([^/]+)/?$", uri)
        if s3_bucket_match:
            return StorageURI(
                backend="s3", bucket=s3_bucket_match.group(1), key="", original=uri
            )

        # Azure URI: azblob://container/path
        azure_match = re.match(r"^azblob://([^/]+)/(.+)$", uri)
        if azure_match:
            return StorageURI(
                backend="azure",
                bucket=azure_match.group(1),
                key=azure_match.group(2),
                original=uri,
            )

        # Azure URI without trailing path
        azure_container_match = re.match(r"^azblob://([^/]+)/?$", uri)
        if azure_container_match:
            return StorageURI(
                backend="azure",
                bucket=azure_container_match.group(1),
                key="",
                original=uri,
            )

        # Local filesystem (default for anything without a scheme)
        return StorageURI(backend="local", bucket=None, key=uri, original=uri)

    def to_fsspec_path(self, env_config: Optional[EnvironmentConfig] = None) -> str:
        """Convert to fsspec-compatible path.

        For cloud storage, resolves bucket names from environment config if available.

        Args:
            env_config: Optional environment configuration for bucket resolution

        Returns:
            fsspec-compatible path string

        Example:
            >>> uri = StorageURI.parse("s3://my-bucket/data/file.csv")
            >>> uri.to_fsspec_path()
            's3://my-bucket/data/file.csv'

            >>> # With environment config bucket resolution
            >>> env = EnvironmentConfig(name="dev", s3=S3ConnectionConfig(buckets={"data": "real-bucket"}))
            >>> uri = StorageURI.parse("s3://data/file.csv")
            >>> uri.to_fsspec_path(env)
            's3://real-bucket/file.csv'
        """
        if self.backend == "s3":
            # Resolve bucket name from environment config if available
            bucket = self.bucket
            if env_config and env_config.s3 and bucket:
                bucket = env_config.s3.get_bucket(bucket)

            if self.key:
                return f"s3://{bucket}/{self.key}"
            else:
                return f"s3://{bucket}"

        elif self.backend == "azure":
            # Azure blob storage
            if self.key:
                return f"az://{self.bucket}/{self.key}"
            else:
                return f"az://{self.bucket}"

        else:
            # Local filesystem
            return self.key

    def is_remote(self) -> bool:
        """Check if this is a remote (cloud) storage URI.

        Returns:
            True if backend is S3 or Azure, False for local filesystem
        """
        return self.backend in ("s3", "azure")

    def __str__(self) -> str:
        """String representation of the URI."""
        return self.original

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return f"StorageURI(backend='{self.backend}', bucket={self.bucket!r}, key={self.key!r})"
