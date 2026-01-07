"""AWS S3 storage backend using direct boto3 calls."""

from __future__ import annotations

import fnmatch
import logging
import os
from typing import Any, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pipelines.lib.storage.base import FileInfo, StorageBackend, StorageResult

logger = logging.getLogger(__name__)

__all__ = ["S3Storage"]


class S3Storage(StorageBackend):
    """AWS S3 storage backend using direct boto3 calls.

    Provides storage operations on AWS S3.

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
        AWS_S3_SIGNATURE_VERSION: S3 signature version ('s3v4', 's3', or unset for auto)
        AWS_S3_ADDRESSING_STYLE: S3 addressing style ('path', 'virtual', or 'auto')

    Options:
        key: AWS access key (overrides env var)
        secret: AWS secret key (overrides env var)
        region: AWS region (overrides env var)
        endpoint_url: Custom S3 endpoint
        signature_version: S3 signature version ('s3v4' for v4, 's3' for v2)
        addressing_style: URL addressing style ('path' or 'virtual')
        anon: If True, use anonymous access (for public buckets)

    S3-Compatible Storage (Nutanix Objects, MinIO, etc.):
        For S3-compatible storage, you typically need:
        - endpoint_url: Your storage endpoint
        - signature_version: 's3v4' (most common)
        - addressing_style: 'path' (required for most S3-compatible storage)

        Example:
            >>> storage = S3Storage(
            ...     "s3://my-bucket/data/",
            ...     endpoint_url="https://objects.nutanix.local:443",
            ...     signature_version="s3v4",
            ...     addressing_style="path",
            ... )
    """

    def __init__(self, base_path: str, **options: Any) -> None:
        super().__init__(base_path, **options)
        self._client = None
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
    def client(self):
        """Lazy-load the boto3 S3 client."""
        if self._client is None:
            # Build client configuration
            client_kwargs = {}

            # Credentials
            key = self.options.get("key") or os.environ.get("AWS_ACCESS_KEY_ID")
            secret = self.options.get("secret") or os.environ.get("AWS_SECRET_ACCESS_KEY")
            if key and secret:
                client_kwargs["aws_access_key_id"] = key
                client_kwargs["aws_secret_access_key"] = secret

            # Region
            region = self.options.get("region") or os.environ.get("AWS_REGION", "us-east-1")
            client_kwargs["region_name"] = region

            # Custom endpoint (MinIO, LocalStack, Nutanix Objects, etc.)
            endpoint_url = self.options.get("endpoint_url") or os.environ.get("AWS_ENDPOINT_URL")
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url

            # Build botocore Config for signature version and addressing style
            config_kwargs = {}

            signature_version = (
                self.options.get("signature_version")
                or os.environ.get("AWS_S3_SIGNATURE_VERSION")
            )
            if signature_version:
                config_kwargs["signature_version"] = signature_version

            addressing_style = (
                self.options.get("addressing_style")
                or os.environ.get("AWS_S3_ADDRESSING_STYLE")
            )
            if addressing_style:
                config_kwargs["s3"] = {"addressing_style": addressing_style}

            if config_kwargs:
                client_kwargs["config"] = Config(**config_kwargs)

            self._client = boto3.client("s3", **client_kwargs)

        return self._client

    def _get_s3_key(self, path: str) -> str:
        """Get the S3 key (path within bucket) from a relative or absolute path."""
        if path.startswith("s3://"):
            # Extract key from full S3 URI
            uri_path = path[5:]
            parts = uri_path.split("/", 1)
            return parts[1] if len(parts) > 1 else ""

        if not path:
            return self._prefix.rstrip("/")

        prefix = self._prefix.rstrip("/")
        if prefix:
            return f"{prefix}/{path.lstrip('/')}"
        else:
            return path.lstrip("/")

    def _glob_to_prefix_and_pattern(self, glob_pattern: str) -> tuple:
        """Convert glob pattern to S3 prefix and fnmatch pattern.

        Example: 'bronze/system=retail/dt=*/data.parquet'
        Returns: ('bronze/system=retail/dt=', '*/data.parquet')
        """
        # Find the first glob character
        for i, char in enumerate(glob_pattern):
            if char in "*?[":
                # Split at the last / before the glob
                prefix_end = glob_pattern.rfind("/", 0, i)
                if prefix_end == -1:
                    return "", glob_pattern
                return glob_pattern[: prefix_end + 1], glob_pattern[prefix_end + 1 :]
        # No glob characters - return as prefix
        return glob_pattern, ""

    def glob(self, pattern: str) -> List[str]:
        """Match files using glob pattern.

        Args:
            pattern: Glob pattern (e.g., 's3://bucket/prefix/*/data.parquet')

        Returns:
            List of matching S3 keys (without bucket prefix)
        """
        s3_pattern = self._get_s3_key(pattern)
        prefix, fnmatch_pattern = self._glob_to_prefix_and_pattern(s3_pattern)

        matches = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                # If no pattern, everything under prefix matches
                if not fnmatch_pattern:
                    matches.append(key)
                # Apply fnmatch to the part after the prefix
                elif fnmatch.fnmatch(key[len(prefix) :], fnmatch_pattern):
                    matches.append(key)

        return matches

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        s3_key = self._get_s3_key(path)

        # Handle glob patterns
        if "*" in s3_key or "?" in s3_key:
            try:
                matches = self.glob(path)
                return len(matches) > 0
            except Exception:
                return False

        try:
            self.client.head_object(Bucket=self._bucket, Key=s3_key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                # Check if it's a "directory" (has objects under it)
                paginator = self.client.get_paginator("list_objects_v2")
                prefix = s3_key.rstrip("/") + "/"
                for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, MaxKeys=1):
                    if page.get("KeyCount", 0) > 0:
                        return True
                return False
            raise
        except Exception as e:
            logger.warning("Error checking existence of s3://%s/%s: %s", self._bucket, s3_key, e)
            return False

    def list_files(
        self,
        path: str = "",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileInfo]:
        """List files at a path."""
        s3_prefix = self._get_s3_key(path)
        if s3_prefix and not s3_prefix.endswith("/"):
            s3_prefix += "/"

        files: List[FileInfo] = []
        paginator = self.client.get_paginator("list_objects_v2")

        paginate_kwargs = {"Bucket": self._bucket, "Prefix": s3_prefix}
        if not recursive:
            paginate_kwargs["Delimiter"] = "/"

        try:
            for page in paginator.paginate(**paginate_kwargs):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    name = key.split("/")[-1]

                    # Skip "directory" markers (empty keys ending with /)
                    if not name:
                        continue

                    # Apply pattern filter
                    if pattern and not fnmatch.fnmatch(name, pattern):
                        continue

                    files.append(
                        FileInfo(
                            path=key,
                            size=obj.get("Size", 0),
                            modified=obj.get("LastModified"),
                        )
                    )

            return sorted(files, key=lambda f: f.path)

        except Exception as e:
            logger.warning("Error listing s3://%s/%s: %s", self._bucket, s3_prefix, e)
            return []

    def read_bytes(self, path: str) -> bytes:
        """Read file contents as bytes."""
        s3_key = self._get_s3_key(path)
        response = self.client.get_object(Bucket=self._bucket, Key=s3_key)
        return response["Body"].read()

    def write_bytes(self, path: str, data: bytes) -> StorageResult:
        """Write bytes to a file."""
        s3_key = self._get_s3_key(path)

        try:
            self.client.put_object(
                Bucket=self._bucket,
                Key=s3_key,
                Body=data,
            )

            return StorageResult(
                success=True,
                path=f"s3://{self._bucket}/{s3_key}",
                files_written=[f"s3://{self._bucket}/{s3_key}"],
                bytes_written=len(data),
            )
        except Exception as e:
            logger.error("Failed to write s3://%s/%s: %s", self._bucket, s3_key, e)
            return StorageResult(
                success=False,
                path=f"s3://{self._bucket}/{s3_key}",
                error=str(e),
            )

    def delete(self, path: str) -> bool:
        """Delete a file or directory."""
        s3_key = self._get_s3_key(path)

        try:
            # Try to delete as single object first
            try:
                self.client.delete_object(Bucket=self._bucket, Key=s3_key)
            except ClientError:
                pass

            # Also delete all objects with this prefix (directory-like behavior)
            paginator = self.client.get_paginator("list_objects_v2")
            prefix = s3_key.rstrip("/") + "/"

            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                if "Contents" not in page:
                    continue
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    self.client.delete_objects(
                        Bucket=self._bucket,
                        Delete={"Objects": objects},
                    )

            return True
        except Exception as e:
            logger.error("Failed to delete s3://%s/%s: %s", self._bucket, s3_key, e)
            return False

    def makedirs(self, path: str) -> None:
        """Create directories (no-op for S3 as directories don't exist)."""
        pass

    def copy(self, src: str, dst: str) -> StorageResult:
        """Copy a file (uses server-side copy for efficiency)."""
        src_key = self._get_s3_key(src)
        dst_key = self._get_s3_key(dst)

        try:
            self.client.copy_object(
                Bucket=self._bucket,
                CopySource={"Bucket": self._bucket, "Key": src_key},
                Key=dst_key,
            )

            # Get size of copied object
            response = self.client.head_object(Bucket=self._bucket, Key=dst_key)
            size = response.get("ContentLength", 0)

            return StorageResult(
                success=True,
                path=f"s3://{self._bucket}/{dst_key}",
                files_written=[f"s3://{self._bucket}/{dst_key}"],
                bytes_written=size,
            )
        except Exception as e:
            logger.error(
                "Failed to copy s3://%s/%s to s3://%s/%s: %s",
                self._bucket,
                src_key,
                self._bucket,
                dst_key,
                e,
            )
            return StorageResult(
                success=False,
                path=f"s3://{self._bucket}/{dst_key}",
                error=str(e),
            )
