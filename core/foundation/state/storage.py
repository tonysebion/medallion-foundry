"""Base class for state storage backends (watermarks, manifests).

This module provides a common base class for JSON state storage operations
supporting both local filesystem and S3 backends. Used by WatermarkStore
and ManifestTracker to avoid code duplication.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, cast

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StateStorageBackend:
    """Base class for JSON state storage (watermarks, manifests).

    Provides common methods for loading, saving, deleting, and listing
    JSON data from local filesystem or S3. Subclasses inherit these
    methods and use them to implement their specific storage logic.

    Attributes:
        storage_backend: Storage backend type ("local" or "s3")
        s3_bucket: S3 bucket name (if using S3 backend)
    """

    storage_backend: str = "local"
    s3_bucket: Optional[str] = None

    @staticmethod
    def _parse_s3_path(path: str) -> Tuple[str, str]:
        """Parse an S3 path into bucket and key.

        Args:
            path: S3 path in format "s3://bucket/key" or "bucket/key"

        Returns:
            Tuple of (bucket, key)
        """
        clean = path.replace("s3://", "")
        parts = clean.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    @staticmethod
    def _get_boto3_client() -> Any:
        """Get a boto3 S3 client with lazy import.

        Returns:
            boto3 S3 client

        Raises:
            ImportError: If boto3 is not installed
        """
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required for S3 storage")
        return boto3.client("s3")

    def _load_json_local(self, path: Path) -> Dict[str, Any]:
        """Load JSON data from a local file.

        Args:
            path: Path to local JSON file

        Returns:
            Parsed JSON data as dictionary

        Raises:
            FileNotFoundError: If file does not exist
        """
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            return cast(Dict[str, Any], json.load(f))

    def _load_json_s3(self, bucket: str, key: str) -> Dict[str, Any]:
        """Load JSON data from S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Parsed JSON data as dictionary

        Raises:
            FileNotFoundError: If object does not exist
        """
        s3 = self._get_boto3_client()
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            return cast(Dict[str, Any], json.loads(response["Body"].read().decode("utf-8")))
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}")

    def _save_json_local(self, path: Path, data: Dict[str, Any]) -> None:
        """Save JSON data to a local file.

        Args:
            path: Path to local JSON file
            data: Data to serialize as JSON
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        logger.info("Saved JSON to %s", path)

    def _save_json_s3(self, bucket: str, key: str, data: Dict[str, Any]) -> None:
        """Save JSON data to S3.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            data: Data to serialize as JSON
        """
        s3 = self._get_boto3_client()
        body = json.dumps(data, indent=2)
        s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
        logger.info("Saved JSON to s3://%s/%s", bucket, key)

    def _delete_local(self, path: Path) -> bool:
        """Delete a local file.

        Args:
            path: Path to local file

        Returns:
            True if file was deleted, False if not found
        """
        if path.exists():
            path.unlink()
            logger.info("Deleted %s", path)
            return True
        return False

    def _delete_s3(self, bucket: str, key: str) -> bool:
        """Delete an S3 object.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            True if deletion succeeded
        """
        s3 = self._get_boto3_client()
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            logger.info("Deleted s3://%s/%s", bucket, key)
            return True
        except Exception:
            return False

    def _list_json_local(self, directory: Path, pattern: str) -> List[Path]:
        """List JSON files in a local directory matching a pattern.

        Args:
            directory: Directory to search
            pattern: Glob pattern (e.g., "*_watermark.json")

        Returns:
            List of matching file paths
        """
        if not directory.exists():
            return []
        return list(directory.glob(pattern))

    def _list_json_s3(self, bucket: str, prefix: str, suffix: str = ".json") -> List[str]:
        """List JSON objects in S3 matching a prefix and suffix.

        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix
            suffix: File suffix to filter (default: ".json")

        Returns:
            List of matching S3 keys
        """
        s3 = self._get_boto3_client()
        keys: List[str] = []

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(suffix):
                    keys.append(obj["Key"])

        return keys

    def _load_json_s3_by_key(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Load JSON from S3 by key, returning None on error.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Parsed JSON data or None if error
        """
        try:
            return self._load_json_s3(bucket, key)
        except Exception as e:
            logger.warning("Could not load s3://%s/%s: %s", bucket, key, e)
            return None

    def _dispatch_storage_operation(
        self,
        local_op: Callable[[], T],
        s3_op: Callable[[], T],
    ) -> T:
        """Dispatch to local or S3 operation based on storage_backend.

        Args:
            local_op: Function to call for local storage
            s3_op: Function to call for S3 storage

        Returns:
            Result from the appropriate operation
        """
        if self.storage_backend == "s3":
            return s3_op()
        return local_op()
