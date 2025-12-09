"""S3-compatible storage backend for medallion-foundry."""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .base import BaseCloudStorage, HealthCheckResult, HealthCheckTracker
from .helpers import get_env_value

logger = logging.getLogger(__name__)


class S3Storage(BaseCloudStorage):
    """S3-compatible storage backend using boto3.

    Supports AWS S3, MinIO, and any S3-compatible object storage.
    Inherits resilience patterns (circuit breakers, retry) from BaseCloudStorage.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize S3 storage backend from configuration.

        Args:
            config: Platform configuration dictionary containing:
                - bronze.s3_bucket: S3 bucket name
                - bronze.s3_prefix: Optional prefix for all paths
                - s3_connection.endpoint_url_env: Environment variable for endpoint URL
                - s3_connection.access_key_env: Environment variable for access key
                - s3_connection.secret_key_env: Environment variable for secret key
        """
        bronze_cfg = config.get("bronze", {})
        s3_cfg = config.get("s3_connection", {})

        # Storage settings
        self.bucket = bronze_cfg.get("s3_bucket")
        if not self.bucket:
            raise ValueError("s3_bucket is required in bronze configuration")

        # Get credentials from environment
        endpoint_env = s3_cfg.get("endpoint_url_env")
        access_key_env = s3_cfg.get("access_key_env", "AWS_ACCESS_KEY_ID")
        secret_key_env = s3_cfg.get("secret_key_env", "AWS_SECRET_ACCESS_KEY")

        endpoint_url = get_env_value(endpoint_env)
        access_key = get_env_value(access_key_env)
        secret_key = get_env_value(secret_key_env)

        # Build boto3 client
        session_kwargs: Dict[str, Any] = {}
        if access_key and secret_key:
            session_kwargs["aws_access_key_id"] = access_key
            session_kwargs["aws_secret_access_key"] = secret_key

        try:
            self.client = boto3.client(
                "s3", endpoint_url=endpoint_url, **session_kwargs
            )
            logger.debug(
                "Created S3 client for bucket '%s' with endpoint: %s",
                self.bucket,
                endpoint_url or "default",
            )
        except Exception as e:
            logger.error("Failed to create S3 client: %s", e)
            raise

        # Initialize circuit breakers from base class
        super().__init__(config, prefix_key="s3_prefix")

    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        return "s3"

    def _build_remote_path(self, remote_path: str) -> str:
        """Build full S3 key from remote path and prefix."""
        return self._apply_prefix(remote_path)

    def _should_retry(self, exc: BaseException) -> bool:
        """Determine if an S3 exception should trigger a retry."""
        if isinstance(exc, BotoCoreError):
            return True
        if isinstance(exc, ClientError):
            try:
                status = int(
                    exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
                )
            except Exception:
                status = 0
            code = (
                exc.response.get("Error", {}).get("Code")
                if hasattr(exc, "response")
                else None
            )
            return (
                status == 429
                or status >= 500
                or code in {"SlowDown", "RequestLimitExceeded"}
            )
        return False

    def _get_delay_from_s3_exception(
        self, exc: BaseException, attempt: int, default_delay: float
    ) -> float | None:
        """Extract retry delay from S3 exception headers."""
        if isinstance(exc, ClientError):
            headers = (
                exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                if hasattr(exc, "response")
                else {}
            )
            retry_after = headers.get("retry-after") or headers.get("x-amz-retry-after")
            if retry_after:
                try:
                    return float(retry_after)
                except Exception:
                    return None
        return None

    def _do_upload(self, local_path: str, remote_key: str) -> bool:
        """Perform the actual S3 upload."""
        self.client.upload_file(local_path, self.bucket, remote_key)
        logger.info(
            "Uploaded %s to s3://%s/%s",
            Path(local_path).name,
            self.bucket,
            remote_key,
        )
        return True

    def _do_download(self, remote_key: str, local_path: str) -> bool:
        """Perform the actual S3 download."""
        self.client.download_file(self.bucket, remote_key, local_path)
        logger.info("Downloaded s3://%s/%s to %s", self.bucket, remote_key, local_path)
        return True

    def _do_list(self, prefix: str) -> List[str]:
        """Perform the actual S3 list operation."""
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        files = [obj["Key"] for obj in response.get("Contents", [])]
        logger.debug("Listed %d files with prefix '%s'", len(files), prefix)
        return files

    def _do_delete(self, remote_key: str) -> bool:
        """Perform the actual S3 delete."""
        self.client.delete_object(Bucket=self.bucket, Key=remote_key)
        logger.info("Deleted s3://%s/%s", self.bucket, remote_key)
        return True

    def health_check(self) -> HealthCheckResult:
        """Verify S3 connectivity and permissions.

        Checks:
        - Bucket exists and is accessible (HEAD bucket)
        - Write permission (put test object)
        - Read permission (get test object)
        - List permission (list objects)
        - Delete permission (delete test object)
        - Versioning capability

        Returns:
            HealthCheckResult with permission checks, capabilities, and any errors
        """
        tracker = HealthCheckTracker(
            capabilities={
                "versioning": False,
                "multipart_upload": True,  # S3 always supports multipart
            },
        )
        permissions = tracker.permissions

        test_key = self._build_remote_path(f"_health_check_{uuid.uuid4().hex}.tmp")
        test_content = b"health_check_test"

        try:
            # Check bucket exists
            try:
                self.client.head_bucket(Bucket=self.bucket)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                tracker.add_error(f"Bucket access failed: {error_code}")
                return tracker.finalize()

            # Test write permission
            try:
                self.client.put_object(
                    Bucket=self.bucket,
                    Key=test_key,
                    Body=test_content,
                )
                permissions["write"] = True
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                tracker.add_error(f"Write permission failed: {error_code}")

            # Test read permission
            if permissions["write"]:
                try:
                    response = self.client.get_object(Bucket=self.bucket, Key=test_key)
                    content = response["Body"].read()
                    permissions["read"] = content == test_content
                    if not permissions["read"]:
                        tracker.add_error("Read content mismatch")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "Unknown")
                    tracker.add_error(f"Read permission failed: {error_code}")

            # Test list permission
            try:
                self.client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=self._build_remote_path("_health_check_"),
                    MaxKeys=1,
                )
                permissions["list"] = True
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                tracker.add_error(f"List permission failed: {error_code}")

            # Test delete permission
            if permissions["write"]:
                try:
                    self.client.delete_object(Bucket=self.bucket, Key=test_key)
                    permissions["delete"] = True
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "Unknown")
                    tracker.add_error(f"Delete permission failed: {error_code}")

            # Check versioning capability
            try:
                versioning = self.client.get_bucket_versioning(Bucket=self.bucket)
                tracker.capabilities["versioning"] = versioning.get("Status") == "Enabled"
            except ClientError:
                # Not an error - versioning check is optional
                pass

        except BotoCoreError as e:
            tracker.add_error(f"S3 connection error: {e}")
        except Exception as e:
            tracker.add_error(f"Unexpected error during health check: {e}")

        return tracker.finalize()

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to S3.

        Args:
            local_path: Path to local file
            remote_path: Destination path in S3 (relative to bucket/prefix)

        Returns:
            True if upload succeeded

        Raises:
            BotoCoreError, ClientError: If upload fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_upload(local_path, remote_key),
                "s3_upload",
                breaker_key="upload",
                retry_if=self._should_retry,
                delay_from_exception=self._get_delay_from_s3_exception,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error("Failed to upload %s to S3: %s", local_path, e)
            raise
        except Exception as e:
            logger.error("Unexpected error uploading to S3: %s", e)
            raise

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from S3.

        Args:
            remote_path: Path in S3 (relative to bucket/prefix)
            local_path: Destination path on local filesystem

        Returns:
            True if download succeeded

        Raises:
            BotoCoreError, ClientError: If download fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_download(remote_key, local_path),
                "s3_download",
                breaker_key="download",
                retry_if=self._should_retry,
                delay_from_exception=self._get_delay_from_s3_exception,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error("Failed to download %s from S3: %s", remote_path, e)
            raise
        except Exception as e:
            logger.error("Unexpected error downloading from S3: %s", e)
            raise

    def list_files(self, prefix: str) -> List[str]:
        """List files in S3 with given prefix.

        Args:
            prefix: Path prefix to filter files

        Returns:
            List of file keys matching the prefix

        Raises:
            BotoCoreError, ClientError: If listing fails
        """
        full_prefix = self._build_remote_path(prefix)
        try:
            return self._execute_with_resilience(
                lambda: self._do_list(full_prefix),
                "s3_list",
                breaker_key="list",
                retry_if=self._should_retry,
                delay_from_exception=self._get_delay_from_s3_exception,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error("Failed to list files in S3: %s", e)
            raise
        except Exception as e:
            logger.error("Unexpected error listing S3 files: %s", e)
            raise

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from S3.

        Args:
            remote_path: Path to file in S3

        Returns:
            True if deletion succeeded

        Raises:
            BotoCoreError, ClientError: If deletion fails after retries
        """
        remote_key = self._build_remote_path(remote_path)
        try:
            return self._execute_with_resilience(
                lambda: self._do_delete(remote_key),
                "s3_delete",
                breaker_key="delete",
                retry_if=self._should_retry,
                delay_from_exception=self._get_delay_from_s3_exception,
            )
        except (BotoCoreError, ClientError) as e:
            logger.error("Failed to delete %s from S3: %s", remote_path, e)
            raise
        except Exception as e:
            logger.error("Unexpected error deleting from S3: %s", e)
            raise
