"""S3-compatible storage backend for medallion-foundry."""

import logging
from typing import Dict, Any, List
from pathlib import Path
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from core.storage.backend import StorageBackend
from core.infrastructure.resilience.retry import RetryPolicy, execute_with_retry, CircuitBreaker

logger = logging.getLogger(__name__)


class S3Storage(StorageBackend):
    """S3-compatible storage backend using boto3.

    Supports AWS S3, MinIO, and any S3-compatible object storage.
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

        self.prefix = bronze_cfg.get("s3_prefix", "").rstrip("/")

        # Get credentials from environment
        endpoint_env = s3_cfg.get("endpoint_url_env")
        access_key_env = s3_cfg.get("access_key_env", "AWS_ACCESS_KEY_ID")
        secret_key_env = s3_cfg.get("secret_key_env", "AWS_SECRET_ACCESS_KEY")

        endpoint_url = os.environ.get(endpoint_env) if endpoint_env else None
        access_key = os.environ.get(access_key_env)
        secret_key = os.environ.get(secret_key_env)

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
                f"Created S3 client for bucket '{self.bucket}' with endpoint: {endpoint_url or 'default'}"
            )
        except Exception as e:
            logger.error(f"Failed to create S3 client: {e}")
            raise

        # circuit breakers per operation
        def _emit(state: str) -> None:
            logger.info("metric=breaker_state component=s3_storage state=%s", state)

        self._breaker_upload = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_download = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_list = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )
        self._breaker_delete = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit,
        )

    def _build_key(self, remote_path: str) -> str:
        """Build full S3 key from remote path and prefix."""
        if self.prefix:
            return f"{self.prefix}/{remote_path}"
        return remote_path

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
        key = self._build_key(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, BotoCoreError):
                return True
            if isinstance(exc, ClientError):
                try:
                    status = int(
                        exc.response.get("ResponseMetadata", {}).get(
                            "HTTPStatusCode", 0
                        )
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

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> float | None:
            if isinstance(exc, ClientError):
                headers = (
                    exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                    if hasattr(exc, "response")
                    else {}
                )
                retry_after = headers.get("retry-after") or headers.get(
                    "x-amz-retry-after"
                )
                if retry_after:
                    try:
                        return float(retry_after)
                    except Exception:
                        return None
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        try:

            def _once() -> bool:
                self.client.upload_file(local_path, self.bucket, key)
                logger.info(
                    f"Uploaded {Path(local_path).name} to s3://{self.bucket}/{key}"
                )
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_upload,
                operation_name="s3_upload",
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}")
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
        key = self._build_key(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, BotoCoreError):
                return True
            if isinstance(exc, ClientError):
                try:
                    status = int(
                        exc.response.get("ResponseMetadata", {}).get(
                            "HTTPStatusCode", 0
                        )
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

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> float | None:
            if isinstance(exc, ClientError):
                headers = (
                    exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                    if hasattr(exc, "response")
                    else {}
                )
                retry_after = headers.get("retry-after") or headers.get(
                    "x-amz-retry-after"
                )
                if retry_after:
                    try:
                        return float(retry_after)
                    except Exception:
                        return None
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        try:

            def _once() -> bool:
                self.client.download_file(self.bucket, key, local_path)
                logger.info(f"Downloaded s3://{self.bucket}/{key} to {local_path}")
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_download,
                operation_name="s3_download",
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to download {remote_path} from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error downloading from S3: {e}")
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
        full_prefix = self._build_key(prefix)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, BotoCoreError):
                return True
            if isinstance(exc, ClientError):
                try:
                    status = int(
                        exc.response.get("ResponseMetadata", {}).get(
                            "HTTPStatusCode", 0
                        )
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

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> float | None:
            if isinstance(exc, ClientError):
                headers = (
                    exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                    if hasattr(exc, "response")
                    else {}
                )
                retry_after = headers.get("retry-after") or headers.get(
                    "x-amz-retry-after"
                )
                if retry_after:
                    try:
                        return float(retry_after)
                    except Exception:
                        return None
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        try:

            def _once() -> List[str]:
                response = self.client.list_objects_v2(
                    Bucket=self.bucket, Prefix=full_prefix
                )
                files = [obj["Key"] for obj in response.get("Contents", [])]
                logger.debug(f"Listed {len(files)} files with prefix '{full_prefix}'")
                return files

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_list,
                operation_name="s3_list",
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to list files in S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing S3 files: {e}")
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
        key = self._build_key(remote_path)

        def _retry_if(exc: BaseException) -> bool:
            if isinstance(exc, BotoCoreError):
                return True
            if isinstance(exc, ClientError):
                try:
                    status = int(
                        exc.response.get("ResponseMetadata", {}).get(
                            "HTTPStatusCode", 0
                        )
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

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> float | None:
            if isinstance(exc, ClientError):
                headers = (
                    exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                    if hasattr(exc, "response")
                    else {}
                )
                retry_after = headers.get("retry-after") or headers.get(
                    "x-amz-retry-after"
                )
                if retry_after:
                    try:
                        return float(retry_after)
                    except Exception:
                        return None
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        try:

            def _once() -> bool:
                self.client.delete_object(Bucket=self.bucket, Key=key)
                logger.info(f"Deleted s3://{self.bucket}/{key}")
                return True

            return execute_with_retry(
                _once,
                policy=policy,
                breaker=self._breaker_delete,
                operation_name="s3_delete",
            )
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to delete {remote_path} from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting from S3: {e}")
            raise

    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        return "s3"


class S3StorageBackend(S3Storage):
    """Legacy-friendly wrapper that exposes upload/download hooks."""

    def upload(self, local_path: Path | str, remote_path: str) -> bool:
        return self.upload_file(str(local_path), remote_path)

    def download(self, remote_path: str, local_path: Path | str) -> bool:
        return self.download_file(remote_path, str(local_path))

    def delete(self, remote_path: str) -> bool:
        return self.delete_file(remote_path)


# Backward compatibility functions (deprecated)
def build_s3_client(platform_cfg: Dict[str, Any]) -> boto3.client:
    """Build an S3 client from platform configuration.

    DEPRECATED: Use S3Storage class instead.
    """
    storage = S3Storage(platform_cfg)
    return storage.client


def upload_to_s3(
    local_path: Path, platform_cfg: Dict[str, Any], relative_path: str
) -> None:
    """Upload a file to S3 with retry logic.

    DEPRECATED: Use S3Storage.upload_file() instead.
    """
    storage = S3Storage(platform_cfg)
    # Build remote path from relative_path and filename
    remote_path = f"{relative_path}{local_path.name}"
    storage.upload_file(str(local_path), remote_path)
