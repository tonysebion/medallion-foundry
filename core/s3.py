"""S3-compatible storage backend for medallion-foundry."""

import logging
from typing import Dict, Any, List
from pathlib import Path
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from core.storage import StorageBackend

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
            self.client = boto3.client("s3", endpoint_url=endpoint_url, **session_kwargs)
            logger.debug(f"Created S3 client for bucket '{self.bucket}' with endpoint: {endpoint_url or 'default'}")
        except Exception as e:
            logger.error(f"Failed to create S3 client: {e}")
            raise
    
    def _build_key(self, remote_path: str) -> str:
        """Build full S3 key from remote path and prefix."""
        if self.prefix:
            return f"{self.prefix}/{remote_path}"
        return remote_path
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((BotoCoreError, ClientError)),
        reraise=True
    )
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
        
        try:
            self.client.upload_file(local_path, self.bucket, key)
            logger.info(f"Uploaded {Path(local_path).name} to s3://{self.bucket}/{key}")
            return True
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading to S3: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((BotoCoreError, ClientError)),
        reraise=True
    )
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
        
        try:
            self.client.download_file(self.bucket, key, local_path)
            logger.info(f"Downloaded s3://{self.bucket}/{key} to {local_path}")
            return True
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
        
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix
            )
            
            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.debug(f"Listed {len(files)} files with prefix '{full_prefix}'")
            return files
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to list files in S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing S3 files: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((BotoCoreError, ClientError)),
        reraise=True
    )
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
        
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            logger.info(f"Deleted s3://{self.bucket}/{key}")
            return True
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to delete {remote_path} from S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting from S3: {e}")
            raise
    
    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        return "s3"


# Backward compatibility functions (deprecated)
def build_s3_client(platform_cfg: Dict[str, Any]) -> boto3.client:
    """Build an S3 client from platform configuration.
    
    DEPRECATED: Use S3Storage class instead.
    """
    storage = S3Storage(platform_cfg)
    return storage.client


def upload_to_s3(local_path: Path, platform_cfg: Dict[str, Any], relative_path: str) -> None:
    """Upload a file to S3 with retry logic.
    
    DEPRECATED: Use S3Storage.upload_file() instead.
    """
    storage = S3Storage(platform_cfg)
    # Build remote path from relative_path and filename
    remote_path = f"{relative_path}{local_path.name}"
    storage.upload_file(str(local_path), remote_path)
