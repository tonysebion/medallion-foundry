"""Storage backend abstraction for medallion-foundry.

This module provides the abstract interface for storage backends,
allowing the framework to support multiple storage systems (S3, Azure, GCS, etc.)
in a pluggable manner.
"""

from abc import ABC, abstractmethod
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    """Abstract base class for storage backends.
    
    All storage backends (S3, Azure, GCS, etc.) must implement this interface.
    The framework uses this abstraction to remain storage-agnostic.
    """
    
    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to remote storage.
        
        Args:
            local_path: Path to the local file to upload
            remote_path: Destination path in remote storage (relative to bucket/container)
            
        Returns:
            True if upload succeeded, False otherwise
            
        Raises:
            Exception: If upload fails after retries
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from remote storage.
        
        Args:
            remote_path: Path in remote storage (relative to bucket/container)
            local_path: Destination path on local filesystem
            
        Returns:
            True if download succeeded, False otherwise
            
        Raises:
            Exception: If download fails after retries
        """
        pass
    
    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """List files in remote storage with given prefix.
        
        Args:
            prefix: Path prefix to filter files
            
        Returns:
            List of file paths matching the prefix
            
        Raises:
            Exception: If listing fails
        """
        pass
    
    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from remote storage.
        
        Args:
            remote_path: Path to file in remote storage
            
        Returns:
            True if deletion succeeded, False otherwise
            
        Raises:
            Exception: If deletion fails
        """
        pass
    
    @abstractmethod
    def get_backend_type(self) -> str:
        """Get the backend type identifier.
        
        Returns:
            String identifier for this backend (e.g., 's3', 'azure', 'gcs')
        """
        pass


_STORAGE_BACKEND_CACHE: Dict[int, StorageBackend] = {}

def get_storage_backend(config: dict, use_cache: bool = True) -> StorageBackend:
    """Factory function to create appropriate storage backend.
    
    Args:
        config: Platform configuration dictionary
        use_cache: When True, reuse backend instances for the same config dict
    
    Returns:
        StorageBackend instance based on configuration
        
    Raises:
        ValueError: If backend type is unknown or unsupported
        ImportError: If required dependencies are not installed
    """
    cache_key = id(config)
    if use_cache and cache_key in _STORAGE_BACKEND_CACHE:
        return _STORAGE_BACKEND_CACHE[cache_key]

    backend_type = config.get("bronze", {}).get("storage_backend", "s3")
    
    if backend_type == "s3":
        from core.s3 import S3Storage
        backend = S3Storage(config)
    
    elif backend_type == "azure":
        try:
            from core.azure_storage import AzureStorage
            backend = AzureStorage(config)
        except ImportError as e:
            raise ImportError(
                f"Azure storage backend requires additional packages. "
                f"Install with: pip install azure-storage-blob azure-identity\n"
                f"Original error: {e}"
            )
    
    elif backend_type == "gcs":
        try:
            from core.gcs_storage import GCSStorage
            backend = GCSStorage(config)
        except ImportError as e:
            raise ImportError(
                f"Google Cloud Storage backend requires additional packages. "
                f"Install with: pip install google-cloud-storage\n"
                f"Original error: {e}"
            )
    
    elif backend_type == "local":
        from core.local_storage import LocalStorage
        backend = LocalStorage(config)
    
    else:
        raise ValueError(
            f"Unknown storage backend: '{backend_type}'. "
            f"Supported backends: s3, azure, gcs, local"
        )

    if use_cache:
        _STORAGE_BACKEND_CACHE[cache_key] = backend
    return backend
