"""Storage backends for medallion-foundry.

Available backends:
- S3Storage: AWS S3 and S3-compatible storage
- AzureStorage: Azure Blob Storage / ADLS Gen2
- LocalStorage: Local filesystem (for testing)

Usage:
    from core.infrastructure.storage import get_storage_backend, StorageBackend

    backend = get_storage_backend(config)
    backend.upload_file(local_path, remote_path)
"""

from .backend import (
    StorageBackend,
    get_storage_backend,
    register_backend,
    register_storage_backend,
    list_backends,
    resolve_backend_type,
    get_backend_factory,
    BACKEND_REGISTRY,
)
from .policy import (
    enforce_storage_scope,
    validate_storage_metadata,
    VALID_BOUNDARIES,
    VALID_CLOUD_PROVIDERS,
    VALID_PROVIDER_TYPES,
    StorageMetadata,
)
from .checksum import (
    compute_file_sha256,
    write_checksum_manifest,
    verify_checksum_manifest,
)

# Re-export concrete backends for direct import
from .s3 import S3Storage, S3StorageBackend
from .local import LocalStorage

# Azure is optional
try:
    from .azure import AzureStorage, AzureStorageBackend
except ImportError:
    pass

__all__ = [
    # Base class and factory
    "StorageBackend",
    "get_storage_backend",
    # Registry
    "register_backend",
    "register_storage_backend",
    "list_backends",
    "resolve_backend_type",
    "get_backend_factory",
    "BACKEND_REGISTRY",
    # Policy
    "VALID_BOUNDARIES",
    "VALID_CLOUD_PROVIDERS",
    "VALID_PROVIDER_TYPES",
    "enforce_storage_scope",
    "validate_storage_metadata",
    # Checksum utilities
    "compute_file_sha256",
    "write_checksum_manifest",
    "verify_checksum_manifest",
    # Concrete backends
    "S3Storage",
    "S3StorageBackend",
    "LocalStorage",
    "AzureStorage",
    "AzureStorageBackend",
]
