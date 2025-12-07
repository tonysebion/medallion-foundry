"""Storage backends and utilities.

This package provides:
- StorageBackend: Abstract base class for all storage backends
- S3Storage, AzureStorage, LocalStorage: Backend implementations
- Checksum, URI, and path utilities
- File locking and filesystem abstractions
"""

# Re-export from core.io.storage (canonical location during migration)
from core.io.storage import (
    # Base classes and registry
    BACKEND_REGISTRY,
    BaseCloudStorage,
    HealthCheckResult,
    StorageBackend,
    _STORAGE_BACKEND_CACHE,
    get_backend_factory,
    get_storage_backend,
    list_backends,
    register_backend,
    register_storage_backend,
    resolve_backend_type,
    # Backend implementations
    AzureStorage,
    AzureStorageBackend,
    S3Storage,
    S3StorageBackend,
    LocalStorage,
    # Policy and metadata
    StorageMetadata,
    VALID_BOUNDARIES,
    VALID_CLOUD_PROVIDERS,
    VALID_PROVIDER_TYPES,
    enforce_storage_scope,
    validate_storage_metadata,
    # Checksum utilities
    ChecksumVerificationResult,
    compute_file_sha256,
    write_checksum_manifest,
    verify_checksum_manifest,
    verify_checksum_manifest_with_result,
    # Quarantine utilities
    QuarantineConfig,
    QuarantineResult,
    quarantine_corrupted_files,
    # Filesystem utilities
    create_filesystem,
    get_fs_for_path,
    # Locking
    file_lock,
    LockAcquireError,
    # Path utilities
    build_partition_path,
    sanitize_partition_value,
    # URI parsing
    StorageURI,
)

__all__ = [
    # Base classes and registry
    "BACKEND_REGISTRY",
    "BaseCloudStorage",
    "HealthCheckResult",
    "StorageBackend",
    "_STORAGE_BACKEND_CACHE",
    "get_backend_factory",
    "get_storage_backend",
    "list_backends",
    "register_backend",
    "register_storage_backend",
    "resolve_backend_type",
    # Backend implementations
    "AzureStorage",
    "AzureStorageBackend",
    "S3Storage",
    "S3StorageBackend",
    "LocalStorage",
    # Policy and metadata
    "StorageMetadata",
    "VALID_BOUNDARIES",
    "VALID_CLOUD_PROVIDERS",
    "VALID_PROVIDER_TYPES",
    "enforce_storage_scope",
    "validate_storage_metadata",
    # Checksum utilities
    "ChecksumVerificationResult",
    "compute_file_sha256",
    "write_checksum_manifest",
    "verify_checksum_manifest",
    "verify_checksum_manifest_with_result",
    # Quarantine utilities
    "QuarantineConfig",
    "QuarantineResult",
    "quarantine_corrupted_files",
    # Filesystem utilities
    "create_filesystem",
    "get_fs_for_path",
    # Locking
    "file_lock",
    "LockAcquireError",
    # Path utilities
    "build_partition_path",
    "sanitize_partition_value",
    # URI parsing
    "StorageURI",
]
