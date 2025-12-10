"""Storage I/O abstractions and implementations."""
from __future__ import annotations

from .base import (
    BACKEND_REGISTRY,
    BaseCloudStorage,
    HealthCheckResult,
    StorageBackend,
    _STORAGE_BACKEND_CACHE,
    get_backend_factory,
    get_storage_backend,
    list_backends,
    register_backend,
    resolve_backend_type,
)
from .checksum import (
    ChecksumVerificationResult,
    compute_file_sha256,
    verify_checksum_manifest,
    verify_checksum_manifest_with_result,
    write_checksum_manifest,
)
from .quarantine import (
    QuarantineConfig,
    QuarantineResult,
    quarantine_corrupted_files,
)
from .fsspec import create_filesystem, get_fs_for_path
from .locks import LockAcquireError, file_lock
from .local import LocalStorage
from .plan import ChunkWriterConfig, StoragePlan
from .policy import (
    StorageMetadata,
    VALID_BOUNDARIES,
    VALID_CLOUD_PROVIDERS,
    VALID_PROVIDER_TYPES,
    enforce_storage_scope,
    validate_storage_metadata,
)
from .path_utils import build_partition_path, sanitize_partition_value
from .azure import AzureStorage
from .s3 import S3Storage
from .uri import StorageURI

__all__ = [
    "BACKEND_REGISTRY",
    "BaseCloudStorage",
    "HealthCheckResult",
    "StorageBackend",
    "_STORAGE_BACKEND_CACHE",
    "get_backend_factory",
    "get_storage_backend",
    "list_backends",
    "register_backend",
    "resolve_backend_type",
    "AzureStorage",
    "S3Storage",
    "LocalStorage",
    "StorageMetadata",
    "VALID_BOUNDARIES",
    "VALID_CLOUD_PROVIDERS",
    "VALID_PROVIDER_TYPES",
    "enforce_storage_scope",
    "validate_storage_metadata",
    "ChecksumVerificationResult",
    "compute_file_sha256",
    "write_checksum_manifest",
    "verify_checksum_manifest",
    "verify_checksum_manifest_with_result",
    "QuarantineConfig",
    "QuarantineResult",
    "quarantine_corrupted_files",
    "create_filesystem",
    "get_fs_for_path",
    "file_lock",
    "LockAcquireError",
    "ChunkWriterConfig",
    "StoragePlan",
    "build_partition_path",
    "sanitize_partition_value",
    "StorageURI",
]
