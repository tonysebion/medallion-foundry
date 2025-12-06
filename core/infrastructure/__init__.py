"""Cross-cutting infrastructure concerns for bronze-foundry.

This package contains infrastructure components:
- resilience/: Retry policies, circuit breakers, rate limiting, late data handling
- storage/: Storage backends (S3, Azure, Local)
- config/: Configuration loading and validation
"""

from .resilience import (
    RetryPolicy,
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    execute_with_retry,
    execute_with_retry_async,
    wrap_storage_error,
    wrap_extraction_error,
    wrap_requests_exception,
    wrap_boto3_exception,
    wrap_azure_exception,
    LateDataMode,
    LateDataConfig,
    LateDataResult,
    LateDataHandler,
    BackfillWindow,
    build_late_data_handler,
    parse_backfill_window,
)
from .storage import (
    StorageBackend,
    get_storage_backend,
    register_backend,
    register_storage_backend,
    list_backends,
    resolve_backend_type,
    get_backend_factory,
    BACKEND_REGISTRY,
    enforce_storage_scope,
    validate_storage_metadata,
    VALID_BOUNDARIES,
    VALID_CLOUD_PROVIDERS,
    VALID_PROVIDER_TYPES,
    S3Storage,
    S3StorageBackend,
    LocalStorage,
)
from .config import (
    build_relative_path,
    load_config,
    load_configs,
    ensure_root_config,
    DatasetConfig,
)

__all__ = [
    # Resilience
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "RateLimiter",
    "execute_with_retry",
    "execute_with_retry_async",
    "wrap_storage_error",
    "wrap_extraction_error",
    "wrap_requests_exception",
    "wrap_boto3_exception",
    "wrap_azure_exception",
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    "LateDataHandler",
    "BackfillWindow",
    "build_late_data_handler",
    "parse_backfill_window",
    # Storage
    "StorageBackend",
    "get_storage_backend",
    "register_backend",
    "register_storage_backend",
    "list_backends",
    "resolve_backend_type",
    "get_backend_factory",
    "BACKEND_REGISTRY",
    "enforce_storage_scope",
    "validate_storage_metadata",
    "VALID_BOUNDARIES",
    "VALID_CLOUD_PROVIDERS",
    "VALID_PROVIDER_TYPES",
    "S3Storage",
    "S3StorageBackend",
    "LocalStorage",
    # Config
    "build_relative_path",
    "load_config",
    "load_configs",
    "ensure_root_config",
    "DatasetConfig",
]
