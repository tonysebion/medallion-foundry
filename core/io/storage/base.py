"""Storage backend abstraction and registry.

This module provides:
- StorageBackend: Abstract base class for all storage backends
- BaseCloudStorage: Abstract base for cloud backends with resilience patterns
- HealthCheckResult: Result of a storage backend health check
- Backend registry: Register and retrieve backend factories
- get_storage_backend(): Factory function to get configured backend
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from core.runtime.config import RootConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


# =============================================================================
# Health Check Result
# =============================================================================


@dataclass
class HealthCheckResult:
    """Result of a storage backend health check.

    Attributes:
        is_healthy: Whether the backend is operational
        capabilities: Dict of capability flags (e.g., versioning, multipart_upload)
        errors: List of error messages if any checks failed
        latency_ms: Round-trip latency in milliseconds (if measured)
        checked_permissions: Dict of permission checks and their results
    """

    is_healthy: bool
    capabilities: Dict[str, bool] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    latency_ms: Optional[float] = None
    checked_permissions: Dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "is_healthy": self.is_healthy,
            "capabilities": self.capabilities,
            "errors": self.errors,
            "latency_ms": self.latency_ms,
            "checked_permissions": self.checked_permissions,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HealthCheckResult":
        """Create from dictionary."""
        return cls(
            is_healthy=data.get("is_healthy", False),
            capabilities=data.get("capabilities", {}),
            errors=data.get("errors", []),
            latency_ms=data.get("latency_ms"),
            checked_permissions=data.get("checked_permissions", {}),
        )

    def __str__(self) -> str:
        status = "HEALTHY" if self.is_healthy else "UNHEALTHY"
        parts = [f"HealthCheck: {status}"]
        if self.latency_ms is not None:
            parts.append(f"latency={self.latency_ms:.1f}ms")
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        return " ".join(parts)


# =============================================================================
# Storage Backend Base Class
# =============================================================================


class StorageBackend:
    """Abstract base class for storage backends."""

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        raise NotImplementedError

    def download_file(self, remote_path: str, local_path: str) -> bool:
        raise NotImplementedError

    def list_files(self, prefix: str) -> list[str]:
        raise NotImplementedError

    def delete_file(self, remote_path: str) -> bool:
        raise NotImplementedError

    def get_backend_type(self) -> str:
        raise NotImplementedError

    def health_check(self) -> HealthCheckResult:
        """Verify connectivity and permissions before jobs run.

        Performs a pre-flight check to validate:
        - Connectivity to the storage backend
        - Read/write/list/delete permissions
        - Backend-specific capabilities

        Returns:
            HealthCheckResult with is_healthy, capabilities, and any errors

        Note:
            Subclasses should override this method to implement
            backend-specific health checks.
        """
        return HealthCheckResult(
            is_healthy=False,
            errors=["health_check() not implemented for this backend"],
        )


# =============================================================================
# Cloud Storage Base Class with Resilience
# =============================================================================


class BaseCloudStorage(StorageBackend):
    """Abstract base class for cloud storage backends with resilience patterns.

    Provides:
    - Circuit breakers per operation (upload, download, list, delete)
    - Retry policy with exponential backoff
    - Common execute_with_resilience wrapper

    Subclasses must implement:
    - _do_upload(local_path, remote_key) -> bool
    - _do_download(remote_key, local_path) -> bool
    - _do_list(prefix) -> List[str]
    - _do_delete(remote_key) -> bool
    - _build_remote_path(remote_path) -> str
    - _should_retry(exc) -> bool
    - get_backend_type() -> str
    """

    def __init__(self) -> None:
        """Initialize circuit breakers for each operation."""
        from core.resilience import CircuitBreaker

        def _emit_state(state: str) -> None:
            logger.info(
                "metric=breaker_state component=%s state=%s",
                self.get_backend_type(),
                state,
            )

        self._breaker_upload = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit_state,
        )
        self._breaker_download = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit_state,
        )
        self._breaker_list = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit_state,
        )
        self._breaker_delete = CircuitBreaker(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
            on_state_change=_emit_state,
        )

    def _build_retry_policy(
        self,
        retry_if: Callable[[BaseException], bool] | None = None,
        delay_from_exception: Callable[[BaseException, int, float], float | None] | None = None,
    ) -> "RetryPolicy":
        """Build a standard retry policy for cloud operations.

        Args:
            retry_if: Custom predicate to determine if exception is retryable
            delay_from_exception: Optional callback to extract delay from exception

        Returns:
            Configured RetryPolicy instance
        """
        from core.resilience import RetryPolicy

        return RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),
            retry_if=retry_if or self._should_retry,
            delay_from_exception=delay_from_exception,
        )

    def _execute_with_resilience(
        self,
        operation: Callable[[], T],
        breaker: "CircuitBreaker",
        operation_name: str,
        retry_if: Callable[[BaseException], bool] | None = None,
        delay_from_exception: Callable[[BaseException, int, float], float | None] | None = None,
    ) -> T:
        """Execute an operation with circuit breaker and retry logic.

        Args:
            operation: The operation to execute
            breaker: Circuit breaker for this operation type
            operation_name: Name for logging
            retry_if: Optional custom retry predicate
            delay_from_exception: Optional delay extraction callback

        Returns:
            Result of the operation

        Raises:
            Exception from operation if all retries exhausted
        """
        from core.resilience import execute_with_retry

        policy = self._build_retry_policy(retry_if, delay_from_exception)
        return execute_with_retry(
            operation,
            policy=policy,
            breaker=breaker,
            operation_name=operation_name,
        )

    @abstractmethod
    def _should_retry(self, exc: BaseException) -> bool:
        """Determine if an exception should trigger a retry.

        Args:
            exc: The exception that was raised

        Returns:
            True if the operation should be retried
        """
        ...

    @abstractmethod
    def _build_remote_path(self, remote_path: str) -> str:
        """Build the full remote path including any prefix.

        Args:
            remote_path: The relative remote path

        Returns:
            Full remote path with prefix applied
        """
        ...

    @abstractmethod
    def _do_upload(self, local_path: str, remote_key: str) -> bool:
        """Perform the actual upload operation.

        Args:
            local_path: Path to local file
            remote_key: Full remote key/path

        Returns:
            True if upload succeeded
        """
        ...

    @abstractmethod
    def _do_download(self, remote_key: str, local_path: str) -> bool:
        """Perform the actual download operation.

        Args:
            remote_key: Full remote key/path
            local_path: Path to save file locally

        Returns:
            True if download succeeded
        """
        ...

    @abstractmethod
    def _do_list(self, prefix: str) -> List[str]:
        """Perform the actual list operation.

        Args:
            prefix: Full prefix to filter files

        Returns:
            List of file keys matching the prefix
        """
        ...

    @abstractmethod
    def _do_delete(self, remote_key: str) -> bool:
        """Perform the actual delete operation.

        Args:
            remote_key: Full remote key/path

        Returns:
            True if deletion succeeded
        """
        ...

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file with resilience.

        Args:
            local_path: Path to local file
            remote_path: Destination path (relative to prefix)

        Returns:
            True if upload succeeded
        """
        remote_key = self._build_remote_path(remote_path)
        return self._execute_with_resilience(
            lambda: self._do_upload(local_path, remote_key),
            self._breaker_upload,
            f"{self.get_backend_type()}_upload",
        )

    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file with resilience.

        Args:
            remote_path: Path in storage (relative to prefix)
            local_path: Destination path on local filesystem

        Returns:
            True if download succeeded
        """
        remote_key = self._build_remote_path(remote_path)
        return self._execute_with_resilience(
            lambda: self._do_download(remote_key, local_path),
            self._breaker_download,
            f"{self.get_backend_type()}_download",
        )

    def list_files(self, prefix: str) -> List[str]:
        """List files with resilience.

        Args:
            prefix: Path prefix to filter files

        Returns:
            List of file keys matching the prefix
        """
        full_prefix = self._build_remote_path(prefix)
        return self._execute_with_resilience(
            lambda: self._do_list(full_prefix),
            self._breaker_list,
            f"{self.get_backend_type()}_list",
        )

    def delete_file(self, remote_path: str) -> bool:
        """Delete a file with resilience.

        Args:
            remote_path: Path to file in storage

        Returns:
            True if deletion succeeded
        """
        remote_key = self._build_remote_path(remote_path)
        return self._execute_with_resilience(
            lambda: self._do_delete(remote_key),
            self._breaker_delete,
            f"{self.get_backend_type()}_delete",
        )


# Need to import RetryPolicy and CircuitBreaker for type hints
if TYPE_CHECKING:
    from core.resilience import RetryPolicy, CircuitBreaker


# =============================================================================
# Backend Registry
# =============================================================================

BACKEND_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_backend(
    name: str,
) -> Callable[[Callable[[Dict[str, Any]], Any]], Callable[[Dict[str, Any]], Any]]:
    """Decorator to register a storage backend factory.

    Usage:
        @register_backend("my_backend")
        def my_backend_factory(config: Dict[str, Any]) -> StorageBackend:
            return MyBackend(config)
    """
    def decorator(
        factory: Callable[[Dict[str, Any]], Any],
    ) -> Callable[[Dict[str, Any]], Any]:
        BACKEND_REGISTRY[name.lower()] = factory
        return factory

    return decorator


def list_backends() -> List[str]:
    """Return all registered storage backend identifiers."""
    return sorted(BACKEND_REGISTRY.keys())


def resolve_backend_type(config: Dict[str, Any]) -> str:
    """Determine the backend type from the provided config."""
    backend = config.get("bronze", {}).get("storage_backend", "s3")
    if isinstance(backend, Enum):
        backend_value = backend.value
    else:
        backend_value = str(backend)
    backend_value = backend_value.split(".")[-1]
    return backend_value.lower()


def get_backend_factory(
    backend_type: str,
) -> Callable[[Dict[str, Any]], "StorageBackend"]:
    """Get the factory function for a backend type."""
    factory = BACKEND_REGISTRY.get(backend_type)
    if not factory:
        available = list(BACKEND_REGISTRY.keys())
        error_msg = (
            f"Storage backend '{backend_type}' is not available. "
            f"Available backends: {', '.join(available)}."
        )

        # Provide helpful hints for common missing backends
        if backend_type == "azure" and "azure" not in available:
            error_msg += (
                "\n\nTo enable Azure backend, install the required dependencies:\n"
                "  pip install azure-storage-blob>=12.19.0 azure-identity>=1.15.0\n"
                "Or use the extras:\n"
                "  pip install -e .[azure]"
            )

        raise ValueError(error_msg)
    return factory


def register_storage_backend(
    name: str,
) -> Callable[[Callable[[Dict[str, Any]], Any]], Callable[[Dict[str, Any]], Any]]:
    """Alias for register_backend for backward compatibility."""
    return register_backend(name)


# =============================================================================
# Built-in Backend Factories
# =============================================================================


@register_backend("s3")
def _s3_factory(config: Dict[str, Any]) -> StorageBackend:
    from core.io.storage.s3 import S3Storage

    return S3Storage(config)


@register_backend("local")
def _local_factory(config: Dict[str, Any]) -> StorageBackend:
    from core.io.storage.local import LocalStorage

    return LocalStorage(config)


# Try to register Azure backend (optional dependency)
try:
    @register_backend("azure")
    def _azure_factory(config: Dict[str, Any]) -> StorageBackend:
        from core.io.storage.azure import AzureStorage

        return AzureStorage(config)
except ImportError as exc:
    logger.debug("Azure backend not available: %s", exc)


# =============================================================================
# Backend Factory Function
# =============================================================================

_STORAGE_BACKEND_CACHE: Dict[str, StorageBackend] = {}


def _cache_key(config: Dict[str, Any]) -> str:
    normalized = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def get_storage_backend(
    config: Union[Dict[str, Any], "RootConfig"], use_cache: bool = True
) -> StorageBackend:
    """Get a storage backend instance for the given configuration.

    Args:
        config: Platform configuration dict or RootConfig
        use_cache: Whether to cache and reuse backend instances

    Returns:
        Configured StorageBackend instance
    """
    # normalize typed config into dict for caching key + factory
    cfg_dict = config.model_dump() if hasattr(config, "model_dump") else config
    cache_key = _cache_key(cfg_dict)
    if use_cache and cache_key in _STORAGE_BACKEND_CACHE:
        return _STORAGE_BACKEND_CACHE[cache_key]

    backend_type = resolve_backend_type(cfg_dict)
    factory = get_backend_factory(backend_type)
    backend = factory(cfg_dict)

    if use_cache:
        _STORAGE_BACKEND_CACHE[cache_key] = backend
    return backend
