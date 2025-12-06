"""Storage backend abstraction and registry.

This module provides:
- StorageBackend: Abstract base class for all storage backends
- Backend registry: Register and retrieve backend factories
- get_storage_backend(): Factory function to get configured backend
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Callable, Dict, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.typed_models import RootConfig

logger = logging.getLogger(__name__)


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
    from core.storage.s3 import S3Storage
    return S3Storage(config)


@register_backend("local")
def _local_factory(config: Dict[str, Any]) -> StorageBackend:
    from core.storage.local import LocalStorage
    return LocalStorage(config)


# Try to register Azure backend (optional dependency)
try:
    @register_backend("azure")
    def _azure_factory(config: Dict[str, Any]) -> StorageBackend:
        from core.storage.azure import AzureStorage
        return AzureStorage(config)
except ImportError as exc:
    logger.debug("Azure backend not available: %s", exc)


# =============================================================================
# Backend Factory Function
# =============================================================================

_STORAGE_BACKEND_CACHE: Dict[int, StorageBackend] = {}


def _cache_key(config: Dict[str, Any]) -> int:
    return id(config)


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
