from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from core.storage.backend import StorageBackend

from .registry import BACKEND_REGISTRY, register_backend


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
    return register_backend(name)
