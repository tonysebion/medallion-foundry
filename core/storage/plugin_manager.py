from __future__ import annotations

from typing import Any, Callable, Dict, List

from .registry import BACKEND_REGISTRY, register_backend


def list_backends() -> List[str]:
    """Return all registered storage backend identifiers."""
    return sorted(BACKEND_REGISTRY.keys())


def resolve_backend_type(config: Dict[str, Any]) -> str:
    """Determine the backend type from the provided config."""
    return config.get("bronze", {}).get("storage_backend", "s3").lower()


def get_backend_factory(backend_type: str) -> Callable[[Dict[str, Any]], Any]:
    factory = BACKEND_REGISTRY.get(backend_type)
    if not factory:
        raise ValueError(
            f"Unknown storage backend: '{backend_type}'. "
            f"Supported backends: {', '.join(BACKEND_REGISTRY.keys())}"
        )
    return factory


def register_storage_backend(
    name: str,
) -> Callable[[Callable[[Dict[str, Any]], Any]], Callable[[Dict[str, Any]], Any]]:
    return register_backend(name)
