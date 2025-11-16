"""Registry for storage backend factories."""

from __future__ import annotations

from typing import Any, Callable, Dict

BACKEND_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_backend(name: str) -> Callable[[Callable[[Dict[str, Any]], Any]], Callable[[Dict[str, Any]], Any]]:
    def decorator(factory: Callable[[Dict[str, Any]], Any]) -> Callable[[Dict[str, Any]], Any]:
        BACKEND_REGISTRY[name.lower()] = factory
        return factory

    return decorator
