"""Extractor registry utilities."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional, TYPE_CHECKING, Type

if TYPE_CHECKING:
    from .base import BaseExtractor

EXTRACTOR_REGISTRY: Dict[str, Type["BaseExtractor"]] = {}


def register_extractor(
    source_type: str,
) -> Callable[[Type["BaseExtractor"]], Type["BaseExtractor"]]:
    """Register an extractor class for a source type."""

    def decorator(cls: Type["BaseExtractor"]) -> Type["BaseExtractor"]:
        EXTRACTOR_REGISTRY[source_type] = cls
        return cls

    return decorator


def get_extractor_class(source_type: str) -> Optional[Type["BaseExtractor"]]:
    """Get the extractor class for a source type."""

    return EXTRACTOR_REGISTRY.get(source_type)


def list_extractor_types() -> List[str]:
    """List all registered extractor types."""

    return list(EXTRACTOR_REGISTRY.keys())
