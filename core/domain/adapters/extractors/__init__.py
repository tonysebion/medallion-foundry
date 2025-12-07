"""Extractor implementations for various source types."""
from core.domain.adapters.extractors.factory import (
    get_extractor,
    get_extractor_class,
    ensure_extractors_loaded,
    EXTRACTOR_REGISTRY,
)
from core.domain.adapters.extractors.resilience import ResilientExtractorMixin

# base comes from infrastructure layer
from core.infrastructure.io.extractors import base

# Auto-load extractors when package is imported
ensure_extractors_loaded()

__all__ = [
    "base",
    "get_extractor",
    "get_extractor_class",
    "ensure_extractors_loaded",
    "EXTRACTOR_REGISTRY",
    "ResilientExtractorMixin",
]
