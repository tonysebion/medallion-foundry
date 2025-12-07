"""L3 Domain Services - Pipeline processing.

This package contains pipeline processing logic for Bronze and Silver layers.

Subpackages:
    pipelines/   - Bronze and Silver data flow pipelines
    processing/  - Chunk processing utilities
"""

from __future__ import annotations

from core.domain.services import pipelines
from core.domain.services import processing

__all__ = [
    "pipelines",
    "processing",
]
