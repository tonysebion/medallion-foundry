"""L3 Domain Services - Pipeline processing.

This package contains pipeline processing logic for the Bronze layer.

The Silver layer has been migrated to pipelines/lib/silver.py
which provides a simpler, Ibis-based approach.

Subpackages:
    pipelines/   - Bronze data flow pipelines
    processing/  - Chunk processing utilities
"""

from __future__ import annotations

from core.domain.services import pipelines
from core.domain.services import processing

__all__ = [
    "pipelines",
    "processing",
]
