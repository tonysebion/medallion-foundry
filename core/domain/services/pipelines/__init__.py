"""Pipeline processing for Bronze layer.

The Silver layer has been migrated to pipelines/lib/silver.py
which provides a simpler, Ibis-based approach.

Subpackages:
    bronze/  - Bronze extraction and I/O utilities
"""

from __future__ import annotations

from core.domain.services.pipelines import bronze

__all__ = [
    "bronze",
]
