"""Pipeline processing for Bronze and Silver layers.

Subpackages:
    bronze/  - Bronze extraction and I/O utilities
    silver/  - Silver transformation and promotion
"""

from __future__ import annotations

from core.domain.services.pipelines import bronze
from core.domain.services.pipelines import silver

__all__ = [
    "bronze",
    "silver",
]
