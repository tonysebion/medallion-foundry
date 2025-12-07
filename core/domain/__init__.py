"""L3 Domain Layer - Business domain logic.

This layer contains the business logic for data extraction, transformation,
and pipeline processing. It depends on foundation, platform, and infrastructure.

Subpackages:
    adapters/  - External system adapters (extractors, quality, schema, polybase)
    services/  - Pipeline processing (bronze, silver pipelines)
    catalog/   - Catalog utilities (YAML generator from OpenMetadata)

Import examples:
    from core.domain.adapters.extractors import get_extractor
    from core.domain.services.pipelines.bronze import BronzePipeline
    from core.domain.catalog import generate_yaml_skeleton
"""

from __future__ import annotations

# Expose child packages for attribute access
from core.domain import adapters
from core.domain import services
from core.domain import catalog

__all__ = [
    "adapters",
    "services",
    "catalog",
]
