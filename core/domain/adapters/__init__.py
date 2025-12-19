"""External system adapters for bronze-foundry.

This package contains integrations with external systems:
- extractors/: Data source extractors (DB, file)
- polybase/: PolyBase DDL generation
- schema/: Schema validation (future feature)
- quality/: Quality rules engine (future feature)

Note: API extraction has been migrated to pipelines.lib.api.ApiSource

Import from child packages directly:
    from core.domain.adapters.extractors.db_extractor import DbExtractor
    from core.domain.adapters.polybase import generate_polybase_setup
    from core.domain.adapters.schema import SchemaValidator
    from core.domain.adapters.quality import evaluate_rules
"""

# Expose child packages for attribute access
from . import extractors
from . import polybase
from . import schema
from . import quality

__all__ = [
    "extractors",
    "polybase",
    "schema",
    "quality",
]
