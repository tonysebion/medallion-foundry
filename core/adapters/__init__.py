"""External system adapters for bronze-foundry.

This package contains integrations with external systems:
- extractors/: Data source extractors (API, DB, file)
- polybase/: PolyBase DDL generation
- schema/: Schema validation (future feature)
- quality/: Quality rules engine (future feature)

Import from child packages directly:
    from core.adapters.extractors.api_extractor import ApiExtractor
    from core.adapters.polybase import generate_polybase_setup
    from core.adapters.schema import SchemaValidator
    from core.adapters.quality import evaluate_rules
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
