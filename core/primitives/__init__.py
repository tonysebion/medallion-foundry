"""Zero-dependency building blocks for bronze-foundry.

This package contains foundational primitives with no internal dependencies:
- foundations/: LoadPattern, exceptions, logging
- state/: Watermark tracking, file manifests
- catalog/: External catalog hooks, webhooks, tracing

Import from child packages directly:
    from core.primitives.foundations import LoadPattern
    from core.primitives.state import Watermark
    from core.primitives.catalog import notify_catalog
"""

# Expose child packages for attribute access
from . import foundations
from . import state
from . import catalog

__all__ = [
    "foundations",
    "state",
    "catalog",
]
