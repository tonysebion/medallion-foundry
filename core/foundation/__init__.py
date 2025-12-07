"""L0 Foundation Layer - Zero-dependency building blocks.

This layer contains the most fundamental components that have NO internal
dependencies on other core layers. All other layers may depend on this layer.

Subpackages:
    primitives/  - Patterns, exceptions, logging, base models
    state/       - Watermark and manifest tracking
    catalog/     - Hooks, webhooks, tracing for external integrations

Import examples:
    from core.foundation.primitives import LoadPattern
    from core.foundation.state import Watermark
    from core.foundation.catalog import notify_catalog
"""

from __future__ import annotations

# Expose child packages for attribute access
from core.foundation import primitives
from core.foundation import state
from core.foundation import catalog
from core.foundation.time_utils import utc_isoformat, utc_now

__all__ = [
    "primitives",
    "state",
    "catalog",
    "utc_isoformat",
    "utc_now",
]
