"""Backward compatibility shim for core.primitives.

DEPRECATED: Use core.foundation instead.
    - core.foundation.primitives for patterns, exceptions, logging
    - core.foundation.state for watermark, manifest
    - core.foundation.catalog for hooks, webhooks, tracing
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.primitives is deprecated. Use core.foundation.primitives, "
    "core.foundation.state, or core.foundation.catalog instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new locations
from core.foundation.primitives import *  # noqa: F401,F403
from core.foundation.primitives import __all__ as _primitives_all

# Also expose subpackages
from core.foundation import state  # noqa: F401
from core.foundation import catalog  # noqa: F401

# Reconstruct foundations as an alias to primitives for backward compat
foundations = __import__("core.foundation.primitives", fromlist=[""])

__all__ = list(_primitives_all) + ["foundations", "state", "catalog"]
