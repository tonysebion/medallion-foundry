"""Backward compatibility shim for core.observability.

DEPRECATED: Use core.platform.observability instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.observability is deprecated. Use core.platform.observability instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.platform.observability import *  # noqa: F401,F403
