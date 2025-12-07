"""Backward compatibility shim for core.resilience.

DEPRECATED: Use core.platform.resilience instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.resilience is deprecated. Use core.platform.resilience instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.platform.resilience import *  # noqa: F401,F403
from core.platform.resilience import __all__  # noqa: F401
