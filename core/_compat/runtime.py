"""Backward compatibility shim for core.runtime.

DEPRECATED: Use core.infrastructure.runtime instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.runtime is deprecated. Use core.infrastructure.runtime instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.infrastructure.runtime import *  # noqa: F401,F403
from core.infrastructure.runtime import __all__  # noqa: F401
