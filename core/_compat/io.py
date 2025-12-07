"""Backward compatibility shim for core.io.

DEPRECATED: Use core.infrastructure.io instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.io is deprecated. Use core.infrastructure.io instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.infrastructure.io import *  # noqa: F401,F403
from core.infrastructure.io import __all__  # noqa: F401
