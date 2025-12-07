"""Backward compatibility shim for core.config.

DEPRECATED: Use core.infrastructure.config instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.config is deprecated. Use core.infrastructure.config instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.infrastructure.config import *  # noqa: F401,F403
from core.infrastructure.config import __all__  # noqa: F401
