"""Backward compatibility shim for core.storage.

DEPRECATED: Use core.infrastructure.io.storage instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.storage is deprecated. Use core.infrastructure.io.storage instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.infrastructure.io.storage import *  # noqa: F401,F403
from core.infrastructure.io.storage import __all__  # noqa: F401
