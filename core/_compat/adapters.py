"""Backward compatibility shim for core.adapters.

DEPRECATED: Use core.domain.adapters instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.adapters is deprecated. Use core.domain.adapters instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.domain.adapters import *  # noqa: F401,F403
from core.domain.adapters import __all__  # noqa: F401
