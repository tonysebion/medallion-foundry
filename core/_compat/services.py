"""Backward compatibility shim for core.services.

DEPRECATED: Use core.domain.services instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.services is deprecated. Use core.domain.services instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.domain.services import *  # noqa: F401,F403
from core.domain.services import __all__  # noqa: F401
