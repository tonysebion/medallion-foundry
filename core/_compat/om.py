"""Backward compatibility shim for core.om.

DEPRECATED: Use core.platform.om instead.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.om is deprecated. Use core.platform.om instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export everything from new location
from core.platform.om import *  # noqa: F401,F403
from core.platform.om import __all__  # noqa: F401
