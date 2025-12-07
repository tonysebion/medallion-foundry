"""Backward compatibility shim for core.services.

DEPRECATED: Use core.domain.services instead.
"""

from __future__ import annotations

import warnings
import sys

warnings.warn(
    "core.services is deprecated. Use core.domain.services instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Import the actual module
from core.domain import services as _services

# Re-export submodules
pipelines = _services.pipelines
processing = _services.processing

# Re-export __all__ if it exists
if hasattr(_services, '__all__'):
    __all__ = _services.__all__
else:
    __all__ = ['pipelines', 'processing']
