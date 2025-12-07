"""Backward compatibility shim for the old core.infrastructure deprecated shim.

The old core.infrastructure was itself a deprecated shim. Now that infrastructure
is a proper layer, this shim redirects old usage patterns.

DEPRECATED: Use specific packages instead:
    - core.infrastructure.config for configuration
    - core.platform.resilience for retry/circuit breaker
    - core.infrastructure.io.storage for storage backends
"""

from __future__ import annotations

import warnings

warnings.warn(
    "The old core.infrastructure shim is deprecated. Use "
    "core.infrastructure.config, core.platform.resilience, or "
    "core.infrastructure.io.storage instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Redirect to new canonical packages for old shim behavior
from core.infrastructure import config  # noqa: F401
from core.platform import resilience  # noqa: F401
from core.infrastructure.io import storage  # noqa: F401

__all__ = ["config", "resilience", "storage"]
