"""Backward compatibility shims for deprecated import paths.

This package provides re-exports from old import paths to new canonical locations.
All imports through this package emit deprecation warnings.

Migration Guide:
    OLD: from core.primitives import X
    NEW: from core.foundation.primitives import X

    OLD: from core.resilience import X
    NEW: from core.platform.resilience import X

    OLD: from core.config import X
    NEW: from core.infrastructure.config import X

    OLD: from core.storage import X
    NEW: from core.infrastructure.io.storage import X

    OLD: from core.adapters import X
    NEW: from core.domain.adapters import X

    OLD: from core.services import X
    NEW: from core.domain.services import X
"""

from __future__ import annotations

__all__: list[str] = []
