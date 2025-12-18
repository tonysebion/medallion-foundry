"""Deprecated module - functionality moved elsewhere.

This module previously contained extractor mixins that have been consolidated:
- RateLimitMixin -> Use RateLimiter.from_config() directly
- Retry/resilience -> Use ResilientExtractorMixin from
  core.domain.adapters.extractors.resilience

This module is kept only for backward compatibility and will be removed
in a future version.
"""

from __future__ import annotations

import warnings

warnings.warn(
    "core.domain.adapters.extractors.mixins is deprecated and empty. "
    "Use ResilientExtractorMixin from core.domain.adapters.extractors.resilience "
    "for retry functionality, or RateLimiter.from_config() for rate limiting.",
    DeprecationWarning,
    stacklevel=2,
)

__all__: list[str] = []
