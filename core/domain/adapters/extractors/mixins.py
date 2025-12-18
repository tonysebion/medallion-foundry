"""Shared mixins for extractor functionality.

These mixins provide common patterns used across multiple extractor implementations
to reduce code duplication and ensure consistent behavior.

Note: For retry/resilience functionality, use ResilientExtractorMixin from
core.domain.adapters.extractors.resilience instead of decorator-based retries.

Note: RateLimitMixin was deprecated in favor of calling RateLimiter.from_config()
directly. The mixin added unnecessary inheritance complexity for a single method.
"""

from __future__ import annotations

# Module kept for backward compatibility. The RateLimitMixin was consolidated
# into direct RateLimiter.from_config() calls in extractors that need rate limiting.

__all__: list[str] = []
