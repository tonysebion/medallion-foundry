"""L1 Platform Layer - Cross-cutting platform services.

This layer provides cross-cutting concerns that build on the foundation layer.
It includes resilience patterns and observability utilities.

Subpackages:
    resilience/    - Retry policies, circuit breakers, rate limiters
    observability/ - Error handling, logging, tracing utilities
    om/            - OpenMetadata integration client

Import examples:
    from core.platform.resilience import RetryPolicy, CircuitBreaker
    from core.platform.observability import errors, logging, tracing
    from core.platform.om import OpenMetadataClient
"""

from __future__ import annotations

# Expose child packages for attribute access
from core.platform import resilience
from core.platform import observability
from core.platform import om

__all__ = [
    "resilience",
    "observability",
    "om",
]
