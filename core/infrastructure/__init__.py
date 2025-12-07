"""Cross-cutting infrastructure concerns for bronze-foundry.

This package contains infrastructure components:
- resilience/: Retry policies, circuit breakers, rate limiting, late data handling
- config/: Configuration loading and validation

DEPRECATED: Use core.resilience and core.storage instead of core.infrastructure.resilience and core.infrastructure.storage.

Import from canonical packages directly:
    from core.resilience import RetryPolicy, CircuitBreaker
    from core.storage import get_storage_backend
    from core.infrastructure.config import load_configs
"""

# Expose child packages for attribute access
from . import config

# Backward compatibility - redirect to canonical packages
from core import resilience
from core import storage

__all__ = [
    "resilience",
    "storage",
    "config",
]
