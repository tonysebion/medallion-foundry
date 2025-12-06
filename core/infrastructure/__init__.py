"""Cross-cutting infrastructure concerns for bronze-foundry.

This package contains infrastructure components:
- resilience/: Retry policies, circuit breakers, rate limiting, late data handling
- storage/: Storage backends (S3, Azure, Local)
- config/: Configuration loading and validation

Import from child packages directly to avoid circular imports:
    from core.infrastructure.resilience import RetryPolicy
    from core.infrastructure.storage import get_storage_backend
    from core.infrastructure.config import load_configs
"""

# Expose child packages for attribute access
from . import resilience
from . import storage
from . import config

__all__ = [
    "resilience",
    "storage",
    "config",
]
