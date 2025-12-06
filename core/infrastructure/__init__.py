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

# Don't import child packages at module level to avoid circular imports
# Users should import from child packages directly

__all__ = [
    "resilience",
    "storage",
    "config",
]


def __getattr__(name):
    """Lazy loading of child packages."""
    if name == "resilience":
        from . import resilience
        return resilience
    elif name == "storage":
        from . import storage
        return storage
    elif name == "config":
        from . import config
        return config
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
