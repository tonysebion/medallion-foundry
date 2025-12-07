"""L2 Infrastructure Layer - Core infrastructure services.

This layer provides core infrastructure services that build on foundation
and platform layers. It includes configuration, I/O operations, and runtime context.

Subpackages:
    config/  - Configuration loading, validation, and typed models
    io/      - Storage backends, HTTP clients, extractor base classes
    runtime/ - Execution context, paths, metadata, file I/O

Import examples:
    from core.infrastructure.config import load_config, RootConfig
    from core.infrastructure.io.storage import get_storage_backend
    from core.infrastructure.runtime import RunContext, build_run_context
"""

from __future__ import annotations

# Expose child packages for attribute access
from core.infrastructure import config
from core.infrastructure import io
from core.infrastructure import runtime

__all__ = [
    "config",
    "io",
    "runtime",
]
