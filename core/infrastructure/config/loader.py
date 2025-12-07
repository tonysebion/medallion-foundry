"""Legacy loader shim for compatibility."""

from core.runtime.config.loaders import (
    ensure_root_config,
    load_config,
    load_config_with_env,
    load_configs,
)

__all__ = [
    "ensure_root_config",
    "load_config",
    "load_config_with_env",
    "load_configs",
]
