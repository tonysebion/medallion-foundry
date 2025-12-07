"""Runtime configuration helpers.

DEPRECATED: Use core.config instead. This module redirects to the canonical location.
"""

# Re-export from canonical core.config location
from core.config import (
    # Models
    DataClassification,
    DatasetConfig,
    EnvironmentConfig,
    PlatformConfig,
    RootConfig,
    S3ConnectionConfig,
    SilverConfig,
    StorageBackend,
    SourceType,
    # Loaders
    ensure_root_config,
    load_config,
    load_config_with_env,
    load_configs,
    parse_root_config,
    # Placeholders
    apply_env_substitution,
    resolve_env_vars,
    substitute_env_vars,
    # Migration
    dataset_to_runtime_config,
    is_new_intent_config,
    legacy_to_dataset,
    # Validation
    validate_config_dict,
)

# v2_validation stays in infrastructure for now
from core.infrastructure.config.v2_validation import validate_v2_config_dict

__all__ = [
    "apply_env_substitution",
    "DatasetConfig",
    "DataClassification",
    "EnvironmentConfig",
    "PlatformConfig",
    "RootConfig",
    "S3ConnectionConfig",
    "SilverConfig",
    "StorageBackend",
    "SourceType",
    "dataset_to_runtime_config",
    "is_new_intent_config",
    "legacy_to_dataset",
    "load_config",
    "load_config_with_env",
    "load_configs",
    "ensure_root_config",
    "parse_root_config",
    "resolve_env_vars",
    "substitute_env_vars",
    "validate_config_dict",
    "validate_v2_config_dict",
]
