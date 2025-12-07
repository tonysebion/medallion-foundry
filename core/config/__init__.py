"""Configuration loading and validation.

This module provides:
- RootConfig, DatasetConfig: Configuration models
- load_config, load_configs: Configuration loaders
- Validation utilities
"""

# Models
from core.config.models.enums import (
    EntityKind,
    HistoryMode,
    InputMode,
    DeleteMode,
    SchemaMode,
)
from core.config.models.intent import BronzeIntent, SilverIntent
from core.config.models.polybase import (
    PolybaseExternalDataSource,
    PolybaseExternalFileFormat,
    PolybaseExternalTable,
    PolybaseSetup,
)
from core.config.models.dataset import (
    DatasetConfig,
    PathStructure,
    is_new_intent_config,
    DEFAULT_BRONZE_BASE,
    DEFAULT_SILVER_BASE,
)
from core.config.models.helpers import (
    require_list_of_strings,
    require_optional_str,
    require_bool,
    ensure_bucket_reference,
)
from core.config.models.environment import EnvironmentConfig, S3ConnectionConfig
from core.config.models.root import (
    DataClassification,
    RootConfig,
    SilverConfig,
    PlatformConfig,
    SourceConfig,
    StorageBackend,
    SourceType,
    parse_root_config,
)

# Loaders
from core.config.loaders import (
    ensure_root_config,
    load_config,
    load_config_with_env,
    load_configs,
)

# Placeholders
from core.config.placeholders import (
    apply_env_substitution,
    resolve_env_vars,
    substitute_env_vars,
)

# Migration
from core.config.migration import (
    dataset_to_runtime_config,
    legacy_to_dataset,
)

# Validation
from core.config.validation import validate_config_dict

__all__ = [
    # Enums
    "EntityKind",
    "HistoryMode",
    "InputMode",
    "DeleteMode",
    "SchemaMode",
    # Intent models
    "BronzeIntent",
    "SilverIntent",
    # Polybase models
    "PolybaseExternalDataSource",
    "PolybaseExternalFileFormat",
    "PolybaseExternalTable",
    "PolybaseSetup",
    # Dataset config
    "DatasetConfig",
    "PathStructure",
    "is_new_intent_config",
    "DEFAULT_BRONZE_BASE",
    "DEFAULT_SILVER_BASE",
    # Environment config
    "EnvironmentConfig",
    "S3ConnectionConfig",
    # Root config (Pydantic)
    "DataClassification",
    "RootConfig",
    "SilverConfig",
    "PlatformConfig",
    "SourceConfig",
    "StorageBackend",
    "SourceType",
    "parse_root_config",
    # Helpers
    "require_list_of_strings",
    "require_optional_str",
    "require_bool",
    "ensure_bucket_reference",
    # Migration
    "dataset_to_runtime_config",
    "legacy_to_dataset",
    # Loaders
    "ensure_root_config",
    "load_config",
    "load_config_with_env",
    "load_configs",
    # Placeholders
    "apply_env_substitution",
    "resolve_env_vars",
    "substitute_env_vars",
    # Validation
    "validate_config_dict",
]
