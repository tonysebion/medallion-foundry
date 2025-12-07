"""Configuration loading and validation.

This module provides:
- RootConfig, DatasetConfig: Configuration models
- load_config, load_configs: Configuration loaders
- Validation utilities
"""

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
from core.config.migration import (
    dataset_to_runtime_config,
    legacy_to_dataset,
)

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
    # Helpers
    "require_list_of_strings",
    "require_optional_str",
    "require_bool",
    "ensure_bucket_reference",
    # Migration
    "dataset_to_runtime_config",
    "legacy_to_dataset",
]
