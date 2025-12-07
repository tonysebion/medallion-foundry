"""Configuration models.

This package contains:
- enums.py: EntityKind, HistoryMode, InputMode, DeleteMode, SchemaMode
- intent.py: BronzeIntent, SilverIntent
- polybase.py: Polybase configuration classes
- dataset.py: DatasetConfig, PathStructure
- root.py: RootConfig, PydanticModels (RootConfig, SilverConfig, etc.)
- environment.py: EnvironmentConfig, S3ConnectionConfig
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
from core.config.models.dataset import DatasetConfig, PathStructure, is_new_intent_config
from core.config.models.environment import EnvironmentConfig, S3ConnectionConfig
from core.config.models.root import (
    DataClassification,
    OwnerConfig,
    SchemaEvolutionConfig,
    ExpectedColumn,
    SchemaConfig,
    StorageBackend,
    SourceType,
    BronzeConfig,
    APIConfig,
    DBConfig,
    FileSourceConfig,
    CustomExtractorConfig,
    RunConfig,
    SourceConfig,
    SilverPartitioning,
    SilverErrorHandling,
    SilverNormalization,
    SilverSchema,
    SilverConfig,
    PlatformConfig,
    RootConfig,
    parse_root_config,
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
    # Environment config
    "EnvironmentConfig",
    "S3ConnectionConfig",
    # Root config (Pydantic models)
    "DataClassification",
    "OwnerConfig",
    "SchemaEvolutionConfig",
    "ExpectedColumn",
    "SchemaConfig",
    "StorageBackend",
    "SourceType",
    "BronzeConfig",
    "APIConfig",
    "DBConfig",
    "FileSourceConfig",
    "CustomExtractorConfig",
    "RunConfig",
    "SourceConfig",
    "SilverPartitioning",
    "SilverErrorHandling",
    "SilverNormalization",
    "SilverSchema",
    "SilverConfig",
    "PlatformConfig",
    "RootConfig",
    "parse_root_config",
]
