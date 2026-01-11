"""YAML configuration loader and validation for Bronze-Foundry pipelines.

Allows non-Python developers to define pipelines using simple YAML files.
Also provides validation helpers for catching common configuration errors.

Example YAML (retail_orders.yaml):
    bronze:
      system: retail
      entity: orders
      source_type: file_csv
      source_path: "./data/orders_{run_date}.csv"

    silver:
      unique_columns: [order_id]
      last_updated_column: updated_at
      attributes: [customer_id, order_total, status]

Usage:
    # Command line
    bronze-foundry run ./pipelines/retail_orders.yaml --date 2025-01-15

    # Python API
    from pipelines.lib.config_loader import load_pipeline
    pipeline = load_pipeline("./pipelines/retail_orders.yaml")
    result = pipeline.run("2025-01-15")
"""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipelines.lib.bronze import (
    BronzeSource,
    InputMode,
    LoadPattern,
    SourceType,
    WatermarkSource,
)
from pipelines.lib.deprecation import warn_deprecated_fields
from pipelines.lib.env import load_env_file
from pipelines.lib.silver import (
    DeleteMode,
    EntityKind,
    HistoryMode,
    MODEL_SPECS,
    SilverEntity,
    SilverModel,
    SILVER_MODEL_PRESETS,
)
from pipelines.lib.storage_config import S3_YAML_TO_STORAGE_OPTIONS
from pipelines.lib.validators import (
    ValidationIssue,
    ValidationSeverity,
    format_validation_report,
    validate_and_raise,
    validate_bronze_silver_compatibility,
    validate_bronze_source,
    validate_silver_entity,
)

logger = logging.getLogger(__name__)


# ============================================
# Model Validation Constants - Derived from MODEL_SPECS (single source of truth)
# ============================================

# Models that require unique_columns and last_updated_column
# Derived from MODEL_SPECS.requires_keys
MODELS_REQUIRING_KEYS: frozenset[str] = frozenset(
    name for name, spec in MODEL_SPECS.items() if spec.requires_keys
)

# CDC models that require bronze.load_pattern=cdc
# Derived from MODEL_SPECS.requires_cdc_bronze
CDC_MODELS: frozenset[str] = frozenset(
    name for name, spec in MODEL_SPECS.items() if spec.requires_cdc_bronze
)

# Models that use append_log input_mode (accumulate data over time)
# Derived from MODEL_SPECS.input_mode
APPEND_LOG_MODELS: frozenset[str] = frozenset(
    name for name, spec in MODEL_SPECS.items() if spec.input_mode == "append_log"
)

# ============================================
# Pydantic Configuration Models (formerly validate.py)
# ============================================


class BronzeConfig(BaseModel):
    """Pydantic model for Bronze source configuration validation.

    Use this for type-safe configuration from YAML/JSON files or
    environment variables.

    Example:
        >>> config = BronzeConfig(
        ...     system="claims",
        ...     entity="header",
        ...     source_type="DATABASE_MSSQL",
        ...     target_path="s3://bronze/",
        ...     options={"host": "server.com", "database": "claims_db"}
        ... )
        >>> # Creates BronzeSource from validated config
        >>> source = BronzeSource(**config.model_dump())
    """

    system: str = Field(..., min_length=1, description="Source system name")
    entity: str = Field(..., min_length=1, description="Entity/table name")
    source_type: str = Field(
        ..., description="Source type (DATABASE_MSSQL, FILE_CSV, etc.)"
    )
    target_path: str = Field(
        ..., min_length=1, description="Output path for Bronze data"
    )
    load_pattern: str = Field(default="FULL_SNAPSHOT", description="Load pattern")
    source_path: Optional[str] = Field(
        default=None, description="Source file path for file sources"
    )
    query: Optional[str] = Field(
        default=None, description="SQL query for database sources"
    )
    watermark_column: Optional[str] = Field(
        default=None, description="Column for incremental loads"
    )
    options: Dict[str, Any] = Field(
        default_factory=dict, description="Source-specific options"
    )

    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        """Validate source_type is a known value."""
        valid_types = [
            "DATABASE_MSSQL",
            "DATABASE_POSTGRES",
            "FILE_CSV",
            "FILE_PARQUET",
            "FILE_FIXED_WIDTH",
            "FILE_SPACE_DELIMITED",
            "FILE_JSON",
            "FILE_EXCEL",
            "API_REST",
        ]
        if v.upper() not in valid_types:
            raise ValueError(
                f"source_type '{v}' is not valid. Must be one of: {', '.join(valid_types)}. "
                "See pipelines/examples/ for examples of each source type."
            )
        return v.upper()

    @field_validator("load_pattern")
    @classmethod
    def validate_load_pattern(cls, v: str) -> str:
        """Validate load_pattern is a known value."""
        valid_patterns = ["FULL_SNAPSHOT", "INCREMENTAL_APPEND", "INCREMENTAL_MERGE"]
        if v.upper() not in valid_patterns:
            raise ValueError(
                f"load_pattern '{v}' is not valid. Must be one of: {', '.join(valid_patterns)}. "
                "Use FULL_SNAPSHOT for complete refreshes, INCREMENTAL_APPEND for new records only."
            )
        return v.upper()

    @model_validator(mode="after")
    def validate_database_options(self) -> "BronzeConfig":
        """Validate database sources have required options."""
        if self.source_type in ("DATABASE_MSSQL", "DATABASE_POSTGRES"):
            if "host" not in self.options:
                raise ValueError(
                    "Database sources require 'host' in options. "
                    "Add 'host: your-server.database.com' under bronze.options in your YAML."
                )
            if "database" not in self.options:
                raise ValueError(
                    "Database sources require 'database' in options. "
                    "Add 'database: YourDatabaseName' under bronze.options in your YAML."
                )
        return self

    @model_validator(mode="after")
    def validate_file_source_path(self) -> "BronzeConfig":
        """Validate file sources have source_path."""
        file_types = (
            "FILE_CSV",
            "FILE_PARQUET",
            "FILE_FIXED_WIDTH",
            "FILE_SPACE_DELIMITED",
            "FILE_JSON",
            "FILE_EXCEL",
        )
        if self.source_type in file_types and not self.source_path:
            raise ValueError(
                f"{self.source_type} requires source_path. "
                "Add 'source_path: ./data/myfile.csv' to your bronze config. "
                "Use {{run_date}} placeholder for date-partitioned files."
            )
        return self

    @model_validator(mode="after")
    def validate_incremental_watermark(self) -> "BronzeConfig":
        """Validate incremental loads have watermark_column."""
        if self.load_pattern == "INCREMENTAL_APPEND" and not self.watermark_column:
            raise ValueError(
                "INCREMENTAL_APPEND requires watermark_column to track progress. "
                "Set watermark_column to a timestamp or ID column that increases over time "
                "(e.g., 'updated_at' or 'id')."
            )
        return self


class SilverConfig(BaseModel):
    """Pydantic model for Silver entity configuration validation.

    Example:
        >>> config = SilverConfig(
        ...     source_path="s3://bronze/claims/header/",
        ...     target_path="s3://silver/claims_header/",
        ...     unique_columns=["claim_id"],
        ...     last_updated_column="updated_at"
        ... )
        >>> entity = SilverEntity(**config.model_dump())
    """

    source_path: str = Field(..., min_length=1, description="Bronze source path")
    target_path: str = Field(..., min_length=1, description="Silver output path")
    unique_columns: List[str] = Field(
        ..., min_length=1, description="Columns that uniquely identify each row"
    )
    last_updated_column: str = Field(
        ..., min_length=1, description="Column showing when each row was last modified"
    )
    entity_kind: str = Field(default="STATE", description="Entity kind (STATE, EVENT)")
    history_mode: str = Field(default="CURRENT_ONLY", description="History mode")
    attributes: Optional[List[str]] = Field(
        default=None, description="Columns to include"
    )
    exclude_columns: Optional[List[str]] = Field(
        default=None, description="Columns to exclude"
    )
    validate_source: str = Field(default="skip", description="Source validation mode")

    @field_validator("entity_kind")
    @classmethod
    def validate_entity_kind(cls, v: str) -> str:
        """Validate entity_kind is a known value."""
        valid_kinds = ["STATE", "EVENT"]
        if v.upper() not in valid_kinds:
            raise ValueError(
                f"entity_kind '{v}' is not valid. Must be one of: {', '.join(valid_kinds)}. "
                "Use STATE for dimensions (customers, products), EVENT for facts (orders, clicks)."
            )
        return v.upper()

    @field_validator("history_mode")
    @classmethod
    def validate_history_mode(cls, v: str) -> str:
        """Validate history_mode is a known value."""
        valid_modes = ["CURRENT_ONLY", "FULL_HISTORY"]
        if v.upper() not in valid_modes:
            raise ValueError(
                f"history_mode '{v}' is not valid. Must be one of: {', '.join(valid_modes)}. "
                "Use CURRENT_ONLY for SCD Type 1, FULL_HISTORY for SCD Type 2 with effective dates."
            )
        return v.upper()

    @field_validator("validate_source")
    @classmethod
    def validate_validate_source(cls, v: str) -> str:
        """Validate validate_source mode."""
        valid_modes = ["skip", "warn", "strict"]
        if v.lower() not in valid_modes:
            raise ValueError(
                f"validate_source '{v}' is not valid. Must be one of: {', '.join(valid_modes)}. "
                "Use 'skip' to disable, 'warn' to log warnings, 'strict' to fail on invalid checksums."
            )
        return v.lower()

    @model_validator(mode="after")
    def warn_attributes_and_exclude(self) -> "SilverConfig":
        """Warn if both attributes and exclude_columns are set."""
        if self.attributes and self.exclude_columns:
            logger.warning("Both attributes and exclude_columns are set - use only one")
        return self


class LoggingConfig(BaseModel):
    """Pydantic model for logging configuration.

    Configurable via YAML environment files or pipeline configs.
    Supports both local file logging and structured JSON output for Splunk.

    Example YAML:
        logging:
          level: INFO
          format: json          # 'json' for Splunk, 'console' for human-readable
          file: ./logs/pipeline.log  # Optional file output
          console: true         # Also output to console (default: true)

    Example Python:
        >>> config = LoggingConfig(level="DEBUG", format="console")
        >>> from pipelines.lib.observability import setup_structlog
        >>> setup_structlog(
        ...     verbose=(config.level == "DEBUG"),
        ...     json_format=(config.format == "json"),
        ...     log_file=config.file,
        ... )
    """

    level: str = Field(
        default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)"
    )
    format: str = Field(
        default="console",
        description="Output format: 'json' for Splunk, 'console' for human-readable",
    )
    file: Optional[str] = Field(default=None, description="Optional log file path")
    console: bool = Field(
        default=True, description="Output to console (in addition to file)"
    )
    include_timestamp: bool = Field(
        default=True, description="Include ISO timestamp in logs"
    )
    include_source: bool = Field(
        default=True, description="Include source file/line info"
    )

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level is a known value."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(
                f"logging level '{v}' is not valid. Must be one of: {', '.join(valid_levels)}."
            )
        return v.upper()

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        """Validate format is a known value."""
        valid_formats = ["json", "console", "text"]
        if v.lower() not in valid_formats:
            raise ValueError(
                f"logging format '{v}' is not valid. Must be one of: {', '.join(valid_formats)}. "
                "Use 'json' for Splunk/log aggregation, 'console' for human-readable output."
            )
        return v.lower()


class PipelineSettings(BaseSettings):
    """Environment-based pipeline settings using pydantic-settings.

    Automatically loads from environment variables with PIPELINE_ prefix.

    Example:
        >>> # Set environment variables:
        >>> # PIPELINE_BRONZE_PATH=s3://bronze/
        >>> # PIPELINE_SILVER_PATH=s3://silver/
        >>> # PIPELINE_LOG_LEVEL=DEBUG
        >>> settings = PipelineSettings()
        >>> print(settings.bronze_path)
        s3://bronze/
    """

    bronze_path: str = Field(
        default="./bronze", description="Default Bronze output path"
    )
    silver_path: str = Field(
        default="./silver", description="Default Silver output path"
    )
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(
        default="console", description="Log format: 'json' or 'console'"
    )
    log_file: Optional[str] = Field(default=None, description="Optional log file path")
    max_retries: int = Field(default=3, ge=1, le=10, description="Max retry attempts")
    retry_delay: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Retry delay in seconds"
    )
    chunk_size: int = Field(
        default=100000, ge=1000, description="Default chunk size for writes"
    )
    validate_checksums: bool = Field(
        default=True, description="Validate Bronze checksums"
    )

    model_config = SettingsConfigDict(
        env_prefix="PIPELINE_",
        env_file=".env",
        env_file_encoding="utf-8",
    )


# ============================================
# YAML Configuration Loader
# ============================================

__all__ = [
    # Pydantic validation models
    "BronzeConfig",
    "LoggingConfig",
    "PipelineSettings",
    "SilverConfig",
    "ValidationIssue",
    "ValidationSeverity",
    "format_validation_report",
    "validate_and_raise",
    "validate_bronze_source",
    "validate_silver_entity",
    # YAML loader
    "load_pipeline",
    "load_bronze_from_yaml",
    "load_silver_from_yaml",
    "load_logging_from_yaml",
    "validate_yaml_config",
    "YAMLConfigError",
    "load_with_inheritance",
]


class YAMLConfigError(Exception):
    """Error in YAML pipeline configuration."""

    pass


# ============================================
# YAML String to Enum Mappings (auto-generated)
# ============================================


def _enum_to_map(enum_class: Type[Enum]) -> Dict[str, Enum]:
    """Generate YAML key -> enum mapping from enum values."""
    return {member.value: member for member in enum_class}


T = TypeVar("T", bound=Enum)


def _convert_enum(
    config: Dict[str, Any],
    key: str,
    enum_map: Dict[str, T],
    default: Optional[T] = None,
    fallback_dict: Optional[Dict[str, str]] = None,
) -> Optional[T]:
    """Convert YAML string value to enum with validation.

    This helper consolidates the repeated pattern of:
    1. Get string value from config (with optional fallback dict)
    2. Lowercase and validate against enum map
    3. Raise YAMLConfigError with helpful message if invalid

    Args:
        config: Config dictionary to read from
        key: Key to look up in config
        enum_map: Map of lowercase strings to enum values
        default: Default enum value if key not in config (None = required)
        fallback_dict: Optional dict to check if key not in config (e.g., model_defaults)

    Returns:
        Enum value, or None if key not present and default is None

    Raises:
        YAMLConfigError: If value is invalid
    """
    # Get raw value from config or fallback dict
    raw_value = config.get(key)
    if raw_value is None and fallback_dict:
        raw_value = fallback_dict.get(key)

    # If still None, return default
    if raw_value is None:
        return default

    # Normalize and validate
    value_str = str(raw_value).lower()
    if value_str not in enum_map:
        valid = ", ".join(sorted(enum_map.keys()))
        raise YAMLConfigError(f"Invalid {key} '{raw_value}'. Valid options: {valid}")

    return enum_map[value_str]


# Auto-generated from enum values (ensures maps stay in sync with enums)
SOURCE_TYPE_MAP = _enum_to_map(SourceType)
ENTITY_KIND_MAP = _enum_to_map(EntityKind)
DELETE_MODE_MAP = _enum_to_map(DeleteMode)
SILVER_MODEL_MAP = _enum_to_map(SilverModel)
INPUT_MODE_MAP = _enum_to_map(InputMode)
WATERMARK_SOURCE_MAP = _enum_to_map(WatermarkSource)

# Maps with aliases (base auto-generated + explicit aliases)
LOAD_PATTERN_MAP = _enum_to_map(LoadPattern) | {
    "incremental_append": LoadPattern.INCREMENTAL_APPEND
}
HISTORY_MODE_MAP = _enum_to_map(HistoryMode) | {
    "scd1": HistoryMode.CURRENT_ONLY,
    "scd2": HistoryMode.FULL_HISTORY,
}

# Pattern name aliases for validation (incremental_append is internal, exposed as "incremental")
_LOAD_PATTERN_ALIASES = {"incremental_append": "incremental"}


def _normalize_load_pattern_name(name: str) -> str:
    """Normalize load pattern name for validation comparisons.

    Maps internal enum values (e.g., 'incremental_append') to their canonical
    names used in MODEL_SPECS validation (e.g., 'incremental').
    """
    return _LOAD_PATTERN_ALIASES.get(name.lower(), name.lower())


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary (parent config)
        override: Override dictionary (child config)

    Returns:
        New dictionary with merged values
    """
    result = dict(base)

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Recursively merge nested dicts
            result[key] = _deep_merge(result[key], value)
        else:
            # Override the value
            result[key] = value

    return result


def load_with_inheritance(
    config_path: Union[str, Path],
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """Load a YAML config, resolving parent inheritance via 'extends' key.

    Child configs can extend parent configs using:
        extends: ./base_config.yaml

    The child config is deep-merged with the parent, with child values
    taking precedence over parent values.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Tuple of (merged_config, parent_config_or_none)

    Raises:
        FileNotFoundError: If config or parent file doesn't exist
        YAMLConfigError: If YAML is invalid

    Example YAML:
        # child.yaml
        extends: ./base.yaml

        bronze:
          entity: specific_table  # Override parent's entity
          # system, host, database inherited from parent
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        try:
            child_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise YAMLConfigError(f"Invalid YAML syntax in {config_path}: {e}")

    if "extends" not in child_config:
        return child_config, None

    # Resolve parent path relative to child config file
    parent_ref = child_config["extends"]
    parent_path = (config_path.parent / parent_ref).resolve()

    if not parent_path.exists():
        raise FileNotFoundError(
            f"Parent config not found: {parent_path} (referenced from {config_path})"
        )

    with open(parent_path, "r", encoding="utf-8") as f:
        try:
            parent_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise YAMLConfigError(f"Invalid YAML syntax in {parent_path}: {e}")

    # Deep merge: child values override parent values
    merged = _deep_merge(parent_config, child_config)

    # Remove the 'extends' key from the final merged config
    merged.pop("extends", None)

    logger.debug(
        "Loaded config with inheritance",
        extra={
            "child_path": str(config_path),
            "parent_path": str(parent_path),
        },
    )

    return merged, parent_config


def load_logging_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
) -> LoggingConfig:
    """Create a LoggingConfig from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'logging' section)
        config_dir: Directory containing the YAML file (for relative path resolution)

    Returns:
        Configured LoggingConfig instance

    Example YAML:
        logging:
          level: INFO
          format: json          # 'json' for Splunk, 'console' for human-readable
          file: ./logs/pipeline.log  # Optional file output
          console: true         # Also output to console
    """
    config_dir = config_dir or Path.cwd()

    # Resolve file path if provided
    log_file = config.get("file")
    if log_file:
        log_file = _resolve_path(log_file, config_dir)

    # Build LoggingConfig using Pydantic validation
    try:
        return LoggingConfig(
            level=config.get("level", "INFO"),
            format=config.get("format", "console"),
            file=log_file,
            console=config.get("console", True),
            include_timestamp=config.get("include_timestamp", True),
            include_source=config.get("include_source", True),
        )
    except ValueError as e:
        raise YAMLConfigError(f"Invalid logging configuration: {e}")


def _resolve_path(path: str, config_dir: Path) -> str:
    """Resolve relative paths based on config file location.

    Paths starting with "./" are resolved relative to the YAML config file.
    Paths with environment variables like ${VAR} are left as-is for runtime expansion.
    Absolute paths and paths starting with s3:// or abfs:// are unchanged.
    """
    if not path:
        return path

    # Cloud paths are unchanged
    if path.startswith(("s3://", "abfs://", "http://", "https://")):
        return path

    # Absolute paths are unchanged
    if os.path.isabs(path):
        return path

    # Relative paths: resolve from config file directory
    if path.startswith("./") or path.startswith("../"):
        resolved = config_dir / path
        return str(resolved)

    # Other paths (like templates with {run_date}) - leave as-is
    return path


def load_bronze_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
) -> BronzeSource:
    """Create a BronzeSource from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'bronze' section)
        config_dir: Directory containing the YAML file (for relative path resolution)

    Returns:
        Configured BronzeSource instance

    Raises:
        YAMLConfigError: If configuration is invalid
    """
    config_dir = config_dir or Path.cwd()

    # Collect ALL validation errors before failing (user-friendly!)
    errors: List[str] = []

    # Check required fields
    if "system" not in config:
        errors.append("bronze.system is required (e.g., 'CRM', 'ERP', 'Salesforce')")
    if "entity" not in config:
        errors.append("bronze.entity is required (e.g., 'customers', 'orders')")
    if "source_type" not in config:
        errors.append(
            "bronze.source_type is required (e.g., 'file_csv', 'database_mssql')"
        )

    # If required fields are missing, report ALL of them at once
    if errors:
        raise YAMLConfigError(
            "Missing required fields in bronze section:\n  - " + "\n  - ".join(errors)
        )

    from typing import cast

    # Convert enum fields using shared helper
    source_type_enum = _convert_enum(config, "source_type", SOURCE_TYPE_MAP)
    if source_type_enum is None:
        raise YAMLConfigError("source_type is required")
    source_type = cast(SourceType, source_type_enum)

    load_pattern_enum = _convert_enum(
        config, "load_pattern", LOAD_PATTERN_MAP, LoadPattern.FULL_SNAPSHOT
    )
    if load_pattern_enum is None:
        raise YAMLConfigError("load_pattern is required")
    load_pattern = cast(LoadPattern, load_pattern_enum)

    from typing import cast

    input_mode_val = _convert_enum(config, "input_mode", INPUT_MODE_MAP)
    input_mode = cast(Optional[InputMode], input_mode_val)

    watermark_source_enum = _convert_enum(
        config, "watermark_source", WATERMARK_SOURCE_MAP, WatermarkSource.DESTINATION
    )
    if watermark_source_enum is None:
        raise YAMLConfigError("watermark_source is required")
    watermark_source = cast(WatermarkSource, watermark_source_enum)

    # Resolve source_path relative to config file
    source_path = config.get("source_path", "")
    if source_path:
        source_path = _resolve_path(source_path, config_dir)

    # Resolve target_path relative to config file
    target_path = config.get("target_path", "")
    if target_path:
        target_path = _resolve_path(target_path, config_dir)

    # Build options dict from YAML
    options = dict(config.get("options", {}))

    # Merge top-level S3 storage options into options dict
    # These take precedence over options.s3_* for cleaner YAML syntax
    for yaml_key, options_key in S3_YAML_TO_STORAGE_OPTIONS:
        if yaml_key in config:
            options[options_key] = config[yaml_key]

    # Build BronzeSource
    return BronzeSource(
        system=config["system"],
        entity=config["entity"],
        source_type=source_type,
        source_path=source_path,
        target_path=target_path,
        load_pattern=load_pattern,
        input_mode=input_mode,
        # Accept either 'incremental_column' (preferred) or 'watermark_column' (legacy)
        watermark_column=config.get("incremental_column")
        or config.get("watermark_column"),
        watermark_source=watermark_source,
        connection=config.get("connection"),
        host=config.get("host"),
        database=config.get("database"),
        query=config.get("query"),
        options=options,
        partition_by=config.get("partition_by", ["_load_date"]),
        chunk_size=config.get("chunk_size"),
        full_refresh_days=config.get("full_refresh_days"),
        write_checksums=config.get("write_checksums", True),
        write_metadata=config.get("write_metadata", True),
    )


def _validate_silver_model_config(
    config: Dict[str, Any],
    bronze: Optional[BronzeSource],
) -> None:
    """Validate Silver config against model requirements and Bronze compatibility.

    Collects all errors and raises once with all issues.

    Args:
        config: Silver section from YAML
        bronze: Optional Bronze source for compatibility checks

    Raises:
        YAMLConfigError: If validation fails (with all errors listed)
    """
    model_str = config.get("model", "").lower()
    model_spec = MODEL_SPECS.get(model_str) if model_str else None
    requires_keys = model_spec.requires_keys if model_spec else True

    errors: List[str] = []

    # Check required fields based on model spec
    # Accept both new names (unique_columns, last_updated_column) and legacy names (natural_keys, change_timestamp)
    if requires_keys:
        has_unique_cols = "unique_columns" in config or "natural_keys" in config
        has_last_updated = (
            "last_updated_column" in config or "change_timestamp" in config
        )
        if not has_unique_cols:
            errors.append(
                f"silver.unique_columns is required for model '{model_str or 'default'}'. "
                "This is the column(s) that uniquely identify each row (like order_id). "
                "Run 'python -m pipelines inspect-source --file your_data.csv' for suggestions. "
                "Use model: periodic_snapshot if you don't need deduplication."
            )
        if not has_last_updated:
            errors.append(
                f"silver.last_updated_column is required for model '{model_str or 'default'}'. "
                "This is the column showing when each row was last modified (like updated_at). "
                "Run 'python -m pipelines inspect-source --file your_data.csv' for suggestions. "
                "Use model: periodic_snapshot if you don't need change tracking."
            )

    # Bronze pattern validation using MODEL_SPECS
    if bronze is not None and model_spec and model_spec.valid_bronze_patterns:
        bronze_pattern_name = _normalize_load_pattern_name(bronze.load_pattern.value)
        if bronze_pattern_name not in model_spec.valid_bronze_patterns:
            valid_patterns = ", ".join(model_spec.valid_bronze_patterns)
            errors.append(
                f"Model '{model_str}' requires bronze.load_pattern to be one of: {valid_patterns} "
                f"(currently: {bronze.load_pattern.value})"
            )

    # Delete mode requires CDC
    delete_mode_str = config.get("delete_mode", "ignore").lower()
    if delete_mode_str in ("tombstone", "hard_delete"):
        is_cdc = (bronze is not None and bronze.load_pattern == LoadPattern.CDC) or (
            model_spec is not None and model_spec.requires_cdc_bronze
        )
        if not is_cdc:
            errors.append(
                f"delete_mode: {delete_mode_str} only makes sense with CDC data. "
                "Either set bronze.load_pattern: cdc or use a cdc_* Silver model."
            )

    # Validate required fields
    if not config.get("domain"):
        errors.append("silver.domain is required")
    if not config.get("subject"):
        errors.append("silver.subject is required")

    if errors:
        raise YAMLConfigError(
            "Configuration errors in silver section:\n  - " + "\n  - ".join(errors)
        )


def _emit_silver_warnings(
    config: Dict[str, Any],
    bronze: Optional[BronzeSource],
) -> None:
    """Emit warnings for suboptimal Bronze/Silver configurations.

    Args:
        config: Silver section from YAML
        bronze: Optional Bronze source for compatibility checks
    """
    model_str = config.get("model", "").lower()
    model_spec = MODEL_SPECS.get(model_str) if model_str else None

    # Warning for inefficient Bronze pattern with Silver model
    if bronze is not None and model_spec and model_spec.warns_on_bronze_patterns:
        bronze_pattern_name = _normalize_load_pattern_name(bronze.load_pattern.value)
        if bronze_pattern_name in model_spec.warns_on_bronze_patterns:
            if bronze_pattern_name == "full_snapshot":
                warnings.warn(
                    f"bronze.load_pattern: {bronze.load_pattern.value} with silver.model: {model_str} is inefficient. "
                    "Each Bronze partition contains ALL records (overlapping data). Silver will union "
                    "all partitions and dedupe, which works but wastes storage and processing time. "
                    "Consider: 1) Use periodic_snapshot if you just need the latest snapshot, or "
                    "2) Change Bronze to incremental with watermark_column for efficient accumulation.",
                    UserWarning,
                    stacklevel=3,
                )
            elif bronze_pattern_name == "cdc":
                warnings.warn(
                    f"bronze.load_pattern: cdc but silver.model: {model_str or 'default'} is not CDC-aware. "
                    "Consider using a cdc_* model (cdc_current, cdc_history, etc.) to properly handle "
                    "insert/update/delete operations.",
                    UserWarning,
                    stacklevel=3,
                )

    # Warning: Bronze CDC pattern with no model specified
    if (
        bronze is not None
        and bronze.load_pattern == LoadPattern.CDC
        and model_spec is None
    ):
        warnings.warn(
            f"bronze.load_pattern: cdc but silver.model: {model_str or 'default'} is not CDC-aware. "
            "Consider using a cdc_* model (cdc_current, cdc_history, etc.) to properly handle "
            "insert/update/delete operations.",
            UserWarning,
            stacklevel=3,
        )

    # Warning: Explicit input_mode conflicts with Bronze pattern
    if bronze is not None and "input_mode" in config:
        explicit_input = config["input_mode"].lower()
        expected_input = (
            "append_log"
            if bronze.load_pattern in (LoadPattern.INCREMENTAL_APPEND, LoadPattern.CDC)
            else "replace_daily"
        )
        if explicit_input != expected_input:
            warnings.warn(
                f"Explicit silver.input_mode: {explicit_input} conflicts with "
                f"bronze.load_pattern: {bronze.load_pattern.value} (expected: {expected_input}). "
                "This may cause unexpected behavior. "
                "Note: input_mode is typically auto-derived from the model and Bronze pattern.",
                UserWarning,
                stacklevel=3,
            )


def _normalize_list_field(config: Dict[str, Any], key: str) -> Optional[List[str]]:
    """Normalize a field that can be string or list to list or None."""
    value = config.get(key)
    if value is None:
        return None
    return [value] if isinstance(value, str) else value


def _build_silver_storage_options(
    config: Dict[str, Any],
    bronze: Optional[BronzeSource],
) -> Optional[Dict[str, Any]]:
    """Build storage options from Silver config or Bronze fallback.

    Args:
        config: Silver section from YAML
        bronze: Optional Bronze source for fallback options

    Returns:
        Storage options dict or None
    """
    # Build from Silver config's top-level S3 options
    storage_options: Optional[Dict[str, Any]] = None
    for yaml_key, storage_key in S3_YAML_TO_STORAGE_OPTIONS:
        if yaml_key in config:
            if storage_options is None:
                storage_options = {}
            storage_options[storage_key] = config[yaml_key]

    # Auto-wire from Bronze if Silver doesn't have its own
    if storage_options is None and bronze and bronze.options:
        storage_keys = frozenset(
            {
                "s3_signature_version",
                "s3_addressing_style",
                "s3_verify_ssl",
                "endpoint_url",
                "key",
                "secret",
                "region",
            }
        )
        storage_options = {k: v for k, v in bronze.options.items() if k in storage_keys}
        if not storage_options:
            storage_options = None

    return storage_options


# Mapping of handle_deletes values to delete_mode values
# handle_deletes is the user-friendly name, delete_mode is the internal name
HANDLE_DELETES_MAP: Dict[str, str] = {
    "ignore": "ignore",
    "flag": "tombstone",  # "flag" = add _deleted flag (more intuitive name)
    "remove": "hard_delete",  # "remove" = remove from Silver (more intuitive name)
    # Also accept the internal names for backwards compatibility
    "tombstone": "tombstone",
    "hard_delete": "hard_delete",
}


def _build_cdc_model_defaults(config: Dict[str, Any]) -> Dict[str, str]:
    """Build model defaults for the unified CDC model based on explicit options.

    When model: cdc is used, the user can specify:
    - keep_history: true/false (default: false)
      - false = current_only (SCD Type 1)
      - true = full_history (SCD Type 2)
    - handle_deletes: ignore/flag/remove (default: ignore)
      - ignore = filter out delete records
      - flag = add _deleted=true flag (tombstone)
      - remove = remove records from Silver (hard_delete)

    Args:
        config: Silver config dict

    Returns:
        Dict with entity_kind, history_mode, input_mode, delete_mode
    """
    # Parse keep_history option (default: false = current_only)
    keep_history = config.get("keep_history", False)
    if isinstance(keep_history, str):
        keep_history = keep_history.lower() in ("true", "yes", "1")
    history_mode = "full_history" if keep_history else "current_only"

    # Parse handle_deletes option (default: ignore)
    handle_deletes = str(config.get("handle_deletes", "ignore")).lower()
    if handle_deletes not in HANDLE_DELETES_MAP:
        valid_options = ", ".join(
            sorted(set(HANDLE_DELETES_MAP.keys()) - {"tombstone", "hard_delete"})
        )
        raise YAMLConfigError(
            f"Invalid handle_deletes value '{handle_deletes}'. "
            f"Valid options: {valid_options}"
        )
    delete_mode = HANDLE_DELETES_MAP[handle_deletes]

    return {
        "entity_kind": "state",
        "history_mode": history_mode,
        "input_mode": "append_log",
        "delete_mode": delete_mode,
    }


def load_silver_from_yaml(
    config: Dict[str, Any],
    config_dir: Optional[Path] = None,
    bronze: Optional[BronzeSource] = None,
) -> SilverEntity:
    """Create a SilverEntity from YAML configuration.

    Args:
        config: Dictionary from parsed YAML (the 'silver' section)
        config_dir: Directory containing the YAML file (for relative path resolution)
        bronze: Optional BronzeSource for auto-wiring source_path

    Returns:
        Configured SilverEntity instance

    Raises:
        YAMLConfigError: If configuration is invalid
    """
    config_dir = config_dir or Path.cwd()

    # Validate configuration (raises YAMLConfigError with all issues)
    _validate_silver_model_config(config, bronze)
    _emit_silver_warnings(config, bronze)

    # Expand model preset for defaults
    model_defaults: Dict[str, str] = {}
    if "model" in config:
        model = _convert_enum(config, "model", SILVER_MODEL_MAP)
        if model:
            if model.value == "cdc":
                # Handle unified CDC model with explicit options
                model_defaults = _build_cdc_model_defaults(config)
                logger.debug(
                    "Expanding unified CDC model with options",
                    extra={"defaults": model_defaults},
                )
            else:
                model_defaults = SILVER_MODEL_PRESETS[model.value]
                logger.debug(
                    "Expanding Silver model preset",
                    extra={"model": model.value, "defaults": model_defaults},
                )

    # Convert enum fields with model preset fallback
    from typing import cast

    entity_kind_enum = _convert_enum(
        config, "entity_kind", ENTITY_KIND_MAP, EntityKind.STATE, model_defaults
    )
    if entity_kind_enum is None:
        raise YAMLConfigError("entity_kind is required")
    entity_kind = cast(EntityKind, entity_kind_enum)

    history_mode_enum = _convert_enum(
        config,
        "history_mode",
        HISTORY_MODE_MAP,
        HistoryMode.CURRENT_ONLY,
        model_defaults,
    )
    if history_mode_enum is None:
        raise YAMLConfigError("history_mode is required")
    history_mode = cast(HistoryMode, history_mode_enum)

    from typing import cast

    input_mode_val = _convert_enum(
        config, "input_mode", INPUT_MODE_MAP, None, model_defaults
    )
    input_mode = cast(Optional[InputMode], input_mode_val)

    delete_mode_enum = _convert_enum(
        config, "delete_mode", DELETE_MODE_MAP, DeleteMode.IGNORE, model_defaults
    )
    if delete_mode_enum is None:
        raise YAMLConfigError("delete_mode is required")
    delete_mode = cast(DeleteMode, delete_mode_enum)

    # Resolve paths
    source_path = config.get("source_path", "")
    if source_path:
        source_path = _resolve_path(source_path, config_dir)
    target_path = config.get("target_path", "")
    if target_path:
        target_path = _resolve_path(target_path, config_dir)

    # Normalize list fields
    # Accept both new names and legacy names (unique_columns/natural_keys, last_updated_column/change_timestamp)
    unique_columns = _normalize_list_field(
        config, "unique_columns"
    ) or _normalize_list_field(config, "natural_keys")
    last_updated_column = config.get("last_updated_column") or config.get(
        "change_timestamp"
    )
    attributes = _normalize_list_field(config, "attributes")
    exclude_columns = _normalize_list_field(config, "exclude_columns")
    partition_by = _normalize_list_field(config, "partition_by")
    output_formats = _normalize_list_field(config, "output_formats") or ["parquet"]

    # Validate column_mapping type
    column_mapping = config.get("column_mapping")
    if column_mapping is not None and not isinstance(column_mapping, dict):
        raise YAMLConfigError(
            "column_mapping must be an object with {old_column: new_column} pairs"
        )

    # Parse cdc_options
    cdc_options = None
    if "cdc_options" in config:
        cdc_options = config["cdc_options"]
        if not isinstance(cdc_options, dict):
            raise YAMLConfigError("cdc_options must be an object")
        if "operation_column" not in cdc_options:
            raise YAMLConfigError("cdc_options.operation_column is required")

    return SilverEntity(
        unique_columns=unique_columns,
        last_updated_column=last_updated_column,
        domain=config.get("domain", ""),
        subject=config.get("subject", ""),
        source_path=source_path,
        target_path=target_path,
        attributes=attributes,
        exclude_columns=exclude_columns,
        column_mapping=column_mapping,
        entity_kind=entity_kind,
        history_mode=history_mode,
        input_mode=input_mode,
        delete_mode=delete_mode,
        cdc_options=cdc_options,
        partition_by=partition_by,
        output_formats=output_formats,
        parquet_compression=config.get("parquet_compression", "snappy"),
        validate_source=config.get("validate_source", "skip"),
        storage_options=_build_silver_storage_options(config, bronze),
    )


def load_pipeline(
    config_path: Union[str, Path],
) -> "PipelineFromYAML":
    """Load a pipeline from a YAML configuration file.

    Supports parent-child inheritance via the 'extends' key:
        extends: ./base_config.yaml

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        PipelineFromYAML instance with run() methods

    Raises:
        YAMLConfigError: If configuration is invalid
        FileNotFoundError: If config file doesn't exist

    Example:
        pipeline = load_pipeline("./pipelines/retail_orders.yaml")
        result = pipeline.run("2025-01-15")

        # Child config inheriting from parent:
        # child.yaml:
        #   extends: ./base.yaml
        #   bronze:
        #     entity: specific_table  # Override
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config_dir = config_path.parent.resolve()

    # Load with inheritance support
    config, parent_config = load_with_inheritance(config_path)

    if parent_config:
        logger.info(
            "Loaded config with inheritance from parent",
            extra={"config_path": str(config_path)},
        )

    if not config:
        raise YAMLConfigError("Empty configuration file")

    # Check for deprecated fields and emit warnings
    warn_deprecated_fields(config, str(config_path))

    # Load environment file if specified (before any ${VAR} expansion happens)
    if "env_file" in config:
        env_file_path = config["env_file"]
        # Resolve relative to config file directory
        if not os.path.isabs(env_file_path):
            env_file_path = config_dir / env_file_path
        else:
            env_file_path = Path(env_file_path)

        if not env_file_path.exists():
            raise YAMLConfigError(
                f"Environment file not found: {env_file_path} "
                f"(referenced from {config_path})"
            )

        loaded = load_env_file(env_file_path)
        if loaded:
            logger.info(
                "Loaded environment variables from file",
                extra={"env_file": str(env_file_path)},
            )
        else:
            logger.warning(
                "Failed to load environment file",
                extra={"env_file": str(env_file_path)},
            )

    # Parse logging section (optional)
    logging_config = None
    if "logging" in config:
        logging_config = load_logging_from_yaml(config["logging"], config_dir)

    # Parse bronze section
    bronze = None
    if "bronze" in config:
        bronze = load_bronze_from_yaml(config["bronze"], config_dir)

    # Parse silver section
    silver = None
    if "silver" in config:
        silver = load_silver_from_yaml(config["silver"], config_dir, bronze)

    if not bronze and not silver:
        raise YAMLConfigError(
            "Configuration must have at least 'bronze' or 'silver' section"
        )

    # Validate Bronze/Silver compatibility
    if bronze and silver and "bronze" in config and "silver" in config:
        compatibility_issues = validate_bronze_silver_compatibility(
            config["bronze"], config["silver"]
        )
        for issue in compatibility_issues:
            if issue.severity == ValidationSeverity.ERROR:
                raise YAMLConfigError(str(issue))
            else:
                logger.warning(str(issue))

    # Create and return pipeline wrapper
    return PipelineFromYAML(
        bronze=bronze,
        silver=silver,
        config_path=config_path,
        config=config,
        logging_config=logging_config,
    )


def validate_yaml_config(config_path: Union[str, Path]) -> List[str]:
    """Validate a YAML configuration file without creating the pipeline.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        List of validation errors (empty if valid)
    """
    errors: List[str] = []

    try:
        pipeline = load_pipeline(config_path)

        # Validate bronze
        if pipeline.bronze:
            bronze_issues = pipeline.bronze.validate(check_connectivity=False)
            errors.extend(f"bronze: {issue}" for issue in bronze_issues)

        # Validate silver
        if pipeline.silver:
            silver_issues = pipeline.silver.validate(check_source=False)
            errors.extend(f"silver: {issue}" for issue in silver_issues)

    except YAMLConfigError as e:
        errors.append(str(e))
    except FileNotFoundError as e:
        errors.append(str(e))
    except ValueError as e:
        errors.append(str(e))

    return errors


class PipelineFromYAML:
    """Pipeline wrapper created from YAML configuration.

    Provides the same interface as the Python Pipeline class.
    """

    def __init__(
        self,
        bronze: Optional[BronzeSource],
        silver: Optional[SilverEntity],
        config_path: Path,
        config: Dict[str, Any],
        logging_config: Optional[LoggingConfig] = None,
    ):
        self.bronze = bronze
        self.silver = silver
        self.config_path = config_path
        self.config = config
        self.logging_config = logging_config

        # If both bronze and silver are present, wire them together
        if bronze and silver:
            # Set Bronze references for source_path placeholder substitution
            # This allows {system} and {entity} in Silver's source_path to resolve
            silver._bronze_system = bronze.system
            silver._bronze_entity = bronze.entity

            if not silver.source_path:
                # Auto-wire Silver source from Bronze target
                # Substitute {system} and {entity} placeholders with actual values
                bronze_target = bronze.target_path.format(
                    system=bronze.system,
                    entity=bronze.entity,
                    run_date="{run_date}",  # Keep {run_date} for later substitution
                )
                silver.source_path = bronze_target.rstrip("/") + "/*.parquet"

        if silver and not silver.target_path:
            # Auto-generate Silver target using Hive-style partitioning
            silver.target_path = f"./silver/domain={silver.domain}/subject={silver.subject}/dt={{run_date}}/"

        # Auto-wire input_mode from Bronze to Silver if not explicitly set
        if (
            bronze
            and silver
            and silver.input_mode is None
            and bronze.input_mode is not None
        ):
            silver.input_mode = bronze.input_mode

    def setup_logging(self) -> None:
        """Configure logging based on the YAML logging config.

        This initializes structlog with settings from the logging section
        of the pipeline YAML file. Call this before running the pipeline
        to enable proper structured logging.

        If no logging config is present, this is a no-op.

        Example:
            pipeline = load_pipeline("./my_pipeline.yaml")
            pipeline.setup_logging()  # Configure logging from YAML
            result = pipeline.run("2025-01-15")
        """
        if not self.logging_config:
            return

        from pipelines.lib.observability import setup_structlog

        setup_structlog(
            verbose=(self.logging_config.level == "DEBUG"),
            json_format=(self.logging_config.format == "json"),
            log_file=self.logging_config.file,
        )

    @property
    def name(self) -> str:
        """Get pipeline name from config or derive from file."""
        if "name" in self.config:
            name_val = self.config["name"]
            return str(name_val) if name_val is not None else "unnamed"
        if self.bronze:
            return f"{self.bronze.system}.{self.bronze.entity}"
        return self.config_path.stem

    def run(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run the full Bronze -> Silver pipeline.

        Args:
            run_date: The date for this pipeline run (YYYY-MM-DD format)
            dry_run: If True, validate without executing
            target_override: Override target paths for local development

        Returns:
            Dictionary with results from both layers
        """
        from pipelines.lib.trace import step, get_tracer, PipelineStep

        tracer = get_tracer()

        # Log config loaded (config was already loaded in __init__)
        with step(PipelineStep.LOAD_CONFIG, str(self.config_path)):
            tracer.detail(f"Pipeline: {self.name}")
            if self.bronze:
                tracer.detail(f"Bronze: {self.bronze.system}.{self.bronze.entity}")
            if self.silver:
                tracer.detail(f"Silver: {self.silver.domain}.{self.silver.subject}")

        result: Dict[str, Any] = {}

        if self.bronze:
            result["bronze"] = self.bronze.run(
                run_date,
                dry_run=dry_run,
                target_override=target_override,
            )

        if self.silver:
            result["silver"] = self.silver.run(
                run_date,
                dry_run=dry_run,
                target_override=target_override,
            )

        return result

    def run_bronze(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run only the Bronze extraction layer.

        Args:
            run_date: The date for this pipeline run
            dry_run: If True, validate without executing
            target_override: Override target path

        Returns:
            Bronze layer results
        """
        if not self.bronze:
            raise ValueError("This pipeline has no bronze layer defined")

        return self.bronze.run(
            run_date,
            dry_run=dry_run,
            target_override=target_override,
        )

    def run_silver(
        self,
        run_date: str,
        *,
        dry_run: bool = False,
        target_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run only the Silver curation layer.

        Args:
            run_date: The date for this pipeline run
            dry_run: If True, validate without executing
            target_override: Override target path

        Returns:
            Silver layer results
        """
        if not self.silver:
            raise ValueError("This pipeline has no silver layer defined")

        return self.silver.run(
            run_date,
            dry_run=dry_run,
            target_override=target_override,
        )

    def validate(self, run_date: Optional[str] = None) -> List[str]:
        """Validate the pipeline configuration.

        Args:
            run_date: Optional date for path validation

        Returns:
            List of validation issues (empty if valid)
        """
        issues: List[str] = []

        if self.bronze:
            bronze_issues = self.bronze.validate(run_date, check_connectivity=False)
            issues.extend(f"bronze: {issue}" for issue in bronze_issues)

        if self.silver:
            silver_issues = self.silver.validate(run_date, check_source=False)
            issues.extend(f"silver: {issue}" for issue in silver_issues)

        return issues

    def explain(self) -> str:
        """Return a human-readable explanation of what this pipeline does."""
        lines = [
            f"Pipeline: {self.name}",
            f"Config:   {self.config_path}",
            "",
        ]

        if self.bronze:
            input_mode_str = (
                self.bronze.input_mode.value if self.bronze.input_mode else "(not set)"
            )
            lines.extend(
                [
                    "BRONZE LAYER:",
                    f"  System:       {self.bronze.system}",
                    f"  Entity:       {self.bronze.entity}",
                    f"  Source Type:  {self.bronze.source_type.value}",
                    f"  Source Path:  {self.bronze.source_path or '(from database)'}",
                    f"  Target Path:  {self.bronze.target_path}",
                    f"  Load Pattern: {self.bronze.load_pattern.value}",
                    f"  Input Mode:   {input_mode_str}",
                    "",
                ]
            )

        if self.silver:
            lines.extend(
                [
                    "SILVER LAYER:",
                    f"  Unique Columns:     {', '.join(self.silver.unique_columns or [])}",
                    f"  Last Updated Col:   {self.silver.last_updated_column}",
                    f"  Entity Kind:  {self.silver.entity_kind.value}",
                    f"  History Mode: {self.silver.history_mode.value}",
                    f"  Source Path:  {self.silver.source_path}",
                    f"  Target Path:  {self.silver.target_path}",
                    "",
                ]
            )

        return "\n".join(lines)
