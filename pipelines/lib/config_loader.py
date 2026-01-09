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
      natural_keys: [order_id]
      change_timestamp: updated_at
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
from typing import Any, Dict, List, Optional, Type, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pipelines.lib.bronze import BronzeSource, InputMode, LoadPattern, SourceType, WatermarkSource
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
    validate_bronze_source,
    validate_silver_entity,
)

logger = logging.getLogger(__name__)


# ============================================
# Model Validation Constants - Derived from MODEL_SPECS (single source of truth)
# ============================================

# Models that require natural_keys and change_timestamp
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
    source_type: str = Field(..., description="Source type (DATABASE_MSSQL, FILE_CSV, etc.)")
    target_path: str = Field(..., min_length=1, description="Output path for Bronze data")
    load_pattern: str = Field(default="FULL_SNAPSHOT", description="Load pattern")
    source_path: Optional[str] = Field(default=None, description="Source file path for file sources")
    query: Optional[str] = Field(default=None, description="SQL query for database sources")
    watermark_column: Optional[str] = Field(default=None, description="Column for incremental loads")
    options: Dict[str, Any] = Field(default_factory=dict, description="Source-specific options")

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
            raise ValueError(f"source_type must be one of: {valid_types}")
        return v.upper()

    @field_validator("load_pattern")
    @classmethod
    def validate_load_pattern(cls, v: str) -> str:
        """Validate load_pattern is a known value."""
        valid_patterns = ["FULL_SNAPSHOT", "INCREMENTAL_APPEND", "INCREMENTAL_MERGE"]
        if v.upper() not in valid_patterns:
            raise ValueError(f"load_pattern must be one of: {valid_patterns}")
        return v.upper()

    @model_validator(mode="after")
    def validate_database_options(self) -> "BronzeConfig":
        """Validate database sources have required options."""
        if self.source_type in ("DATABASE_MSSQL", "DATABASE_POSTGRES"):
            if "host" not in self.options:
                raise ValueError("Database sources require 'host' in options")
            if "database" not in self.options:
                raise ValueError("Database sources require 'database' in options")
        return self

    @model_validator(mode="after")
    def validate_file_source_path(self) -> "BronzeConfig":
        """Validate file sources have source_path."""
        file_types = ("FILE_CSV", "FILE_PARQUET", "FILE_FIXED_WIDTH", "FILE_SPACE_DELIMITED", "FILE_JSON", "FILE_EXCEL")
        if self.source_type in file_types and not self.source_path:
            raise ValueError(f"{self.source_type} requires source_path")
        return self

    @model_validator(mode="after")
    def validate_incremental_watermark(self) -> "BronzeConfig":
        """Validate incremental loads have watermark_column."""
        if self.load_pattern == "INCREMENTAL_APPEND" and not self.watermark_column:
            raise ValueError("INCREMENTAL_APPEND requires watermark_column")
        return self


class SilverConfig(BaseModel):
    """Pydantic model for Silver entity configuration validation.

    Example:
        >>> config = SilverConfig(
        ...     source_path="s3://bronze/claims/header/",
        ...     target_path="s3://silver/claims_header/",
        ...     natural_keys=["claim_id"],
        ...     change_timestamp="updated_at"
        ... )
        >>> entity = SilverEntity(**config.model_dump())
    """

    source_path: str = Field(..., min_length=1, description="Bronze source path")
    target_path: str = Field(..., min_length=1, description="Silver output path")
    natural_keys: List[str] = Field(..., min_length=1, description="Natural key columns")
    change_timestamp: str = Field(..., min_length=1, description="Timestamp column for changes")
    entity_kind: str = Field(default="STATE", description="Entity kind (STATE, EVENT)")
    history_mode: str = Field(default="CURRENT_ONLY", description="History mode")
    attributes: Optional[List[str]] = Field(default=None, description="Columns to include")
    exclude_columns: Optional[List[str]] = Field(default=None, description="Columns to exclude")
    validate_source: str = Field(default="skip", description="Source validation mode")

    @field_validator("entity_kind")
    @classmethod
    def validate_entity_kind(cls, v: str) -> str:
        """Validate entity_kind is a known value."""
        valid_kinds = ["STATE", "EVENT"]
        if v.upper() not in valid_kinds:
            raise ValueError(f"entity_kind must be one of: {valid_kinds}")
        return v.upper()

    @field_validator("history_mode")
    @classmethod
    def validate_history_mode(cls, v: str) -> str:
        """Validate history_mode is a known value."""
        valid_modes = ["CURRENT_ONLY", "FULL_HISTORY"]
        if v.upper() not in valid_modes:
            raise ValueError(f"history_mode must be one of: {valid_modes}")
        return v.upper()

    @field_validator("validate_source")
    @classmethod
    def validate_validate_source(cls, v: str) -> str:
        """Validate validate_source mode."""
        valid_modes = ["skip", "warn", "strict"]
        if v.lower() not in valid_modes:
            raise ValueError(f"validate_source must be one of: {valid_modes}")
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

    level: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")
    format: str = Field(default="console", description="Output format: 'json' for Splunk, 'console' for human-readable")
    file: Optional[str] = Field(default=None, description="Optional log file path")
    console: bool = Field(default=True, description="Output to console (in addition to file)")
    include_timestamp: bool = Field(default=True, description="Include ISO timestamp in logs")
    include_source: bool = Field(default=True, description="Include source file/line info")

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level is a known value."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"level must be one of: {valid_levels}")
        return v.upper()

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        """Validate format is a known value."""
        valid_formats = ["json", "console", "text"]
        if v.lower() not in valid_formats:
            raise ValueError(f"format must be one of: {valid_formats}")
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

    bronze_path: str = Field(default="./bronze", description="Default Bronze output path")
    silver_path: str = Field(default="./silver", description="Default Silver output path")
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="console", description="Log format: 'json' or 'console'")
    log_file: Optional[str] = Field(default=None, description="Optional log file path")
    max_retries: int = Field(default=3, ge=1, le=10, description="Max retry attempts")
    retry_delay: float = Field(default=1.0, ge=0.1, le=60.0, description="Retry delay in seconds")
    chunk_size: int = Field(default=100000, ge=1000, description="Default chunk size for writes")
    validate_checksums: bool = Field(default=True, description="Validate Bronze checksums")

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


# Auto-generated from enum values (ensures maps stay in sync with enums)
SOURCE_TYPE_MAP = _enum_to_map(SourceType)
ENTITY_KIND_MAP = _enum_to_map(EntityKind)
DELETE_MODE_MAP = _enum_to_map(DeleteMode)
SILVER_MODEL_MAP = _enum_to_map(SilverModel)
INPUT_MODE_MAP = _enum_to_map(InputMode)
WATERMARK_SOURCE_MAP = _enum_to_map(WatermarkSource)

# Maps with aliases (base auto-generated + explicit aliases)
LOAD_PATTERN_MAP = _enum_to_map(LoadPattern) | {"incremental_append": LoadPattern.INCREMENTAL_APPEND}
HISTORY_MODE_MAP = _enum_to_map(HistoryMode) | {"scd1": HistoryMode.CURRENT_ONLY, "scd2": HistoryMode.FULL_HISTORY}


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
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
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
            f"Parent config not found: {parent_path} "
            f"(referenced from {config_path})"
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
        }
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
        errors.append("bronze.source_type is required (e.g., 'file_csv', 'database_mssql')")

    # If required fields are missing, report ALL of them at once
    if errors:
        raise YAMLConfigError(
            "Missing required fields in bronze section:\n  - " + "\n  - ".join(errors)
        )

    # Convert source_type string to enum
    source_type_str = config["source_type"].lower()
    if source_type_str not in SOURCE_TYPE_MAP:
        valid = ", ".join(sorted(SOURCE_TYPE_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid source_type '{config['source_type']}'. "
            f"Valid options: {valid}"
        )
    source_type = SOURCE_TYPE_MAP[source_type_str]

    # Convert load_pattern string to enum (optional)
    load_pattern = LoadPattern.FULL_SNAPSHOT
    if "load_pattern" in config:
        pattern_str = config["load_pattern"].lower()
        if pattern_str not in LOAD_PATTERN_MAP:
            valid = ", ".join(sorted(LOAD_PATTERN_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid load_pattern '{config['load_pattern']}'. "
                f"Valid options: {valid}"
            )
        load_pattern = LOAD_PATTERN_MAP[pattern_str]

    # Convert input_mode string to enum (optional)
    input_mode = None
    if "input_mode" in config:
        mode_str = config["input_mode"].lower()
        if mode_str not in INPUT_MODE_MAP:
            valid = ", ".join(sorted(INPUT_MODE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid input_mode '{config['input_mode']}'. "
                f"Valid options: {valid}"
            )
        input_mode = INPUT_MODE_MAP[mode_str]

    # Convert watermark_source string to enum (optional, default: destination)
    watermark_source = WatermarkSource.DESTINATION
    if "watermark_source" in config:
        ws_str = config["watermark_source"].lower()
        if ws_str not in WATERMARK_SOURCE_MAP:
            valid = ", ".join(sorted(WATERMARK_SOURCE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid watermark_source '{config['watermark_source']}'. "
                f"Valid options: {valid}"
            )
        watermark_source = WATERMARK_SOURCE_MAP[ws_str]

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
        watermark_column=config.get("watermark_column"),
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

    # Get model name and spec for validation
    model_str = config.get("model", "").lower()
    model_spec = MODEL_SPECS.get(model_str) if model_str else None

    # Determine if keys are required based on model spec
    # When no model is specified, default behavior requires keys
    requires_keys = (model_spec.requires_keys if model_spec else True)

    # Collect ALL validation errors before failing (user-friendly!)
    errors: List[str] = []

    # Check required fields based on model spec
    if requires_keys:
        if "natural_keys" not in config:
            errors.append(
                f"silver.natural_keys is required for model '{model_str or 'default'}'. "
                "Use model: periodic_snapshot if you don't need deduplication."
            )
        if "change_timestamp" not in config:
            errors.append(
                f"silver.change_timestamp is required for model '{model_str or 'default'}'. "
                "Use model: periodic_snapshot if you don't need change tracking."
            )

    # Bronze pattern validation using MODEL_SPECS (single source of truth)
    if bronze is not None and model_spec:
        bronze_pattern_name = bronze.load_pattern.value.lower()
        # Normalize pattern names for comparison
        if bronze_pattern_name == "incremental_append":
            bronze_pattern_name = "incremental"

        # Check if Bronze pattern is valid for this model
        if model_spec.valid_bronze_patterns:
            if bronze_pattern_name not in model_spec.valid_bronze_patterns:
                valid_patterns = ", ".join(model_spec.valid_bronze_patterns)
                errors.append(
                    f"Model '{model_str}' requires bronze.load_pattern to be one of: {valid_patterns} "
                    f"(currently: {bronze.load_pattern.value})"
                )
            # Check if this combination produces a warning (works but suboptimal)
            elif model_spec.warns_on_bronze_patterns and bronze_pattern_name in model_spec.warns_on_bronze_patterns:
                # Different warning messages based on the Bronze pattern
                if bronze_pattern_name == "full_snapshot":
                    warnings.warn(
                        f"bronze.load_pattern: {bronze.load_pattern.value} with silver.model: {model_str} is inefficient. "
                        f"Each Bronze partition contains ALL records (overlapping data). Silver will union "
                        "all partitions and dedupe, which works but wastes storage and processing time. "
                        "Consider: 1) Use periodic_snapshot if you just need the latest snapshot, or "
                        "2) Change Bronze to incremental with watermark_column for efficient accumulation.",
                        UserWarning,
                        stacklevel=2,
                    )
                elif bronze_pattern_name == "cdc":
                    warnings.warn(
                        f"bronze.load_pattern: cdc but silver.model: {model_str or 'default'} is not CDC-aware. "
                        "Consider using a cdc_* model (cdc_current, cdc_history, etc.) to properly handle "
                        "insert/update/delete operations.",
                        UserWarning,
                        stacklevel=2,
                    )

    # Delete mode requires CDC load pattern or CDC model
    delete_mode_str = config.get("delete_mode", "ignore").lower()
    if delete_mode_str in ("tombstone", "hard_delete"):
        is_cdc = (
            (bronze is not None and bronze.load_pattern == LoadPattern.CDC) or
            (model_spec is not None and model_spec.requires_cdc_bronze)
        )
        if not is_cdc:
            errors.append(
                f"delete_mode: {delete_mode_str} only makes sense with CDC data. "
                "Either set bronze.load_pattern: cdc or use a cdc_* Silver model."
            )

    # If any errors accumulated, report ALL of them at once
    if errors:
        raise YAMLConfigError(
            "Configuration errors in silver section:\n  - " + "\n  - ".join(errors)
        )

    # Warning: Bronze CDC pattern with no model specified should use CDC-aware config
    if bronze is not None and bronze.load_pattern == LoadPattern.CDC and model_spec is None:
        warnings.warn(
            f"bronze.load_pattern: cdc but silver.model: {model_str or 'default'} is not CDC-aware. "
            "Consider using a cdc_* model (cdc_current, cdc_history, etc.) to properly handle "
            "insert/update/delete operations.",
            UserWarning,
            stacklevel=2,
        )

    # Warning: Explicit input_mode should match Bronze pattern (deprecation candidate)
    if bronze is not None and "input_mode" in config:
        explicit_input = config["input_mode"].lower()
        expected_input = "append_log" if bronze.load_pattern in (LoadPattern.INCREMENTAL_APPEND, LoadPattern.CDC) else "replace_daily"
        if explicit_input != expected_input:
            warnings.warn(
                f"Explicit silver.input_mode: {explicit_input} conflicts with "
                f"bronze.load_pattern: {bronze.load_pattern.value} (expected: {expected_input}). "
                "This may cause unexpected behavior. "
                "Note: input_mode is typically auto-derived from the model and Bronze pattern.",
                UserWarning,
                stacklevel=2,
            )

    # Ensure natural_keys is a list (or None for periodic_snapshot)
    natural_keys = config.get("natural_keys")
    if natural_keys is not None and isinstance(natural_keys, str):
        natural_keys = [natural_keys]

    # Expand model preset if specified (before parsing individual settings)
    # Model provides defaults; explicit settings can override them
    model_defaults: Dict[str, str] = {}
    if "model" in config:
        model_str = config["model"].lower()
        if model_str not in SILVER_MODEL_MAP:
            valid = ", ".join(sorted(SILVER_MODEL_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid model '{config['model']}'. "
                f"Valid options: {valid}"
            )
        model_defaults = SILVER_MODEL_PRESETS[model_str]
        logger.debug(
            "Expanding Silver model preset",
            extra={"model": model_str, "defaults": model_defaults}
        )

    # Convert entity_kind string to enum (from config or model preset)
    entity_kind_str = config.get("entity_kind") or model_defaults.get("entity_kind") or "state"
    entity_kind_str = entity_kind_str.lower()
    if entity_kind_str not in ENTITY_KIND_MAP:
        valid = ", ".join(sorted(ENTITY_KIND_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid entity_kind '{entity_kind_str}'. "
            f"Valid options: {valid}"
        )
    entity_kind = ENTITY_KIND_MAP[entity_kind_str]

    # Convert history_mode string to enum (from config or model preset)
    history_mode_str = config.get("history_mode") or model_defaults.get("history_mode") or "current_only"
    history_mode_str = history_mode_str.lower()
    if history_mode_str not in HISTORY_MODE_MAP:
        valid = ", ".join(sorted(HISTORY_MODE_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid history_mode '{history_mode_str}'. "
            f"Valid options: {valid}"
        )
    history_mode = HISTORY_MODE_MAP[history_mode_str]

    # Resolve source_path
    source_path = config.get("source_path", "")
    if source_path:
        source_path = _resolve_path(source_path, config_dir)

    # Resolve target_path
    target_path = config.get("target_path", "")
    if target_path:
        target_path = _resolve_path(target_path, config_dir)

    # Handle attributes - can be list or None
    attributes = config.get("attributes")
    if isinstance(attributes, str):
        attributes = [attributes]

    # Handle exclude_columns - can be list or None
    exclude_columns = config.get("exclude_columns")
    if isinstance(exclude_columns, str):
        exclude_columns = [exclude_columns]

    # Handle column_mapping - dictionary of old_name: new_name
    column_mapping = config.get("column_mapping")
    if column_mapping is not None and not isinstance(column_mapping, dict):
        raise YAMLConfigError(
            "column_mapping must be an object with {old_column: new_column} pairs"
        )

    # Handle partition_by
    partition_by = config.get("partition_by")
    if isinstance(partition_by, str):
        partition_by = [partition_by]

    # Handle output_formats
    output_formats = config.get("output_formats", ["parquet"])
    if isinstance(output_formats, str):
        output_formats = [output_formats]

    # Get system and entity from config or infer from bronze
    system = config.get("system", "")
    entity = config.get("entity", "")

    # Convert input_mode string to enum (from config, model preset, or auto-wired from Bronze)
    input_mode = None
    input_mode_str = config.get("input_mode") or model_defaults.get("input_mode")
    if input_mode_str:
        input_mode_str = input_mode_str.lower()
        if input_mode_str not in INPUT_MODE_MAP:
            valid = ", ".join(sorted(INPUT_MODE_MAP.keys()))
            raise YAMLConfigError(
                f"Invalid input_mode '{input_mode_str}'. "
                f"Valid options: {valid}"
            )
        input_mode = INPUT_MODE_MAP[input_mode_str]

    # Convert delete_mode string to enum (from config or model preset)
    delete_mode_str = config.get("delete_mode") or model_defaults.get("delete_mode") or "ignore"
    delete_mode_str = delete_mode_str.lower()
    if delete_mode_str not in DELETE_MODE_MAP:
        valid = ", ".join(sorted(DELETE_MODE_MAP.keys()))
        raise YAMLConfigError(
            f"Invalid delete_mode '{delete_mode_str}'. "
            f"Valid options: {valid}"
        )
    delete_mode = DELETE_MODE_MAP[delete_mode_str]

    # Parse cdc_options (for CDC load pattern)
    cdc_options = None
    if "cdc_options" in config:
        cdc_options = config["cdc_options"]
        if not isinstance(cdc_options, dict):
            raise YAMLConfigError("cdc_options must be an object")
        if "operation_column" not in cdc_options:
            raise YAMLConfigError("cdc_options.operation_column is required")

    # Build storage options from Silver config's top-level S3 options
    storage_options: Optional[Dict[str, Any]] = None
    for yaml_key, storage_key in S3_YAML_TO_STORAGE_OPTIONS:
        if yaml_key in config:
            if storage_options is None:
                storage_options = {}
            storage_options[storage_key] = config[yaml_key]

    # Auto-wire from Bronze if not specified in Silver config
    if bronze:
        if not system:
            system = bronze.system
        if not entity:
            entity = bronze.entity
        # Auto-wire storage options from Bronze (for S3-compatible storage)
        # Only if Silver doesn't have its own S3 options
        if storage_options is None and bronze.options:
            storage_keys = [
                "s3_signature_version",
                "s3_addressing_style",
                "s3_verify_ssl",
                "endpoint_url",
                "key",
                "secret",
                "region",
            ]
            storage_options = {
                k: v for k, v in bronze.options.items() if k in storage_keys
            }
            if not storage_options:
                storage_options = None

    # Build SilverEntity
    return SilverEntity(
        natural_keys=natural_keys,
        change_timestamp=config.get("change_timestamp"),
        system=system,
        entity=entity,
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
        storage_options=storage_options,
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
            extra={"config_path": str(config_path)}
        )

    if not config:
        raise YAMLConfigError("Empty configuration file")

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
        if bronze and silver and not silver.source_path:
            # Auto-wire Silver source from Bronze target
            # Substitute {system} and {entity} placeholders with actual values
            bronze_target = bronze.target_path.format(
                system=bronze.system,
                entity=bronze.entity,
                run_date="{run_date}",  # Keep {run_date} for later substitution
            )
            silver.source_path = bronze_target.rstrip("/") + "/*.parquet"

        if bronze and silver and not silver.target_path:
            # Auto-generate Silver target using Hive-style partitioning for consistency
            # Include dt={run_date} to match Bronze date partitioning
            silver.target_path = f"./silver/system={bronze.system}/entity={bronze.entity}/dt={{run_date}}/"

        # Auto-wire input_mode from Bronze to Silver if not explicitly set
        if bronze and silver and silver.input_mode is None and bronze.input_mode is not None:
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
            return self.config["name"]
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
                tracer.detail(f"Silver: {self.silver.entity}")

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
            input_mode_str = self.bronze.input_mode.value if self.bronze.input_mode else "(not set)"
            lines.extend([
                "BRONZE LAYER:",
                f"  System:       {self.bronze.system}",
                f"  Entity:       {self.bronze.entity}",
                f"  Source Type:  {self.bronze.source_type.value}",
                f"  Source Path:  {self.bronze.source_path or '(from database)'}",
                f"  Target Path:  {self.bronze.target_path}",
                f"  Load Pattern: {self.bronze.load_pattern.value}",
                f"  Input Mode:   {input_mode_str}",
                "",
            ])

        if self.silver:
            lines.extend([
                "SILVER LAYER:",
                f"  Natural Keys: {', '.join(self.silver.natural_keys)}",
                f"  Change Col:   {self.silver.change_timestamp}",
                f"  Entity Kind:  {self.silver.entity_kind.value}",
                f"  History Mode: {self.silver.history_mode.value}",
                f"  Source Path:  {self.silver.source_path}",
                f"  Target Path:  {self.silver.target_path}",
                "",
            ])

        return "\n".join(lines)
