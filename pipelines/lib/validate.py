"""Validation helpers for pipeline configuration.

Provides validation functions that catch common configuration errors
early and provide helpful error messages for non-Python developers.

Uses Pydantic v2 for type-safe configuration validation when available,
with fallback to original dataclass-based validation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from pipelines.lib.bronze import BronzeSource
    from pipelines.lib.silver import SilverEntity

logger = logging.getLogger(__name__)

__all__ = [
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
]


# ============================================
# Pydantic Configuration Models
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


class ValidationSeverity(Enum):
    """Severity of validation issues."""

    ERROR = "error"  # Will cause pipeline to fail
    WARNING = "warning"  # May cause issues, but pipeline can run


@dataclass
class ValidationIssue:
    """A validation issue found in configuration."""

    severity: ValidationSeverity
    message: str
    field: str
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        prefix = "ERROR" if self.severity == ValidationSeverity.ERROR else "WARNING"
        result = f"[{prefix}] {self.field}: {self.message}"
        if self.suggestion:
            result += f"\n  Fix: {self.suggestion}"
        return result


def validate_bronze_source(source: "BronzeSource") -> List[ValidationIssue]:
    """Validate a BronzeSource configuration.

    Returns a list of validation issues. Empty list means valid.

    Args:
        source: BronzeSource instance to validate

    Returns:
        List of ValidationIssue objects

    Example:
        >>> issues = validate_bronze_source(my_source)
        >>> if issues:
        ...     for issue in issues:
        ...         print(issue)
    """
    from pipelines.lib.bronze import SourceType, LoadPattern

    issues: List[ValidationIssue] = []

    # Check required fields
    if not source.system:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="system",
                message="System name is required",
                suggestion="Add system='your_system_name' to your BronzeSource",
            )
        )

    if not source.entity:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="entity",
                message="Entity name is required",
                suggestion="Add entity='your_table_name' to your BronzeSource",
            )
        )

    if not source.target_path:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="target_path",
                message="Target path is required",
                suggestion="Add target_path='s3://bronze/system={system}/entity={entity}/dt={run_date}/'",
            )
        )

    # Database-specific validation
    if source.source_type in (SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES):
        if "host" not in source.options:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="options.host",
                    message="Database sources require 'host' in options",
                    suggestion='Add options={"host": "your-server.database.com", ...}',
                )
            )

        if "database" not in source.options:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="options.database",
                    message="Database sources require 'database' in options",
                    suggestion='Add "database": "YourDatabase" to options',
                )
            )

    # File-specific validation
    if source.source_type in (
        SourceType.FILE_CSV,
        SourceType.FILE_PARQUET,
        SourceType.FILE_SPACE_DELIMITED,
    ):
        if not source.source_path:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="source_path",
                    message="File sources require a source_path",
                    suggestion="Add source_path='/path/to/files/{run_date}/*.csv'",
                )
            )

    # Fixed-width specific validation
    if source.source_type == SourceType.FILE_FIXED_WIDTH:
        if "columns" not in source.options:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="options.columns",
                    message="Fixed-width files require 'columns' list in options",
                    suggestion='Add options={"columns": [...], "widths": [...]}',
                )
            )
        if "widths" not in source.options:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="options.widths",
                    message="Fixed-width files require 'widths' list in options",
                    suggestion='Add "widths": [10, 20, ...] to options',
                )
            )

    # Incremental load validation
    if source.load_pattern == LoadPattern.INCREMENTAL_APPEND:
        if not source.watermark_column:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.ERROR,
                    field="watermark_column",
                    message="Incremental loads require a watermark_column",
                    suggestion="Add watermark_column='LastUpdated'",
                )
            )

    # Warnings
    if source.load_pattern == LoadPattern.FULL_SNAPSHOT and source.watermark_column:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="watermark_column",
                message="watermark_column is set but load_pattern is FULL_SNAPSHOT",
                suggestion="Use INCREMENTAL_APPEND or remove watermark_column",
            )
        )

    return issues


def validate_silver_entity(entity: "SilverEntity") -> List[ValidationIssue]:
    """Validate a SilverEntity configuration.

    Returns a list of validation issues. Empty list means valid.

    Args:
        entity: SilverEntity instance to validate

    Returns:
        List of ValidationIssue objects

    Example:
        >>> issues = validate_silver_entity(my_entity)
        >>> if issues:
        ...     for issue in issues:
        ...         print(issue)
    """
    from pipelines.lib.silver import EntityKind, HistoryMode

    issues: List[ValidationIssue] = []

    # Check required fields
    if not entity.source_path:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="source_path",
                message="Source path is required",
                suggestion="Add source_path='s3://bronze/system=x/entity=y/dt={run_date}/*.parquet'",
            )
        )

    if not entity.target_path:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="target_path",
                message="Target path is required",
                suggestion="Add target_path='s3://silver/your/path/'",
            )
        )

    if not entity.natural_keys:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="natural_keys",
                message="At least one natural key is required",
                suggestion="Add natural_keys=['id_column']",
            )
        )

    if not entity.change_timestamp:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                field="change_timestamp",
                message="Change timestamp column is required",
                suggestion="Add change_timestamp='LastUpdated'",
            )
        )

    # Event-specific warnings
    if entity.entity_kind == EntityKind.EVENT:
        if entity.history_mode == HistoryMode.FULL_HISTORY:
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field="history_mode",
                    message="FULL_HISTORY is typically not needed for EVENT entities",
                    suggestion="Events are immutable; use CURRENT_ONLY",
                )
            )

    # Attribute warnings
    if entity.attributes and entity.exclude_columns:
        issues.append(
            ValidationIssue(
                severity=ValidationSeverity.WARNING,
                field="attributes/exclude_columns",
                message="Both attributes and exclude_columns are set",
                suggestion="Use only one: attributes OR exclude_columns",
            )
        )

    return issues


def validate_and_raise(
    source: Optional["BronzeSource"] = None,
    entity: Optional["SilverEntity"] = None,
) -> None:
    """Validate configuration and raise exception if errors found.

    Convenience function that validates and raises a clear exception
    with all error messages if any validation errors are found.

    Args:
        source: Optional BronzeSource to validate
        entity: Optional SilverEntity to validate

    Raises:
        ValueError: If any validation errors are found

    Example:
        >>> validate_and_raise(source=my_source, entity=my_entity)
        # Raises if invalid, returns None if valid
    """
    all_issues: List[ValidationIssue] = []

    if source:
        all_issues.extend(validate_bronze_source(source))

    if entity:
        all_issues.extend(validate_silver_entity(entity))

    errors = [i for i in all_issues if i.severity == ValidationSeverity.ERROR]
    warnings = [i for i in all_issues if i.severity == ValidationSeverity.WARNING]

    # Log warnings
    for warning in warnings:
        logger.warning(str(warning))

    # Raise on errors
    if errors:
        error_messages = "\n\n".join(str(e) for e in errors)
        raise ValueError(f"Configuration validation failed:\n\n{error_messages}")


def format_validation_report(issues: List[ValidationIssue]) -> str:
    """Format validation issues as a readable report.

    Args:
        issues: List of validation issues

    Returns:
        Formatted string report
    """
    if not issues:
        return "Configuration is valid."

    errors = [i for i in issues if i.severity == ValidationSeverity.ERROR]
    warnings = [i for i in issues if i.severity == ValidationSeverity.WARNING]

    lines = []

    if errors:
        lines.append(f"Found {len(errors)} error(s):")
        lines.append("-" * 40)
        for error in errors:
            lines.append(str(error))
            lines.append("")

    if warnings:
        lines.append(f"Found {len(warnings)} warning(s):")
        lines.append("-" * 40)
        for warning in warnings:
            lines.append(str(warning))
            lines.append("")

    return "\n".join(lines)
