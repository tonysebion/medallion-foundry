"""Validation utilities for Bronze and Silver configurations.

This module provides validation classes and functions used to validate
pipeline configurations. Extracted to break circular dependencies between
bronze.py and config_loader.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Type, TypeVar

if TYPE_CHECKING:
    from pipelines.lib.bronze import BronzeSource as BronzeSourceType
    from pipelines.lib.silver import SilverEntity as SilverEntityType

logger = logging.getLogger(__name__)

__all__ = [
    "ValidationSeverity",
    "ValidationIssue",
    "validate_bronze_source",
    "validate_silver_entity",
    "validate_bronze_silver_compatibility",
    "validate_and_raise",
    "validate_enum",
    "format_validation_report",
]

# TypeVar for enum validation
E = TypeVar("E", bound=Enum)


def validate_enum(value: str, enum_class: Type[E], field_name: str) -> E:
    """Validate and convert string to enum member.

    Case-insensitive lookup that raises ValueError with helpful message
    listing all valid options if the value is invalid.

    Args:
        value: String value to convert
        enum_class: Enum class to validate against
        field_name: Name of field for error messages

    Returns:
        The matching enum member

    Raises:
        ValueError: If value is not a valid member of the enum

    Example:
        >>> validate_enum("full_snapshot", LoadPattern, "load_pattern")
        <LoadPattern.FULL_SNAPSHOT: 'full_snapshot'>
    """
    normalized = value.upper().replace("-", "_")
    try:
        return enum_class[normalized]
    except KeyError:
        valid = ", ".join(e.name.lower() for e in enum_class)
        raise ValueError(f"Invalid {field_name} '{value}'. Must be one of: {valid}")


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
    see_also: Optional[str] = None  # Path to example file

    @classmethod
    def error(
        cls,
        field: str,
        message: str,
        suggestion: Optional[str] = None,
        see_also: Optional[str] = None,
    ) -> "ValidationIssue":
        """Create an ERROR severity issue."""
        return cls(ValidationSeverity.ERROR, message, field, suggestion, see_also)

    @classmethod
    def warning(
        cls,
        field: str,
        message: str,
        suggestion: Optional[str] = None,
        see_also: Optional[str] = None,
    ) -> "ValidationIssue":
        """Create a WARNING severity issue."""
        return cls(ValidationSeverity.WARNING, message, field, suggestion, see_also)

    def __str__(self) -> str:
        prefix = "ERROR" if self.severity == ValidationSeverity.ERROR else "WARNING"
        result = f"[{prefix}] {self.field}: {self.message}"
        if self.suggestion:
            result += f"\n  Fix: {self.suggestion}"
        if self.see_also:
            result += f"\n  Example: {self.see_also}"
        return result


def validate_bronze_source(source: "BronzeSourceType") -> List[ValidationIssue]:
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
    # Import here to avoid circular dependency
    from pipelines.lib.bronze import LoadPattern, SourceType

    issues: List[ValidationIssue] = []

    # Check required fields
    if not source.system:
        issues.append(
            ValidationIssue.error(
                "system",
                "System name is required",
                "Add system='your_system_name' to your BronzeSource",
            )
        )

    if not source.entity:
        issues.append(
            ValidationIssue.error(
                "entity",
                "Entity name is required",
                "Add entity='your_table_name' to your BronzeSource",
            )
        )

    if not source.target_path:
        issues.append(
            ValidationIssue.error(
                "target_path",
                "Target path is required",
                "Add target_path='s3://bronze/system={system}/entity={entity}/dt={run_date}/'",
            )
        )

    # Database-specific validation
    if source.source_type in (SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES):
        if "host" not in source.options:
            issues.append(
                ValidationIssue.error(
                    "options.host",
                    "Database sources require 'host' in options",
                    'Add options={"host": "your-server.database.com", ...}',
                    see_also="pipelines/examples/mssql_dimension.yaml",
                )
            )

        if "database" not in source.options:
            issues.append(
                ValidationIssue.error(
                    "options.database",
                    "Database sources require 'database' in options",
                    'Add "database": "YourDatabase" to options',
                    see_also="pipelines/examples/mssql_dimension.yaml",
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
                ValidationIssue.error(
                    "source_path",
                    "File sources require a source_path",
                    "Add source_path='/path/to/files/{run_date}/*.csv'",
                    see_also="pipelines/examples/csv_snapshot.yaml",
                )
            )

    # Fixed-width specific validation
    if source.source_type == SourceType.FILE_FIXED_WIDTH:
        # Multi-record-type mode uses record_types instead of columns/widths
        has_multi_record = (
            "record_type_position" in source.options
            and "record_types" in source.options
        )

        if not has_multi_record:
            # Standard single-record mode requires columns and widths
            if "columns" not in source.options:
                issues.append(
                    ValidationIssue.error(
                        "options.columns",
                        "Fixed-width files require 'columns' list in options",
                        'Add options={"columns": [...], "widths": [...]} or use record_types for multi-record files',
                        see_also="pipelines/examples/fixed_width.yaml",
                    )
                )
            if "widths" not in source.options:
                issues.append(
                    ValidationIssue.error(
                        "options.widths",
                        "Fixed-width files require 'widths' list in options",
                        'Add "widths": [10, 20, ...] to options or use record_types for multi-record files',
                        see_also="pipelines/examples/fixed_width.yaml",
                    )
                )

    # Incremental load validation
    if source.load_pattern == LoadPattern.INCREMENTAL_APPEND:
        if not source.watermark_column:
            issues.append(
                ValidationIssue.error(
                    "incremental_column",
                    "Incremental loads require incremental_column to track which rows have been extracted",
                    "Add incremental_column: updated_at (or another column that increases over time)",
                    see_also="pipelines/examples/incremental_load.yaml",
                )
            )

    # Warnings
    if source.load_pattern == LoadPattern.FULL_SNAPSHOT and source.watermark_column:
        issues.append(
            ValidationIssue.warning(
                "incremental_column",
                "incremental_column is set but load_pattern is full_snapshot",
                "Use load_pattern: incremental or remove incremental_column",
            )
        )

    return issues


def validate_silver_entity(entity: "SilverEntityType") -> List[ValidationIssue]:
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
    # Import here to avoid circular dependency
    from pipelines.lib.silver import EntityKind, HistoryMode

    issues: List[ValidationIssue] = []

    # Check required fields
    if not entity.source_path:
        issues.append(
            ValidationIssue.error(
                "source_path",
                "Source path is required",
                "Add source_path='s3://bronze/system=x/entity=y/dt={run_date}/*.parquet'",
            )
        )

    if not entity.target_path:
        issues.append(
            ValidationIssue.error(
                "target_path",
                "Target path is required",
                "Add target_path='s3://silver/your/path/'",
            )
        )

    if not entity.unique_columns:
        issues.append(
            ValidationIssue.error(
                "unique_columns",
                "At least one unique column is required (to identify each row)",
                "Add unique_columns=['id_column']. Run 'python -m pipelines inspect-source' for suggestions.",
                see_also="pipelines/examples/retail_orders.yaml",
            )
        )

    if not entity.last_updated_column:
        issues.append(
            ValidationIssue.error(
                "last_updated_column",
                "Last updated column is required (to track changes)",
                "Add last_updated_column='LastUpdated'. Run 'python -m pipelines inspect-source' for suggestions.",
                see_also="pipelines/examples/retail_orders.yaml",
            )
        )

    # Event-specific warnings
    if entity.entity_kind == EntityKind.EVENT:
        if entity.history_mode == HistoryMode.FULL_HISTORY:
            issues.append(
                ValidationIssue.warning(
                    "history_mode",
                    "FULL_HISTORY is typically not needed for EVENT entities",
                    "Events are immutable; use CURRENT_ONLY",
                    see_also="pipelines/examples/event_log.yaml",
                )
            )

    # Attribute warnings
    if entity.attributes and entity.exclude_columns:
        issues.append(
            ValidationIssue.warning(
                "attributes/exclude_columns",
                "Both attributes and exclude_columns are set",
                "Use only one: attributes OR exclude_columns",
            )
        )

    return issues


def validate_bronze_silver_compatibility(
    bronze_config: dict,
    silver_config: dict,
) -> List[ValidationIssue]:
    """Validate Bronze and Silver configuration compatibility.

    Checks that the Bronze load_pattern is compatible with the Silver model.

    Args:
        bronze_config: Bronze section from YAML config
        silver_config: Silver section from YAML config

    Returns:
        List of ValidationIssue objects

    Example:
        >>> issues = validate_bronze_silver_compatibility(
        ...     {"load_pattern": "full_snapshot"},
        ...     {"model": "cdc_current"}
        ... )
        >>> if issues:
        ...     for issue in issues:
        ...         print(issue)  # Error: CDC model requires load_pattern: cdc
    """
    issues: List[ValidationIssue] = []

    bronze_pattern = bronze_config.get("load_pattern", "full_snapshot").lower()
    silver_model = (
        silver_config.get("model", "").lower() if silver_config.get("model") else ""
    )

    # Normalize bronze pattern (incremental_append -> incremental)
    if bronze_pattern == "incremental_append":
        bronze_pattern = "incremental"

    # CDC models require CDC bronze
    cdc_models = {
        "cdc",
        "cdc_current",
        "cdc_current_tombstone",
        "cdc_current_hard_delete",
        "cdc_history",
        "cdc_history_tombstone",
        "cdc_history_hard_delete",
    }
    if silver_model in cdc_models and bronze_pattern != "cdc":
        issues.append(
            ValidationIssue.error(
                field="silver.model",
                message=f"Model '{silver_model}' requires bronze.load_pattern: cdc (currently: {bronze_pattern})",
                suggestion="Change bronze.load_pattern to 'cdc' or use a non-CDC model like 'full_merge_dedupe'",
                see_also="pipelines/examples/cdc_orders.yaml",
            )
        )

    # periodic_snapshot only works well with full_snapshot
    if silver_model == "periodic_snapshot" and bronze_pattern != "full_snapshot":
        issues.append(
            ValidationIssue.warning(
                field="silver.model",
                message=f"periodic_snapshot with '{bronze_pattern}' may accumulate duplicate data",
                suggestion="Consider 'full_merge_dedupe' for deduplication across partitions",
                see_also="pipelines/examples/retail_orders.yaml",
            )
        )

    # Warn if using non-CDC model with CDC bronze (loses delete info)
    non_cdc_models = {
        "full_merge_dedupe",
        "incremental_merge",
        "scd_type_2",
    }
    if bronze_pattern == "cdc" and silver_model in non_cdc_models:
        issues.append(
            ValidationIssue.warning(
                field="silver.model",
                message=f"bronze.load_pattern is 'cdc' but silver.model '{silver_model}' ignores CDC operations",
                suggestion="Use model: cdc (with keep_history and handle_deletes options) to properly handle CDC operations",
                see_also="pipelines/examples/cdc_orders.yaml",
            )
        )

    return issues


def validate_and_raise(
    source: Optional["BronzeSourceType"] = None,
    entity: Optional["SilverEntityType"] = None,
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
    warning_issues = [i for i in all_issues if i.severity == ValidationSeverity.WARNING]

    # Log warnings
    for warning_issue in warning_issues:
        logger.warning(str(warning_issue))

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
    warning_issues = [i for i in issues if i.severity == ValidationSeverity.WARNING]

    lines = []

    if errors:
        lines.append(f"Found {len(errors)} error(s):")
        lines.append("-" * 40)
        for error in errors:
            lines.append(str(error))
            lines.append("")

    if warning_issues:
        lines.append(f"Found {len(warning_issues)} warning(s):")
        lines.append("-" * 40)
        for warning_issue in warning_issues:
            lines.append(str(warning_issue))
            lines.append("")

    return "\n".join(lines)
