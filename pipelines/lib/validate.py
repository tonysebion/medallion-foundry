"""Validation helpers for pipeline configuration.

Provides validation functions that catch common configuration errors
early and provide helpful error messages for non-Python developers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pipelines.lib.bronze import BronzeSource
    from pipelines.lib.silver import SilverEntity

logger = logging.getLogger(__name__)

__all__ = [
    "ValidationIssue",
    "ValidationSeverity",
    "format_validation_report",
    "validate_and_raise",
    "validate_bronze_source",
    "validate_silver_entity",
]


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
