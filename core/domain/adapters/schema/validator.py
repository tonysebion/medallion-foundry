"""Schema validation implementation per spec Section 6.

Validates DataFrames against expected schema specifications.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from .types import ColumnSpec, DataType, SchemaSpec, parse_schema_config

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Single validation error."""

    column: str
    error_type: str
    message: str
    severity: str = "error"  # error, warning

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "column": self.column,
            "error_type": self.error_type,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass
class ValidationResult:
    """Result of schema validation."""

    valid: bool = True
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    columns_validated: int = 0
    columns_missing: List[str] = field(default_factory=list)
    columns_extra: List[str] = field(default_factory=list)
    type_mismatches: List[str] = field(default_factory=list)
    null_violations: List[str] = field(default_factory=list)

    def add_error(self, error: ValidationError) -> None:
        """Add an error to the result."""
        if error.severity == "warning":
            self.warnings.append(error)
        else:
            self.errors.append(error)
            self.valid = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "valid": self.valid,
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [w.to_dict() for w in self.warnings],
            "columns_validated": self.columns_validated,
            "columns_missing": self.columns_missing,
            "columns_extra": self.columns_extra,
            "type_mismatches": self.type_mismatches,
            "null_violations": self.null_violations,
        }


class SchemaValidator:
    """Validates DataFrames against schema specifications."""

    def __init__(
        self,
        schema: Optional[SchemaSpec] = None,
        strict: bool = True,
        coerce_types: bool = False,
    ):
        """Initialize validator.

        Args:
            schema: Schema specification to validate against
            strict: If True, fail on extra columns
            coerce_types: If True, attempt to coerce types
        """
        self.schema = schema
        self.strict = strict
        self.coerce_types = coerce_types

    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Validate DataFrame against schema.

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult with errors and warnings
        """
        if self.schema is None:
            return ValidationResult(valid=True, columns_validated=len(df.columns))

        result = ValidationResult()

        # Check for missing columns
        actual_columns = set(df.columns)
        expected_columns = set(self.schema.column_names)

        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        result.columns_missing = list(missing)
        result.columns_extra = list(extra)

        # Missing required columns are errors
        for col_name in missing:
            col_spec = self.schema.get_column(col_name)
            if col_spec and not col_spec.nullable:
                result.add_error(ValidationError(
                    column=col_name,
                    error_type="missing_required",
                    message=f"Required column '{col_name}' is missing",
                ))
            else:
                result.add_error(ValidationError(
                    column=col_name,
                    error_type="missing_column",
                    message=f"Expected column '{col_name}' is missing",
                    severity="warning" if col_spec and col_spec.nullable else "error",
                ))

        # Extra columns in strict mode
        if self.strict and extra:
            for col_name in extra:
                result.add_error(ValidationError(
                    column=col_name,
                    error_type="unexpected_column",
                    message=f"Unexpected column '{col_name}' found (strict mode)",
                ))

        # Validate existing columns
        for col_spec in self.schema.columns:
            if col_spec.name not in actual_columns:
                continue

            result.columns_validated += 1
            column_errors = self._validate_column(df, col_spec)
            for error in column_errors:
                result.add_error(error)

        return result

    def _validate_column(
        self,
        df: pd.DataFrame,
        col_spec: ColumnSpec,
    ) -> List[ValidationError]:
        """Validate a single column."""
        errors: List[ValidationError] = []
        col_name = col_spec.name
        series = df[col_name]

        # Check nullability
        if not col_spec.nullable and series.isna().any():
            null_count = series.isna().sum()
            errors.append(ValidationError(
                column=col_name,
                error_type="null_violation",
                message=f"Column '{col_name}' has {null_count} null values but is not nullable",
            ))

        # Check type compatibility
        actual_type = str(series.dtype)
        if not col_spec.matches_type(actual_type):
            errors.append(ValidationError(
                column=col_name,
                error_type="type_mismatch",
                message=f"Column '{col_name}' has type '{actual_type}', expected '{col_spec.type.value}'",
                severity="warning" if self.coerce_types else "error",
            ))

        return errors

    def coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        """Attempt to coerce DataFrame types to match schema.

        Args:
            df: DataFrame to coerce

        Returns:
            DataFrame with coerced types
        """
        if self.schema is None:
            return df

        result = df.copy()

        for col_spec in self.schema.columns:
            if col_spec.name not in result.columns:
                continue

            result[col_spec.name] = self._coerce_column(
                result[col_spec.name], col_spec
            )

        return result

    def _coerce_column(
        self,
        series: pd.Series,
        col_spec: ColumnSpec,
    ) -> pd.Series:
        """Coerce a single column to expected type."""
        try:
            if col_spec.type == DataType.STRING:
                return series.astype(str)
            elif col_spec.type == DataType.INTEGER:
                return pd.to_numeric(series, errors="coerce").astype("Int64")
            elif col_spec.type == DataType.BIGINT:
                return pd.to_numeric(series, errors="coerce").astype("Int64")
            elif col_spec.type in (DataType.FLOAT, DataType.DOUBLE, DataType.DECIMAL):
                return pd.to_numeric(series, errors="coerce")
            elif col_spec.type == DataType.BOOLEAN:
                return series.astype(bool)
            elif col_spec.type in (DataType.TIMESTAMP, DataType.DATETIME):
                return pd.to_datetime(series, errors="coerce", format=col_spec.format)
            elif col_spec.type == DataType.DATE:
                return pd.to_datetime(series, errors="coerce", format=col_spec.format).dt.date
        except Exception as e:
            logger.warning("Could not coerce column '%s': %s", col_spec.name, e)

        return series


def validate_schema(
    df: pd.DataFrame,
    config: Dict[str, Any],
    strict: bool = True,
    fail_on_error: bool = False,
) -> ValidationResult:
    """Convenience function to validate DataFrame against config schema.

    Args:
        df: DataFrame to validate
        config: Pipeline config with schema section
        strict: If True, fail on extra columns
        fail_on_error: If True, raise exception on validation errors

    Returns:
        ValidationResult

    Raises:
        ValueError: If fail_on_error=True and validation fails
    """
    schema = parse_schema_config(config)

    if schema is None:
        return ValidationResult(valid=True, columns_validated=len(df.columns))

    validator = SchemaValidator(schema, strict=strict)
    result = validator.validate(df)

    if fail_on_error and not result.valid:
        error_msgs = [e.message for e in result.errors]
        raise ValueError(f"Schema validation failed: {'; '.join(error_msgs)}")

    return result
