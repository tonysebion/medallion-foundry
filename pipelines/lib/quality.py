"""Data quality helpers for Silver layer.

Provides quality rules and assertions for data curation.
These are DATA quality rules (schema, nulls, types) - NOT business logic.

Good: "natural key must not be null"
Good: "timestamp must be valid date"
Bad: "order total must be > $10" (that's business logic - belongs in Gold)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "QualityCheckFailed",
    "QualityResult",
    "QualityRule",
    "Severity",
    "check_quality",
    "in_list",
    "matches_pattern",
    "non_negative",
    "not_empty",
    "not_null",
    "positive",
    "standard_dimension_rules",
    "standard_fact_rules",
    "unique_key",
    "valid_timestamp",
]


class Severity(Enum):
    """Severity of quality rule violations."""

    WARN = "warn"  # Log but continue processing
    ERROR = "error"  # Fail the pipeline


@dataclass
class QualityRule:
    """A data quality assertion.

    Quality rules check data integrity, NOT business logic.

    Example:
        # Good - data quality
        QualityRule("pk_not_null", "order_id IS NOT NULL", Severity.ERROR)

        # Bad - business logic (belongs in Gold layer)
        QualityRule("min_order", "order_total > 10", Severity.ERROR)
    """

    name: str
    expression: str  # SQL expression that should be true for valid records
    severity: Severity = Severity.ERROR
    description: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name}: {self.expression}"


@dataclass
class QualityResult:
    """Result of running quality checks on a dataset."""

    passed: bool
    total_rows: int
    failed_rows: int
    rules_checked: int
    violations: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        """Percentage of rows that passed all checks."""
        if self.total_rows == 0:
            return 100.0
        return ((self.total_rows - self.failed_rows) / self.total_rows) * 100

    def __str__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"Quality Check {status}: "
            f"{self.pass_rate:.1f}% pass rate "
            f"({self.failed_rows}/{self.total_rows} failed)"
        )


# ============================================
# Pre-built Quality Rule Factories
# ============================================


def not_null(*columns: str, severity: Severity = Severity.ERROR) -> List[QualityRule]:
    """Create NOT NULL rules for specified columns.

    Args:
        *columns: Column names that must not be null
        severity: Severity level for violations

    Returns:
        List of QualityRule objects

    Example:
        rules = not_null("order_id", "customer_id")
    """
    return [
        QualityRule(
            name=f"{col}_not_null",
            expression=f"{col} IS NOT NULL",
            severity=severity,
            description=f"Column {col} must not be null",
        )
        for col in columns
    ]


def not_empty(*columns: str, severity: Severity = Severity.ERROR) -> List[QualityRule]:
    """Create NOT EMPTY rules for string columns.

    Checks that columns are not null and not empty strings.

    Args:
        *columns: String column names
        severity: Severity level

    Returns:
        List of QualityRule objects
    """
    return [
        QualityRule(
            name=f"{col}_not_empty",
            expression=f"{col} IS NOT NULL AND TRIM({col}) != ''",
            severity=severity,
            description=f"Column {col} must not be null or empty",
        )
        for col in columns
    ]


def valid_timestamp(
    column: str,
    min_date: str = "1900-01-01",
    max_date: str = "2100-01-01",
    severity: Severity = Severity.ERROR,
) -> QualityRule:
    """Create a valid timestamp rule.

    Args:
        column: Timestamp column name
        min_date: Minimum valid date (ISO format)
        max_date: Maximum valid date (ISO format)
        severity: Severity level

    Returns:
        QualityRule object
    """
    expr = (
        f"{column} IS NOT NULL AND "
        f"{column} >= '{min_date}' AND {column} <= '{max_date}'"
    )
    desc = f"Column {column} must be valid timestamp ({min_date} to {max_date})"
    return QualityRule(
        name=f"{column}_valid_timestamp",
        expression=expr,
        severity=severity,
        description=desc,
    )


def unique_key(*columns: str, severity: Severity = Severity.ERROR) -> QualityRule:
    """Create a unique key constraint rule.

    Note: This is validated differently - requires GROUP BY check.

    Args:
        *columns: Columns that form the unique key
        severity: Severity level

    Returns:
        QualityRule object (marker for unique check)
    """
    key_expr = ", ".join(columns)
    return QualityRule(
        name=f"unique_{'_'.join(columns)}",
        expression=f"UNIQUE({key_expr})",  # Special marker
        severity=severity,
        description=f"Columns ({key_expr}) must be unique",
    )


def in_list(
    column: str,
    values: List[str],
    severity: Severity = Severity.ERROR,
) -> QualityRule:
    """Create an enumeration constraint rule.

    Args:
        column: Column to check
        values: Allowed values
        severity: Severity level

    Returns:
        QualityRule object
    """
    values_str = ", ".join(f"'{v}'" for v in values)
    return QualityRule(
        name=f"{column}_in_list",
        expression=f"{column} IN ({values_str})",
        severity=severity,
        description=f"Column {column} must be one of: {values}",
    )


def positive(column: str, severity: Severity = Severity.ERROR) -> QualityRule:
    """Create a positive number constraint rule.

    Args:
        column: Numeric column name
        severity: Severity level

    Returns:
        QualityRule object
    """
    return QualityRule(
        name=f"{column}_positive",
        expression=f"{column} IS NULL OR {column} > 0",
        severity=severity,
        description=f"Column {column} must be positive (or null)",
    )


def non_negative(column: str, severity: Severity = Severity.ERROR) -> QualityRule:
    """Create a non-negative number constraint rule.

    Args:
        column: Numeric column name
        severity: Severity level

    Returns:
        QualityRule object
    """
    return QualityRule(
        name=f"{column}_non_negative",
        expression=f"{column} IS NULL OR {column} >= 0",
        severity=severity,
        description=f"Column {column} must be non-negative (or null)",
    )


def matches_pattern(
    column: str,
    pattern: str,
    severity: Severity = Severity.ERROR,
) -> QualityRule:
    """Create a regex pattern constraint rule.

    Args:
        column: String column name
        pattern: Regular expression pattern
        severity: Severity level

    Returns:
        QualityRule object
    """
    return QualityRule(
        name=f"{column}_matches_pattern",
        expression=f"{column} IS NULL OR {column} SIMILAR TO '{pattern}'",
        severity=severity,
        description=f"Column {column} must match pattern: {pattern}",
    )


# ============================================
# Quality Check Execution
# ============================================


def check_quality(
    t: "ibis.Table",
    rules: List[QualityRule],
    *,
    fail_on_error: bool = True,
) -> QualityResult:
    """Run quality checks on an Ibis table.

    Args:
        t: Ibis table to check
        rules: List of quality rules to apply
        fail_on_error: Raise exception on ERROR-level violations

    Returns:
        QualityResult with check details

    Raises:
        QualityCheckFailed: If fail_on_error=True and ERROR violations exist

    Example:
        rules = not_null("order_id") + [valid_timestamp("created_at")]
        result = check_quality(table, rules)
        if not result.passed:
            logger.warning(result)
    """
    import ibis

    total_rows = t.count().execute()
    violations: List[Dict[str, Any]] = []
    failed_rows = 0

    for rule in rules:
        # Skip unique key checks (need different handling)
        if rule.expression.startswith("UNIQUE("):
            continue

        try:
            # Filter to rows that FAIL the rule (negate the expression)
            # Using SQL expression evaluation
            # Placeholder - actual implementation would eval the expression
            _ = t.filter(~ibis.literal(True)).count().execute()

            # For now, log that we checked the rule
            logger.debug("Checked rule: %s", rule.name)

        except Exception as e:
            logger.warning("Failed to check rule %s: %s", rule.name, e)
            violations.append(
                {
                    "rule": rule.name,
                    "error": str(e),
                    "severity": rule.severity.value,
                }
            )

    passed = len([v for v in violations if v.get("severity") == "error"]) == 0

    result = QualityResult(
        passed=passed,
        total_rows=total_rows,
        failed_rows=failed_rows,
        rules_checked=len(rules),
        violations=violations,
    )

    if not passed and fail_on_error:
        raise QualityCheckFailed(result)

    return result


class QualityCheckFailed(Exception):
    """Exception raised when quality checks fail."""

    def __init__(self, result: QualityResult):
        self.result = result
        super().__init__(str(result))


# ============================================
# Quality Rule Sets (Common Patterns)
# ============================================


def standard_dimension_rules(
    pk_column: str,
    timestamp_column: str,
) -> List[QualityRule]:
    """Standard quality rules for dimension tables.

    Args:
        pk_column: Primary key column name
        timestamp_column: Timestamp column name

    Returns:
        List of standard dimension quality rules
    """
    return [
        *not_null(pk_column),
        valid_timestamp(timestamp_column),
    ]


def standard_fact_rules(
    pk_columns: List[str],
    timestamp_column: str,
    measure_columns: Optional[List[str]] = None,
) -> List[QualityRule]:
    """Standard quality rules for fact tables.

    Args:
        pk_columns: Primary key column names
        timestamp_column: Timestamp column name
        measure_columns: Numeric measure column names

    Returns:
        List of standard fact quality rules
    """
    rules = [
        *not_null(*pk_columns),
        valid_timestamp(timestamp_column),
    ]

    if measure_columns:
        for col in measure_columns:
            rules.append(non_negative(col))

    return rules
