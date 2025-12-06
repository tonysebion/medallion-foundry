"""Quality rule results reporting per spec Section 7.

Provides:
- RuleResult: Result of a single rule evaluation
- QualityReport: Aggregated report of all rule evaluations
- Formatting functions for human-readable output
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from .rules import RuleLevel

logger = logging.getLogger(__name__)


@dataclass
class RuleResult:
    """Result of evaluating a single quality rule."""

    rule_id: str
    level: RuleLevel
    expression: str
    passed: bool
    total_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    failed_indices: List[int] = field(default_factory=list)
    description: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def pass_rate(self) -> float:
        """Percentage of records that passed."""
        if self.total_count == 0:
            return 100.0
        return (self.passed_count / self.total_count) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "rule_id": self.rule_id,
            "level": self.level.value,
            "expression": self.expression,
            "passed": self.passed,
            "total_count": self.total_count,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "pass_rate": round(self.pass_rate, 2),
            "description": self.description,
            "error_message": self.error_message,
        }

    def to_metadata_dict(self) -> Dict[str, Any]:
        """Convert to metadata format per spec Section 8."""
        return {
            "rule_id": self.rule_id,
            "level": self.level.value,
            "expression": self.expression,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "total_count": self.total_count,
        }


@dataclass
class QualityReport:
    """Aggregated quality report for a dataset."""

    total_records: int = 0
    results: List[RuleResult] = field(default_factory=list)
    evaluated_at: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.evaluated_at is None:
            self.evaluated_at = datetime.utcnow().isoformat() + "Z"

    @property
    def rule_count(self) -> int:
        """Total number of rules evaluated."""
        return len(self.results)

    @property
    def passed_count(self) -> int:
        """Number of rules that passed."""
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        """Number of rules that failed."""
        return sum(1 for r in self.results if not r.passed)

    @property
    def error_count(self) -> int:
        """Number of error-level rules that failed."""
        return sum(
            1 for r in self.results
            if not r.passed and r.level == RuleLevel.ERROR
        )

    @property
    def warn_count(self) -> int:
        """Number of warn-level rules that failed."""
        return sum(
            1 for r in self.results
            if not r.passed and r.level == RuleLevel.WARN
        )

    @property
    def all_passed(self) -> bool:
        """Check if all rules passed."""
        return all(r.passed for r in self.results)

    @property
    def has_errors(self) -> bool:
        """Check if any error-level rules failed."""
        return self.error_count > 0

    @property
    def has_warnings(self) -> bool:
        """Check if any warn-level rules failed."""
        return self.warn_count > 0

    @property
    def failed_record_count(self) -> int:
        """Total unique records that failed at least one rule."""
        # This is an approximation; actual unique count requires tracking indices
        max_failed = max((r.failed_count for r in self.results), default=0)
        return max_failed

    @property
    def error_results(self) -> List[RuleResult]:
        """Get failed error-level results."""
        return [r for r in self.results if not r.passed and r.level == RuleLevel.ERROR]

    @property
    def warn_results(self) -> List[RuleResult]:
        """Get failed warn-level results."""
        return [r for r in self.results if not r.passed and r.level == RuleLevel.WARN]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "evaluated_at": self.evaluated_at,
            "total_records": self.total_records,
            "rule_count": self.rule_count,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "error_count": self.error_count,
            "warn_count": self.warn_count,
            "all_passed": self.all_passed,
            "results": [r.to_dict() for r in self.results],
            "metadata": self.metadata,
        }

    def to_metadata_dict(self) -> Dict[str, Any]:
        """Convert to metadata format per spec Section 8.

        Returns format suitable for run metadata:
        {
            "quality_check_passed": bool,
            "rule_results": [...]
        }
        """
        return {
            "quality_check_passed": self.all_passed,
            "error_count": self.error_count,
            "warn_count": self.warn_count,
            "rule_results": [r.to_metadata_dict() for r in self.results],
        }


def format_quality_report(report: QualityReport) -> str:
    """Format quality report as human-readable string.

    Args:
        report: QualityReport to format

    Returns:
        Formatted string representation
    """
    lines = [
        "=" * 60,
        "QUALITY REPORT",
        "=" * 60,
        f"Evaluated: {report.evaluated_at}",
        f"Total Records: {report.total_records}",
        f"Rules Evaluated: {report.rule_count}",
        "",
        f"Results: {report.passed_count} passed, {report.failed_count} failed",
        f"  Errors: {report.error_count}",
        f"  Warnings: {report.warn_count}",
        "",
    ]

    if report.error_results:
        lines.append("ERRORS:")
        lines.append("-" * 40)
        for result in report.error_results:
            lines.append(f"  [{result.rule_id}] {result.expression}")
            lines.append(f"    Failed: {result.failed_count}/{result.total_count} ({100-result.pass_rate:.1f}%)")
            if result.description:
                lines.append(f"    Description: {result.description}")
        lines.append("")

    if report.warn_results:
        lines.append("WARNINGS:")
        lines.append("-" * 40)
        for result in report.warn_results:
            lines.append(f"  [{result.rule_id}] {result.expression}")
            lines.append(f"    Failed: {result.failed_count}/{result.total_count} ({100-result.pass_rate:.1f}%)")
            if result.description:
                lines.append(f"    Description: {result.description}")
        lines.append("")

    if report.all_passed:
        lines.append("STATUS: ALL RULES PASSED")
    elif report.has_errors:
        lines.append("STATUS: FAILED (errors detected)")
    else:
        lines.append("STATUS: PASSED WITH WARNINGS")

    lines.append("=" * 60)

    return "\n".join(lines)


def log_quality_report(report: QualityReport) -> None:
    """Log quality report at appropriate levels."""
    if report.all_passed:
        logger.info(
            f"Quality check passed: {report.rule_count} rules, "
            f"{report.total_records} records"
        )
        return

    if report.error_results:
        for result in report.error_results:
            logger.error(
                f"Quality rule failed [{result.rule_id}]: "
                f"{result.failed_count}/{result.total_count} records "
                f"({result.expression})"
            )

    if report.warn_results:
        for result in report.warn_results:
            logger.warning(
                f"Quality rule warning [{result.rule_id}]: "
                f"{result.failed_count}/{result.total_count} records "
                f"({result.expression})"
            )
