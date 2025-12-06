"""Quality rules evaluation engine per spec Section 7.

The engine evaluates rules against records and collects results.
Rules with level='error' cause job failure; level='warn' logs warnings.

Usage:
```python
from core.quality import QualityEngine, evaluate_rules

engine = QualityEngine(config)
report = engine.evaluate(records)

if report.has_errors:
    raise ValueError(f"Quality check failed: {report.error_count} errors")
```
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from .rules import QualityRule, RuleLevel, parse_rules
from .reporter import RuleResult, QualityReport

logger = logging.getLogger(__name__)


class QualityEngine:
    """Engine for evaluating quality rules against data.

    Supports both record-by-record evaluation and bulk DataFrame evaluation.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        rules: Optional[List[QualityRule]] = None,
    ):
        """Initialize engine with rules.

        Args:
            config: Pipeline config with quality_rules section
            rules: Pre-parsed list of QualityRule objects
        """
        if rules:
            self.rules = rules
        elif config:
            self.rules = parse_rules(config)
        else:
            self.rules = []

        logger.info(f"QualityEngine initialized with {len(self.rules)} rules")

    def evaluate(
        self,
        records: List[Dict[str, Any]],
        fail_on_error: bool = False,
    ) -> QualityReport:
        """Evaluate all rules against records.

        Args:
            records: List of record dictionaries to evaluate
            fail_on_error: If True, raise exception on first error-level failure

        Returns:
            QualityReport with results for all rules

        Raises:
            ValueError: If fail_on_error=True and error-level rule fails
        """
        if not self.rules:
            return QualityReport(total_records=len(records))

        results: List[RuleResult] = []
        total_records = len(records)

        for rule in self.rules:
            result = self._evaluate_rule(rule, records)
            results.append(result)

            if fail_on_error and not result.passed and rule.level == RuleLevel.ERROR:
                raise ValueError(
                    f"Quality rule '{rule.id}' failed: "
                    f"{result.failed_count}/{total_records} records failed"
                )

        report = QualityReport(
            total_records=total_records,
            results=results,
        )

        if report.error_count > 0:
            logger.error(
                f"Quality check found {report.error_count} error(s) "
                f"in {report.failed_record_count} records"
            )
        if report.warn_count > 0:
            logger.warning(
                f"Quality check found {report.warn_count} warning(s)"
            )

        return report

    def _evaluate_rule(
        self,
        rule: QualityRule,
        records: List[Dict[str, Any]],
    ) -> RuleResult:
        """Evaluate a single rule against all records."""
        total = len(records)
        results = rule.evaluate_batch(records)

        passed_count = sum(1 for r in results if r)
        failed_count = total - passed_count

        # Collect sample of failed records (for debugging)
        failed_indices = [i for i, r in enumerate(results) if not r]
        sample_failures = failed_indices[:5]  # Keep first 5 failures

        return RuleResult(
            rule_id=rule.id,
            level=rule.level,
            expression=rule.expression,
            passed=failed_count == 0,
            total_count=total,
            passed_count=passed_count,
            failed_count=failed_count,
            failed_indices=sample_failures,
            description=rule.description,
        )

    def evaluate_dataframe(
        self,
        df: pd.DataFrame,
        fail_on_error: bool = False,
    ) -> QualityReport:
        """Evaluate rules against a pandas DataFrame.

        More efficient than record-by-record evaluation for large datasets.

        Args:
            df: DataFrame to evaluate
            fail_on_error: If True, raise exception on first error-level failure

        Returns:
            QualityReport with results
        """
        # Convert to records for evaluation
        # Future optimization: use DataFrame.query() for bulk evaluation
        records = df.to_dict("records")
        return self.evaluate(records, fail_on_error=fail_on_error)

    def add_rule(self, rule: QualityRule) -> None:
        """Add a rule to the engine."""
        self.rules.append(rule)

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a rule by ID.

        Returns:
            True if rule was found and removed
        """
        for i, rule in enumerate(self.rules):
            if rule.id == rule_id:
                self.rules.pop(i)
                return True
        return False

    @property
    def rule_count(self) -> int:
        """Number of rules in the engine."""
        return len(self.rules)

    @property
    def error_rules(self) -> List[QualityRule]:
        """Rules with error level."""
        return [r for r in self.rules if r.level == RuleLevel.ERROR]

    @property
    def warn_rules(self) -> List[QualityRule]:
        """Rules with warn level."""
        return [r for r in self.rules if r.level == RuleLevel.WARN]


def evaluate_rules(
    records: List[Dict[str, Any]],
    config: Dict[str, Any],
    fail_on_error: bool = False,
) -> QualityReport:
    """Convenience function to evaluate rules from config.

    Args:
        records: List of record dictionaries
        config: Pipeline config with quality_rules section
        fail_on_error: If True, raise exception on error-level failures

    Returns:
        QualityReport with results
    """
    engine = QualityEngine(config)
    return engine.evaluate(records, fail_on_error=fail_on_error)


def build_quality_engine(config: Dict[str, Any]) -> QualityEngine:
    """Build a QualityEngine from config.

    Args:
        config: Full pipeline config dictionary

    Returns:
        Configured QualityEngine instance
    """
    return QualityEngine(config)
