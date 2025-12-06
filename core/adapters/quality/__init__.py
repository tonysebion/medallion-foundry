"""Quality rules engine per spec Section 7.

This module provides:
- Rule definition classes for expressing data quality rules
- Rule evaluation engine for running rules against data
- Results collection and reporting

Example config:
```yaml
quality_rules:
  - id: non_null_claim_id
    level: error
    expression: "claim_id IS NOT NULL"
    description: "Claim ID must not be null"
  - id: valid_amount
    level: warn
    expression: "amount >= 0"
    description: "Amount should be non-negative"
```
"""

from .rules import QualityRule, RuleLevel, RuleDefinition
from .engine import QualityEngine, evaluate_rules
from .reporter import RuleResult, QualityReport, format_quality_report

__all__ = [
    "QualityRule",
    "RuleLevel",
    "RuleDefinition",
    "QualityEngine",
    "evaluate_rules",
    "RuleResult",
    "QualityReport",
    "format_quality_report",
]
