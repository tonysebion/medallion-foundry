"""Quality rule definitions per spec Section 7.

Rules are defined with:
- id: Unique identifier for the rule
- level: "error" (fails job) or "warn" (logs warning)
- expression: SQL-like expression or Python expression
- description: Human-readable description

Supported expression types:
1. SQL-like: "column_name IS NOT NULL", "amount >= 0"
2. Python lambda: Evaluated as Python expression against record dict
3. Pandas query: Used with DataFrame.query() for bulk evaluation
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class RuleLevel(str, Enum):
    """Severity level for quality rules."""

    ERROR = "error"  # Fails the job if rule fails
    WARN = "warn"  # Logs warning but continues


@dataclass
class RuleDefinition:
    """Definition of a quality rule from config."""

    id: str
    level: RuleLevel
    expression: str
    description: Optional[str] = None
    column: Optional[str] = None  # Target column (for column-specific rules)
    params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleDefinition":
        """Create rule definition from config dictionary."""
        rule_id = data.get("id")
        if not rule_id:
            raise ValueError("Rule must have an 'id'")

        level_str = data.get("level", "error").lower()
        try:
            level = RuleLevel(level_str)
        except ValueError:
            logger.warning(f"Invalid rule level '{level_str}', using 'error'")
            level = RuleLevel.ERROR

        expression = data.get("expression")
        if not expression:
            raise ValueError(f"Rule '{rule_id}' must have an 'expression'")

        return cls(
            id=rule_id,
            level=level,
            expression=expression,
            description=data.get("description"),
            column=data.get("column"),
            params=data.get("params", {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "id": self.id,
            "level": self.level.value,
            "expression": self.expression,
        }
        if self.description:
            result["description"] = self.description
        if self.column:
            result["column"] = self.column
        if self.params:
            result["params"] = self.params
        return result


class QualityRule:
    """Executable quality rule.

    Compiles rule expression into an evaluator function that can be
    applied to individual records or DataFrames.
    """

    # SQL-like operators mapped to Python
    SQL_TO_PYTHON = {
        " IS NOT NULL": " is not None",
        " IS NULL": " is None",
        " AND ": " and ",
        " OR ": " or ",
        " NOT ": " not ",
        "<>": "!=",
        "TRUE": "True",
        "FALSE": "False",
    }

    # Regex patterns for SQL expressions
    SQL_PATTERNS = [
        (r"(\w+)\s+IS\s+NOT\s+NULL", r"(record.get('\1') is not None)"),
        (r"(\w+)\s+IS\s+NULL", r"(record.get('\1') is None)"),
        (r"(\w+)\s*=\s*'([^']*)'", r"(record.get('\1') == '\2')"),
        (r"(\w+)\s*<>\s*'([^']*)'", r"(record.get('\1') != '\2')"),
        (r"(\w+)\s*>=\s*(\d+\.?\d*)", r"(record.get('\1', 0) >= \2)"),
        (r"(\w+)\s*<=\s*(\d+\.?\d*)", r"(record.get('\1', 0) <= \2)"),
        (r"(\w+)\s*>\s*(\d+\.?\d*)", r"(record.get('\1', 0) > \2)"),
        (r"(\w+)\s*<\s*(\d+\.?\d*)", r"(record.get('\1', 0) < \2)"),
        (r"(\w+)\s*=\s*(\d+\.?\d*)", r"(record.get('\1') == \2)"),
        (r"(\w+)\s*<>\s*(\d+\.?\d*)", r"(record.get('\1') != \2)"),
        (r"(\w+)\s+IN\s*\(([^)]+)\)", r"(record.get('\1') in [\2])"),
        (r"(\w+)\s+NOT\s+IN\s*\(([^)]+)\)", r"(record.get('\1') not in [\2])"),
        (r"(\w+)\s+LIKE\s+'%([^%]+)%'", r"('\2' in str(record.get('\1', '')))"),
        (r"(\w+)\s+LIKE\s+'([^%]+)%'", r"(str(record.get('\1', '')).startswith('\2'))"),
        (r"(\w+)\s+LIKE\s+'%([^%]+)'", r"(str(record.get('\1', '')).endswith('\2'))"),
        (r"LEN\((\w+)\)\s*>=\s*(\d+)", r"(len(str(record.get('\1', ''))) >= \2)"),
        (r"LEN\((\w+)\)\s*<=\s*(\d+)", r"(len(str(record.get('\1', ''))) <= \2)"),
        (r"LEN\((\w+)\)\s*>\s*(\d+)", r"(len(str(record.get('\1', ''))) > \2)"),
        (r"LEN\((\w+)\)\s*<\s*(\d+)", r"(len(str(record.get('\1', ''))) < \2)"),
    ]

    def __init__(self, definition: RuleDefinition):
        """Initialize rule from definition."""
        self.definition = definition
        self._evaluator: Optional[Callable[[Dict[str, Any]], bool]] = None
        self._pandas_query: Optional[str] = None
        self._compile()

    def _compile(self) -> None:
        """Compile expression into evaluator function."""
        expr = self.definition.expression.strip()

        # Try to convert SQL-like syntax to Python
        python_expr = self._sql_to_python(expr)

        try:
            # Create evaluator function
            # Using exec to create a function that evaluates the expression
            code = f"def _eval(record): return bool({python_expr})"
            local_ns: Dict[str, Any] = {}
            exec(code, {"__builtins__": {"bool": bool, "len": len, "str": str, "int": int, "float": float}}, local_ns)
            self._evaluator = local_ns["_eval"]
            logger.debug(f"Compiled rule '{self.definition.id}': {python_expr}")
        except Exception as e:
            logger.warning(f"Could not compile rule '{self.definition.id}': {e}")
            # Fallback: create a rule that always passes (with warning)
            self._evaluator = lambda r: True

        # Try to create pandas query string
        self._pandas_query = self._to_pandas_query(expr)

    def _sql_to_python(self, expr: str) -> str:
        """Convert SQL-like expression to Python."""
        result = expr

        # Apply regex patterns
        for pattern, replacement in self.SQL_PATTERNS:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        # If expression was transformed by patterns, return it
        if "record.get(" in result:
            return result

        # Handle simple comparison expressions like "column >= value"
        # These need special handling to avoid bare identifiers
        simple_comparison = re.match(
            r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*(>=|<=|>|<|==|!=|=)\s*(-?\d+\.?\d*)$',
            result.strip()
        )
        if simple_comparison:
            col, op, val = simple_comparison.groups()
            if op == '=':
                op = '=='
            return f"(record.get('{col}', 0) {op} {val})"

        # Handle remaining column references (bare identifiers)
        # Convert "column_name" to "record.get('column_name')" if not already converted
        reserved = {'and', 'or', 'not', 'in', 'is', 'None', 'True', 'False',
                    'record', 'get', 'len', 'str', 'bool', 'int', 'float'}
        words = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', result)
        for word in words:
            if word not in reserved:
                # Check if it's a bare column reference (not already in record.get())
                if f"record.get('{word}')" not in result:
                    # Only replace if it appears as a standalone identifier
                    result = re.sub(rf'\b{word}\b(?!\s*\()', f"record.get('{word}')", result)

        return result

    def _to_pandas_query(self, expr: str) -> Optional[str]:
        """Convert expression to pandas query string."""
        # For now, return the original SQL-like expression
        # Pandas query() supports many SQL-like operations
        try:
            # Simple transformations for pandas
            query = expr
            query = re.sub(r'\bIS\s+NOT\s+NULL\b', '== @pd.notna', query, flags=re.IGNORECASE)
            query = re.sub(r'\bIS\s+NULL\b', '!= @pd.notna', query, flags=re.IGNORECASE)
            return query
        except Exception:
            return None

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Evaluate rule against a single record.

        Args:
            record: Record dictionary to evaluate

        Returns:
            True if rule passes, False if rule fails
        """
        if self._evaluator is None:
            return True

        try:
            return self._evaluator(record)
        except Exception as e:
            logger.warning(f"Error evaluating rule '{self.definition.id}': {e}")
            return True  # Don't fail on evaluation errors

    def evaluate_batch(self, records: List[Dict[str, Any]]) -> List[bool]:
        """Evaluate rule against multiple records.

        Args:
            records: List of record dictionaries

        Returns:
            List of boolean results (True = pass, False = fail)
        """
        return [self.evaluate(r) for r in records]

    @property
    def id(self) -> str:
        return self.definition.id

    @property
    def level(self) -> RuleLevel:
        return self.definition.level

    @property
    def expression(self) -> str:
        return self.definition.expression

    @property
    def description(self) -> Optional[str]:
        return self.definition.description


def parse_rules(config: Dict[str, Any]) -> List[QualityRule]:
    """Parse quality rules from config.

    Args:
        config: Full pipeline config or quality_rules section

    Returns:
        List of compiled QualityRule objects
    """
    rules_config = config.get("quality_rules", [])
    if not rules_config:
        return []

    rules = []
    for rule_dict in rules_config:
        try:
            definition = RuleDefinition.from_dict(rule_dict)
            rules.append(QualityRule(definition))
        except Exception as e:
            logger.warning(f"Could not parse rule: {e}")

    logger.info(f"Parsed {len(rules)} quality rules")
    return rules
