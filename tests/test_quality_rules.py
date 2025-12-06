"""Tests for Quality Rules Engine per spec Section 7.

Tests quality rule evaluation:
- Rule definition and parsing
- Expression evaluation
- Error vs warn level distinction
- Rule results in metadata
"""

from typing import Any, Dict, List

import pytest

from core.quality.rules import QualityRule, RuleLevel, RuleDefinition, parse_rules
from core.quality.engine import QualityEngine
from core.quality.reporter import RuleResult, QualityReport


class TestQualityRuleDefinition:
    """Test quality rule definition and parsing."""

    def test_rule_definition_from_dict(self):
        """RuleDefinition should parse from dictionary."""
        rule_dict = {
            "id": "not_null_id",
            "expression": "id IS NOT NULL",
            "level": "error",
            "description": "ID must not be null",
        }
        defn = RuleDefinition.from_dict(rule_dict)

        assert defn.id == "not_null_id"
        assert defn.expression == "id IS NOT NULL"
        assert defn.level == RuleLevel.ERROR
        assert defn.description == "ID must not be null"

    def test_rule_level_defaults_to_error(self):
        """Rule level should default to error."""
        rule_dict = {
            "id": "test_rule",
            "expression": "value > 0",
        }
        defn = RuleDefinition.from_dict(rule_dict)
        assert defn.level == RuleLevel.ERROR

    def test_rule_level_warn(self):
        """Rule should accept warn level."""
        rule_dict = {
            "id": "test_rule",
            "expression": "value > 0",
            "level": "warn",
        }
        defn = RuleDefinition.from_dict(rule_dict)
        assert defn.level == RuleLevel.WARN

    def test_rule_requires_id(self):
        """Rule should require id."""
        with pytest.raises(ValueError, match="'id'"):
            RuleDefinition.from_dict({"expression": "x > 0"})

    def test_rule_requires_expression(self):
        """Rule should require expression."""
        with pytest.raises(ValueError, match="'expression'"):
            RuleDefinition.from_dict({"id": "test"})


class TestRuleCompilation:
    """Test rule expression compilation."""

    def _make_rule(self, expression: str, rule_id: str = "test") -> QualityRule:
        """Helper to create a QualityRule for testing."""
        defn = RuleDefinition(id=rule_id, level=RuleLevel.ERROR, expression=expression)
        return QualityRule(defn)

    def test_compile_not_null_expression(self):
        """Should compile IS NOT NULL expression."""
        rule = self._make_rule("id IS NOT NULL")

        assert rule.evaluate({"id": 1}) is True
        assert rule.evaluate({"id": None}) is False

    def test_compile_is_null_expression(self):
        """Should compile IS NULL expression."""
        rule = self._make_rule("value IS NULL")

        assert rule.evaluate({"value": None}) is True
        assert rule.evaluate({"value": 0}) is False

    def test_compile_comparison_expression(self):
        """Should compile comparison expressions."""
        rule = self._make_rule("amount >= 0")

        assert rule.evaluate({"amount": 100}) is True
        assert rule.evaluate({"amount": 0}) is True
        assert rule.evaluate({"amount": -1}) is False

    def test_compile_in_expression(self):
        """Should compile IN expression."""
        rule = self._make_rule("status IN ('active', 'pending')")

        assert rule.evaluate({"status": "active"}) is True
        assert rule.evaluate({"status": "pending"}) is True
        assert rule.evaluate({"status": "inactive"}) is False

    def test_compile_len_expression(self):
        """Should compile LEN function."""
        rule = self._make_rule("LEN(name) > 0")

        assert rule.evaluate({"name": "test"}) is True
        assert rule.evaluate({"name": ""}) is False


class TestQualityEngine:
    """Test quality engine evaluation."""

    def test_engine_evaluates_all_rules(self):
        """Engine should evaluate all rules."""
        rules = [
            {"id": "rule1", "expression": "id IS NOT NULL"},
            {"id": "rule2", "expression": "amount >= 0"},
        ]
        engine = QualityEngine(rules)

        records = [
            {"id": 1, "amount": 100},
            {"id": 2, "amount": 200},
        ]

        report = engine.evaluate(records)

        assert report.total_records == 2
        assert report.rules_evaluated == 2
        assert report.all_passed

    def test_engine_reports_failures(self):
        """Engine should report rule failures."""
        rules = [
            {"id": "positive_amount", "expression": "amount >= 0", "level": "error"},
        ]
        engine = QualityEngine(rules)

        records = [
            {"id": 1, "amount": 100},
            {"id": 2, "amount": -50},  # Fails
            {"id": 3, "amount": 200},
        ]

        report = engine.evaluate(records)

        assert not report.all_passed
        assert report.error_count == 1

        # Check rule result
        rule_result = report.rule_results["positive_amount"]
        assert not rule_result.passed
        assert rule_result.failed_count == 1

    def test_engine_distinguishes_error_and_warn(self):
        """Engine should distinguish error and warn levels."""
        rules = [
            {"id": "required_id", "expression": "id IS NOT NULL", "level": "error"},
            {"id": "preferred_name", "expression": "LEN(name) > 0", "level": "warn"},
        ]
        engine = QualityEngine(rules)

        records = [
            {"id": 1, "name": ""},  # Warn level fails
            {"id": None, "name": "Test"},  # Error level fails
        ]

        report = engine.evaluate(records)

        assert report.error_count == 1
        assert report.warn_count == 1
        assert not report.rule_results["required_id"].passed
        assert not report.rule_results["preferred_name"].passed

    def test_engine_fail_on_error_raises(self):
        """Engine should raise exception when fail_on_error=True."""
        rules = [
            {"id": "not_null", "expression": "id IS NOT NULL", "level": "error"},
        ]
        engine = QualityEngine(rules)

        records = [{"id": None}]

        with pytest.raises(ValueError, match="Quality validation failed"):
            engine.evaluate(records, fail_on_error=True)

    def test_engine_fail_on_error_ignores_warnings(self):
        """fail_on_error should not raise for warn-level failures."""
        rules = [
            {"id": "warn_rule", "expression": "value > 0", "level": "warn"},
        ]
        engine = QualityEngine(rules)

        records = [{"value": 0}]

        # Should not raise
        report = engine.evaluate(records, fail_on_error=True)
        assert report.warn_count == 1

    def test_engine_handles_missing_columns(self):
        """Engine should handle missing columns gracefully."""
        rules = [
            {"id": "check_value", "expression": "value IS NOT NULL"},
        ]
        engine = QualityEngine(rules)

        records = [{"id": 1}]  # Missing 'value' column

        report = engine.evaluate(records)
        # Missing column should be treated as null
        assert not report.rule_results["check_value"].passed


class TestQualityReport:
    """Test quality report generation."""

    def test_report_to_dict(self):
        """Report should convert to dictionary."""
        rules = [
            {"id": "rule1", "expression": "id IS NOT NULL"},
        ]
        engine = QualityEngine(rules)
        report = engine.evaluate([{"id": 1}, {"id": 2}])

        report_dict = report.to_dict()

        assert "total_records" in report_dict
        assert "rules_evaluated" in report_dict
        assert "error_count" in report_dict
        assert "warn_count" in report_dict
        assert "rule_results" in report_dict

    def test_report_includes_rule_details(self):
        """Report should include detailed rule results."""
        rules = [
            {"id": "rule1", "expression": "amount >= 0", "description": "Amount must be non-negative"},
        ]
        engine = QualityEngine(rules)
        records = [
            {"amount": 100},
            {"amount": -10},
        ]
        report = engine.evaluate(records)

        report_dict = report.to_dict()
        rule_detail = report_dict["rule_results"]["rule1"]

        assert "passed" in rule_detail
        assert "failed_count" in rule_detail
        assert "level" in rule_detail


class TestConfigValidationWithQualityRules:
    """Test config validation includes quality rules validation."""

    def test_validate_quality_rules_config(self, temp_dir):
        """Quality rules in config should validate."""
        from core.config.validation import validate_config_dict

        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "data",
                "type": "file",
                "file": {"path": str(temp_dir)},
                "run": {"load_pattern": "snapshot"},
            },
            "quality_rules": [
                {"id": "not_null_id", "expression": "id IS NOT NULL", "level": "error"},
                {"id": "positive_amount", "expression": "amount >= 0", "level": "warn"},
            ],
        }

        result = validate_config_dict(config)
        assert len(result["quality_rules"]) == 2

    def test_invalid_quality_rule_level_rejected(self, temp_dir):
        """Invalid rule level should be rejected."""
        from core.config.validation import validate_config_dict

        config = {
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "local_path": str(temp_dir),
                }
            },
            "source": {
                "system": "test",
                "table": "data",
                "type": "file",
                "file": {"path": str(temp_dir)},
                "run": {"load_pattern": "snapshot"},
            },
            "quality_rules": [
                {"id": "test", "expression": "x > 0", "level": "invalid"},
            ],
        }

        with pytest.raises(ValueError, match="level must be one of"):
            validate_config_dict(config)
