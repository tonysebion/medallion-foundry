"""Tests for pipelines.lib.quality module."""

import pandas as pd
import pytest

from pipelines.lib.quality import (
    QualityCheckFailed,
    QualityResult,
    QualityRule,
    Severity,
    check_quality,
    check_quality_pandera,
    create_pandera_schema,
    in_list,
    matches_pattern,
    non_negative,
    not_empty,
    not_null,
    positive,
    standard_dimension_rules,
    standard_fact_rules,
    unique_key,
    valid_timestamp,
)


# ============================================
# Severity enum tests
# ============================================


class TestSeverity:
    """Tests for Severity enum."""

    def test_warn_value(self):
        """WARN has correct value."""
        assert Severity.WARN.value == "warn"

    def test_error_value(self):
        """ERROR has correct value."""
        assert Severity.ERROR.value == "error"


# ============================================
# QualityRule tests
# ============================================


class TestQualityRule:
    """Tests for QualityRule dataclass."""

    def test_basic_rule_creation(self):
        """Create a basic quality rule."""
        rule = QualityRule(
            name="pk_not_null",
            expression="order_id IS NOT NULL",
            severity=Severity.ERROR,
        )
        assert rule.name == "pk_not_null"
        assert rule.expression == "order_id IS NOT NULL"
        assert rule.severity == Severity.ERROR

    def test_default_severity_is_error(self):
        """Default severity is ERROR."""
        rule = QualityRule(name="test", expression="col IS NOT NULL")
        assert rule.severity == Severity.ERROR

    def test_rule_with_description(self):
        """Rule can have optional description."""
        rule = QualityRule(
            name="test",
            expression="col > 0",
            description="Column must be positive",
        )
        assert rule.description == "Column must be positive"

    def test_str_representation(self):
        """String representation is name: expression."""
        rule = QualityRule(name="my_rule", expression="x > 0")
        assert str(rule) == "my_rule: x > 0"


# ============================================
# QualityResult tests
# ============================================


class TestQualityResult:
    """Tests for QualityResult dataclass."""

    def test_passed_result(self):
        """Result with no violations passes."""
        result = QualityResult(
            passed=True,
            total_rows=100,
            failed_rows=0,
            rules_checked=5,
        )
        assert result.passed is True
        assert result.pass_rate == 100.0

    def test_failed_result(self):
        """Result with violations fails."""
        result = QualityResult(
            passed=False,
            total_rows=100,
            failed_rows=25,
            rules_checked=5,
            violations=[{"rule": "test", "error": "failed"}],
        )
        assert result.passed is False
        assert result.pass_rate == 75.0

    def test_pass_rate_zero_rows(self):
        """Pass rate is 100% for empty dataset."""
        result = QualityResult(
            passed=True,
            total_rows=0,
            failed_rows=0,
            rules_checked=1,
        )
        assert result.pass_rate == 100.0

    def test_pass_rate_all_failed(self):
        """Pass rate is 0% when all rows fail."""
        result = QualityResult(
            passed=False,
            total_rows=50,
            failed_rows=50,
            rules_checked=1,
        )
        assert result.pass_rate == 0.0

    def test_str_representation_passed(self):
        """String shows PASSED status."""
        result = QualityResult(
            passed=True,
            total_rows=100,
            failed_rows=0,
            rules_checked=3,
        )
        s = str(result)
        assert "PASSED" in s
        assert "100.0%" in s

    def test_str_representation_failed(self):
        """String shows FAILED status."""
        result = QualityResult(
            passed=False,
            total_rows=100,
            failed_rows=10,
            rules_checked=3,
        )
        s = str(result)
        assert "FAILED" in s
        assert "90.0%" in s


# ============================================
# Rule factory function tests
# ============================================


class TestNotNull:
    """Tests for not_null() factory."""

    def test_single_column(self):
        """Creates rule for single column."""
        rules = not_null("order_id")
        assert len(rules) == 1
        assert rules[0].name == "order_id_not_null"
        assert "order_id IS NOT NULL" in rules[0].expression

    def test_multiple_columns(self):
        """Creates rules for multiple columns."""
        rules = not_null("id", "name", "email")
        assert len(rules) == 3
        assert rules[0].name == "id_not_null"
        assert rules[1].name == "name_not_null"
        assert rules[2].name == "email_not_null"

    def test_custom_severity(self):
        """Respects custom severity."""
        rules = not_null("id", severity=Severity.WARN)
        assert rules[0].severity == Severity.WARN


class TestNotEmpty:
    """Tests for not_empty() factory."""

    def test_creates_not_empty_rule(self):
        """Creates not-empty rule for string column."""
        rules = not_empty("name")
        assert len(rules) == 1
        assert "TRIM" in rules[0].expression
        assert "!= ''" in rules[0].expression

    def test_multiple_columns(self):
        """Creates rules for multiple columns."""
        rules = not_empty("name", "email")
        assert len(rules) == 2


class TestValidTimestamp:
    """Tests for valid_timestamp() factory."""

    def test_default_date_range(self):
        """Uses default date range."""
        rule = valid_timestamp("created_at")
        assert "1900-01-01" in rule.expression
        assert "2100-01-01" in rule.expression

    def test_custom_date_range(self):
        """Respects custom date range."""
        rule = valid_timestamp(
            "event_time", min_date="2020-01-01", max_date="2030-12-31"
        )
        assert "2020-01-01" in rule.expression
        assert "2030-12-31" in rule.expression

    def test_rule_name(self):
        """Rule name includes column name."""
        rule = valid_timestamp("updated_at")
        assert rule.name == "updated_at_valid_timestamp"


class TestUniqueKey:
    """Tests for unique_key() factory."""

    def test_single_column_key(self):
        """Creates unique key rule for single column."""
        rule = unique_key("id")
        assert "UNIQUE" in rule.expression
        assert "id" in rule.expression
        assert rule.name == "unique_id"

    def test_composite_key(self):
        """Creates unique key rule for composite key."""
        rule = unique_key("order_id", "line_number")
        assert "order_id" in rule.expression
        assert "line_number" in rule.expression
        assert rule.name == "unique_order_id_line_number"


class TestInList:
    """Tests for in_list() factory."""

    def test_creates_in_list_rule(self):
        """Creates IN list constraint."""
        rule = in_list("status", ["active", "inactive", "pending"])
        assert "status IN" in rule.expression
        assert "'active'" in rule.expression
        assert "'inactive'" in rule.expression
        assert "'pending'" in rule.expression

    def test_rule_name(self):
        """Rule name includes column."""
        rule = in_list("type", ["a", "b"])
        assert rule.name == "type_in_list"


class TestPositive:
    """Tests for positive() factory."""

    def test_creates_positive_rule(self):
        """Creates positive number constraint."""
        rule = positive("amount")
        assert "amount" in rule.expression
        assert "> 0" in rule.expression

    def test_allows_null(self):
        """Rule allows NULL values."""
        rule = positive("quantity")
        assert "IS NULL OR" in rule.expression


class TestNonNegative:
    """Tests for non_negative() factory."""

    def test_creates_non_negative_rule(self):
        """Creates non-negative constraint."""
        rule = non_negative("balance")
        assert "balance" in rule.expression
        assert ">= 0" in rule.expression

    def test_allows_null(self):
        """Rule allows NULL values."""
        rule = non_negative("count")
        assert "IS NULL OR" in rule.expression


class TestMatchesPattern:
    """Tests for matches_pattern() factory."""

    def test_creates_pattern_rule(self):
        """Creates regex pattern constraint."""
        rule = matches_pattern("email", "[a-z]+@[a-z]+\\.[a-z]+")
        assert "email" in rule.expression
        assert "SIMILAR TO" in rule.expression

    def test_allows_null(self):
        """Rule allows NULL values."""
        rule = matches_pattern("code", "[A-Z]{3}")
        assert "IS NULL OR" in rule.expression


# ============================================
# Rule composition tests
# ============================================


class TestRuleComposition:
    """Tests for composing rules."""

    def test_concatenate_rule_lists(self):
        """Can concatenate rule lists."""
        rules = not_null("id", "name") + [positive("amount")]
        assert len(rules) == 3

    def test_add_single_rule(self):
        """Can add single rule to list."""
        rules = not_null("id") + [valid_timestamp("created_at")]
        assert len(rules) == 2


# ============================================
# Pandera schema creation tests
# ============================================


class TestCreatePanderaSchema:
    """Tests for create_pandera_schema()."""

    def test_creates_schema_from_not_null_rules(self):
        """Creates Pandera schema from not_null rules."""
        rules = not_null("id", "name")
        schema = create_pandera_schema(rules)
        assert schema is not None
        # Schema should have columns for id and name
        assert "id" in schema.columns
        assert "name" in schema.columns

    def test_creates_schema_from_positive_rules(self):
        """Creates Pandera schema from positive rules."""
        rules = [positive("amount")]
        schema = create_pandera_schema(rules)
        assert "amount" in schema.columns

    def test_creates_schema_from_in_list_rules(self):
        """Creates Pandera schema from in_list rules."""
        rules = [in_list("status", ["active", "inactive"])]
        schema = create_pandera_schema(rules)
        assert "status" in schema.columns


# ============================================
# check_quality_pandera tests
# ============================================


class TestCheckQualityPandera:
    """Tests for check_quality_pandera()."""

    def test_passes_with_valid_data(self):
        """Passes when data meets all rules."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        rules = not_null("id", "name")
        result = check_quality_pandera(df, rules, fail_on_error=False)
        assert result.passed is True

    def test_fails_with_null_values(self):
        """Fails when not_null rule is violated."""
        df = pd.DataFrame(
            {
                "id": [1, None, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        rules = not_null("id")
        result = check_quality_pandera(df, rules, fail_on_error=False)
        assert result.passed is False

    def test_raises_exception_on_error(self):
        """Raises QualityCheckFailed when fail_on_error=True."""
        df = pd.DataFrame(
            {
                "id": [1, None, 3],
            }
        )
        rules = not_null("id")
        with pytest.raises(QualityCheckFailed):
            check_quality_pandera(df, rules, fail_on_error=True)

    def test_returns_result_on_fail_on_error_false(self):
        """Returns result without raising when fail_on_error=False."""
        df = pd.DataFrame(
            {
                "id": [1, None, 3],
            }
        )
        rules = not_null("id")
        result = check_quality_pandera(df, rules, fail_on_error=False)
        assert isinstance(result, QualityResult)


# ============================================
# check_quality (Ibis) tests
# ============================================


class TestCheckQuality:
    """Tests for check_quality() with Ibis tables."""

    def test_passes_with_valid_data(self):
        """Passes when data meets all rules."""
        import ibis

        t = ibis.memtable({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        rules = not_null("id")
        result = check_quality(t, rules, fail_on_error=False)
        assert result.passed is True

    def test_returns_total_rows(self):
        """Returns correct total row count."""
        import ibis

        t = ibis.memtable({"id": [1, 2, 3, 4, 5]})
        rules = not_null("id")
        result = check_quality(t, rules, fail_on_error=False)
        assert result.total_rows == 5

    def test_skips_unique_key_rules(self):
        """Skips UNIQUE rules (handled differently)."""
        import ibis

        t = ibis.memtable({"id": [1, 2, 3]})
        rules = [unique_key("id")]
        result = check_quality(t, rules, fail_on_error=False)
        # Should complete without error
        assert result.rules_checked == 1


# ============================================
# QualityCheckFailed exception tests
# ============================================


class TestQualityCheckFailed:
    """Tests for QualityCheckFailed exception."""

    def test_exception_contains_result(self):
        """Exception contains the QualityResult."""
        result = QualityResult(
            passed=False,
            total_rows=100,
            failed_rows=10,
            rules_checked=1,
        )
        exc = QualityCheckFailed(result)
        assert exc.result is result

    def test_exception_message(self):
        """Exception message includes result details."""
        result = QualityResult(
            passed=False,
            total_rows=100,
            failed_rows=10,
            rules_checked=1,
        )
        exc = QualityCheckFailed(result)
        assert "FAILED" in str(exc)


# ============================================
# Standard rule set tests
# ============================================


class TestStandardDimensionRules:
    """Tests for standard_dimension_rules()."""

    def test_includes_pk_not_null(self):
        """Includes NOT NULL rule for primary key."""
        rules = standard_dimension_rules("customer_id", "updated_at")
        pk_rules = [r for r in rules if "customer_id" in r.expression]
        assert len(pk_rules) >= 1

    def test_includes_timestamp_rule(self):
        """Includes valid timestamp rule."""
        rules = standard_dimension_rules("id", "last_modified")
        ts_rules = [r for r in rules if "last_modified" in r.expression]
        assert len(ts_rules) >= 1


class TestStandardFactRules:
    """Tests for standard_fact_rules()."""

    def test_includes_pk_not_null(self):
        """Includes NOT NULL rules for composite key."""
        rules = standard_fact_rules(["order_id", "line_id"], "order_date")
        pk_rules = [
            r for r in rules if "order_id" in r.expression or "line_id" in r.expression
        ]
        assert len(pk_rules) >= 2

    def test_includes_measure_rules(self):
        """Includes non-negative rules for measures."""
        rules = standard_fact_rules(
            ["id"], "date", measure_columns=["amount", "quantity"]
        )
        measure_rules = [
            r for r in rules if "amount" in r.expression or "quantity" in r.expression
        ]
        assert len(measure_rules) == 2

    def test_no_measure_rules_when_none(self):
        """No measure rules when measure_columns not provided."""
        rules = standard_fact_rules(["id"], "date")
        # Should only have pk and timestamp rules
        assert len(rules) == 2
