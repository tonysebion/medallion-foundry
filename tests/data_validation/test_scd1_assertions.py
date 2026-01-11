"""Tests for SCD1 assertion helpers."""

from __future__ import annotations

import pandas as pd

from tests.data_validation.assertions import SCD1Assertions


class TestSCD1UniqueKeys:
    """Test assert_unique_keys."""

    def test_unique_keys_pass(self):
        """All keys unique should pass."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        result = SCD1Assertions.assert_unique_keys(df, ["id"])
        assert result.passed is True

    def test_unique_keys_fail_duplicates(self):
        """Duplicate keys should fail."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 1, "name": "Bob"},  # Duplicate
                {"id": 2, "name": "Charlie"},
            ]
        )
        result = SCD1Assertions.assert_unique_keys(df, ["id"])
        assert result.passed is False
        assert "duplicate" in result.message.lower()

    def test_unique_composite_keys(self):
        """Composite keys should be validated together."""
        df = pd.DataFrame(
            [
                {"region": "US", "product": "A", "price": 100},
                {"region": "US", "product": "B", "price": 200},
                {"region": "EU", "product": "A", "price": 90},
            ]
        )
        result = SCD1Assertions.assert_unique_keys(df, ["region", "product"])
        assert result.passed is True

    def test_empty_dataframe(self):
        """Empty DataFrame should pass."""
        df = pd.DataFrame({"id": [], "name": []})
        result = SCD1Assertions.assert_unique_keys(df, ["id"])
        assert result.passed is True


class TestSCD1NoSCD2Columns:
    """Test assert_no_scd2_columns."""

    def test_no_scd2_columns_pass(self):
        """DataFrame without SCD2 columns should pass."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "status": "active"},
            ]
        )
        result = SCD1Assertions.assert_no_scd2_columns(df)
        assert result.passed is True

    def test_has_scd2_columns_fail(self):
        """DataFrame with SCD2 columns should fail."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "effective_from": "2025-01-01"},
            ]
        )
        result = SCD1Assertions.assert_no_scd2_columns(df)
        assert result.passed is False

    def test_custom_column_names(self):
        """Custom SCD2 column names should be detected."""
        df = pd.DataFrame(
            [
                {"id": 1, "valid_from": "2025-01-01"},
            ]
        )
        result = SCD1Assertions.assert_no_scd2_columns(
            df,
            effective_from="valid_from",
            effective_to="valid_to",
            is_current="current_flag",
        )
        assert result.passed is False


class TestSCD1LatestValues:
    """Test assert_latest_values."""

    def test_latest_values_match(self):
        """Expected values should match."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "status": "active"},
                {"id": 2, "name": "Bob", "status": "inactive"},
            ]
        )
        result = SCD1Assertions.assert_latest_values(
            df,
            keys=["id"],
            order_by="status",
            expected_values={
                1: {"name": "Alice", "status": "active"},
                2: {"name": "Bob", "status": "inactive"},
            },
        )
        assert result.passed is True

    def test_latest_values_mismatch(self):
        """Wrong values should fail."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "status": "active"},
            ]
        )
        result = SCD1Assertions.assert_latest_values(
            df,
            keys=["id"],
            order_by="status",
            expected_values={
                1: {"name": "Bob"},  # Wrong name
            },
        )
        assert result.passed is False

    def test_key_not_found(self):
        """Missing key should fail."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice"},
            ]
        )
        result = SCD1Assertions.assert_latest_values(
            df,
            keys=["id"],
            order_by="name",
            expected_values={
                99: {"name": "NotFound"},
            },
        )
        assert result.passed is False
