"""Tests for SCD2 assertion helpers."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from tests.data_validation.assertions import SCD2Assertions


class TestSCD2HasColumns:
    """Test assert_has_scd2_columns."""

    def test_all_columns_present(self):
        """DataFrame with all SCD2 columns should pass."""
        df = pd.DataFrame([
            {
                "id": 1,
                "name": "Alice",
                "effective_from": datetime(2025, 1, 1),
                "effective_to": None,
                "is_current": 1
            },
        ])
        result = SCD2Assertions.assert_has_scd2_columns(df)
        assert result.passed is True

    def test_missing_columns_fail(self):
        """Missing SCD2 columns should fail."""
        df = pd.DataFrame([
            {"id": 1, "name": "Alice"},
        ])
        result = SCD2Assertions.assert_has_scd2_columns(df)
        assert result.passed is False
        assert len(result.details["missing_columns"]) == 3

    def test_custom_column_names(self):
        """Custom SCD2 column names should be validated."""
        df = pd.DataFrame([
            {
                "id": 1,
                "valid_from": datetime(2025, 1, 1),
                "valid_to": None,
                "current_flag": 1
            },
        ])
        result = SCD2Assertions.assert_has_scd2_columns(
            df,
            effective_from="valid_from",
            effective_to="valid_to",
            is_current="current_flag"
        )
        assert result.passed is True


class TestSCD2EffectiveFromEqualsTs:
    """Test assert_effective_from_equals_ts."""

    def test_effective_from_matches_ts(self):
        """effective_from should equal timestamp column."""
        ts = datetime(2025, 1, 10)
        df = pd.DataFrame([
            {"id": 1, "updated_at": ts, "effective_from": ts},
        ])
        result = SCD2Assertions.assert_effective_from_equals_ts(df, "updated_at")
        assert result.passed is True

    def test_effective_from_mismatch(self):
        """Mismatched effective_from should fail."""
        df = pd.DataFrame([
            {
                "id": 1,
                "updated_at": datetime(2025, 1, 10),
                "effective_from": datetime(2025, 1, 15)  # Different!
            },
        ])
        result = SCD2Assertions.assert_effective_from_equals_ts(df, "updated_at")
        assert result.passed is False


class TestSCD2CurrentViewUnique:
    """Test assert_current_view_unique."""

    def test_current_unique(self):
        """Current records with unique keys should pass."""
        df = pd.DataFrame([
            {"id": 1, "version": "old", "is_current": 0},
            {"id": 1, "version": "new", "is_current": 1},
            {"id": 2, "version": "only", "is_current": 1},
        ])
        result = SCD2Assertions.assert_current_view_unique(df, ["id"])
        assert result.passed is True

    def test_current_duplicates_fail(self):
        """Multiple current records per key should fail."""
        df = pd.DataFrame([
            {"id": 1, "version": "v1", "is_current": 1},
            {"id": 1, "version": "v2", "is_current": 1},  # Both current!
        ])
        result = SCD2Assertions.assert_current_view_unique(df, ["id"])
        assert result.passed is False


class TestSCD2OneCurrentPerKey:
    """Test assert_one_current_per_key."""

    def test_exactly_one_current(self):
        """Exactly one current per key should pass."""
        df = pd.DataFrame([
            {"id": 1, "is_current": 0},
            {"id": 1, "is_current": 1},
            {"id": 2, "is_current": 1},
        ])
        result = SCD2Assertions.assert_one_current_per_key(df, ["id"])
        assert result.passed is True

    def test_zero_current_fail(self):
        """Zero current records for a key should fail."""
        df = pd.DataFrame([
            {"id": 1, "is_current": 0},
            {"id": 1, "is_current": 0},
        ])
        result = SCD2Assertions.assert_one_current_per_key(df, ["id"])
        assert result.passed is False
        assert result.details["zero_current"] > 0

    def test_multiple_current_fail(self):
        """Multiple current records for a key should fail."""
        df = pd.DataFrame([
            {"id": 1, "is_current": 1},
            {"id": 1, "is_current": 1},
        ])
        result = SCD2Assertions.assert_one_current_per_key(df, ["id"])
        assert result.passed is False
        assert result.details["multiple_current"] > 0


class TestSCD2EffectiveDatesContiguous:
    """Test assert_effective_dates_contiguous."""

    def test_contiguous_dates(self):
        """Contiguous effective dates should pass."""
        df = pd.DataFrame([
            {"id": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(2025, 1, 10)},
            {"id": 1, "effective_from": datetime(2025, 1, 10), "effective_to": datetime(2025, 1, 20)},
            {"id": 1, "effective_from": datetime(2025, 1, 20), "effective_to": None},
        ])
        result = SCD2Assertions.assert_effective_dates_contiguous(df, ["id"])
        assert result.passed is True

    def test_gap_in_dates_fail(self):
        """Gap in effective dates should fail."""
        df = pd.DataFrame([
            {"id": 1, "effective_from": datetime(2025, 1, 1), "effective_to": datetime(2025, 1, 10)},
            {"id": 1, "effective_from": datetime(2025, 1, 15), "effective_to": None},  # Gap!
        ])
        result = SCD2Assertions.assert_effective_dates_contiguous(df, ["id"])
        assert result.passed is False


class TestSCD2CurrentHasNullEffectiveTo:
    """Test assert_current_has_null_effective_to."""

    def test_current_null_effective_to(self):
        """Current records with NULL effective_to should pass."""
        df = pd.DataFrame([
            {"id": 1, "is_current": 0, "effective_to": datetime(2025, 1, 10)},
            {"id": 1, "is_current": 1, "effective_to": None},
        ])
        result = SCD2Assertions.assert_current_has_null_effective_to(df)
        assert result.passed is True

    def test_current_non_null_effective_to_fail(self):
        """Current records with non-NULL effective_to should fail."""
        df = pd.DataFrame([
            {"id": 1, "is_current": 1, "effective_to": datetime(2025, 1, 10)},
        ])
        result = SCD2Assertions.assert_current_has_null_effective_to(df)
        assert result.passed is False


class TestSCD2HistoryPreserved:
    """Test assert_history_preserved."""

    def test_expected_versions(self):
        """Expected version counts should pass."""
        df = pd.DataFrame([
            {"id": 1, "version": "v1"},
            {"id": 1, "version": "v2"},
            {"id": 1, "version": "v3"},
            {"id": 2, "version": "v1"},
            {"id": 2, "version": "v2"},
        ])
        result = SCD2Assertions.assert_history_preserved(
            df,
            keys=["id"],
            expected_counts={1: 3, 2: 2}
        )
        assert result.passed is True

    def test_wrong_version_count_fail(self):
        """Wrong version counts should fail."""
        df = pd.DataFrame([
            {"id": 1, "version": "v1"},
            {"id": 1, "version": "v2"},
        ])
        result = SCD2Assertions.assert_history_preserved(
            df,
            keys=["id"],
            expected_counts={1: 5}  # Expected 5, got 2
        )
        assert result.passed is False
