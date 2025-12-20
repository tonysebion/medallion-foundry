"""Tests for dedupe_exact function with data validation.

dedupe_exact removes exact duplicate rows (all columns identical).
Used for event logs where we only eliminate truly identical records.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_exact


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestDedupeExactBasic:
    """Basic tests for dedupe_exact functionality."""

    def test_removes_exact_duplicates(self, con):
        """Exact duplicate rows are removed."""
        df = pd.DataFrame([
            {"id": 1, "value": "A", "amount": 100},
            {"id": 1, "value": "A", "amount": 100},  # Exact duplicate
            {"id": 2, "value": "B", "amount": 200},
        ])

        t = con.create_table("test_exact", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # One duplicate removed

    def test_preserves_near_duplicates(self, con):
        """Near duplicates (same key, different values) are preserved."""
        df = pd.DataFrame([
            {"id": 1, "value": "A", "amount": 100},
            {"id": 1, "value": "A", "amount": 101},  # Different amount - preserved
            {"id": 1, "value": "B", "amount": 100},  # Different value - preserved
        ])

        t = con.create_table("test_near", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 3  # All preserved (no exact duplicates)

    def test_no_duplicates_unchanged(self, con):
        """Table without duplicates passes through unchanged."""
        df = pd.DataFrame([
            {"id": 1, "value": "A"},
            {"id": 2, "value": "B"},
            {"id": 3, "value": "C"},
        ])

        t = con.create_table("test_no_dupes", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 3


class TestDedupeExactMultipleDuplicates:
    """Test dedupe_exact with multiple duplicates of same row."""

    def test_multiple_duplicates_reduced_to_one(self, con):
        """Multiple copies of same row reduced to one."""
        df = pd.DataFrame([
            {"id": 1, "value": "A"},
            {"id": 1, "value": "A"},  # Duplicate 1
            {"id": 1, "value": "A"},  # Duplicate 2
            {"id": 1, "value": "A"},  # Duplicate 3
        ])

        t = con.create_table("test_multi_dupe", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 1  # All duplicates removed

    def test_mixed_duplicates(self, con):
        """Mix of unique rows and various duplicate groups."""
        df = pd.DataFrame([
            {"id": 1, "value": "A"},  # Unique
            {"id": 2, "value": "B"},
            {"id": 2, "value": "B"},  # Duplicate of above
            {"id": 3, "value": "C"},
            {"id": 3, "value": "C"},
            {"id": 3, "value": "C"},  # Two duplicates
            {"id": 4, "value": "D"},  # Unique
        ])

        t = con.create_table("test_mixed", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 4  # 4 unique rows


class TestDedupeExactDataTypes:
    """Test dedupe_exact with various data types."""

    def test_with_timestamps(self, con):
        """Exact duplicates with timestamp columns."""
        df = pd.DataFrame([
            {"id": 1, "ts": datetime(2025, 1, 15, 10, 0, 0), "value": 100},
            {"id": 1, "ts": datetime(2025, 1, 15, 10, 0, 0), "value": 100},  # Exact
            {"id": 1, "ts": datetime(2025, 1, 15, 10, 0, 1), "value": 100},  # Different ts
        ])

        t = con.create_table("test_ts", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # One duplicate removed

    def test_with_nulls(self, con):
        """Nulls are considered equal for exact duplicate detection."""
        df = pd.DataFrame([
            {"id": 1, "value": None},
            {"id": 1, "value": None},  # Exact duplicate (both NULL)
            {"id": 2, "value": "A"},
        ])

        t = con.create_table("test_nulls", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # NULL duplicate removed

    def test_with_floats(self, con):
        """Exact duplicates with float columns."""
        df = pd.DataFrame([
            {"id": 1, "score": 1.5},
            {"id": 1, "score": 1.5},  # Exact
            {"id": 1, "score": 1.50001},  # Different
        ])

        t = con.create_table("test_floats", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # One exact duplicate removed


class TestDedupeExactEdgeCases:
    """Edge case tests for dedupe_exact."""

    def test_empty_table(self, con):
        """Empty table returns empty result."""
        df = pd.DataFrame({
            "id": pd.Series([], dtype=int),
            "value": pd.Series([], dtype=str)
        })

        t = con.create_table("test_empty", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 0

    def test_single_row(self, con):
        """Single row table unchanged."""
        df = pd.DataFrame([
            {"id": 1, "value": "A"}
        ])

        t = con.create_table("test_single", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 1

    def test_all_columns_must_match(self, con):
        """All columns must match for exact duplicate."""
        df = pd.DataFrame([
            {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
            {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},  # Exact duplicate
            {"a": 1, "b": 2, "c": 3, "d": 4, "e": 6},  # One column different
        ])

        t = con.create_table("test_all_cols", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2

    def test_preserves_column_order(self, con):
        """Column order in output matches input."""
        df = pd.DataFrame([
            {"z_col": 1, "a_col": 2, "m_col": 3},
            {"z_col": 1, "a_col": 2, "m_col": 3},  # Duplicate
        ])

        t = con.create_table("test_col_order", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert list(result_df.columns) == ["z_col", "a_col", "m_col"]


class TestDedupeExactLargeScale:
    """Test dedupe_exact with larger datasets."""

    def test_high_duplicate_rate(self, con):
        """Handle high duplicate rate efficiently."""
        # Create 100 records where each row appears 10 times
        records = []
        for i in range(10):
            for _ in range(10):
                records.append({"id": i, "value": f"val_{i}"})

        df = pd.DataFrame(records)
        t = con.create_table("test_high_dupe", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 10  # 10 unique rows

    def test_no_duplicates_large(self, con):
        """Large table with no duplicates."""
        df = pd.DataFrame([
            {"id": i, "value": f"val_{i}"}
            for i in range(1000)
        ])

        t = con.create_table("test_large_no_dupes", df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 1000  # All unique
