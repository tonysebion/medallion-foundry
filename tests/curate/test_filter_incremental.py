"""Tests for filter_incremental function with data validation.

filter_incremental filters to records after a watermark value,
used for incremental loading patterns.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import filter_incremental


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestFilterIncrementalBasic:
    """Basic tests for filter_incremental functionality."""

    def test_filters_after_watermark(self, con):
        """Records after watermark are kept."""
        df = pd.DataFrame(
            [
                {"id": 1, "ts": datetime(2025, 1, 10)},  # Before watermark
                {"id": 2, "ts": datetime(2025, 1, 15)},  # Equal to watermark (excluded)
                {"id": 3, "ts": datetime(2025, 1, 16)},  # After watermark
                {"id": 4, "ts": datetime(2025, 1, 20)},  # After watermark
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-15")
        result_df = result.execute()

        assert len(result_df) == 2
        assert set(result_df["id"]) == {3, 4}

    def test_excludes_equal_to_watermark(self, con):
        """Records equal to watermark are excluded (strict greater than)."""
        df = pd.DataFrame(
            [
                {"id": 1, "ts": datetime(2025, 1, 15, 10, 0, 0)},
                {"id": 2, "ts": datetime(2025, 1, 15, 10, 0, 0)},  # Exact match
            ]
        )

        t = ibis.memtable(df)
        watermark = datetime(2025, 1, 15, 10, 0, 0).isoformat()
        result = filter_incremental(t, "ts", watermark)
        result_df = result.execute()

        assert len(result_df) == 0  # None after watermark

    def test_all_after_watermark(self, con):
        """All records after watermark returned."""
        df = pd.DataFrame(
            [
                {"id": 1, "ts": datetime(2025, 1, 20)},
                {"id": 2, "ts": datetime(2025, 1, 21)},
                {"id": 3, "ts": datetime(2025, 1, 22)},
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-15")
        result_df = result.execute()

        assert len(result_df) == 3


class TestFilterIncrementalDataTypes:
    """Test filter_incremental with various data types."""

    def test_datetime_watermark(self, con):
        """Filter with datetime watermark column."""
        df = pd.DataFrame(
            [
                {"id": 1, "updated_at": datetime(2025, 1, 10, 12, 0, 0)},
                {"id": 2, "updated_at": datetime(2025, 1, 15, 12, 0, 0)},
                {"id": 3, "updated_at": datetime(2025, 1, 20, 12, 0, 0)},
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "updated_at", "2025-01-15T00:00:00")
        result_df = result.execute()

        assert len(result_df) == 2
        assert set(result_df["id"]) == {2, 3}

    def test_integer_watermark(self, con):
        """Filter with integer watermark column (version number)."""
        df = pd.DataFrame(
            [
                {"id": 1, "version": 1},
                {"id": 2, "version": 5},
                {"id": 3, "version": 10},
                {"id": 4, "version": 15},
            ]
        )

        t = ibis.memtable(df)
        # Use integer comparison
        result = filter_incremental(t, "version", 5)
        result_df = result.execute()

        assert len(result_df) == 2
        assert set(result_df["id"]) == {3, 4}

    def test_string_watermark(self, con):
        """Filter with string watermark column (lexicographic comparison)."""
        df = pd.DataFrame(
            [
                {"id": 1, "batch_id": "batch_001"},
                {"id": 2, "batch_id": "batch_005"},
                {"id": 3, "batch_id": "batch_010"},
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "batch_id", "batch_005")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 3


class TestFilterIncrementalEdgeCases:
    """Edge case tests for filter_incremental."""

    def test_empty_result(self, con):
        """All records before watermark returns empty."""
        df = pd.DataFrame(
            [
                {"id": 1, "ts": datetime(2025, 1, 1)},
                {"id": 2, "ts": datetime(2025, 1, 5)},
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-10")
        result_df = result.execute()

        assert len(result_df) == 0

    def test_empty_input_table(self, con):
        """Empty input table returns empty result."""
        df = pd.DataFrame(
            {
                "id": pd.array([], dtype="int64"),
                "ts": pd.array([], dtype="datetime64[ns]"),
            }
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-15")
        result_df = result.execute()

        assert len(result_df) == 0

    def test_preserves_all_columns(self, con):
        """All columns preserved in filtered output."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "col_a": "A",
                    "col_b": 100,
                    "col_c": 1.5,
                    "ts": datetime(2025, 1, 20),
                },
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-15")
        result_df = result.execute()

        expected_cols = {"id", "col_a", "col_b", "col_c", "ts"}
        assert set(result_df.columns) == expected_cols


class TestFilterIncrementalWithNulls:
    """Test filter_incremental behavior with null values."""

    def test_nulls_excluded(self, con):
        """Null watermark values are excluded (not greater than anything)."""
        df = pd.DataFrame(
            [
                {"id": 1, "ts": datetime(2025, 1, 20)},
                {"id": 2, "ts": None},  # NULL timestamp
                {"id": 3, "ts": datetime(2025, 1, 25)},
            ]
        )

        t = ibis.memtable(df)
        result = filter_incremental(t, "ts", "2025-01-15")
        result_df = result.execute()

        # NULL is not > watermark, so excluded
        assert len(result_df) == 2
        assert set(result_df["id"]) == {1, 3}


class TestFilterIncrementalLargeScale:
    """Test filter_incremental with larger datasets."""

    def test_large_dataset_filter(self, con):
        """Filter large dataset efficiently."""
        # 1000 records, 500 before watermark, 500 after
        df = pd.DataFrame(
            [
                {"id": i, "ts": datetime(2025, 1, 1) + pd.Timedelta(hours=i)}
                for i in range(1000)
            ]
        )

        t = ibis.memtable(df)
        # Watermark at hour 500
        watermark = (datetime(2025, 1, 1) + pd.Timedelta(hours=499)).isoformat()
        result = filter_incremental(t, "ts", watermark)
        result_df = result.execute()

        assert len(result_df) == 500  # Hours 500-999
