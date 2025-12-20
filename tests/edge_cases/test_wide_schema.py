"""Tests for wide schema handling (many columns).

Tests transformations with large numbers of columns.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, build_history, dedupe_exact


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


def create_wide_df(num_columns: int, num_rows: int = 5) -> pd.DataFrame:
    """Create a DataFrame with many columns."""
    data = {}

    # Key and timestamp columns
    data["id"] = list(range(1, num_rows + 1))
    # Use Timedelta to handle any number of rows
    data["ts"] = [datetime(2025, 1, 1) + pd.Timedelta(hours=i) for i in range(num_rows)]

    # Add many additional columns
    for i in range(num_columns - 2):
        col_name = f"col_{i:03d}"
        if i % 3 == 0:
            # String column
            data[col_name] = [f"val_{j}_{i}" for j in range(num_rows)]
        elif i % 3 == 1:
            # Integer column
            data[col_name] = [j * (i + 1) for j in range(num_rows)]
        else:
            # Float column
            data[col_name] = [float(j) / (i + 1) for j in range(num_rows)]

    return pd.DataFrame(data)


class TestWideSchemaBasic:
    """Test basic operations with wide schemas."""

    def test_30_columns_dedupe_latest(self, con):
        """Dedupe latest with 30 columns."""
        df = create_wide_df(30, num_rows=10)

        # Add duplicate key with different timestamp
        extra_row = df.iloc[0].copy()
        extra_row["ts"] = datetime(2025, 1, 15)
        extra_row["col_000"] = "updated"
        df = pd.concat([df, pd.DataFrame([extra_row])], ignore_index=True)

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Should have original 10 rows (one updated)
        assert len(result_df) == 10

        # All 30 columns preserved
        assert len(result_df.columns) == 30

    def test_60_columns_dedupe_latest(self, con):
        """Dedupe latest with 60 columns."""
        df = create_wide_df(60, num_rows=10)

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 10
        assert len(result_df.columns) == 60

    def test_wide_schema_scd2_history(self, con):
        """Build SCD2 history with wide schema."""
        df = create_wide_df(40, num_rows=3)

        # Create multiple versions for one key
        extra_rows = []
        for i in range(2):
            row = df.iloc[0].copy()
            row["ts"] = datetime(2025, 1, 10 + i)
            row["col_000"] = f"version_{i + 2}"
            extra_rows.append(row)
        df = pd.concat([df, pd.DataFrame(extra_rows)], ignore_index=True)

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # Original columns + 3 SCD2 columns
        assert len(result_df.columns) == 40 + 3

        # Check SCD2 columns present
        assert "effective_from" in result_df.columns
        assert "effective_to" in result_df.columns
        assert "is_current" in result_df.columns


class TestWideSchemaWithNulls:
    """Test wide schema handling with NULL values."""

    def test_sparse_wide_schema(self, con):
        """Wide schema with many NULL values."""
        data = {
            "id": [1, 2, 3],
            "ts": [datetime(2025, 1, i + 1) for i in range(3)],
        }

        # Add 50 columns, most with NULL values
        for i in range(50):
            col_name = f"col_{i:03d}"
            if i < 5:
                # First 5 columns have values
                data[col_name] = [f"val_{j}" for j in range(3)]
            else:
                # Rest are NULL
                data[col_name] = [None, None, None]

        df = pd.DataFrame(data)
        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 3
        assert len(result_df.columns) == 52

    def test_wide_schema_null_propagation(self, con):
        """NULLs propagate correctly in wide schema operations."""
        data = {
            "id": [1, 1],
            "ts": [datetime(2025, 1, 10), datetime(2025, 1, 15)],
            "name": ["Alice", "Alice Updated"],
            "nullable_col": [None, "now_has_value"],
        }

        # Add more columns
        for i in range(30):
            data[f"extra_{i}"] = [f"v{j}" for j in range(2)]

        df = pd.DataFrame(data)
        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["nullable_col"] == "now_has_value"


class TestWideSchemaPerformance:
    """Test performance with wide schemas."""

    def test_100_columns_dedupe(self, con):
        """Handle 100 columns efficiently."""
        df = create_wide_df(100, num_rows=100)

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 100
        assert len(result_df.columns) == 100

    def test_wide_schema_with_duplicates(self, con):
        """Wide schema with many duplicate keys."""
        base_df = create_wide_df(50, num_rows=10)

        # Create 5 versions for each key
        all_rows = []
        for version in range(5):
            version_df = base_df.copy()
            version_df["ts"] = [
                datetime(2025, 1, version * 5 + i + 1)
                for i in range(10)
            ]
            version_df["col_000"] = [f"v{version}_{i}" for i in range(10)]
            all_rows.append(version_df)

        combined_df = pd.concat(all_rows, ignore_index=True)

        t = ibis.memtable(combined_df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Should keep only latest version per key
        assert len(result_df) == 10

        # All columns preserved
        assert len(result_df.columns) == 50
