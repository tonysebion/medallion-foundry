"""Tests for dedupe_latest function with data validation.

dedupe_latest implements SCD Type 1 behavior - keeping only the latest
record per natural key combination.
"""

from __future__ import annotations

from datetime import datetime
import uuid

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


def unique_name(prefix: str = "t") -> str:
    """Generate unique table name to avoid conflicts."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


class TestDedupeLatestBasic:
    """Basic tests for dedupe_latest functionality."""

    def test_keeps_latest_by_timestamp(self, con):
        """Verify dedupe_latest keeps the row with max timestamp."""
        df = pd.DataFrame([
            {"id": 1, "version": "v1", "amount": 100, "ts": datetime(2025, 1, 10, 10, 0, 0)},
            {"id": 1, "version": "v2", "amount": 110, "ts": datetime(2025, 1, 15, 10, 0, 0)},
            {"id": 1, "version": "v3", "amount": 120, "ts": datetime(2025, 1, 20, 10, 0, 0)},  # Latest
            {"id": 2, "version": "v1", "amount": 200, "ts": datetime(2025, 1, 12, 10, 0, 0)},  # Only version
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Should have 2 unique ids
        assert len(result_df) == 2

        # ID 1 should keep v3 (latest)
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["version"] == "v3"
        assert id1["amount"] == 120

        # ID 2 should keep v1 (only version)
        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["version"] == "v1"
        assert id2["amount"] == 200

    def test_single_record_per_key_unchanged(self, con):
        """Records without duplicates pass through unchanged."""
        df = pd.DataFrame([
            {"id": 1, "value": "A", "ts": datetime(2025, 1, 10)},
            {"id": 2, "value": "B", "ts": datetime(2025, 1, 11)},
            {"id": 3, "value": "C", "ts": datetime(2025, 1, 12)},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 3
        assert set(result_df["id"]) == {1, 2, 3}

    def test_natural_key_uniqueness_after_dedupe(self, con):
        """After dedupe, each natural key should appear exactly once."""
        df = pd.DataFrame([
            {"id": 1, "value": "old", "ts": datetime(2025, 1, 1)},
            {"id": 1, "value": "new", "ts": datetime(2025, 1, 2)},
            {"id": 2, "value": "old", "ts": datetime(2025, 1, 1)},
            {"id": 2, "value": "mid", "ts": datetime(2025, 1, 2)},
            {"id": 2, "value": "new", "ts": datetime(2025, 1, 3)},
            {"id": 3, "value": "only", "ts": datetime(2025, 1, 1)},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Verify uniqueness
        assert result_df["id"].is_unique
        assert len(result_df) == 3


class TestDedupeLatestCompositeKeys:
    """Tests for dedupe_latest with composite natural keys."""

    def test_composite_two_column_key(self, con):
        """Test dedupe with two-column composite key."""
        df = pd.DataFrame([
            {"region": "US", "product": "A", "price": 100, "ts": datetime(2025, 1, 10)},
            {"region": "US", "product": "A", "price": 110, "ts": datetime(2025, 1, 15)},  # Latest US+A
            {"region": "EU", "product": "A", "price": 90, "ts": datetime(2025, 1, 12)},   # Only EU+A
            {"region": "US", "product": "B", "price": 200, "ts": datetime(2025, 1, 14)},  # Only US+B
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["region", "product"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 3  # 3 unique region+product combinations

        # Verify US+A kept latest
        us_a = result_df[(result_df["region"] == "US") & (result_df["product"] == "A")].iloc[0]
        assert us_a["price"] == 110

        # Verify EU+A kept only version
        eu_a = result_df[(result_df["region"] == "EU") & (result_df["product"] == "A")].iloc[0]
        assert eu_a["price"] == 90

    def test_composite_three_column_key(self, con):
        """Test dedupe with three-column composite key."""
        df = pd.DataFrame([
            {"year": 2024, "region": "US", "product": "A", "sales": 100, "ts": datetime(2025, 1, 10)},
            {"year": 2024, "region": "US", "product": "A", "sales": 150, "ts": datetime(2025, 1, 15)},  # Latest
            {"year": 2025, "region": "US", "product": "A", "sales": 200, "ts": datetime(2025, 1, 12)},  # Different year
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["year", "region", "product"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 2  # 2024+US+A and 2025+US+A

        # Verify 2024 kept latest
        y2024 = result_df[result_df["year"] == 2024].iloc[0]
        assert y2024["sales"] == 150

        # Verify 2025 kept only version
        y2025 = result_df[result_df["year"] == 2025].iloc[0]
        assert y2025["sales"] == 200


class TestDedupeLatestEdgeCases:
    """Edge case tests for dedupe_latest."""

    def test_handles_same_timestamp_tie(self, con):
        """When timestamps are equal, any tied record is acceptable."""
        df = pd.DataFrame([
            {"id": 1, "value": 100, "ts": datetime(2025, 1, 15, 10, 0, 0)},
            {"id": 1, "value": 200, "ts": datetime(2025, 1, 15, 10, 0, 0)},  # Same timestamp
            {"id": 1, "value": 300, "ts": datetime(2025, 1, 15, 10, 0, 0)},  # Same timestamp
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Should have exactly 1 row
        assert len(result_df) == 1

        # Value should be one of the tied values (any is acceptable)
        assert result_df.iloc[0]["value"] in [100, 200, 300]

    def test_handles_null_in_non_key_columns(self, con):
        """Null values in non-key columns should not affect deduplication."""
        df = pd.DataFrame([
            {"id": 1, "value": None, "ts": datetime(2025, 1, 10)},
            {"id": 1, "value": "filled", "ts": datetime(2025, 1, 15)},  # Latest
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "filled"

    def test_empty_table(self, con):
        """Empty table should return empty result."""
        # Use memtable with explicit schema for empty tables
        df = pd.DataFrame({
            "id": pd.array([], dtype="int64"),
            "value": pd.array([], dtype="string"),
            "ts": pd.array([], dtype="datetime64[ns]")
        })

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 0

    def test_preserves_all_columns(self, con):
        """All columns from input should be preserved in output."""
        df = pd.DataFrame([
            {"id": 1, "col_a": "A", "col_b": 100, "col_c": 1.5, "ts": datetime(2025, 1, 10)},
            {"id": 1, "col_a": "B", "col_b": 200, "col_c": 2.5, "ts": datetime(2025, 1, 15)},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # All original columns preserved
        expected_cols = {"id", "col_a", "col_b", "col_c", "ts"}
        assert set(result_df.columns) == expected_cols

        # Latest values kept
        assert result_df.iloc[0]["col_a"] == "B"
        assert result_df.iloc[0]["col_b"] == 200
        assert result_df.iloc[0]["col_c"] == 2.5


class TestDedupeLatestDataTypes:
    """Test dedupe_latest with various data types."""

    def test_string_timestamp_ordering(self, con):
        """Test with ISO string timestamps."""
        df = pd.DataFrame([
            {"id": 1, "value": "old", "ts": "2025-01-10T10:00:00"},
            {"id": 1, "value": "new", "ts": "2025-01-15T10:00:00"},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "new"

    def test_integer_ordering_column(self, con):
        """Test with integer ordering column (version number)."""
        df = pd.DataFrame([
            {"id": 1, "value": "v1", "version": 1},
            {"id": 1, "value": "v3", "version": 3},
            {"id": 1, "value": "v2", "version": 2},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="version")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "v3"
        assert result_df.iloc[0]["version"] == 3

    def test_float_ordering_column(self, con):
        """Test with float ordering column."""
        df = pd.DataFrame([
            {"id": 1, "value": "low", "score": 1.5},
            {"id": 1, "value": "high", "score": 9.9},
            {"id": 1, "value": "mid", "score": 5.0},
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="score")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "high"


class TestDedupeLatestLargeScale:
    """Test dedupe_latest with larger datasets."""

    def test_many_records_per_key(self, con):
        """Test with many versions per key."""
        # Create 100 versions for 10 keys
        records = []
        for key in range(1, 11):
            for version in range(1, 101):
                records.append({
                    "id": key,
                    "value": f"v{version}",
                    "ts": datetime(2025, 1, 1) + pd.Timedelta(hours=version),
                })

        df = pd.DataFrame(records)
        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 10  # 10 unique keys

        # Each key should have its latest version (v100)
        for _, row in result_df.iterrows():
            assert row["value"] == "v100"

    def test_many_unique_keys(self, con):
        """Test with many unique keys (no duplicates)."""
        df = pd.DataFrame([
            {"id": i, "value": f"val_{i}", "ts": datetime(2025, 1, 1) + pd.Timedelta(minutes=i)}
            for i in range(1, 1001)
        ])

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1000  # All unique, so all preserved
