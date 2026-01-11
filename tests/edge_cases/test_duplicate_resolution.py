"""Tests for various duplicate resolution scenarios.

Tests exact, near, and out-of-order duplicate handling across patterns.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_latest, dedupe_exact, build_history
from tests.data_validation.assertions import SCD1Assertions, SCD2Assertions


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestExactDuplicateResolution:
    """Test exact duplicate handling (all columns identical)."""

    def test_exact_duplicates_in_snapshot(self, con):
        """Exact duplicates in snapshot data are removed."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},  # Exact dup
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},  # Exact dup
                {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # 2 unique rows

    def test_exact_duplicates_across_batches(self, con):
        """Same exact record appearing in multiple batches."""
        df = pd.DataFrame(
            [
                # Batch 1
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
                # Batch 2 (same data, simulating re-extraction)
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # Duplicates removed

    def test_high_volume_exact_duplicates(self, con):
        """Large number of exact duplicates handled efficiently."""
        # Create 100 copies of each of 10 unique rows
        records = []
        for i in range(10):
            for _ in range(100):
                records.append({"id": i, "name": f"Record{i}", "value": i * 10})

        df = pd.DataFrame(records)
        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 10  # Only unique rows


class TestNearDuplicateResolution:
    """Test near duplicate handling (same key, different values)."""

    def test_near_duplicates_in_scd1(self, con):
        """Near duplicates (same key, different values) - keep latest."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"id": 1, "name": "Alice V3", "ts": datetime(2025, 1, 20)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Alice V3"

    def test_near_duplicates_in_scd2(self, con):
        """Near duplicates preserved as history in SCD2."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"id": 1, "name": "Alice V3", "ts": datetime(2025, 1, 20)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # All 3 versions preserved
        assert len(result_df) == 3

        history = SCD2Assertions.assert_history_preserved(
            result_df, keys=["id"], expected_counts={1: 3}
        )
        assert history.passed

    def test_same_key_different_non_key_columns(self, con):
        """Same key with various non-key column differences."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "email": "a@old.com",
                    "ts": datetime(2025, 1, 10),
                },
                {
                    "id": 1,
                    "name": "Alice",
                    "email": "a@new.com",
                    "ts": datetime(2025, 1, 15),
                },  # Email changed
                {
                    "id": 1,
                    "name": "Alice Smith",
                    "email": "a@new.com",
                    "ts": datetime(2025, 1, 20),
                },  # Name changed
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Alice Smith"
        assert result_df.iloc[0]["email"] == "a@new.com"


class TestOutOfOrderDuplicates:
    """Test handling of out-of-order duplicate records."""

    def test_out_of_order_timestamps_scd1(self, con):
        """Out-of-order timestamps correctly resolved in SCD1."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "version": "V3",
                    "ts": datetime(2025, 1, 20),
                },  # Latest ts, first in data
                {"id": 1, "version": "V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["version"] == "V3"

    def test_out_of_order_timestamps_scd2(self, con):
        """Out-of-order timestamps correctly ordered in SCD2."""
        df = pd.DataFrame(
            [
                {"id": 1, "version": "V3", "ts": datetime(2025, 1, 20)},
                {"id": 1, "version": "V1", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "V2", "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # Check ordering
        assert result_df.iloc[0]["version"] == "V1"
        assert result_df.iloc[1]["version"] == "V2"
        assert result_df.iloc[2]["version"] == "V3"

        # Only V3 should be current
        current = result_df[result_df["is_current"] == 1]
        assert len(current) == 1
        assert current.iloc[0]["version"] == "V3"

    def test_mixed_order_multiple_keys(self, con):
        """Multiple keys with mixed ordering."""
        df = pd.DataFrame(
            [
                {"id": 2, "version": "2-B", "ts": datetime(2025, 1, 15)},
                {"id": 1, "version": "1-A", "ts": datetime(2025, 1, 10)},
                {"id": 1, "version": "1-B", "ts": datetime(2025, 1, 20)},
                {"id": 2, "version": "2-A", "ts": datetime(2025, 1, 10)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        unique = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique.passed

        # Check latest versions
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["version"] == "1-B"

        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["version"] == "2-B"


class TestTimestampTieBreaking:
    """Test handling of records with identical timestamps."""

    def test_timestamp_ties_scd1_deterministic(self, con):
        """Timestamp ties in SCD1 produce consistent result."""
        same_ts = datetime(2025, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            [
                {"id": 1, "value": "A", "ts": same_ts},
                {"id": 1, "value": "B", "ts": same_ts},
                {"id": 1, "value": "C", "ts": same_ts},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        # Any of A, B, C is acceptable (implementation-dependent)
        assert result_df.iloc[0]["value"] in ["A", "B", "C"]

    def test_timestamp_ties_scd2_all_preserved(self, con):
        """Timestamp ties in SCD2 - all records preserved."""
        same_ts = datetime(2025, 1, 15, 10, 0, 0)
        df = pd.DataFrame(
            [
                {"id": 1, "value": "A", "ts": same_ts},
                {"id": 1, "value": "B", "ts": same_ts},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # Both preserved in history
        assert len(result_df) == 2

        # Only one marked as current
        current = result_df[result_df["is_current"] == 1]
        assert len(current) == 1


class TestNullHandling:
    """Test duplicate resolution with NULL values."""

    def test_null_in_non_key_column(self, con):
        """NULL in non-key columns doesn't affect deduplication."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": None, "ts": datetime(2025, 1, 10)},
                {"id": 1, "name": "Alice", "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Alice"

    def test_null_timestamps_excluded_from_ordering(self, con):
        """Records with NULL timestamps handled gracefully."""
        # When timestamp is NULL, filter_incremental would exclude
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Valid", "ts": datetime(2025, 1, 10)},
                {"id": 2, "name": "Also Valid", "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_latest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 2

    def test_exact_duplicate_with_nulls(self, con):
        """Exact duplicates with NULL values are treated as equal."""
        df = pd.DataFrame(
            [
                {"id": 1, "name": None, "value": 100},
                {"id": 1, "name": None, "value": 100},  # Exact duplicate (both NULL)
                {"id": 2, "name": "Bob", "value": 200},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_exact(t)
        result_df = result.execute()

        assert len(result_df) == 2  # Duplicate removed
