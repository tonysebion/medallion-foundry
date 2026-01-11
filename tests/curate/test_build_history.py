"""Tests for build_history (SCD2) function with data validation.

build_history creates SCD Type 2 history with effective dates:
- effective_from: When this version became active
- effective_to: When this version was superseded (NULL if current)
- is_current: 1 if current version, 0 otherwise
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import build_history


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestBuildHistoryBasic:
    """Basic tests for build_history functionality."""

    def test_adds_scd2_columns(self, con):
        """Verify SCD2 columns are added."""
        df = pd.DataFrame(
            [
                {"id": 1, "status": "active", "ts": datetime(2025, 1, 10)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # SCD2 columns present
        assert "effective_from" in result_df.columns
        assert "effective_to" in result_df.columns
        assert "is_current" in result_df.columns

    def test_single_version_is_current(self, con):
        """Single version per entity should be marked as current."""
        df = pd.DataFrame(
            [
                {"id": 1, "status": "active", "ts": datetime(2025, 1, 10)},
                {"id": 2, "status": "pending", "ts": datetime(2025, 1, 11)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # All should be current
        assert all(result_df["is_current"] == 1)

        # effective_to should be NULL for current records
        assert all(result_df["effective_to"].isna())

    def test_effective_dates_correct_sequence(self, con):
        """Verify effective_from/to form correct time ranges."""
        df = pd.DataFrame(
            [
                {"id": 1, "status": "pending", "ts": datetime(2025, 1, 10, 10, 0, 0)},
                {"id": 1, "status": "approved", "ts": datetime(2025, 1, 15, 10, 0, 0)},
                {"id": 1, "status": "completed", "ts": datetime(2025, 1, 20, 10, 0, 0)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # Version 1 (pending)
        v1 = result_df.iloc[0]
        assert v1["status"] == "pending"
        assert v1["effective_from"] == datetime(2025, 1, 10, 10, 0, 0)
        assert v1["effective_to"] == datetime(
            2025, 1, 15, 10, 0, 0
        )  # Next version's ts
        assert v1["is_current"] == 0

        # Version 2 (approved)
        v2 = result_df.iloc[1]
        assert v2["status"] == "approved"
        assert v2["effective_from"] == datetime(2025, 1, 15, 10, 0, 0)
        assert v2["effective_to"] == datetime(2025, 1, 20, 10, 0, 0)
        assert v2["is_current"] == 0

        # Version 3 (completed) - current
        v3 = result_df.iloc[2]
        assert v3["status"] == "completed"
        assert v3["effective_from"] == datetime(2025, 1, 20, 10, 0, 0)
        assert pd.isna(v3["effective_to"])  # NULL for current
        assert v3["is_current"] == 1

    def test_effective_from_equals_ts_col(self, con):
        """effective_from should always equal the timestamp column."""
        df = pd.DataFrame(
            [
                {"id": 1, "val": "A", "change_ts": datetime(2025, 1, 10)},
                {"id": 1, "val": "B", "change_ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="change_ts")
        result_df = result.execute()

        # effective_from should equal change_ts for all rows
        pd.testing.assert_series_equal(
            result_df["effective_from"], result_df["change_ts"], check_names=False
        )


class TestBuildHistoryMultipleEntities:
    """Test build_history with multiple independent entities."""

    def test_independent_entity_histories(self, con):
        """Each entity's history is built independently."""
        df = pd.DataFrame(
            [
                {"id": 1, "val": "1A", "ts": datetime(2025, 1, 10)},
                {"id": 1, "val": "1B", "ts": datetime(2025, 1, 15)},
                {"id": 2, "val": "2A", "ts": datetime(2025, 1, 12)},
                {"id": 2, "val": "2B", "ts": datetime(2025, 1, 18)},
                {"id": 2, "val": "2C", "ts": datetime(2025, 1, 20)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # ID 1 should have 2 versions
        id1_history = result_df[result_df["id"] == 1].sort_values("ts")
        assert len(id1_history) == 2
        assert id1_history["is_current"].sum() == 1

        # ID 1 effective_to chain
        assert (
            id1_history.iloc[0]["effective_to"] == id1_history.iloc[1]["effective_from"]
        )
        assert pd.isna(id1_history.iloc[1]["effective_to"])

        # ID 2 should have 3 versions
        id2_history = result_df[result_df["id"] == 2].sort_values("ts")
        assert len(id2_history) == 3
        assert id2_history["is_current"].sum() == 1

        # Only latest is current
        assert id2_history.iloc[2]["is_current"] == 1

    def test_current_view_unique_per_entity(self, con):
        """Current view (is_current=1) should have exactly one row per entity."""
        df = pd.DataFrame(
            [
                {
                    "id": i,
                    "version": v,
                    "ts": datetime(2025, 1, 1) + pd.Timedelta(days=v),
                }
                for i in range(1, 11)
                for v in range(1, 6)
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # Total rows unchanged
        assert len(result_df) == 50

        # Current view
        current = result_df[result_df["is_current"] == 1]
        assert len(current) == 10  # One per entity
        assert current["id"].is_unique

        # All current rows should have version 5 (latest)
        assert all(current["version"] == 5)


class TestBuildHistoryCompositeKeys:
    """Test build_history with composite natural keys."""

    def test_composite_key_history(self, con):
        """Build history with composite key."""
        df = pd.DataFrame(
            [
                {
                    "region": "US",
                    "product": "A",
                    "price": 100,
                    "ts": datetime(2025, 1, 10),
                },
                {
                    "region": "US",
                    "product": "A",
                    "price": 110,
                    "ts": datetime(2025, 1, 15),
                },
                {
                    "region": "EU",
                    "product": "A",
                    "price": 90,
                    "ts": datetime(2025, 1, 12),
                },
                {
                    "region": "EU",
                    "product": "A",
                    "price": 95,
                    "ts": datetime(2025, 1, 20),
                },
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["region", "product"], ts_col="ts")
        result_df = result.execute()

        # US+A history
        us_a = result_df[
            (result_df["region"] == "US") & (result_df["product"] == "A")
        ].sort_values("ts")
        assert len(us_a) == 2
        assert us_a.iloc[0]["is_current"] == 0
        assert us_a.iloc[1]["is_current"] == 1
        assert us_a.iloc[0]["effective_to"] == us_a.iloc[1]["effective_from"]

        # EU+A history
        eu_a = result_df[
            (result_df["region"] == "EU") & (result_df["product"] == "A")
        ].sort_values("ts")
        assert len(eu_a) == 2
        assert eu_a.iloc[1]["is_current"] == 1


class TestBuildHistoryCustomColumnNames:
    """Test build_history with custom SCD2 column names."""

    def test_custom_column_names(self, con):
        """Use custom names for SCD2 columns."""
        df = pd.DataFrame(
            [
                {"id": 1, "val": "A", "ts": datetime(2025, 1, 10)},
                {"id": 1, "val": "B", "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = build_history(
            t,
            keys=["id"],
            ts_col="ts",
            effective_from_name="valid_from",
            effective_to_name="valid_to",
            is_current_name="current_flag",
        )
        result_df = result.execute()

        # Custom column names used
        assert "valid_from" in result_df.columns
        assert "valid_to" in result_df.columns
        assert "current_flag" in result_df.columns

        # Default names not present
        assert "effective_from" not in result_df.columns
        assert "effective_to" not in result_df.columns
        assert "is_current" not in result_df.columns


class TestBuildHistoryEdgeCases:
    """Edge case tests for build_history."""

    def test_empty_table(self, con):
        """Empty table returns empty result with SCD2 columns."""
        df = pd.DataFrame(
            {
                "id": pd.array([], dtype="int64"),
                "val": pd.array([], dtype="string"),
                "ts": pd.array([], dtype="datetime64[ns]"),
            }
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        assert len(result_df) == 0
        assert "effective_from" in result_df.columns
        assert "effective_to" in result_df.columns
        assert "is_current" in result_df.columns

    def test_preserves_original_columns(self, con):
        """Original columns preserved alongside SCD2 columns."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "age": 30,
                    "city": "NYC",
                    "ts": datetime(2025, 1, 10),
                },
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # All original columns present
        for col in ["id", "name", "age", "city", "ts"]:
            assert col in result_df.columns

        # Values preserved
        assert result_df.iloc[0]["name"] == "Alice"
        assert result_df.iloc[0]["age"] == 30
        assert result_df.iloc[0]["city"] == "NYC"

    def test_handles_same_timestamp(self, con):
        """Records with same timestamp handled gracefully."""
        df = pd.DataFrame(
            [
                {"id": 1, "val": "A", "ts": datetime(2025, 1, 15)},
                {"id": 1, "val": "B", "ts": datetime(2025, 1, 15)},  # Same timestamp
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute()

        # Both rows preserved
        assert len(result_df) == 2

        # One should be current
        assert result_df["is_current"].sum() == 1


class TestBuildHistoryNoGaps:
    """Verify effective date ranges have no gaps."""

    def test_effective_dates_contiguous(self, con):
        """effective_to of one version equals effective_from of next."""
        # Use Timedelta to avoid invalid day values
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "version": i,
                    "ts": datetime(2025, 1, 1) + pd.Timedelta(days=i * 3),
                }
                for i in range(1, 10)
            ]
        )

        t = ibis.memtable(df)
        result = build_history(t, keys=["id"], ts_col="ts")
        result_df = result.execute().sort_values("ts")

        # Check contiguity for all but last version
        for i in range(len(result_df) - 1):
            current = result_df.iloc[i]
            next_ver = result_df.iloc[i + 1]

            # effective_to equals next effective_from (no gap)
            assert current["effective_to"] == next_ver["effective_from"], (
                f"Gap between version {i + 1} and {i + 2}"
            )

        # Last version has null effective_to
        assert pd.isna(result_df.iloc[-1]["effective_to"])
