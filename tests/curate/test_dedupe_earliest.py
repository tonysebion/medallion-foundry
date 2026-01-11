"""Tests for dedupe_earliest function with data validation.

dedupe_earliest keeps only the earliest (first) record per natural key,
useful for tracking first occurrence of events.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import dedupe_earliest


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestDedupeEarliestBasic:
    """Basic tests for dedupe_earliest functionality."""

    def test_keeps_earliest_by_timestamp(self, con):
        """Verify dedupe_earliest keeps the row with min timestamp."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "event": "first",
                    "ts": datetime(2025, 1, 10, 10, 0, 0),
                },  # Earliest
                {"id": 1, "event": "second", "ts": datetime(2025, 1, 15, 10, 0, 0)},
                {"id": 1, "event": "third", "ts": datetime(2025, 1, 20, 10, 0, 0)},
                {"id": 2, "event": "only", "ts": datetime(2025, 1, 12, 10, 0, 0)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        # Should have 2 unique ids
        assert len(result_df) == 2

        # ID 1 should keep first (earliest)
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["event"] == "first"

        # ID 2 should keep only version
        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["event"] == "only"

    def test_natural_key_uniqueness(self, con):
        """After dedupe_earliest, each natural key appears exactly once."""
        df = pd.DataFrame(
            [
                {
                    "user_id": 1,
                    "action": "signup",
                    "ts": datetime(2025, 1, 1),
                },  # First for user 1
                {"user_id": 1, "action": "login", "ts": datetime(2025, 1, 5)},
                {"user_id": 1, "action": "purchase", "ts": datetime(2025, 1, 10)},
                {
                    "user_id": 2,
                    "action": "signup",
                    "ts": datetime(2025, 1, 3),
                },  # First for user 2
                {"user_id": 2, "action": "login", "ts": datetime(2025, 1, 7)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["user_id"], order_by="ts")
        result_df = result.execute()

        assert result_df["user_id"].is_unique
        assert len(result_df) == 2

        # Both users should have their signup (first action)
        for _, row in result_df.iterrows():
            assert row["action"] == "signup"


class TestDedupeEarliestCompositeKeys:
    """Tests for dedupe_earliest with composite keys."""

    def test_composite_key_first_occurrence(self, con):
        """Test finding first occurrence per composite key."""
        df = pd.DataFrame(
            [
                {
                    "customer": "C1",
                    "product": "P1",
                    "event": "view",
                    "ts": datetime(2025, 1, 10),
                },
                {
                    "customer": "C1",
                    "product": "P1",
                    "event": "add_cart",
                    "ts": datetime(2025, 1, 11),
                },
                {
                    "customer": "C1",
                    "product": "P1",
                    "event": "purchase",
                    "ts": datetime(2025, 1, 12),
                },
                {
                    "customer": "C1",
                    "product": "P2",
                    "event": "view",
                    "ts": datetime(2025, 1, 9),
                },  # First for C1+P2
                {
                    "customer": "C2",
                    "product": "P1",
                    "event": "view",
                    "ts": datetime(2025, 1, 8),
                },  # First for C2+P1
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["customer", "product"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 3  # 3 unique customer+product pairs

        # All first interactions should be "view"
        for _, row in result_df.iterrows():
            assert row["event"] == "view"


class TestDedupeEarliestEdgeCases:
    """Edge case tests for dedupe_earliest."""

    def test_handles_timestamp_tie(self, con):
        """When timestamps are equal, any tied record is acceptable."""
        df = pd.DataFrame(
            [
                {"id": 1, "value": "A", "ts": datetime(2025, 1, 1, 0, 0, 0)},
                {
                    "id": 1,
                    "value": "B",
                    "ts": datetime(2025, 1, 1, 0, 0, 0),
                },  # Same timestamp
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] in ["A", "B"]

    def test_empty_table(self, con):
        """Empty table returns empty result."""
        df = pd.DataFrame(
            {
                "id": pd.array([], dtype="int64"),
                "value": pd.array([], dtype="string"),
                "ts": pd.array([], dtype="datetime64[ns]"),
            }
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 0

    def test_preserves_all_columns(self, con):
        """All columns from input preserved in output."""
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "col_a": "A",
                    "col_b": 100,
                    "ts": datetime(2025, 1, 10),
                },  # Earliest
                {"id": 1, "col_a": "B", "col_b": 200, "ts": datetime(2025, 1, 15)},
            ]
        )

        t = ibis.memtable(df)
        result = dedupe_earliest(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        expected_cols = {"id", "col_a", "col_b", "ts"}
        assert set(result_df.columns) == expected_cols

        # Earliest values kept
        assert result_df.iloc[0]["col_a"] == "A"
        assert result_df.iloc[0]["col_b"] == 100


class TestDedupeEarliestVsLatest:
    """Compare dedupe_earliest vs dedupe_latest behavior."""

    def test_earliest_vs_latest_opposite_results(self, con):
        """Verify earliest and latest return opposite records."""
        from pipelines.lib.curate import dedupe_latest

        df = pd.DataFrame(
            [
                {"id": 1, "version": "oldest", "ts": datetime(2025, 1, 1)},
                {"id": 1, "version": "middle", "ts": datetime(2025, 1, 15)},
                {"id": 1, "version": "newest", "ts": datetime(2025, 1, 30)},
            ]
        )

        t = ibis.memtable(df)

        earliest_df = dedupe_earliest(t, keys=["id"], order_by="ts").execute()
        latest_df = dedupe_latest(t, keys=["id"], order_by="ts").execute()

        assert earliest_df.iloc[0]["version"] == "oldest"
        assert latest_df.iloc[0]["version"] == "newest"
