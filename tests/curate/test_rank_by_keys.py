"""Tests for rank_by_keys function with data validation.

rank_by_keys adds a rank column partitioned by keys,
useful for top-N per group operations.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import rank_by_keys


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestRankByKeysBasic:
    """Basic tests for rank_by_keys functionality."""

    def test_adds_rank_column(self, con):
        """Rank column is added to output."""
        df = pd.DataFrame([
            {"id": 1, "value": 100, "ts": datetime(2025, 1, 10)},
        ])

        t = con.create_table("test_add_rank", df)
        result = rank_by_keys(t, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert "_rank" in result_df.columns

    def test_rank_descending_by_default(self, con):
        """Rank 0 is highest value by default (descending)."""
        df = pd.DataFrame([
            {"id": 1, "value": "low", "score": 10},
            {"id": 1, "value": "high", "score": 100},
            {"id": 1, "value": "mid", "score": 50},
        ])

        t = con.create_table("test_desc", df)
        result = rank_by_keys(t, keys=["id"], order_by="score", descending=True)
        result_df = result.execute()

        # Rank 0 should be highest score (100)
        rank_0 = result_df[result_df["_rank"] == 0].iloc[0]
        assert rank_0["value"] == "high"
        assert rank_0["score"] == 100

    def test_rank_ascending(self, con):
        """Rank 0 is lowest value when ascending."""
        df = pd.DataFrame([
            {"id": 1, "value": "low", "score": 10},
            {"id": 1, "value": "high", "score": 100},
            {"id": 1, "value": "mid", "score": 50},
        ])

        t = con.create_table("test_asc", df)
        result = rank_by_keys(t, keys=["id"], order_by="score", descending=False)
        result_df = result.execute()

        # Rank 0 should be lowest score (10)
        rank_0 = result_df[result_df["_rank"] == 0].iloc[0]
        assert rank_0["value"] == "low"
        assert rank_0["score"] == 10

    def test_ranks_within_partition(self, con):
        """Ranks are calculated independently per key partition."""
        df = pd.DataFrame([
            {"category": "A", "item": "A1", "score": 100},
            {"category": "A", "item": "A2", "score": 50},
            {"category": "B", "item": "B1", "score": 75},
            {"category": "B", "item": "B2", "score": 25},
        ])

        t = con.create_table("test_partition", df)
        result = rank_by_keys(t, keys=["category"], order_by="score", descending=True)
        result_df = result.execute()

        # Category A: A1 should be rank 0 (higher score)
        a1 = result_df[(result_df["category"] == "A") & (result_df["item"] == "A1")].iloc[0]
        a2 = result_df[(result_df["category"] == "A") & (result_df["item"] == "A2")].iloc[0]
        assert a1["_rank"] == 0
        assert a2["_rank"] == 1

        # Category B: B1 should be rank 0 (higher score)
        b1 = result_df[(result_df["category"] == "B") & (result_df["item"] == "B1")].iloc[0]
        b2 = result_df[(result_df["category"] == "B") & (result_df["item"] == "B2")].iloc[0]
        assert b1["_rank"] == 0
        assert b2["_rank"] == 1


class TestRankByKeysCustomColumnName:
    """Test rank_by_keys with custom rank column name."""

    def test_custom_rank_column(self, con):
        """Use custom name for rank column."""
        df = pd.DataFrame([
            {"id": 1, "score": 100},
            {"id": 1, "score": 50},
        ])

        t = con.create_table("test_custom_name", df)
        result = rank_by_keys(t, keys=["id"], order_by="score", rank_column="my_rank")
        result_df = result.execute()

        assert "my_rank" in result_df.columns
        assert "_rank" not in result_df.columns


class TestRankByKeysCompositeKeys:
    """Test rank_by_keys with composite partition keys."""

    def test_composite_partition_key(self, con):
        """Rank within composite key partitions."""
        df = pd.DataFrame([
            {"region": "US", "product": "A", "sale": 100, "ts": datetime(2025, 1, 10)},
            {"region": "US", "product": "A", "sale": 150, "ts": datetime(2025, 1, 15)},
            {"region": "US", "product": "B", "sale": 200, "ts": datetime(2025, 1, 12)},
            {"region": "EU", "product": "A", "sale": 80, "ts": datetime(2025, 1, 11)},
        ])

        t = con.create_table("test_composite", df)
        result = rank_by_keys(t, keys=["region", "product"], order_by="ts", descending=True)
        result_df = result.execute()

        # US+A: latest should be rank 0
        us_a_latest = result_df[
            (result_df["region"] == "US") &
            (result_df["product"] == "A") &
            (result_df["sale"] == 150)
        ].iloc[0]
        assert us_a_latest["_rank"] == 0

        # US+B: only one record, rank 0
        us_b = result_df[
            (result_df["region"] == "US") &
            (result_df["product"] == "B")
        ].iloc[0]
        assert us_b["_rank"] == 0


class TestRankByKeysTopN:
    """Test using rank_by_keys for top-N per group selection."""

    def test_top_3_per_category(self, con):
        """Select top 3 per category using rank."""
        df = pd.DataFrame([
            {"category": "A", "item": f"A{i}", "score": i * 10}
            for i in range(1, 11)
        ] + [
            {"category": "B", "item": f"B{i}", "score": i * 5}
            for i in range(1, 11)
        ])

        t = con.create_table("test_top3", df)
        result = rank_by_keys(t, keys=["category"], order_by="score", descending=True)
        result_df = result.execute()

        # Filter to top 3 per category (rank 0, 1, 2)
        top3 = result_df[result_df["_rank"] < 3]

        # Category A: top 3 should be A10, A9, A8 (scores 100, 90, 80)
        a_top3 = top3[top3["category"] == "A"].sort_values("_rank")
        assert list(a_top3["item"]) == ["A10", "A9", "A8"]

        # Category B: top 3 should be B10, B9, B8 (scores 50, 45, 40)
        b_top3 = top3[top3["category"] == "B"].sort_values("_rank")
        assert list(b_top3["item"]) == ["B10", "B9", "B8"]


class TestRankByKeysEdgeCases:
    """Edge case tests for rank_by_keys."""

    def test_empty_table(self, con):
        """Empty table returns empty result with rank column."""
        df = pd.DataFrame({
            "id": pd.Series([], dtype=int),
            "score": pd.Series([], dtype=int)
        })

        t = con.create_table("test_empty", df)
        result = rank_by_keys(t, keys=["id"], order_by="score")
        result_df = result.execute()

        assert len(result_df) == 0
        assert "_rank" in result_df.columns

    def test_single_record_per_key(self, con):
        """Single record per key gets rank 0."""
        df = pd.DataFrame([
            {"id": 1, "score": 100},
            {"id": 2, "score": 200},
            {"id": 3, "score": 300},
        ])

        t = con.create_table("test_single_per_key", df)
        result = rank_by_keys(t, keys=["id"], order_by="score")
        result_df = result.execute()

        # All should have rank 0
        assert all(result_df["_rank"] == 0)

    def test_preserves_all_columns(self, con):
        """All original columns preserved."""
        df = pd.DataFrame([
            {"id": 1, "col_a": "A", "col_b": 100, "score": 50},
        ])

        t = con.create_table("test_preserve", df)
        result = rank_by_keys(t, keys=["id"], order_by="score")
        result_df = result.execute()

        expected_cols = {"id", "col_a", "col_b", "score", "_rank"}
        assert set(result_df.columns) == expected_cols

    def test_handles_ties(self, con):
        """Tied scores get sequential ranks (row_number behavior)."""
        df = pd.DataFrame([
            {"id": 1, "item": "A", "score": 100},
            {"id": 1, "item": "B", "score": 100},  # Tie
            {"id": 1, "item": "C", "score": 100},  # Tie
        ])

        t = con.create_table("test_ties", df)
        result = rank_by_keys(t, keys=["id"], order_by="score")
        result_df = result.execute()

        # Should have ranks 0, 1, 2 (row_number, not dense_rank)
        assert set(result_df["_rank"]) == {0, 1, 2}
