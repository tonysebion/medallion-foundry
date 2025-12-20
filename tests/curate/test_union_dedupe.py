"""Tests for union_dedupe function with data validation.

union_dedupe unions multiple tables and deduplicates by keys,
keeping the latest version of each record.
"""

from __future__ import annotations

from datetime import datetime

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import union_dedupe


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


class TestUnionDedupeBasic:
    """Basic tests for union_dedupe functionality."""

    def test_unions_two_tables(self, con):
        """Two tables unioned correctly."""
        df1 = pd.DataFrame([
            {"id": 1, "value": "A", "ts": datetime(2025, 1, 10)},
            {"id": 2, "value": "B", "ts": datetime(2025, 1, 10)},
        ])
        df2 = pd.DataFrame([
            {"id": 3, "value": "C", "ts": datetime(2025, 1, 15)},
            {"id": 4, "value": "D", "ts": datetime(2025, 1, 15)},
        ])

        t1 = ibis.memtable(df1)
        t2 = ibis.memtable(df2)

        result = union_dedupe([t1, t2], keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 4
        assert set(result_df["id"]) == {1, 2, 3, 4}

    def test_dedupes_across_tables(self, con):
        """Duplicate keys across tables deduplicated."""
        df1 = pd.DataFrame([
            {"id": 1, "value": "old", "ts": datetime(2025, 1, 10)},
        ])
        df2 = pd.DataFrame([
            {"id": 1, "value": "new", "ts": datetime(2025, 1, 15)},  # Same key, newer
        ])

        t1 = ibis.memtable(df1)
        t2 = ibis.memtable(df2)

        result = union_dedupe([t1, t2], keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "new"  # Latest kept

    def test_keeps_latest_version(self, con):
        """Latest version kept when same key in multiple tables."""
        df1 = pd.DataFrame([
            {"id": 1, "value": "v1", "ts": datetime(2025, 1, 1)},
        ])
        df2 = pd.DataFrame([
            {"id": 1, "value": "v2", "ts": datetime(2025, 1, 10)},
        ])
        df3 = pd.DataFrame([
            {"id": 1, "value": "v3", "ts": datetime(2025, 1, 20)},  # Latest
        ])

        t1 = ibis.memtable(df1)
        t2 = ibis.memtable(df2)
        t3 = ibis.memtable(df3)

        result = union_dedupe([t1, t2, t3], keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "v3"


class TestUnionDedupeManyTables:
    """Test union_dedupe with many tables."""

    def test_union_five_tables(self, con):
        """Union five tables correctly."""
        tables = []
        for i in range(5):
            df = pd.DataFrame([
                {"id": i * 10 + j, "value": f"t{i}_{j}", "ts": datetime(2025, 1, i + 1)}
                for j in range(3)
            ])
            tables.append(ibis.memtable(df))

        result = union_dedupe(tables, keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 15  # 5 tables * 3 unique keys each

    def test_single_table(self, con):
        """Single table returned as-is (deduplicated)."""
        df = pd.DataFrame([
            {"id": 1, "value": "A", "ts": datetime(2025, 1, 10)},
            {"id": 1, "value": "B", "ts": datetime(2025, 1, 15)},  # Duplicate key
        ])

        t = ibis.memtable(df)

        result = union_dedupe([t], keys=["id"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == "B"  # Latest


class TestUnionDedupeCompositeKeys:
    """Test union_dedupe with composite keys."""

    def test_composite_key_dedupe(self, con):
        """Deduplicate with composite key."""
        df1 = pd.DataFrame([
            {"region": "US", "product": "A", "price": 100, "ts": datetime(2025, 1, 10)},
        ])
        df2 = pd.DataFrame([
            {"region": "US", "product": "A", "price": 110, "ts": datetime(2025, 1, 15)},  # Update
            {"region": "EU", "product": "A", "price": 90, "ts": datetime(2025, 1, 15)},   # New
        ])

        t1 = ibis.memtable(df1)
        t2 = ibis.memtable(df2)

        result = union_dedupe([t1, t2], keys=["region", "product"], order_by="ts")
        result_df = result.execute()

        assert len(result_df) == 2  # US+A and EU+A

        # US+A should have latest price
        us_a = result_df[(result_df["region"] == "US") & (result_df["product"] == "A")].iloc[0]
        assert us_a["price"] == 110


class TestUnionDedupeEdgeCases:
    """Edge case tests for union_dedupe."""

    def test_empty_tables_list_raises(self, con):
        """Empty list of tables raises ValueError."""
        with pytest.raises(ValueError, match="At least one table required"):
            union_dedupe([], keys=["id"], order_by="ts")

    def test_preserves_all_columns(self, con):
        """All columns from tables preserved."""
        df1 = pd.DataFrame([
            {"id": 1, "col_a": "A", "col_b": 100, "ts": datetime(2025, 1, 10)},
        ])
        df2 = pd.DataFrame([
            {"id": 2, "col_a": "B", "col_b": 200, "ts": datetime(2025, 1, 15)},
        ])

        t1 = ibis.memtable(df1)
        t2 = ibis.memtable(df2)

        result = union_dedupe([t1, t2], keys=["id"], order_by="ts")
        result_df = result.execute()

        expected_cols = {"id", "col_a", "col_b", "ts"}
        assert set(result_df.columns) == expected_cols


class TestUnionDedupeTypicalUseCase:
    """Test typical Bronze partition union use case."""

    def test_union_bronze_partitions(self, con):
        """Union multiple Bronze date partitions."""
        # Simulate Bronze partitions from different days
        partition_2025_01_15 = pd.DataFrame([
            {"order_id": "ORD001", "status": "pending", "updated_at": datetime(2025, 1, 15, 10, 0, 0)},
            {"order_id": "ORD002", "status": "pending", "updated_at": datetime(2025, 1, 15, 11, 0, 0)},
        ])

        partition_2025_01_16 = pd.DataFrame([
            {"order_id": "ORD001", "status": "shipped", "updated_at": datetime(2025, 1, 16, 9, 0, 0)},  # Update
            {"order_id": "ORD003", "status": "pending", "updated_at": datetime(2025, 1, 16, 12, 0, 0)},  # New
        ])

        partition_2025_01_17 = pd.DataFrame([
            {"order_id": "ORD001", "status": "delivered", "updated_at": datetime(2025, 1, 17, 14, 0, 0)},  # Update
            {"order_id": "ORD002", "status": "shipped", "updated_at": datetime(2025, 1, 17, 10, 0, 0)},    # Update
        ])

        t1 = ibis.memtable(partition_2025_01_15)
        t2 = ibis.memtable(partition_2025_01_16)
        t3 = ibis.memtable(partition_2025_01_17)

        result = union_dedupe([t1, t2, t3], keys=["order_id"], order_by="updated_at")
        result_df = result.execute()

        # Should have 3 unique orders
        assert len(result_df) == 3
        assert result_df["order_id"].is_unique

        # Verify latest status for each order
        ord001 = result_df[result_df["order_id"] == "ORD001"].iloc[0]
        assert ord001["status"] == "delivered"

        ord002 = result_df[result_df["order_id"] == "ORD002"].iloc[0]
        assert ord002["status"] == "shipped"

        ord003 = result_df[result_df["order_id"] == "ORD003"].iloc[0]
        assert ord003["status"] == "pending"
