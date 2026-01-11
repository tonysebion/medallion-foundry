"""Tests for CDC with multiple operations per key in single batch.

Tests scenarios where a single key has many operations in one batch:
- I -> U -> U -> D (4 ops)
- Rapid updates (10+ ops in quick succession)
- Only the latest operation should matter

These scenarios occur when:
- Source system has high update frequency
- Batching includes multiple changes to same record
- Micro-batching from streaming sources
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCMultipleOpsPerKey:
    """Tests for multiple operations on same key."""

    def test_four_operations_same_key_ending_delete(self):
        """I -> U -> U -> D for same key - delete wins."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V1", "V2", "V3", "V4"],
                "op": ["I", "U", "U", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:01:00",
                    "2025-01-10 10:02:00",
                    "2025-01-10 10:03:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Final op is D, so record should be filtered
        assert len(result_df) == 0, "Delete should win"

    def test_four_operations_same_key_ending_update(self):
        """I -> U -> D -> U for same key - update wins."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V1", "V2", "V3", "V4"],
                "op": ["I", "U", "D", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:01:00",
                    "2025-01-10 10:02:00",
                    "2025-01-10 10:03:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Final op is U, so record should exist
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "V4"

    def test_rapid_update_sequence_10_ops(self):
        """10 rapid updates to same key - latest wins."""
        con = ibis.duckdb.connect()

        records = []
        base_time = datetime(2025, 1, 10, 10, 0, 0)

        for i in range(10):
            records.append(
                {
                    "id": 1,
                    "name": f"V{i}",
                    "value": i * 100,
                    "op": "I" if i == 0 else "U",
                    "updated_at": (base_time + timedelta(seconds=i)).isoformat(),
                }
            )

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "V9", "Should have latest version"
        assert result_df.iloc[0]["value"] == 900

    def test_only_latest_operation_matters(self):
        """Intermediate operations should not affect final state.

        All these patterns should produce the same result:
        - I -> U -> U -> U (final: active with V3)
        - I -> D -> I -> U (final: active with V3)
        - I -> D -> D -> U (final: active with V3, assuming D->D is ok)
        """
        con = ibis.duckdb.connect()

        # Pattern 1: Simple updates
        pattern1 = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V0", "V1", "V2", "V3"],
                "op": ["I", "U", "U", "U"],
                "updated_at": ["2025-01-10 10:00", "2025-01-10 10:01", "2025-01-10 10:02", "2025-01-10 10:03"],
            }
        )

        # Pattern 2: Delete in middle
        pattern2 = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V0", "V1", "V2", "V3"],
                "op": ["I", "D", "I", "U"],
                "updated_at": ["2025-01-10 10:00", "2025-01-10 10:01", "2025-01-10 10:02", "2025-01-10 10:03"],
            }
        )

        t1 = con.create_table("pattern1", pattern1)
        t2 = con.create_table("pattern2", pattern2)

        result1 = apply_cdc(
            t1, keys=["id"], order_by="updated_at", delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        ).execute()

        result2 = apply_cdc(
            t2, keys=["id"], order_by="updated_at", delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        ).execute()

        # Both should produce same final state
        assert len(result1) == 1
        assert len(result2) == 1
        assert result1.iloc[0]["name"] == "V3"
        assert result2.iloc[0]["name"] == "V3"

    def test_multiple_ops_preserves_all_columns(self):
        """All columns should be preserved from latest operation."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Alice", "Alice Updated", "Alice Final"],
                "value": [100, 150, 200],
                "status": ["new", "active", "premium"],
                "category": ["A", "B", "C"],
                "op": ["I", "U", "U"],
                "updated_at": ["2025-01-10 10:00", "2025-01-10 11:00", "2025-01-10 12:00"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        row = result_df.iloc[0]
        assert row["name"] == "Alice Final"
        assert row["value"] == 200
        assert row["status"] == "premium"
        assert row["category"] == "C"


class TestMultipleOpsMultipleKeys:
    """Tests for multiple ops across multiple keys."""

    def test_multiple_keys_each_with_multiple_ops(self):
        """Each key has its own sequence of operations."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 1, 2, 1, 2],
                "name": ["A1", "B1", "A2", "B2", "A3", "B3"],
                "op": ["I", "I", "U", "U", "U", "D"],
                "updated_at": [
                    "2025-01-10 10:00",
                    "2025-01-10 10:00",
                    "2025-01-10 11:00",
                    "2025-01-10 11:00",
                    "2025-01-10 12:00",
                    "2025-01-10 12:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Key 1: I -> U -> U = A3 (active)
        # Key 2: I -> U -> D = deleted
        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 1
        assert result_df.iloc[0]["name"] == "A3"

    def test_interleaved_operations_different_keys(self):
        """Operations for different keys interleaved by timestamp."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 1, 2, 3, 1, 2, 3],
                "name": ["A1", "B1", "C1", "A2", "B2", "C2", "A3", "B3", "C3"],
                "op": ["I", "I", "I", "U", "D", "U", "U", "I", "D"],
                "updated_at": [
                    "2025-01-10 10:00",
                    "2025-01-10 10:01",
                    "2025-01-10 10:02",
                    "2025-01-10 11:00",
                    "2025-01-10 11:01",
                    "2025-01-10 11:02",
                    "2025-01-10 12:00",
                    "2025-01-10 12:01",
                    "2025-01-10 12:02",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        # Key 1: I -> U -> U = A3
        # Key 2: I -> D -> I = B3 (re-inserted)
        # Key 3: I -> U -> D = deleted
        assert len(result_df) == 2
        assert set(result_df["id"]) == {1, 2}

        k1 = result_df[result_df["id"] == 1].iloc[0]
        k2 = result_df[result_df["id"] == 2].iloc[0]

        assert k1["name"] == "A3"
        assert k2["name"] == "B3"


class TestMultipleOpsEdgeCases:
    """Edge cases for multiple operations."""

    def test_many_deletes_same_key(self):
        """Multiple deletes for same key - result is still deleted."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V1", "V2", "V3", "V4"],
                "op": ["I", "D", "D", "D"],  # Multiple deletes
                "updated_at": [
                    "2025-01-10 10:00",
                    "2025-01-10 11:00",
                    "2025-01-10 12:00",
                    "2025-01-10 13:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 0, "Multiple deletes should still result in deletion"

    def test_many_inserts_same_key(self):
        """Multiple inserts for same key - latest wins.

        This is unusual in CDC but should be handled gracefully.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Insert1", "Insert2", "Insert3"],
                "op": ["I", "I", "I"],  # Multiple inserts
                "updated_at": [
                    "2025-01-10 10:00",
                    "2025-01-10 11:00",
                    "2025-01-10 12:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Insert3", "Latest insert should win"

    def test_update_without_prior_insert(self):
        """Update without prior insert - should still work.

        In CDC, updates can appear without explicit insert if
        the initial state was captured before CDC started.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Update1", "Update2"],
                "op": ["U", "U"],  # Only updates, no insert
                "updated_at": ["2025-01-10 10:00", "2025-01-10 11:00"],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Updates are valid operations, should result in 1 record
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Update2"
