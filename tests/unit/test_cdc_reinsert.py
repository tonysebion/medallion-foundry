"""Tests for CDC re-insert after delete scenarios.

Tests the resurrection pattern where a record is:
- Inserted (I)
- Deleted (D)
- Re-inserted (I) with the same natural key

This is a critical production scenario where records can be
deleted and recreated in source systems.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCReinsertAfterDelete:
    """Tests for re-insert after delete scenarios."""

    def test_insert_delete_reinsert_single_day(self):
        """I -> D -> I same key same day should result in active record.

        When a record goes through insert -> delete -> insert in the same batch,
        the final insert should win since it has the latest timestamp.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Alice V1", "Alice V1", "Alice V2"],
                "op": ["I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",
                    "2025-01-10 12:00:00",
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

        # Should have 1 record (the re-insert)
        assert len(result_df) == 1, "Re-insert should result in one active record"
        assert result_df.iloc[0]["id"] == 1
        assert result_df.iloc[0]["name"] == "Alice V2", "Should have re-inserted values"
        assert "op" not in result_df.columns, "Operation column should be dropped"

    def test_insert_delete_reinsert_across_days(self):
        """Day 1: I, Day 2: D, Day 3: I should result in active record.

        Multi-day scenario where a record is inserted, deleted the next day,
        then re-inserted on the third day.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Alice Day1", "Alice Day1", "Alice Day3"],
                "op": ["I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
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

        assert len(result_df) == 1, "Re-insert across days should result in one record"
        assert result_df.iloc[0]["name"] == "Alice Day3", "Should have Day 3 values"

    def test_reinsert_after_delete_tombstone_mode(self):
        """Tombstone should be cleared when record is re-inserted.

        With tombstone mode, a deleted record gets _deleted=True.
        When the same key is re-inserted, _deleted should become False.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Alice", "Alice", "Alice Reborn"],
                "op": ["I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1, "Should have one record after re-insert"
        assert "_deleted" in result_df.columns, "Tombstone mode should add _deleted"
        assert (
            bool(result_df.iloc[0]["_deleted"]) is False
        ), "Re-inserted record should NOT be marked as deleted"
        assert result_df.iloc[0]["name"] == "Alice Reborn"

    def test_reinsert_after_delete_hard_delete_mode(self):
        """Record should reappear after hard delete when re-inserted.

        With hard_delete mode, deleted records are filtered out.
        A subsequent insert should make the record appear again.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Alice", "Alice", "Alice Returns"],
                "op": ["I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="hard_delete",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1, "Re-inserted record should appear"
        assert result_df.iloc[0]["name"] == "Alice Returns"
        assert "_deleted" not in result_df.columns, "hard_delete should not add _deleted"

    def test_reinsert_with_different_values(self):
        """Re-inserted record should have new values, not original.

        When a key is re-inserted, the new attribute values should be used,
        not the values from the original insert.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Original Name", "Original Name", "New Name"],
                "value": [100, 100, 999],
                "status": ["active", "active", "pending"],
                "op": ["I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
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
        row = result_df.iloc[0]
        assert row["name"] == "New Name", "Should have new name"
        assert row["value"] == 999, "Should have new value"
        assert row["status"] == "pending", "Should have new status"

    def test_multiple_delete_reinsert_cycles(self):
        """Multiple I -> D -> I cycles should end with final state.

        Test pattern: I -> D -> I -> D -> I (two full cycles)
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1, 1],
                "name": ["V1", "V1", "V2", "V2", "V3"],
                "op": ["I", "D", "I", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
                    "2025-01-13 10:00:00",
                    "2025-01-14 10:00:00",
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
        assert result_df.iloc[0]["name"] == "V3", "Should have final version"

    def test_reinsert_mixed_with_other_keys(self):
        """Re-insert should not affect other keys.

        Key 1 goes through I -> D -> I while Key 2 just has I -> U.
        Both should be handled correctly.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 1, 2, 1],
                "name": ["Alice", "Bob", "Alice", "Bob Updated", "Alice Reborn"],
                "op": ["I", "I", "D", "U", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
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

        assert len(result_df) == 2, "Both keys should be present"
        assert result_df.iloc[0]["name"] == "Alice Reborn", "Key 1 should be re-inserted"
        assert result_df.iloc[1]["name"] == "Bob Updated", "Key 2 should be updated"

    def test_reinsert_ends_with_delete_tombstone(self):
        """I -> D -> I -> D pattern should end with tombstone.

        If the final operation is delete with tombstone mode,
        the record should be marked as deleted.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1],
                "name": ["V1", "V1", "V2", "V2"],
                "op": ["I", "D", "I", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
                    "2025-01-13 10:00:00",
                ],
            }
        )
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="updated_at",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1, "Should have one tombstone record"
        assert (
            bool(result_df.iloc[0]["_deleted"]) is True
        ), "Final delete should result in tombstone"
