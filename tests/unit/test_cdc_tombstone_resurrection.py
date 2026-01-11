"""Tests for CDC tombstone resurrection scenarios.

Tests the behavior of tombstone mode when records are deleted
and then re-inserted or updated. Key questions:
- Does a subsequent Insert clear the _deleted flag?
- Does a subsequent Update resurrect a tombstoned record?
- Are timestamps preserved correctly on tombstones?
"""

from __future__ import annotations

import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestTombstoneResurrection:
    """Tests for tombstone resurrection behavior."""

    def test_tombstone_then_insert_clears_deleted_flag(self):
        """D then I should result in _deleted=False.

        When a key is deleted (tombstoned) and then re-inserted,
        the _deleted flag should be False.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Original", "Resurrected"],
                "op": ["D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",  # Insert is later
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

        assert len(result_df) == 1, "Should have one record"
        assert "_deleted" in result_df.columns
        assert (
            bool(result_df.iloc[0]["_deleted"]) is False
        ), "Re-inserted record should NOT be deleted"
        assert result_df.iloc[0]["name"] == "Resurrected"

    def test_tombstone_then_update_clears_deleted_flag(self):
        """D then U should result in _deleted=False.

        An update after a delete should resurrect the record.
        This could happen if a source system corrects a mistaken delete.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Deleted", "Updated"],
                "op": ["D", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",  # Update is later
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

        assert len(result_df) == 1
        # Update is a valid operation (non-delete), so _deleted should be False
        assert (
            bool(result_df.iloc[0]["_deleted"]) is False
        ), "Update should resurrect tombstoned record"
        assert result_df.iloc[0]["name"] == "Updated"

    def test_multi_tombstone_resurrection_cycle(self):
        """I -> D -> I -> D -> I should end with _deleted=False.

        Multiple deletion/resurrection cycles should correctly
        track the final state.
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
                    "2025-01-14 10:00:00",  # Final is Insert
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

        assert len(result_df) == 1
        assert (
            bool(result_df.iloc[0]["_deleted"]) is False
        ), "Final insert should clear tombstone"
        assert result_df.iloc[0]["name"] == "V3"

    def test_multi_tombstone_ending_deleted(self):
        """I -> D -> I -> D should end with _deleted=True."""
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
                    "2025-01-13 10:00:00",  # Final is Delete
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

        assert len(result_df) == 1
        assert (
            bool(result_df.iloc[0]["_deleted"]) is True
        ), "Final delete should be tombstoned"

    def test_tombstone_preserves_deleted_record_values(self):
        """Tombstoned record should retain its last values.

        The record marked as deleted should have the values
        from the delete operation (or last known values).
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Alice", "Alice Final"],
                "value": [100, 999],
                "op": ["I", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
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

        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is True
        # Should have the values from the delete record
        assert result_df.iloc[0]["name"] == "Alice Final"
        assert result_df.iloc[0]["value"] == 999

    def test_tombstone_timestamp_from_delete_operation(self):
        """Tombstoned record should have timestamp from delete."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Alice", "Alice"],
                "op": ["I", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-15 14:30:00",  # Delete timestamp
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

        assert len(result_df) == 1
        # Timestamp should be from the delete operation
        ts = pd.to_datetime(result_df.iloc[0]["updated_at"])
        expected_ts = pd.Timestamp("2025-01-15 14:30:00")
        assert ts == expected_ts, "Tombstone should have delete timestamp"


class TestTombstoneWithMultipleKeys:
    """Tests for tombstone behavior across multiple keys."""

    def test_tombstone_independent_per_key(self):
        """Tombstone state should be independent per key.

        Key 1: I -> D (tombstoned)
        Key 2: I -> U (active)
        Key 3: I -> D -> I (resurrected)
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 1, 2, 3, 3],
                "name": ["K1-V1", "K2-V1", "K3-V1", "K1-V1", "K2-V2", "K3-V1", "K3-V2"],
                "op": ["I", "I", "I", "D", "U", "D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
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
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3, "All three keys should be present"

        k1 = result_df[result_df["id"] == 1].iloc[0]
        k2 = result_df[result_df["id"] == 2].iloc[0]
        k3 = result_df[result_df["id"] == 3].iloc[0]

        assert bool(k1["_deleted"]) is True, "Key 1 should be tombstoned"
        assert bool(k2["_deleted"]) is False, "Key 2 should be active"
        assert bool(k3["_deleted"]) is False, "Key 3 should be resurrected"

    def test_all_keys_tombstoned(self):
        """When all keys are deleted, all should be tombstoned."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 1, 2, 3],
                "name": ["K1", "K2", "K3", "K1", "K2", "K3"],
                "op": ["I", "I", "I", "D", "D", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-11 10:00:00",
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

        assert len(result_df) == 3, "All keys should be present as tombstones"
        assert all(result_df["_deleted"]), "All should be tombstoned"

    def test_all_keys_resurrected(self):
        """When all tombstoned keys are re-inserted, none should be deleted."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 1, 2, 1, 2],
                "name": ["K1-V1", "K2-V1", "K1-V1", "K2-V1", "K1-V2", "K2-V2"],
                "op": ["I", "I", "D", "D", "I", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-11 10:00:00",
                    "2025-01-12 10:00:00",
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

        assert len(result_df) == 2, "Both keys should be present"
        assert not any(result_df["_deleted"]), "None should be tombstoned"


class TestTombstoneEdgeCases:
    """Edge cases for tombstone behavior."""

    def test_delete_only_batch_all_tombstoned(self):
        """Batch with only deletes should all be tombstoned."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "op": ["D", "D", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
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

        assert len(result_df) == 3, "All deletes should be preserved as tombstones"
        assert all(result_df["_deleted"]), "All should be marked deleted"

    def test_single_insert_not_tombstoned(self):
        """Single insert should not have _deleted flag set."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Alice"],
                "op": ["I"],
                "updated_at": ["2025-01-10 10:00:00"],
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

        assert len(result_df) == 1
        assert "_deleted" in result_df.columns
        assert bool(result_df.iloc[0]["_deleted"]) is False

    def test_single_update_not_tombstoned(self):
        """Single update should not have _deleted flag set."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1],
                "name": ["Updated"],
                "op": ["U"],
                "updated_at": ["2025-01-10 10:00:00"],
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

        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is False
