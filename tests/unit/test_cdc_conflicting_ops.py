"""Tests for CDC conflicting operations scenarios.

Tests what happens when conflicting operations occur for the same key,
such as delete followed by update, or multiple operations in the same batch.

These scenarios can occur in real CDC streams due to:
- Out-of-order event delivery
- Data corruption in source
- Race conditions in source system
"""

from __future__ import annotations

import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCConflictingOperations:
    """Tests for conflicting CDC operations."""

    def test_delete_then_update_same_day_update_wins(self):
        """D at T1, U at T2 (T2 > T1) - update should win.

        If a delete is followed by an update (with later timestamp),
        the update should be the final state.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Deleted", "Resurrected"],
                "op": ["D", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",  # Update is later
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

        # Update should win because it's later
        assert len(result_df) == 1, "Update should win over earlier delete"
        assert result_df.iloc[0]["name"] == "Resurrected"

    def test_update_then_delete_same_day_delete_wins(self):
        """U at T1, D at T2 (T2 > T1) - delete should win.

        If an update is followed by a delete (with later timestamp),
        the delete should be the final state.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Updated", "Deleted"],
                "op": ["U", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",  # Delete is later
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

        # Delete should win because it's later (and filtered out in ignore mode)
        assert len(result_df) == 0, "Delete should win over earlier update"

    def test_update_then_delete_tombstone_mode(self):
        """U then D with tombstone mode should show tombstone."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Updated", "Deleted"],
                "op": ["U", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",
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

        assert len(result_df) == 1, "Tombstone mode should keep the record"
        assert bool(result_df.iloc[0]["_deleted"]) is True, "Should be tombstoned"

    def test_full_lifecycle_single_batch(self):
        """I -> U -> D same key in single batch.

        A full lifecycle in one batch - the delete should win
        since it has the latest timestamp.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Created", "Updated", "Deleted"],
                "op": ["I", "U", "D"],
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

        assert len(result_df) == 0, "Final delete should result in no records"

    def test_full_lifecycle_single_batch_tombstone(self):
        """I -> U -> D in single batch with tombstone mode."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["Created", "Updated", "Deleted"],
                "op": ["I", "U", "D"],
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
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1, "Tombstone should preserve record"
        assert bool(result_df.iloc[0]["_deleted"]) is True

    def test_conflicting_ops_deterministic(self):
        """Same input should always produce same output.

        Run the same conflicting operation multiple times
        and verify consistent results.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "name": ["A", "B", "C"],
                "op": ["I", "D", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",
                    "2025-01-10 12:00:00",  # Update is latest
                ],
            }
        )
        t = con.create_table("test", df)

        results = []
        for _ in range(5):
            result = apply_cdc(
                t,
                keys=["id"],
                order_by="updated_at",
                delete_mode="ignore",
                cdc_options={"operation_column": "op"},
            )
            result_df = result.execute()
            results.append(len(result_df))

        # All runs should produce the same count
        assert len(set(results)) == 1, f"Results should be deterministic: {results}"
        # Update is latest, so should have 1 record
        assert results[0] == 1, "Latest update should result in 1 record"

    def test_insert_after_delete_in_wrong_timestamp_order(self):
        """I with later timestamp than D - insert should win.

        This tests out-of-order delivery where the insert event
        has a later timestamp than the delete.
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1],
                "name": ["Deleted", "Inserted"],
                "op": ["D", "I"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",  # Insert is later
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

        # Insert is later, so it should win
        assert len(result_df) == 1, "Insert with later timestamp should win"
        assert result_df.iloc[0]["name"] == "Inserted"


class TestMultiKeyConflicts:
    """Tests for conflicting operations across multiple keys."""

    def test_conflicts_isolated_per_key(self):
        """Conflicts for one key should not affect another key.

        Key 1 has U -> D (delete wins)
        Key 2 has D -> U (update wins)
        """
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 1, 2],
                "name": ["K1-Update", "K2-Delete", "K1-Delete", "K2-Update"],
                "op": ["U", "D", "D", "U"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",  # K1 delete is later
                    "2025-01-10 11:00:00",  # K2 update is later
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

        # Key 1: delete wins (filtered out in ignore mode)
        # Key 2: update wins (kept)
        assert len(result_df) == 1, "Only key 2 should remain"
        assert result_df.iloc[0]["id"] == 2
        assert result_df.iloc[0]["name"] == "K2-Update"

    def test_all_keys_with_conflicts(self):
        """Multiple keys all with conflicting operations."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 1, 2, 3],
                "name": ["K1-I", "K2-I", "K3-I", "K1-D", "K2-U", "K3-D"],
                "op": ["I", "I", "I", "D", "U", "D"],
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:00:00",
                    "2025-01-10 11:00:00",
                    "2025-01-10 11:00:00",
                    "2025-01-10 11:00:00",
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

        # Key 1: D wins (filtered)
        # Key 2: U wins (kept)
        # Key 3: D wins (filtered)
        assert len(result_df) == 1, "Only key 2 should remain"
        assert result_df.iloc[0]["id"] == 2


class TestRapidOperations:
    """Tests for rapid successive operations."""

    def test_rapid_updates_keeps_latest(self):
        """Many rapid updates should keep only the latest."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1] * 10,
                "name": [f"V{i}" for i in range(10)],
                "value": list(range(100, 110)),
                "op": ["I"] + ["U"] * 9,
                "updated_at": [f"2025-01-10 10:00:0{i}" for i in range(10)],
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

        assert len(result_df) == 1, "Should dedupe to one record"
        assert result_df.iloc[0]["name"] == "V9", "Should have latest version"
        assert result_df.iloc[0]["value"] == 109

    def test_rapid_insert_delete_alternation(self):
        """Alternating I/D operations - latest should win."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1, 1, 1],
                "name": ["V1", "V1", "V2", "V2", "V3", "V3"],
                "op": ["I", "D", "I", "D", "I", "D"],  # Ends with D
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:01:00",
                    "2025-01-10 10:02:00",
                    "2025-01-10 10:03:00",
                    "2025-01-10 10:04:00",
                    "2025-01-10 10:05:00",
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

        # Final operation is D, so record should be filtered out
        assert len(result_df) == 0, "Final delete should win"

    def test_rapid_insert_delete_ending_with_insert(self):
        """Alternating I/D ending with I - record should exist."""
        con = ibis.duckdb.connect()
        df = pd.DataFrame(
            {
                "id": [1, 1, 1, 1, 1],
                "name": ["V1", "V1", "V2", "V2", "V3"],
                "op": ["I", "D", "I", "D", "I"],  # Ends with I
                "updated_at": [
                    "2025-01-10 10:00:00",
                    "2025-01-10 10:01:00",
                    "2025-01-10 10:02:00",
                    "2025-01-10 10:03:00",
                    "2025-01-10 10:04:00",
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

        # Final operation is I, so record should exist
        assert len(result_df) == 1, "Final insert should win"
        assert result_df.iloc[0]["name"] == "V3"
