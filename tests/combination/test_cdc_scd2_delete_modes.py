"""Tests for CDC with SCD2 history and delete mode interactions.

Tests the complex interaction between:
- CDC operations (I/U/D)
- SCD2 history mode (full_history)
- Delete modes (ignore, tombstone, hard_delete)

Key questions:
- Does hard_delete remove all history versions or just current?
- How does tombstone interact with SCD2 effective dates?
- Can deleted records be resurrected in SCD2?
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc, build_history, dedupe_latest
from tests.combination.conftest import create_memtable
from tests.data_validation.assertions import SCD2Assertions


def apply_cdc_then_build_history(
    t: ibis.Table,
    keys: list[str],
    ts_col: str,
    delete_mode: str,
    cdc_options: dict,
) -> ibis.Table:
    """Apply CDC processing then build SCD2 history.

    This simulates the full Silver pipeline for CDC -> SCD2.
    """
    # First apply CDC to handle operations
    cdc_result = apply_cdc(t, keys, ts_col, delete_mode, cdc_options)

    # Then build SCD2 history
    # Note: For single-record-per-key after CDC, this adds SCD2 columns
    return build_history(cdc_result, keys, ts_col)


class TestCDCSCD2WithIgnoreDelete:
    """Tests for CDC + SCD2 with delete_mode=ignore."""

    def test_scd2_ignore_delete_filters_out_deletes(self, con):
        """With ignore mode, deleted keys should not appear in history."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"op": "D", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # With ignore mode, delete wins and record is filtered out
        # So history should be empty
        assert len(result_df) == 0, "Deleted key should be filtered out entirely"

    def test_scd2_ignore_keeps_non_deleted_keys(self, con):
        """Non-deleted keys should have full history."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"op": "I", "id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 2, "name": "Bob", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Key 1: Latest is U (kept) -> 1 record in SCD2 after dedupe
        # Key 2: Latest is D (filtered out)
        assert len(result_df) == 1, "Only key 1 should be in history"
        assert result_df.iloc[0]["id"] == 1


class TestCDCSCD2WithTombstone:
    """Tests for CDC + SCD2 with delete_mode=tombstone."""

    def test_scd2_tombstone_preserves_deleted_key(self, con):
        """Tombstoned keys should appear in SCD2 with _deleted flag."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"op": "D", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # With tombstone, the deleted key should be preserved
        assert len(result_df) == 1, "Tombstoned key should be in history"
        assert "_deleted" in result_df.columns
        assert bool(result_df.iloc[0]["_deleted"]) is True

    def test_scd2_tombstone_has_correct_effective_dates(self, con):
        """Tombstoned record should have correct SCD2 columns."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert "effective_from" in result_df.columns
        assert "is_current" in result_df.columns
        # The tombstone should be the current record
        assert result_df.iloc[0]["is_current"] == 1

    def test_scd2_tombstone_resurrection_clears_flag(self, con):
        """Resurrected key should have _deleted=False in SCD2."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 1, "name": "V1", "ts": datetime(2025, 1, 15)},
                {"op": "I", "id": 1, "name": "V2", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Latest operation is I, so _deleted should be False
        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is False
        assert result_df.iloc[0]["name"] == "V2"


class TestCDCSCD2WithHardDelete:
    """Tests for CDC + SCD2 with delete_mode=hard_delete."""

    def test_scd2_hard_delete_removes_key_entirely(self, con):
        """Hard delete should remove the key from SCD2 entirely."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice V1", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 15)},
                {"op": "D", "id": 1, "name": "Alice V2", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="hard_delete",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # With hard_delete, the key should be completely removed
        assert len(result_df) == 0, "Hard delete should remove key entirely"

    def test_scd2_hard_delete_reinsert_creates_new_history(self, con):
        """Reinsert after hard delete should start fresh history."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Original", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 1, "name": "Original", "ts": datetime(2025, 1, 15)},
                {"op": "I", "id": 1, "name": "Reborn", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="hard_delete",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # The reinsert should create a new record
        assert len(result_df) == 1, "Reinserted key should exist"
        assert result_df.iloc[0]["name"] == "Reborn"
        assert "_deleted" not in result_df.columns, "hard_delete doesn't add _deleted"

    def test_scd2_hard_delete_multiple_keys(self, con):
        """Hard delete should only affect the deleted key."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"op": "I", "id": 2, "name": "Bob", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 20)},
                {"op": "U", "id": 2, "name": "Bob Updated", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="hard_delete",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Key 1 should be removed, Key 2 should remain
        assert len(result_df) == 1, "Only key 2 should remain"
        assert result_df.iloc[0]["id"] == 2
        assert result_df.iloc[0]["name"] == "Bob Updated"


class TestCDCSCD2CompareDeleteModes:
    """Compare behavior across all delete modes."""

    @pytest.fixture
    def cdc_with_delete(self, con):
        """Create CDC data with a delete operation."""
        return create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice Updated", "ts": datetime(2025, 1, 15)},
                {"op": "D", "id": 1, "name": "Alice Updated", "ts": datetime(2025, 1, 20)},
            ]
        )

    def test_ignore_vs_tombstone_vs_hard_delete(self, cdc_with_delete):
        """Compare all three delete modes on the same data."""
        results = {}

        for mode in ["ignore", "tombstone", "hard_delete"]:
            result = apply_cdc_then_build_history(
                cdc_with_delete,
                keys=["id"],
                ts_col="ts",
                delete_mode=mode,
                cdc_options={"operation_column": "op"},
            )
            results[mode] = result.execute()

        # ignore: Record filtered out
        assert len(results["ignore"]) == 0, "ignore mode should filter out deleted key"

        # tombstone: Record kept with _deleted=True
        assert len(results["tombstone"]) == 1, "tombstone should keep record"
        assert bool(results["tombstone"].iloc[0]["_deleted"]) is True

        # hard_delete: Record removed
        assert len(results["hard_delete"]) == 0, "hard_delete should remove record"

    def test_all_modes_same_result_for_non_deleted(self, con):
        """All modes should produce same result for non-deleted keys."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "Alice", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "Alice Updated", "ts": datetime(2025, 1, 15)},
            ]
        )

        results = {}
        for mode in ["ignore", "tombstone", "hard_delete"]:
            result = apply_cdc_then_build_history(
                cdc,
                keys=["id"],
                ts_col="ts",
                delete_mode=mode,
                cdc_options={"operation_column": "op"},
            )
            results[mode] = result.execute()

        # All should have 1 record
        for mode, df in results.items():
            assert len(df) == 1, f"{mode} should have 1 record"
            assert df.iloc[0]["name"] == "Alice Updated"

        # tombstone mode adds _deleted column (should be False)
        assert "_deleted" in results["tombstone"].columns
        assert bool(results["tombstone"].iloc[0]["_deleted"]) is False


class TestCDCSCD2ComplexScenarios:
    """Complex real-world scenarios for CDC + SCD2."""

    def test_multiple_keys_different_fates(self, con):
        """Multiple keys with different outcomes (active, deleted, resurrected)."""
        cdc = create_memtable(
            [
                # Key 1: I -> U (active)
                {"op": "I", "id": 1, "name": "K1-V1", "ts": datetime(2025, 1, 10)},
                {"op": "U", "id": 1, "name": "K1-V2", "ts": datetime(2025, 1, 15)},
                # Key 2: I -> D (deleted)
                {"op": "I", "id": 2, "name": "K2-V1", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 2, "name": "K2-V1", "ts": datetime(2025, 1, 20)},
                # Key 3: I -> D -> I (resurrected)
                {"op": "I", "id": 3, "name": "K3-V1", "ts": datetime(2025, 1, 10)},
                {"op": "D", "id": 3, "name": "K3-V1", "ts": datetime(2025, 1, 15)},
                {"op": "I", "id": 3, "name": "K3-V2", "ts": datetime(2025, 1, 20)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3, "All keys should be present"

        k1 = result_df[result_df["id"] == 1].iloc[0]
        k2 = result_df[result_df["id"] == 2].iloc[0]
        k3 = result_df[result_df["id"] == 3].iloc[0]

        assert bool(k1["_deleted"]) is False, "K1 should be active"
        assert k1["name"] == "K1-V2"

        assert bool(k2["_deleted"]) is True, "K2 should be tombstoned"

        assert bool(k3["_deleted"]) is False, "K3 should be resurrected"
        assert k3["name"] == "K3-V2"

    def test_rapid_lifecycle_changes(self, con):
        """Rapid I/U/D changes should produce correct SCD2 state."""
        cdc = create_memtable(
            [
                {"op": "I", "id": 1, "name": "V1", "ts": datetime(2025, 1, 10, 10, 0)},
                {"op": "U", "id": 1, "name": "V2", "ts": datetime(2025, 1, 10, 10, 1)},
                {"op": "U", "id": 1, "name": "V3", "ts": datetime(2025, 1, 10, 10, 2)},
                {"op": "D", "id": 1, "name": "V3", "ts": datetime(2025, 1, 10, 10, 3)},
                {"op": "I", "id": 1, "name": "V4", "ts": datetime(2025, 1, 10, 10, 4)},
                {"op": "U", "id": 1, "name": "V5", "ts": datetime(2025, 1, 10, 10, 5)},
            ]
        )

        result = apply_cdc_then_build_history(
            cdc,
            keys=["id"],
            ts_col="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Latest is U (V5), should be active
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "V5"
        assert bool(result_df.iloc[0]["_deleted"]) is False
