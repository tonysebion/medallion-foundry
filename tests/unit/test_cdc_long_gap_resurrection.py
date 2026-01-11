"""Tests for CDC resurrection after long gaps.

Tests scenarios where records are deleted and then re-inserted
after extended periods (days or weeks):
- Delete on Day 3, re-insert on Day 18 (15-day gap)
- Multiple delete/re-insert cycles with varying gaps
- Resurrection with completely different values
- Resurrection affects tombstone cleanup

These patterns occur in real systems when:
- Customers cancel and then re-activate accounts
- Products are discontinued then brought back
- Data corrections require delete + re-insert
"""

from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCLongGapResurrection:
    """Tests for resurrection after extended delete periods."""

    def test_resurrection_after_15_day_gap(self):
        """Record deleted on Day 3, re-inserted on Day 18.

        15-day gap between delete and re-insert.
        """
        con = ibis.duckdb.connect()

        records = [
            # Day 1: Insert
            {"id": 1, "name": "Customer_Original", "value": 100, "op": "I",
             "ts": "2025-01-01 10:00:00"},
            # Day 3: Delete
            {"id": 1, "name": "Customer_Original", "value": 100, "op": "D",
             "ts": "2025-01-03 10:00:00"},
            # Day 18: Re-insert (15 days later)
            {"id": 1, "name": "Customer_REACTIVATED", "value": 999, "op": "I",
             "ts": "2025-01-18 10:00:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Record should be active with new values
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Customer_REACTIVATED"
        assert result_df.iloc[0]["value"] == 999

    def test_resurrection_after_30_day_gap(self):
        """Record deleted and re-inserted after 30 days (month-long gap)."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "Product_v1", "value": 100, "op": "I",
             "ts": "2025-01-01 10:00:00"},
            {"id": 1, "name": "Product_v1", "value": 100, "op": "D",
             "ts": "2025-01-05 10:00:00"},
            # 30-day gap
            {"id": 1, "name": "Product_v2_RELAUNCHED", "value": 500, "op": "I",
             "ts": "2025-02-04 10:00:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Product_v2_RELAUNCHED"
        assert result_df.iloc[0]["value"] == 500

    def test_resurrection_completely_different_values(self):
        """Re-inserted record has completely different attribute values."""
        con = ibis.duckdb.connect()

        records = [
            # Original customer in category A with low value
            {"id": 100, "name": "John_Doe", "category": "A", "value": 50,
             "op": "I", "ts": "2025-01-01 10:00:00"},
            {"id": 100, "name": "John_Doe", "category": "A", "value": 50,
             "op": "D", "ts": "2025-01-10 10:00:00"},
            # Re-activated as premium customer, different category and value
            {"id": 100, "name": "John_Doe_PREMIUM", "category": "VIP", "value": 10000,
             "op": "I", "ts": "2025-01-25 10:00:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # All new values should be present
        assert len(result_df) == 1
        row = result_df.iloc[0]
        assert row["name"] == "John_Doe_PREMIUM"
        assert row["category"] == "VIP"
        assert row["value"] == 10000


class TestCDCMultipleResurrectionCycles:
    """Tests for multiple delete/re-insert cycles."""

    def test_three_resurrection_cycles(self):
        """Record goes through 3 delete/re-insert cycles.

        I -> D -> I -> D -> I -> D -> I
        """
        con = ibis.duckdb.connect()

        base = datetime(2025, 1, 1, 10, 0, 0)
        records = [
            {"id": 1, "name": "Cycle_1_Insert", "op": "I", "ts": (base).isoformat()},
            {"id": 1, "name": "Cycle_1_Delete", "op": "D", "ts": (base + timedelta(days=5)).isoformat()},
            {"id": 1, "name": "Cycle_2_Insert", "op": "I", "ts": (base + timedelta(days=10)).isoformat()},
            {"id": 1, "name": "Cycle_2_Delete", "op": "D", "ts": (base + timedelta(days=15)).isoformat()},
            {"id": 1, "name": "Cycle_3_Insert", "op": "I", "ts": (base + timedelta(days=20)).isoformat()},
            {"id": 1, "name": "Cycle_3_Delete", "op": "D", "ts": (base + timedelta(days=25)).isoformat()},
            {"id": 1, "name": "Cycle_4_FINAL", "op": "I", "ts": (base + timedelta(days=30)).isoformat()},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Final insert wins
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Cycle_4_FINAL"

    def test_resurrection_cycles_ending_with_delete(self):
        """Multiple cycles ending with a delete."""
        con = ibis.duckdb.connect()

        base = datetime(2025, 1, 1, 10, 0, 0)
        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": (base).isoformat()},
            {"id": 1, "name": "v1", "op": "D", "ts": (base + timedelta(days=5)).isoformat()},
            {"id": 1, "name": "v2", "op": "I", "ts": (base + timedelta(days=10)).isoformat()},
            {"id": 1, "name": "v2", "op": "D", "ts": (base + timedelta(days=15)).isoformat()},
            # Final state is deleted
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Final delete wins, record filtered out
        assert len(result_df) == 0

    def test_varying_gap_lengths(self):
        """Resurrection cycles with varying gap lengths.

        Gaps: 2 days, 10 days, 1 day, 21 days
        """
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-02 10:00"},  # Day 2 delete
            # 2-day gap
            {"id": 1, "name": "v2", "op": "I", "ts": "2025-01-04 10:00"},  # Day 4 re-insert
            {"id": 1, "name": "v2", "op": "D", "ts": "2025-01-05 10:00"},
            # 10-day gap
            {"id": 1, "name": "v3", "op": "I", "ts": "2025-01-15 10:00"},
            {"id": 1, "name": "v3", "op": "D", "ts": "2025-01-16 10:00"},
            # 1-day gap
            {"id": 1, "name": "v4", "op": "I", "ts": "2025-01-17 10:00"},
            {"id": 1, "name": "v4", "op": "D", "ts": "2025-01-18 10:00"},
            # 21-day gap
            {"id": 1, "name": "v5_FINAL", "op": "I", "ts": "2025-02-08 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v5_FINAL"


class TestCDCResurrectionTombstone:
    """Tests for resurrection with tombstone delete mode."""

    def test_tombstone_cleared_on_resurrection(self):
        """Tombstone flag cleared when record is re-inserted."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "Original", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "Original", "op": "D", "ts": "2025-01-05 10:00"},
            {"id": 1, "name": "Resurrected", "op": "I", "ts": "2025-01-20 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Record should be active (not tombstoned)
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Resurrected"
        assert bool(result_df.iloc[0]["_deleted"]) is False

    def test_tombstone_final_state_is_deleted(self):
        """Tombstone shows deleted after I -> D -> I -> D."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-05 10:00"},
            {"id": 1, "name": "v2", "op": "I", "ts": "2025-01-15 10:00"},
            {"id": 1, "name": "v2", "op": "D", "ts": "2025-01-25 10:00"},  # Final delete
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Record should be tombstoned
        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is True

    def test_resurrection_updates_values_not_just_flag(self):
        """Re-insert brings new values, not just clears tombstone flag."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "OLD_NAME", "value": 100, "category": "bronze",
             "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "OLD_NAME", "value": 100, "category": "bronze",
             "op": "D", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "NEW_NAME", "value": 999, "category": "platinum",
             "op": "I", "ts": "2025-01-25 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        row = result_df.iloc[0]
        assert row["name"] == "NEW_NAME"
        assert row["value"] == 999
        assert row["category"] == "platinum"
        assert bool(row["_deleted"]) is False


class TestCDCResurrectionMultipleKeys:
    """Tests for resurrection with multiple keys having different patterns."""

    def test_different_keys_different_resurrection_timing(self):
        """Multiple keys with different resurrection schedules."""
        con = ibis.duckdb.connect()

        records = [
            # Key 1: Delete Day 3, resurrect Day 10
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "K1_v1", "op": "D", "ts": "2025-01-03 10:00"},
            {"id": 1, "name": "K1_v2", "op": "I", "ts": "2025-01-10 10:00"},

            # Key 2: Delete Day 5, resurrect Day 20
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 2, "name": "K2_v1", "op": "D", "ts": "2025-01-05 10:00"},
            {"id": 2, "name": "K2_v2", "op": "I", "ts": "2025-01-20 10:00"},

            # Key 3: Delete Day 2, never resurrect
            {"id": 3, "name": "K3_v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 3, "name": "K3_v1", "op": "D", "ts": "2025-01-02 10:00"},

            # Key 4: Never deleted
            {"id": 4, "name": "K4_v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 4, "name": "K4_v2", "op": "U", "ts": "2025-01-15 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        # Keys 1, 2, 4 should be active
        # Key 3 should be deleted
        assert len(result_df) == 3
        assert set(result_df["id"]) == {1, 2, 4}

        assert result_df[result_df["id"] == 1].iloc[0]["name"] == "K1_v2"
        assert result_df[result_df["id"] == 2].iloc[0]["name"] == "K2_v2"
        assert result_df[result_df["id"] == 4].iloc[0]["name"] == "K4_v2"

    def test_simultaneous_resurrection(self):
        """Multiple keys resurrect at the same time."""
        con = ibis.duckdb.connect()

        records = [
            # Keys 1, 2, 3 all created
            {"id": 1, "name": "K1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 2, "name": "K2", "op": "I", "ts": "2025-01-01 10:01"},
            {"id": 3, "name": "K3", "op": "I", "ts": "2025-01-01 10:02"},

            # All deleted on different days
            {"id": 1, "name": "K1", "op": "D", "ts": "2025-01-05 10:00"},
            {"id": 2, "name": "K2", "op": "D", "ts": "2025-01-06 10:00"},
            {"id": 3, "name": "K3", "op": "D", "ts": "2025-01-07 10:00"},

            # All resurrect on the same day
            {"id": 1, "name": "K1_BACK", "op": "I", "ts": "2025-01-20 10:00"},
            {"id": 2, "name": "K2_BACK", "op": "I", "ts": "2025-01-20 10:01"},
            {"id": 3, "name": "K3_BACK", "op": "I", "ts": "2025-01-20 10:02"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        assert len(result_df) == 3
        assert result_df.iloc[0]["name"] == "K1_BACK"
        assert result_df.iloc[1]["name"] == "K2_BACK"
        assert result_df.iloc[2]["name"] == "K3_BACK"


class TestCDCResurrectionWithUpdates:
    """Tests for resurrection combined with updates."""

    def test_updates_between_delete_and_resurrect_ignored(self):
        """Updates with timestamps between delete and resurrect are ignored.

        These shouldn't happen in well-formed CDC, but test robustness.
        """
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-05 10:00"},
            # Spurious update during "deleted" period - shouldn't affect final state
            {"id": 1, "name": "v_spurious", "op": "U", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "v_resurrected", "op": "I", "ts": "2025-01-20 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Latest operation (resurrect at Day 20) wins
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v_resurrected"

    def test_updates_after_resurrection(self):
        """Updates after resurrection work normally."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-05 10:00"},
            {"id": 1, "name": "v2_resurrected", "op": "I", "ts": "2025-01-20 10:00"},
            {"id": 1, "name": "v3_updated", "op": "U", "ts": "2025-01-21 10:00"},
            {"id": 1, "name": "v4_final", "op": "U", "ts": "2025-01-22 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v4_final"


class TestCDCResurrectionEdgeCases:
    """Edge cases for resurrection scenarios."""

    def test_resurrection_same_values_as_before(self):
        """Re-insert with identical values to original."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "Same", "value": 100, "op": "I", "ts": "2025-01-01 10:00"},
            {"id": 1, "name": "Same", "value": 100, "op": "D", "ts": "2025-01-10 10:00"},
            # Re-insert with identical values
            {"id": 1, "name": "Same", "value": 100, "op": "I", "ts": "2025-01-20 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Same"
        assert result_df.iloc[0]["value"] == 100

    def test_immediate_resurrection(self):
        """Delete and re-insert within seconds."""
        con = ibis.duckdb.connect()

        records = [
            {"id": 1, "name": "v1", "op": "I", "ts": "2025-01-10 10:00:00"},
            {"id": 1, "name": "v1", "op": "D", "ts": "2025-01-10 10:00:05"},  # 5 seconds later
            {"id": 1, "name": "v2", "op": "I", "ts": "2025-01-10 10:00:10"},  # 10 seconds after delete
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Latest insert (v2) wins
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "v2"

    def test_composite_key_resurrection(self):
        """Resurrection with composite keys."""
        con = ibis.duckdb.connect()

        records = [
            {"region": "US", "customer_id": 1, "name": "US_C1_v1", "op": "I", "ts": "2025-01-01 10:00"},
            {"region": "US", "customer_id": 1, "name": "US_C1_v1", "op": "D", "ts": "2025-01-10 10:00"},
            {"region": "US", "customer_id": 1, "name": "US_C1_RESURRECTED", "op": "I", "ts": "2025-01-20 10:00"},

            # Different composite key, not affected
            {"region": "EU", "customer_id": 1, "name": "EU_C1_v1", "op": "I", "ts": "2025-01-01 10:00"},
        ]

        df = pd.DataFrame(records)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["region", "customer_id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values(["region", "customer_id"]).reset_index(drop=True)

        assert len(result_df) == 2

        us_row = result_df[result_df["region"] == "US"].iloc[0]
        assert us_row["name"] == "US_C1_RESURRECTED"

        eu_row = result_df[result_df["region"] == "EU"].iloc[0]
        assert eu_row["name"] == "EU_C1_v1"
