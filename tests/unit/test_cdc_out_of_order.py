"""Tests for CDC processing when batches arrive out of order.

Tests that CDC correctly handles scenarios where data arrives in
non-chronological order, which is common in distributed systems:
- Network delays cause later batches to arrive first
- Retry logic re-sends older batches
- Multiple workers process different time ranges

The key requirement is that the final state should be correct
regardless of arrival order - only logical timestamps matter.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import pandas as pd
import ibis

from pipelines.lib.curate import apply_cdc


class TestCDCOutOfOrderBasic:
    """Basic out-of-order batch arrival tests."""

    def test_later_batch_arrives_first(self):
        """Day 2 batch arrives before Day 1 - final state correct.

        Processing order: Day 2, then Day 1
        Logical order: Day 1 (I), Day 2 (U)
        Result: Day 2's update should win (it's logically later)
        """
        con = ibis.duckdb.connect()

        # Simulate: Day 2 batch arrives first, then Day 1
        batch_day2 = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1_v2"],
            "value": [150],
            "op": ["U"],
            "ts": ["2025-01-11 10:00:00"],  # Day 2
        })
        batch_day1 = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1_v1"],
            "value": [100],
            "op": ["I"],
            "ts": ["2025-01-10 10:00:00"],  # Day 1
        })

        # Union in arrival order (Day 2 first)
        combined = pd.concat([batch_day2, batch_day1], ignore_index=True)
        t = con.create_table("test", combined)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Day 2's update should win regardless of arrival order
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Entity_1_v2"
        assert result_df.iloc[0]["value"] == 150

    def test_delete_arrives_before_insert(self):
        """Delete batch arrives before insert batch.

        Processing order: Day 2 (D), then Day 1 (I)
        Result: Delete wins (logically later)
        """
        con = ibis.duckdb.connect()

        batch_day2_delete = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1"],
            "op": ["D"],
            "ts": ["2025-01-11 10:00:00"],
        })
        batch_day1_insert = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1"],
            "op": ["I"],
            "ts": ["2025-01-10 10:00:00"],
        })

        combined = pd.concat([batch_day2_delete, batch_day1_insert], ignore_index=True)
        t = con.create_table("test", combined)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Delete (Day 2) wins, record should be filtered out
        assert len(result_df) == 0

    def test_reinsert_arrives_before_delete(self):
        """Re-insert (Day 3) arrives before delete (Day 2).

        Processing order: Day 3 (I), Day 2 (D), Day 1 (I)
        Logical order: Day 1 (I) -> Day 2 (D) -> Day 3 (I)
        Result: Final re-insert wins
        """
        con = ibis.duckdb.connect()

        batch_day3_reinsert = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1_RESURRECTED"],
            "value": [999],
            "op": ["I"],
            "ts": ["2025-01-12 10:00:00"],
        })
        batch_day2_delete = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1_v1"],
            "value": [100],
            "op": ["D"],
            "ts": ["2025-01-11 10:00:00"],
        })
        batch_day1_insert = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1_v1"],
            "value": [100],
            "op": ["I"],
            "ts": ["2025-01-10 10:00:00"],
        })

        # Arrival order: Day 3, Day 2, Day 1
        combined = pd.concat(
            [batch_day3_reinsert, batch_day2_delete, batch_day1_insert],
            ignore_index=True
        )
        t = con.create_table("test", combined)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Day 3 re-insert wins
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Entity_1_RESURRECTED"
        assert result_df.iloc[0]["value"] == 999


class TestCDCOutOfOrderMultipleKeys:
    """Out-of-order tests with multiple keys."""

    def test_different_keys_different_arrival_order(self):
        """Multiple keys arrive with different timing patterns.

        Key 1: Day 2 first, Day 1 second
        Key 2: Day 1 first, Day 2 second (normal)
        Key 3: Day 3, Day 1, Day 2 (random)
        """
        con = ibis.duckdb.connect()

        # Simulate chaotic arrival
        records = [
            # Key 1: Day 2 arrives first
            {"id": 1, "name": "K1_v2", "op": "U", "ts": "2025-01-11 10:00"},
            # Key 2: Day 1 arrives first (normal)
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-10 10:00"},
            # Key 3: Day 3 arrives first
            {"id": 3, "name": "K3_v3", "op": "U", "ts": "2025-01-12 10:00"},
            # Key 1: Day 1 arrives second
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},
            # Key 2: Day 2 arrives second
            {"id": 2, "name": "K2_v2", "op": "U", "ts": "2025-01-11 10:00"},
            # Key 3: Day 1 arrives second
            {"id": 3, "name": "K3_v1", "op": "I", "ts": "2025-01-10 10:00"},
            # Key 3: Day 2 arrives third
            {"id": 3, "name": "K3_v2", "op": "U", "ts": "2025-01-11 10:00"},
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

        # All keys should have their logically-latest values
        assert len(result_df) == 3

        k1 = result_df[result_df["id"] == 1].iloc[0]
        assert k1["name"] == "K1_v2"  # Day 2 is logically later

        k2 = result_df[result_df["id"] == 2].iloc[0]
        assert k2["name"] == "K2_v2"  # Day 2

        k3 = result_df[result_df["id"] == 3].iloc[0]
        assert k3["name"] == "K3_v3"  # Day 3

    def test_interleaved_deletes_out_of_order(self):
        """Deletes for different keys arrive in wrong order.

        Key 1: I (Day 1) -> D (Day 3)
        Key 2: I (Day 1) -> D (Day 2)
        Arrival: Key 1's D, Key 2's D, Key 1's I, Key 2's I
        """
        con = ibis.duckdb.connect()

        records = [
            # Deletes arrive first
            {"id": 1, "name": "K1", "op": "D", "ts": "2025-01-12 10:00"},  # Key 1 delete (Day 3)
            {"id": 2, "name": "K2", "op": "D", "ts": "2025-01-11 10:00"},  # Key 2 delete (Day 2)
            # Inserts arrive second
            {"id": 1, "name": "K1", "op": "I", "ts": "2025-01-10 10:00"},  # Key 1 insert (Day 1)
            {"id": 2, "name": "K2", "op": "I", "ts": "2025-01-10 10:00"},  # Key 2 insert (Day 1)
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

        # Both keys deleted (their deletes are logically later)
        assert len(result_df) == 0


class TestCDCOutOfOrderTombstone:
    """Out-of-order tests with tombstone delete mode."""

    def test_delete_before_insert_tombstone_mode(self):
        """Delete arrives before insert with tombstone mode."""
        con = ibis.duckdb.connect()

        # Delete arrives first
        batch_delete = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1"],
            "op": ["D"],
            "ts": ["2025-01-11 10:00:00"],
        })
        batch_insert = pd.DataFrame({
            "id": [1],
            "name": ["Entity_1"],
            "op": ["I"],
            "ts": ["2025-01-10 10:00:00"],
        })

        combined = pd.concat([batch_delete, batch_insert], ignore_index=True)
        t = con.create_table("test", combined)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="tombstone",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute()

        # Record exists as tombstone
        assert len(result_df) == 1
        assert bool(result_df.iloc[0]["_deleted"]) is True

    def test_resurrection_clears_tombstone_out_of_order(self):
        """Re-insert arrives before delete clears tombstone."""
        con = ibis.duckdb.connect()

        # Arrival order: Day 3 (I), Day 2 (D), Day 1 (I)
        records = [
            {"id": 1, "name": "Resurrected", "op": "I", "ts": "2025-01-12 10:00"},
            {"id": 1, "name": "Original", "op": "D", "ts": "2025-01-11 10:00"},
            {"id": 1, "name": "Original", "op": "I", "ts": "2025-01-10 10:00"},
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

        # Final state is resurrected (not deleted)
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Resurrected"
        assert bool(result_df.iloc[0]["_deleted"]) is False


class TestCDCOutOfOrderComplexScenarios:
    """Complex out-of-order scenarios."""

    def test_week_of_data_arrives_backwards(self):
        """7 days of data arrive in reverse order (Day 7 first, Day 1 last)."""
        con = ibis.duckdb.connect()

        records = []
        base_date = datetime(2025, 1, 10, 10, 0, 0)

        # Generate data for 7 days
        for day in range(7, 0, -1):  # Reverse order: 7, 6, 5, 4, 3, 2, 1
            ts = base_date + timedelta(days=day - 1)
            if day == 1:
                records.append({
                    "id": 1, "name": f"Entity_1_day{day}", "value": day * 100,
                    "op": "I", "ts": ts.isoformat()
                })
            else:
                records.append({
                    "id": 1, "name": f"Entity_1_day{day}", "value": day * 100,
                    "op": "U", "ts": ts.isoformat()
                })

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

        # Day 7 is logically latest, should win
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Entity_1_day7"
        assert result_df.iloc[0]["value"] == 700

    def test_random_order_10_batches(self):
        """10 batches arrive in random order - result deterministic."""
        con = ibis.duckdb.connect()

        import random
        random.seed(42)

        # Create 10 updates for the same key
        base_date = datetime(2025, 1, 10, 10, 0, 0)
        records = []
        day_order = list(range(1, 11))
        random.shuffle(day_order)  # Randomize arrival order

        for i, day in enumerate(day_order):
            ts = base_date + timedelta(days=day - 1)
            op = "I" if day == 1 else "U"
            records.append({
                "id": 1,
                "name": f"Version_{day}",
                "value": day * 100,
                "op": op,
                "ts": ts.isoformat(),
            })

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

        # Day 10 is logically latest
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Version_10"
        assert result_df.iloc[0]["value"] == 1000

    def test_multiple_keys_random_order_with_deletes(self):
        """Multiple keys, random arrival order, with deletes scattered throughout."""
        con = ibis.duckdb.connect()

        import random
        random.seed(123)

        # Define logical timeline
        # Key 1: I (D1) -> U (D3) -> U (D5)
        # Key 2: I (D1) -> D (D4)
        # Key 3: I (D2) -> U (D3) -> D (D5) -> I (D6)  # Resurrection

        all_events = [
            {"id": 1, "name": "K1_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "K1_v2", "op": "U", "ts": "2025-01-12 10:00"},
            {"id": 1, "name": "K1_v3", "op": "U", "ts": "2025-01-14 10:00"},
            {"id": 2, "name": "K2_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "K2_v1", "op": "D", "ts": "2025-01-13 10:00"},
            {"id": 3, "name": "K3_v1", "op": "I", "ts": "2025-01-11 10:00"},
            {"id": 3, "name": "K3_v2", "op": "U", "ts": "2025-01-12 10:00"},
            {"id": 3, "name": "K3_v2", "op": "D", "ts": "2025-01-14 10:00"},
            {"id": 3, "name": "K3_RESURRECTED", "op": "I", "ts": "2025-01-15 10:00"},
        ]

        # Shuffle to simulate random arrival
        random.shuffle(all_events)

        df = pd.DataFrame(all_events)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        # Key 1: Active with v3
        # Key 2: Deleted
        # Key 3: Resurrected
        assert len(result_df) == 2  # Keys 1 and 3 only

        k1 = result_df[result_df["id"] == 1].iloc[0]
        assert k1["name"] == "K1_v3"

        k3 = result_df[result_df["id"] == 3].iloc[0]
        assert k3["name"] == "K3_RESURRECTED"


class TestCDCOutOfOrderDeterminism:
    """Tests verifying deterministic results regardless of arrival order."""

    def test_same_data_different_order_same_result(self):
        """Same data processed in different orders produces identical results."""
        con = ibis.duckdb.connect()

        # The logical data
        events = [
            {"id": 1, "name": "A_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 1, "name": "A_v2", "op": "U", "ts": "2025-01-11 10:00"},
            {"id": 2, "name": "B_v1", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "B_v1", "op": "D", "ts": "2025-01-11 10:00"},
            {"id": 3, "name": "C_v1", "op": "I", "ts": "2025-01-10 10:00"},
        ]

        import random

        results = []
        for seed in [1, 42, 99, 123, 456]:
            random.seed(seed)
            shuffled = events.copy()
            random.shuffle(shuffled)

            df = pd.DataFrame(shuffled)
            t = con.create_table(f"test_{seed}", df)

            result = apply_cdc(
                t,
                keys=["id"],
                order_by="ts",
                delete_mode="ignore",
                cdc_options={"operation_column": "op"},
            )
            result_df = result.execute().sort_values("id").reset_index(drop=True)
            results.append(result_df)

        # All results should be identical
        for i in range(1, len(results)):
            pd.testing.assert_frame_equal(
                results[0][["id", "name"]],
                results[i][["id", "name"]],
                check_like=True,
            )

    def test_idempotent_with_duplicates_out_of_order(self):
        """Processing same batch multiple times in different positions is idempotent."""
        con = ibis.duckdb.connect()

        # Original batch
        original = [
            {"id": 1, "name": "A", "op": "I", "ts": "2025-01-10 10:00"},
            {"id": 2, "name": "B", "op": "I", "ts": "2025-01-10 10:00"},
        ]

        # Same batch "arrives" multiple times in different positions
        later_batch = [
            {"id": 1, "name": "A_v2", "op": "U", "ts": "2025-01-11 10:00"},
        ]

        # Simulate: original, original again, later, original again
        combined = original + original + later_batch + original

        df = pd.DataFrame(combined)
        t = con.create_table("test", df)

        result = apply_cdc(
            t,
            keys=["id"],
            order_by="ts",
            delete_mode="ignore",
            cdc_options={"operation_column": "op"},
        )
        result_df = result.execute().sort_values("id").reset_index(drop=True)

        # Should have 2 records with correct latest values
        assert len(result_df) == 2
        assert result_df[result_df["id"] == 1].iloc[0]["name"] == "A_v2"
        assert result_df[result_df["id"] == 2].iloc[0]["name"] == "B"
