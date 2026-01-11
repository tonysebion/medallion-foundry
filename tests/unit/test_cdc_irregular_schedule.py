"""Tests for CDC with irregular schedules: random skips and backfills.

These tests verify CDC handles real-world scenarios where:
- Batches are skipped for several days
- Skipped batches are backfilled later (potentially multiple at once)
- Some days have no data at all
- Backfills include critical operations (inserts, updates, deletes)
"""

from __future__ import annotations

import pandas as pd
import pytest
import ibis

from pipelines.lib.curate import apply_cdc

# Import the scenario generator
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "integration"))
from irregular_pattern_data import IrregularScheduleScenario


class TestCDCIrregularScheduleBasic:
    """Basic tests for irregular schedule handling."""

    def test_skipped_days_no_data_loss(self):
        """Skipping days then backfilling preserves all data."""
        scenario = IrregularScheduleScenario()

        # Collect all data that would be processed
        all_batches = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_batches.append(batch)

        if not all_batches:
            pytest.skip("No data in scenario")

        combined = pd.concat(all_batches, ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        # Verify expected final state
        expected = scenario.get_expected_final_state()

        # Check all expected IDs present
        result_ids = set(result_df["id"].tolist())
        expected_ids = set(expected.keys())
        assert result_ids == expected_ids, f"IDs mismatch: got {result_ids}, expected {expected_ids}"

    def test_backfill_order_does_not_matter(self):
        """Order of backfilled data doesn't affect final result."""
        scenario = IrregularScheduleScenario()

        # Get logical day data
        day4 = scenario.get_data_for_logical_day(4)
        day5 = scenario.get_data_for_logical_day(5)
        day6 = scenario.get_data_for_logical_day(6)

        # Process in order: 4, 5, 6
        in_order = pd.concat([day4, day5, day6], ignore_index=True)
        table_ordered = ibis.memtable(in_order)
        result_ordered = apply_cdc(
            table_ordered,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas()

        # Process in reverse: 6, 5, 4
        reverse_order = pd.concat([day6, day5, day4], ignore_index=True)
        table_reverse = ibis.memtable(reverse_order)
        result_reverse = apply_cdc(
            table_reverse,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas()

        # Results should be identical
        result_ordered = result_ordered.sort_values("id").reset_index(drop=True)
        result_reverse = result_reverse.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(result_ordered, result_reverse)

    def test_backfill_with_initial_data(self):
        """Backfilling after initial data processed works correctly."""
        scenario = IrregularScheduleScenario()

        # Process days 1-3 first
        initial_batches = []
        for day in range(1, 4):
            batch = scenario.get_data_for_physical_day(day)
            if not batch.empty:
                initial_batches.append(batch)

        initial_combined = pd.concat(initial_batches, ignore_index=True)
        table_initial = ibis.memtable(initial_combined)
        result_initial = apply_cdc(
            table_initial,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas()

        # Should have 5 records (IDs 1-5)
        assert len(result_initial) == 5

        # Now simulate backfill on day 8 (days 4, 5, 6, 8)
        day8_batch = scenario.get_data_for_physical_day(8)

        # Combine with previous result and reprocess
        combined = pd.concat([initial_combined, day8_batch], ignore_index=True)
        table_combined = ibis.memtable(combined)
        result_combined = apply_cdc(
            table_combined,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas()

        # ID 1 should be updated, ID 2 deleted (ignored), ID 6 inserted
        assert 1 in result_combined["id"].values
        assert 6 in result_combined["id"].values
        # ID 2 was deleted on day 5 but with ignore mode it's filtered
        # The original insert on day 1 should remain unless we're doing incremental


class TestCDCIrregularScheduleMultiDayBackfill:
    """Tests for backfilling multiple days at once."""

    def test_three_day_backfill_single_batch(self):
        """Processing 3 skipped days in a single batch."""
        scenario = IrregularScheduleScenario()

        # Get day 8's combined data (days 4, 5, 6, 8)
        day8_batch = scenario.get_data_for_physical_day(8)

        # First need initial data from days 1-3
        initial_batches = []
        for day in range(1, 4):
            batch = scenario.get_data_for_physical_day(day)
            if not batch.empty:
                initial_batches.append(batch)

        all_data = pd.concat(initial_batches + [day8_batch], ignore_index=True)
        table = ibis.memtable(all_data)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        # Verify ID 1 has the updated value from day 4
        id1_row = result_df[result_df["id"] == 1].iloc[0]
        assert id1_row["value"] == 150, "ID 1 should have day 4's update"

        # Verify ID 6 exists (inserted on day 6)
        assert 6 in result_df["id"].values, "ID 6 should be inserted from day 6"

    def test_backfill_with_delete_operation(self):
        """Backfilled batch containing delete operation."""
        scenario = IrregularScheduleScenario()

        # Day 5 has a delete for ID 2
        day5 = scenario.get_data_for_logical_day(5)

        # Need initial insert for ID 2
        initial = pd.DataFrame([{
            "id": 2,
            "name": "Entity_2_v1",
            "value": 200,
            "op": "I",
            "ts": "2025-01-06 10:02:00",
        }])

        combined = pd.concat([initial, day5], ignore_index=True)
        table = ibis.memtable(combined)

        # With tombstone mode, should see _deleted flag
        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="tombstone",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 1
        assert result_df.iloc[0]["_deleted"]

    def test_backfill_delete_followed_by_insert(self):
        """Backfill contains delete, later batch has re-insert for same key."""
        # Day 5: delete ID 2
        # Later: re-insert ID 2
        day5_delete = pd.DataFrame([{
            "id": 2,
            "name": "Entity_2_v1",
            "value": 200,
            "op": "D",
            "ts": "2025-01-10 10:00:00",
        }])

        later_reinsert = pd.DataFrame([{
            "id": 2,
            "name": "Entity_2_REBORN",
            "value": 2500,
            "op": "I",
            "ts": "2025-01-20 10:00:00",
        }])

        # Original insert
        original = pd.DataFrame([{
            "id": 2,
            "name": "Entity_2_v1",
            "value": 200,
            "op": "I",
            "ts": "2025-01-06 10:00:00",
        }])

        combined = pd.concat([original, day5_delete, later_reinsert], ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="tombstone",
        )
        result_df = result.to_pandas()

        # Final state should be the re-insert (active)
        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Entity_2_REBORN"
        assert not result_df.iloc[0]["_deleted"]


class TestCDCIrregularScheduleSkippedDays:
    """Tests for completely skipped days (no processing)."""

    def test_empty_days_handled_gracefully(self):
        """Days with no data don't cause issues."""
        scenario = IrregularScheduleScenario()

        # Process each physical day including empty ones
        all_data = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_data.append(batch)

        combined = pd.concat(all_data, ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        # Should have 6 active records (ID 2 deleted)
        expected = scenario.get_expected_final_state()
        assert len(result_df) == len(expected)

    def test_gap_between_batches_preserves_state(self):
        """A gap between processing runs doesn't lose state."""
        # Simulates: process day 1, skip days 2-10, process day 11
        day1 = pd.DataFrame([
            {"id": 1, "name": "A", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
            {"id": 2, "name": "B", "value": 200, "op": "I", "ts": "2025-01-01 10:01:00"},
        ])

        day11 = pd.DataFrame([
            {"id": 1, "name": "A_updated", "value": 150, "op": "U", "ts": "2025-01-11 10:00:00"},
            {"id": 3, "name": "C", "value": 300, "op": "I", "ts": "2025-01-11 10:01:00"},
        ])

        combined = pd.concat([day1, day11], ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 3
        # ID 1 should have updated values
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["value"] == 150
        # ID 2 unchanged
        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["value"] == 200
        # ID 3 new
        assert 3 in result_df["id"].values


class TestCDCIrregularScheduleLateBackfill:
    """Tests for late backfills (data arrives days/weeks later)."""

    def test_late_backfill_with_updates(self):
        """Late backfill of updates correctly applies to existing records."""
        scenario = IrregularScheduleScenario()

        # Get day 16's data which includes backfill of day 12
        day16_batch = scenario.get_data_for_physical_day(16)

        # Need preceding data
        preceding = []
        for day in [1, 2, 3, 7, 8, 9, 10, 11, 13, 14, 15]:
            batch = scenario.get_data_for_physical_day(day)
            if not batch.empty:
                preceding.append(batch)

        all_data = pd.concat(preceding + [day16_batch], ignore_index=True)
        table = ibis.memtable(all_data)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        # ID 3 should have the day 12 update value
        id3 = result_df[result_df["id"] == 3].iloc[0]
        assert id3["value"] == 350

        # ID 7 should be inserted (from day 12 backfill)
        assert 7 in result_df["id"].values

    def test_backfill_two_weeks_late(self):
        """Data from day 3 arrives on day 17."""
        # Original data from day 1
        day1 = pd.DataFrame([
            {"id": 1, "name": "Rec1", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
        ])

        # Backfilled data from day 3 (arrives on day 17)
        day3_late = pd.DataFrame([
            {"id": 1, "name": "Rec1_updated", "value": 130, "op": "U", "ts": "2025-01-03 10:00:00"},
        ])

        # Data from day 17
        day17 = pd.DataFrame([
            {"id": 2, "name": "Rec2", "value": 200, "op": "I", "ts": "2025-01-17 10:00:00"},
        ])

        # Process in arrival order: day1, then day17+day3_late together
        all_data = pd.concat([day1, day17, day3_late], ignore_index=True)
        table = ibis.memtable(all_data)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        # Despite late arrival, day 17's data for ID 2 should win over day 3's update for ID 1
        # But they're different IDs so both should be present
        assert len(result_df) == 2

        # ID 1 should have day 3's update (ts comparison)
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["value"] == 130


class TestCDCIrregularScheduleDeleteModes:
    """Test irregular schedules with different delete modes."""

    def test_irregular_schedule_tombstone_mode(self):
        """Irregular schedule with tombstone delete mode."""
        scenario = IrregularScheduleScenario()

        all_data = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_data.append(batch)

        combined = pd.concat(all_data, ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="tombstone",
        )
        result_df = result.to_pandas()

        # Should have 7 records (6 active + 1 tombstone for ID 2)
        assert len(result_df) == 7

        # Verify tombstone
        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["_deleted"]

    def test_irregular_schedule_hard_delete_mode(self):
        """Irregular schedule with hard delete mode."""
        scenario = IrregularScheduleScenario()

        all_data = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_data.append(batch)

        combined = pd.concat(all_data, ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="hard_delete",
        )
        result_df = result.to_pandas()

        # Should have 6 records (ID 2 hard deleted)
        assert len(result_df) == 6
        assert 2 not in result_df["id"].values


class TestCDCIrregularScheduleCompositeKeys:
    """Test irregular schedules with composite keys."""

    def test_backfill_with_composite_key(self):
        """Backfill works correctly with composite keys."""
        # Day 1: Initial inserts
        day1 = pd.DataFrame([
            {"region": "US", "id": 1, "name": "A", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
            {"region": "EU", "id": 1, "name": "B", "value": 200, "op": "I", "ts": "2025-01-01 10:01:00"},
        ])

        # Days 2-4 skipped, backfilled on day 5
        day3_backfill = pd.DataFrame([
            {"region": "US", "id": 1, "name": "A_updated", "value": 150, "op": "U", "ts": "2025-01-03 10:00:00"},
        ])

        day4_backfill = pd.DataFrame([
            {"region": "EU", "id": 1, "name": "B_updated", "value": 250, "op": "U", "ts": "2025-01-04 10:00:00"},
        ])

        combined = pd.concat([day1, day3_backfill, day4_backfill], ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["region", "id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 2

        us_row = result_df[result_df["region"] == "US"].iloc[0]
        assert us_row["value"] == 150

        eu_row = result_df[result_df["region"] == "EU"].iloc[0]
        assert eu_row["value"] == 250


class TestCDCIrregularScheduleEdgeCases:
    """Edge cases for irregular schedule handling."""

    def test_only_backfill_no_initial_data(self):
        """Processing only backfilled data (no initial load)."""
        # Simulate starting from day 5 with backfill of days 1-4
        backfill = pd.DataFrame([
            {"id": 1, "name": "A", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
            {"id": 2, "name": "B", "value": 200, "op": "I", "ts": "2025-01-02 10:00:00"},
            {"id": 1, "name": "A_v2", "value": 150, "op": "U", "ts": "2025-01-03 10:00:00"},
            {"id": 3, "name": "C", "value": 300, "op": "I", "ts": "2025-01-04 10:00:00"},
        ])

        table = ibis.memtable(backfill)
        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 3
        # ID 1 should have latest update
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["value"] == 150

    def test_full_week_backfill_at_once(self):
        """Processing a full week of backfilled data at once."""
        records = []
        for day in range(1, 8):
            ts = f"2025-01-{day:02d} 10:00:00"
            records.append({
                "id": 1,
                "name": f"Day{day}_update",
                "value": day * 100,
                "op": "U" if day > 1 else "I",
                "ts": ts,
            })

        df = pd.DataFrame(records)
        table = ibis.memtable(df)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 1
        # Should have day 7's value (latest)
        assert result_df.iloc[0]["value"] == 700

    def test_backfill_interleaved_with_new_data(self):
        """New data arrives mixed with backfilled data."""
        # Day 10: new data
        # Day 10 batch also contains backfill from day 3
        mixed_batch = pd.DataFrame([
            {"id": 1, "name": "Backfill_day3", "value": 130, "op": "U", "ts": "2025-01-03 10:00:00"},
            {"id": 2, "name": "New_day10", "value": 1000, "op": "I", "ts": "2025-01-10 10:00:00"},
            {"id": 1, "name": "New_day10", "value": 1010, "op": "U", "ts": "2025-01-10 10:01:00"},
        ])

        # Need initial data
        initial = pd.DataFrame([
            {"id": 1, "name": "Initial", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
        ])

        combined = pd.concat([initial, mixed_batch], ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        )
        result_df = result.to_pandas()

        assert len(result_df) == 2

        # ID 1 should have day 10's update (latest timestamp)
        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["value"] == 1010

        # ID 2 should be new
        id2 = result_df[result_df["id"] == 2].iloc[0]
        assert id2["value"] == 1000

    def test_backfill_with_delete_then_later_update(self):
        """Backfill contains delete, but newer update for same key exists."""
        # Scenario: backfill day 5 (delete), but day 10 has update
        initial = pd.DataFrame([
            {"id": 1, "name": "Initial", "value": 100, "op": "I", "ts": "2025-01-01 10:00:00"},
        ])

        day5_backfill = pd.DataFrame([
            {"id": 1, "name": "Initial", "value": 100, "op": "D", "ts": "2025-01-05 10:00:00"},
        ])

        day10 = pd.DataFrame([
            {"id": 1, "name": "Resurrected", "value": 999, "op": "I", "ts": "2025-01-10 10:00:00"},
        ])

        combined = pd.concat([initial, day5_backfill, day10], ignore_index=True)
        table = ibis.memtable(combined)

        result = apply_cdc(
            table,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="tombstone",
        )
        result_df = result.to_pandas()

        # Day 10 re-insert should win over day 5 delete
        assert len(result_df) == 1
        assert result_df.iloc[0]["value"] == 999
        assert not result_df.iloc[0]["_deleted"]


class TestCDCIrregularScheduleDeterminism:
    """Verify deterministic behavior with irregular schedules."""

    def test_irregular_schedule_deterministic(self):
        """Same input always produces same output regardless of processing order."""
        scenario = IrregularScheduleScenario()

        # Get all data
        all_data = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_data.append(batch)

        combined = pd.concat(all_data, ignore_index=True)

        # Process 3 times
        results = []
        for _ in range(3):
            table = ibis.memtable(combined)
            result = apply_cdc(
                table,
                keys=["id"],
                order_by="ts",
                delete_mode="ignore",
                cdc_options={"operation_column": "op"},
            )
            results.append(result.to_pandas().sort_values("id").reset_index(drop=True))

        # All results should be identical
        pd.testing.assert_frame_equal(results[0], results[1])
        pd.testing.assert_frame_equal(results[1], results[2])

    def test_shuffled_input_same_result(self):
        """Shuffling input rows doesn't change final result."""
        scenario = IrregularScheduleScenario()

        all_data = []
        for physical_day in range(1, 22):
            batch = scenario.get_data_for_physical_day(physical_day)
            if not batch.empty:
                all_data.append(batch)

        combined = pd.concat(all_data, ignore_index=True)

        # Original order
        table_orig = ibis.memtable(combined)
        result_orig = apply_cdc(
            table_orig,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas().sort_values("id").reset_index(drop=True)

        # Shuffled order
        shuffled = combined.sample(frac=1, random_state=42).reset_index(drop=True)
        table_shuffled = ibis.memtable(shuffled)
        result_shuffled = apply_cdc(
            table_shuffled,
            keys=["id"],
            order_by="ts",
            cdc_options={"operation_column": "op"},
            delete_mode="ignore",
        ).to_pandas().sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(result_orig, result_shuffled)
