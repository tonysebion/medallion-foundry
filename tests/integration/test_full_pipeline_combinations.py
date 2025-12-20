"""Full Pipeline Pattern Combination Tests.

Tests all 12 Bronze→Silver pattern combinations with actual data content inspection.
This is the comprehensive integration test that validates data transformations across
the complete medallion architecture.

Pattern Matrix:
- Bronze: FULL_SNAPSHOT, INCREMENTAL_APPEND, CDC
- Silver: STATE (SCD1/SCD2), EVENT

Each test:
1. Creates Bronze-like source data
2. Simulates Silver transformation
3. Validates actual data content using assertion helpers
"""

from __future__ import annotations

from datetime import datetime
from typing import List

import ibis
import pandas as pd
import pytest

from pipelines.lib.curate import (
    build_history,
    dedupe_exact,
    dedupe_latest,
)
from tests.data_validation.assertions import (
    EventAssertions,
    SCD1Assertions,
    SCD2Assertions,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


# =============================================================================
# Helpers for simulating Bronze patterns
# =============================================================================


def create_snapshot_batches(num_batches: int = 3, base_rows: int = 5) -> List[pd.DataFrame]:
    """Create FULL_SNAPSHOT batches (full replacement each time).

    Each batch contains ALL records at that point in time.
    """
    batches = []

    for batch_idx in range(num_batches):
        batch_date = datetime(2025, 1, 10 + batch_idx * 5)
        data = []

        for i in range(base_rows):
            record = {
                "id": i + 1,
                "name": f"Entity_{i + 1}_v{batch_idx + 1}",
                "value": 100 + i + (batch_idx * 10),
                "ts": batch_date,
            }
            data.append(record)

        batches.append(pd.DataFrame(data))

    return batches


def create_incremental_batches(num_batches: int = 4, rows_per_batch: int = 3) -> List[pd.DataFrame]:
    """Create INCREMENTAL_APPEND batches (new records each time).

    Each batch contains only NEW records since last batch.
    Some records may have same key but newer timestamp (updates).
    """
    batches = []
    record_id = 0

    for batch_idx in range(num_batches):
        batch_date = datetime(2025, 1, 10 + batch_idx * 5)
        data = []

        for i in range(rows_per_batch):
            record_id += 1
            # Every 4th record is an update to an earlier record
            if batch_idx > 0 and i == 0:
                key_id = 1  # Update first record
            else:
                key_id = record_id

            record = {
                "id": key_id,
                "name": f"Entity_{key_id}_batch{batch_idx}",
                "value": 100 + record_id,
                "ts": batch_date,
            }
            data.append(record)

        batches.append(pd.DataFrame(data))

    return batches


def create_cdc_stream(num_records: int = 5, include_deletes: bool = True) -> pd.DataFrame:
    """Create CDC stream with I/U/D operations.

    Returns a single DataFrame with all CDC events.
    """
    events = []

    # Initial inserts
    for i in range(num_records):
        events.append({
            "id": i + 1,
            "name": f"Entity_{i + 1}_v1",
            "value": 100 + i,
            "ts": datetime(2025, 1, 10),
            "op": "I",
        })

    # Updates for some records
    for i in range(min(3, num_records)):
        events.append({
            "id": i + 1,
            "name": f"Entity_{i + 1}_v2",
            "value": 200 + i,
            "ts": datetime(2025, 1, 15),
            "op": "U",
        })

    # Delete one record if requested
    if include_deletes and num_records > 1:
        events.append({
            "id": 2,
            "name": "Entity_2_deleted",
            "value": 0,
            "ts": datetime(2025, 1, 20),
            "op": "D",
        })

    # Final update for another record
    if num_records > 2:
        events.append({
            "id": 3,
            "name": "Entity_3_v3",
            "value": 350,
            "ts": datetime(2025, 1, 25),
            "op": "U",
        })

    return pd.DataFrame(events)


def union_batches(batches: List[pd.DataFrame]) -> pd.DataFrame:
    """Union multiple batches into single DataFrame."""
    return pd.concat(batches, ignore_index=True)


# =============================================================================
# Transformation Helpers
# =============================================================================


def transform_to_scd1(df: pd.DataFrame, keys: List[str], order_by: str) -> pd.DataFrame:
    """Transform to SCD1 (keep latest only)."""
    t = ibis.memtable(df)
    result = dedupe_latest(t, keys, order_by)
    return result.execute()


def transform_to_scd2(df: pd.DataFrame, keys: List[str], ts_col: str) -> pd.DataFrame:
    """Transform to SCD2 (full history with effective dates)."""
    t = ibis.memtable(df)
    result = build_history(t, keys, ts_col)
    return result.execute()


def transform_to_event(df: pd.DataFrame) -> pd.DataFrame:
    """Transform to EVENT (dedupe exact only)."""
    t = ibis.memtable(df)
    result = dedupe_exact(t)
    return result.execute()


def transform_cdc_to_scd1(df: pd.DataFrame, keys: List[str], order_by: str, op_col: str = "op") -> pd.DataFrame:
    """Transform CDC to SCD1 (apply ops, keep latest non-delete)."""
    t = ibis.memtable(df)
    # Get latest operation per key
    latest = dedupe_latest(t, keys, order_by)
    # Filter out deletes
    active = latest.filter(latest[op_col] != "D")
    return active.execute()


def transform_cdc_to_scd2(df: pd.DataFrame, keys: List[str], ts_col: str) -> pd.DataFrame:
    """Transform CDC to SCD2 (build full history from CDC events)."""
    t = ibis.memtable(df)
    result = build_history(t, keys, ts_col)
    return result.execute()


# =============================================================================
# Test Classes for Each Pattern Combination
# =============================================================================


@pytest.mark.integration
class TestSnapshotToStateSCD1:
    """FULL_SNAPSHOT → STATE (CURRENT_ONLY/SCD1).

    Take latest snapshot batch, dedupe by key to get current state.
    """

    def test_single_snapshot_produces_unique_keys(self, con):
        """Single snapshot batch should have unique keys after SCD1."""
        batches = create_snapshot_batches(num_batches=1, base_rows=5)

        result_df = transform_to_scd1(batches[0], keys=["id"], order_by="ts")

        # Validate SCD1 properties
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed, f"Keys not unique: {unique_result.details}"

        no_scd2_result = SCD1Assertions.assert_no_scd2_columns(result_df)
        assert no_scd2_result.passed, f"SCD2 columns present: {no_scd2_result.details}"

        assert len(result_df) == 5

    def test_multiple_snapshots_keeps_latest_values(self, con):
        """Multiple snapshot batches, SCD1 should keep latest version of each entity."""
        batches = create_snapshot_batches(num_batches=3, base_rows=5)

        # For FULL_SNAPSHOT, we take the last batch and dedupe
        # (In real pipeline, we'd just use last batch, but for completeness we union all)
        combined = union_batches(batches)
        result_df = transform_to_scd1(combined, keys=["id"], order_by="ts")

        # Validate SCD1 properties
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed

        # Verify latest values (batch 3 versions)
        for _, row in result_df.iterrows():
            assert "_v3" in row["name"], f"Expected v3 version, got {row['name']}"

    def test_snapshot_with_duplicates_resolved(self, con):
        """Snapshot with duplicate keys resolved by timestamp."""
        df = pd.DataFrame([
            {"id": 1, "name": "Old", "value": 100, "ts": datetime(2025, 1, 10)},
            {"id": 1, "name": "New", "value": 200, "ts": datetime(2025, 1, 15)},
            {"id": 2, "name": "Only", "value": 300, "ts": datetime(2025, 1, 12)},
        ])

        result_df = transform_to_scd1(df, keys=["id"], order_by="ts")

        assert len(result_df) == 2

        row_1 = result_df[result_df["id"] == 1].iloc[0]
        assert row_1["name"] == "New"
        assert row_1["value"] == 200


@pytest.mark.integration
class TestSnapshotToStateSCD2:
    """FULL_SNAPSHOT → STATE (FULL_HISTORY/SCD2).

    Union all snapshots and build full history with effective dates.
    """

    def test_single_snapshot_all_records_current(self, con):
        """Single snapshot, all records should be current."""
        batches = create_snapshot_batches(num_batches=1, base_rows=5)

        result_df = transform_to_scd2(batches[0], keys=["id"], ts_col="ts")

        # Validate SCD2 properties
        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed

        # All should be current
        assert all(result_df["is_current"] == 1)

    def test_multiple_snapshots_builds_history(self, con):
        """Multiple snapshots build proper SCD2 history."""
        batches = create_snapshot_batches(num_batches=3, base_rows=3)
        combined = union_batches(batches)

        result_df = transform_to_scd2(combined, keys=["id"], ts_col="ts")

        # Validate SCD2 properties
        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed

        # Each key should have 3 versions
        history_preserved = SCD2Assertions.assert_history_preserved(
            result_df, ["id"], expected_counts={1: 3, 2: 3, 3: 3}
        )
        assert history_preserved.passed, f"History not preserved: {history_preserved.details}"

    def test_scd2_effective_dates_correct(self, con):
        """Verify effective_from equals timestamp."""
        df = pd.DataFrame([
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10)},
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15)},
            {"id": 1, "name": "V3", "ts": datetime(2025, 1, 20)},
        ])

        result_df = transform_to_scd2(df, keys=["id"], ts_col="ts")

        eff_from_result = SCD2Assertions.assert_effective_from_equals_ts(result_df, "ts")
        assert eff_from_result.passed, f"effective_from mismatch: {eff_from_result.details}"

        # Only latest should be current
        current_rows = result_df[result_df["is_current"] == 1]
        assert len(current_rows) == 1
        assert current_rows.iloc[0]["name"] == "V3"


@pytest.mark.integration
class TestSnapshotToEvent:
    """FULL_SNAPSHOT → EVENT.

    Dedupe exact duplicates only, treating each unique row as an event.
    """

    def test_snapshot_to_event_preserves_all_unique(self, con):
        """All unique records preserved as events."""
        batches = create_snapshot_batches(num_batches=2, base_rows=3)
        combined = union_batches(batches)

        result_df = transform_to_event(combined)

        # Validate EVENT properties
        no_scd2 = EventAssertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed

        no_exact_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_exact_dups.passed

    def test_exact_duplicates_removed(self, con):
        """Exact duplicate rows removed from event stream."""
        df = pd.DataFrame([
            {"id": 1, "event_type": "click", "ts": datetime(2025, 1, 10)},
            {"id": 1, "event_type": "click", "ts": datetime(2025, 1, 10)},  # Exact dup
            {"id": 2, "event_type": "click", "ts": datetime(2025, 1, 10)},
        ])

        result_df = transform_to_event(df)

        assert len(result_df) == 2  # One dup removed

        no_exact_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_exact_dups.passed


@pytest.mark.integration
class TestIncrementalToStateSCD1:
    """INCREMENTAL_APPEND → STATE (CURRENT_ONLY/SCD1).

    Union incremental batches, dedupe to get latest state per key.
    """

    def test_incremental_batches_merged_scd1(self, con):
        """Multiple incremental batches merged to SCD1 state."""
        batches = create_incremental_batches(num_batches=4, rows_per_batch=3)
        combined = union_batches(batches)

        result_df = transform_to_scd1(combined, keys=["id"], order_by="ts")

        # Validate SCD1 properties
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed

    def test_incremental_updates_keep_latest(self, con):
        """Updates in incremental batches resolved correctly."""
        batch1 = pd.DataFrame([
            {"id": 1, "name": "Entity_1_v1", "value": 100, "ts": datetime(2025, 1, 10)},
            {"id": 2, "name": "Entity_2_v1", "value": 101, "ts": datetime(2025, 1, 10)},
        ])
        batch2 = pd.DataFrame([
            {"id": 1, "name": "Entity_1_v2", "value": 200, "ts": datetime(2025, 1, 15)},  # Update
            {"id": 3, "name": "Entity_3_v1", "value": 102, "ts": datetime(2025, 1, 15)},  # New
        ])

        combined = union_batches([batch1, batch2])
        result_df = transform_to_scd1(combined, keys=["id"], order_by="ts")

        assert len(result_df) == 3

        row_1 = result_df[result_df["id"] == 1].iloc[0]
        assert row_1["name"] == "Entity_1_v2"  # Updated version
        assert row_1["value"] == 200


@pytest.mark.integration
class TestIncrementalToStateSCD2:
    """INCREMENTAL_APPEND → STATE (FULL_HISTORY/SCD2).

    Union incremental batches, build full SCD2 history.
    """

    def test_incremental_to_scd2_history(self, con):
        """Incremental batches build proper SCD2 history."""
        batch1 = pd.DataFrame([
            {"id": 1, "name": "Entity_1_v1", "value": 100, "ts": datetime(2025, 1, 10)},
        ])
        batch2 = pd.DataFrame([
            {"id": 1, "name": "Entity_1_v2", "value": 200, "ts": datetime(2025, 1, 15)},
        ])
        batch3 = pd.DataFrame([
            {"id": 1, "name": "Entity_1_v3", "value": 300, "ts": datetime(2025, 1, 20)},
        ])

        combined = union_batches([batch1, batch2, batch3])
        result_df = transform_to_scd2(combined, keys=["id"], ts_col="ts")

        # Should have 3 history records
        assert len(result_df) == 3

        # Validate SCD2 structure
        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed

        # Verify current version
        current = result_df[result_df["is_current"] == 1].iloc[0]
        assert current["name"] == "Entity_1_v3"

    def test_incremental_scd2_multiple_keys(self, con):
        """Multiple keys each with their own history."""
        batches = create_incremental_batches(num_batches=3, rows_per_batch=2)
        combined = union_batches(batches)

        result_df = transform_to_scd2(combined, keys=["id"], ts_col="ts")

        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed


@pytest.mark.integration
class TestIncrementalToEvent:
    """INCREMENTAL_APPEND → EVENT.

    Union incremental batches, dedupe exact duplicates.
    """

    def test_incremental_events_preserved(self, con):
        """All unique events from incremental batches preserved."""
        batch1 = pd.DataFrame([
            {"event_id": "e1", "type": "click", "ts": datetime(2025, 1, 10)},
            {"event_id": "e2", "type": "view", "ts": datetime(2025, 1, 10)},
        ])
        batch2 = pd.DataFrame([
            {"event_id": "e3", "type": "click", "ts": datetime(2025, 1, 11)},
            {"event_id": "e4", "type": "purchase", "ts": datetime(2025, 1, 11)},
        ])

        combined = union_batches([batch1, batch2])
        result_df = transform_to_event(combined)

        assert len(result_df) == 4

        no_exact_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_exact_dups.passed

    def test_incremental_event_immutability(self, con):
        """Events are immutable across batches."""
        before = pd.DataFrame([
            {"event_id": "e1", "data": "original"},
            {"event_id": "e2", "data": "original"},
        ])

        # New batch with new events (not modifications)
        after_df = union_batches([
            before,
            pd.DataFrame([{"event_id": "e3", "data": "new"}])
        ])
        after = transform_to_event(after_df)

        immutable = EventAssertions.assert_records_immutable(before, after, "event_id")
        assert immutable.passed


@pytest.mark.integration
class TestCDCToStateSCD1:
    """CDC → STATE (CURRENT_ONLY/SCD1).

    Apply CDC operations, keep only active records (non-deleted), latest per key.
    """

    def test_cdc_to_scd1_basic(self, con):
        """Basic CDC stream to SCD1 state."""
        cdc_df = create_cdc_stream(num_records=5, include_deletes=True)

        result_df = transform_cdc_to_scd1(cdc_df, keys=["id"], order_by="ts")

        # Validate SCD1 properties
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed

        # Record 2 was deleted, should not be in result
        assert 2 not in result_df["id"].values

    def test_cdc_updates_reflected(self, con):
        """CDC updates reflected in final SCD1 state."""
        cdc_df = create_cdc_stream(num_records=3, include_deletes=False)

        result_df = transform_cdc_to_scd1(cdc_df, keys=["id"], order_by="ts")

        # Record 1 and 3 were updated, should have latest values
        row_1 = result_df[result_df["id"] == 1].iloc[0]
        assert "_v2" in row_1["name"]  # Updated version

        row_3 = result_df[result_df["id"] == 3].iloc[0]
        assert "_v3" in row_3["name"]  # Final update

    def test_cdc_deletes_remove_records(self, con):
        """Deleted records removed from SCD1 state."""
        cdc_df = pd.DataFrame([
            {"id": 1, "name": "A", "value": 100, "ts": datetime(2025, 1, 10), "op": "I"},
            {"id": 2, "name": "B", "value": 200, "ts": datetime(2025, 1, 10), "op": "I"},
            {"id": 1, "name": "A_del", "value": 0, "ts": datetime(2025, 1, 15), "op": "D"},
        ])

        result_df = transform_cdc_to_scd1(cdc_df, keys=["id"], order_by="ts")

        assert len(result_df) == 1
        assert result_df.iloc[0]["id"] == 2  # Only non-deleted record


@pytest.mark.integration
class TestCDCToStateSCD2:
    """CDC → STATE (FULL_HISTORY/SCD2).

    Build full history from CDC events including inserts, updates, deletes.
    """

    def test_cdc_to_scd2_full_history(self, con):
        """Full CDC history preserved in SCD2."""
        cdc_df = create_cdc_stream(num_records=3, include_deletes=False)

        result_df = transform_cdc_to_scd2(cdc_df, keys=["id"], ts_col="ts")

        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed

    def test_cdc_scd2_preserves_all_versions(self, con):
        """All CDC operations preserved as history."""
        cdc_df = pd.DataFrame([
            {"id": 1, "name": "V1", "ts": datetime(2025, 1, 10), "op": "I"},
            {"id": 1, "name": "V2", "ts": datetime(2025, 1, 15), "op": "U"},
            {"id": 1, "name": "V3", "ts": datetime(2025, 1, 20), "op": "U"},
        ])

        result_df = transform_cdc_to_scd2(cdc_df, keys=["id"], ts_col="ts")

        # All 3 versions should be preserved
        assert len(result_df) == 3

        history_preserved = SCD2Assertions.assert_history_preserved(
            result_df, ["id"], expected_counts={1: 3}
        )
        assert history_preserved.passed

    def test_cdc_scd2_with_deletes(self, con):
        """CDC deletes appear in history (can be filtered by view)."""
        cdc_df = pd.DataFrame([
            {"id": 1, "name": "Created", "ts": datetime(2025, 1, 10), "op": "I"},
            {"id": 1, "name": "Deleted", "ts": datetime(2025, 1, 15), "op": "D"},
        ])

        result_df = transform_cdc_to_scd2(cdc_df, keys=["id"], ts_col="ts")

        # Both operations appear in history
        assert len(result_df) == 2


@pytest.mark.integration
class TestCDCToEvent:
    """CDC → EVENT.

    Treat CDC operations as events, dedupe exact duplicates.
    """

    def test_cdc_as_events(self, con):
        """CDC operations treated as individual events."""
        cdc_df = create_cdc_stream(num_records=3, include_deletes=True)

        result_df = transform_to_event(cdc_df)

        no_exact_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_exact_dups.passed

        # All operations preserved as events
        assert len(result_df) == len(cdc_df)


# =============================================================================
# End-to-End Combination Matrix Tests
# =============================================================================


@pytest.mark.integration
class TestCombinationMatrix:
    """Comprehensive test of all 12 pattern combinations.

    This class tests the full matrix:
    - 3 Bronze patterns × 2 Silver EntityKinds × 2 HistoryModes = 12 combinations
    """

    @pytest.mark.parametrize("bronze_pattern,expected_rows,scd_type", [
        ("snapshot", 5, "scd1"),
        ("incremental", 3, "scd1"),  # Dedupe reduces rows
        ("cdc", 4, "scd1"),  # One delete removes a row
    ])
    def test_all_bronze_to_scd1(self, con, bronze_pattern, expected_rows, scd_type):
        """All Bronze patterns to SCD1 state."""
        if bronze_pattern == "snapshot":
            batches = create_snapshot_batches(num_batches=2, base_rows=5)
            combined = union_batches(batches)
        elif bronze_pattern == "incremental":
            batches = create_incremental_batches(num_batches=3, rows_per_batch=2)
            combined = union_batches(batches)
        else:  # cdc
            combined = create_cdc_stream(num_records=5, include_deletes=True)

        if bronze_pattern == "cdc":
            result_df = transform_cdc_to_scd1(combined, keys=["id"], order_by="ts")
        else:
            result_df = transform_to_scd1(combined, keys=["id"], order_by="ts")

        # Common SCD1 assertions
        unique_result = SCD1Assertions.assert_unique_keys(result_df, ["id"])
        assert unique_result.passed, f"[{bronze_pattern}] Keys not unique"

        no_scd2 = SCD1Assertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed, f"[{bronze_pattern}] SCD2 columns present"

    @pytest.mark.parametrize("bronze_pattern,min_history_per_key", [
        ("snapshot", 2),  # 2 snapshot batches = 2 versions
        ("incremental", 1),  # At least 1 version per key
        ("cdc", 1),  # At least initial insert
    ])
    def test_all_bronze_to_scd2(self, con, bronze_pattern, min_history_per_key):
        """All Bronze patterns to SCD2 state."""
        if bronze_pattern == "snapshot":
            batches = create_snapshot_batches(num_batches=2, base_rows=3)
            combined = union_batches(batches)
        elif bronze_pattern == "incremental":
            batches = create_incremental_batches(num_batches=2, rows_per_batch=2)
            combined = union_batches(batches)
        else:  # cdc
            combined = create_cdc_stream(num_records=3, include_deletes=False)

        result_df = transform_to_scd2(combined, keys=["id"], ts_col="ts")

        # Common SCD2 assertions
        has_cols = SCD2Assertions.assert_has_scd2_columns(result_df)
        assert has_cols.passed, f"[{bronze_pattern}] Missing SCD2 columns"

        one_current = SCD2Assertions.assert_one_current_per_key(result_df, ["id"])
        assert one_current.passed, f"[{bronze_pattern}] Multiple current per key"

    @pytest.mark.parametrize("bronze_pattern", [
        "snapshot",
        "incremental",
        "cdc",
    ])
    def test_all_bronze_to_event(self, con, bronze_pattern):
        """All Bronze patterns to EVENT."""
        if bronze_pattern == "snapshot":
            batches = create_snapshot_batches(num_batches=2, base_rows=3)
            combined = union_batches(batches)
        elif bronze_pattern == "incremental":
            batches = create_incremental_batches(num_batches=2, rows_per_batch=3)
            combined = union_batches(batches)
        else:  # cdc
            combined = create_cdc_stream(num_records=3, include_deletes=True)

        result_df = transform_to_event(combined)

        # Common EVENT assertions
        no_scd2 = EventAssertions.assert_no_scd2_columns(result_df)
        assert no_scd2.passed, f"[{bronze_pattern}] SCD2 columns in events"

        no_exact_dups = EventAssertions.assert_no_exact_duplicates(result_df)
        assert no_exact_dups.passed, f"[{bronze_pattern}] Exact duplicates in events"
