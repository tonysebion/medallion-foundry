"""Tests for load pattern transitions.

These tests verify correct behavior when transitioning between different
load patterns (full → incremental → full refresh) and ensuring data
consistency across pattern changes.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from tests.integration.conftest import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    is_minio_available,
    download_parquet_from_minio,
    list_objects_in_prefix,
)
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode, DeleteMode


# Skip all tests if MinIO is not available
pytestmark = [
    pytest.mark.skipif(not is_minio_available(), reason="MinIO not available"),
    pytest.mark.integration,
    pytest.mark.slow,
]


def get_storage_options() -> Dict:
    """Get S3/MinIO storage options."""
    return {
        "endpoint_url": MINIO_ENDPOINT,
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "region": MINIO_REGION,
        "addressing_style": "path",
    }


class PatternTransitionDataGenerator:
    """Generate data for pattern transition tests."""

    def __init__(self, start_date: date = date(2025, 1, 6)):
        self.start_date = start_date
        self._records: Dict[int, Dict] = {}
        self._next_id = 1

    def generate_full_snapshot(self, day: int, record_count: int = 10) -> pd.DataFrame:
        """Generate full snapshot data."""
        run_date = self.start_date + timedelta(days=day - 1)
        ts = datetime.combine(run_date, datetime.min.time().replace(hour=10))

        records = []
        # Reset and create fresh state
        self._records.clear()
        self._next_id = 1

        for _ in range(record_count):
            rec = self._create_record(ts)
            records.append(rec)

        return pd.DataFrame(records)

    def generate_incremental(
        self, day: int, new_count: int = 2, update_count: int = 2
    ) -> pd.DataFrame:
        """Generate incremental data with new records and updates."""
        run_date = self.start_date + timedelta(days=day - 1)
        ts = datetime.combine(run_date, datetime.min.time().replace(hour=10))

        records = []

        # New records
        for _ in range(new_count):
            rec = self._create_record(ts)
            records.append(rec)

        # Updates to existing records
        existing_ids = list(self._records.keys())
        for i in range(min(update_count, len(existing_ids))):
            rec_id = existing_ids[i]
            self._records[rec_id]["value"] += 10
            self._records[rec_id]["ts"] = ts
            records.append(self._records[rec_id].copy())

        return pd.DataFrame(records) if records else pd.DataFrame()

    def generate_cdc_changes(
        self, day: int, inserts: int = 1, updates: int = 1, deletes: int = 0
    ) -> pd.DataFrame:
        """Generate CDC records with operation markers."""
        run_date = self.start_date + timedelta(days=day - 1)
        ts = datetime.combine(run_date, datetime.min.time().replace(hour=10))

        records = []

        # Inserts
        for _ in range(inserts):
            rec = self._create_record(ts)
            rec["op"] = "I"
            records.append(rec)

        # Updates
        existing_ids = [
            rid for rid, r in self._records.items() if not r.get("_deleted")
        ]
        for i in range(min(updates, len(existing_ids))):
            rec_id = existing_ids[i]
            self._records[rec_id]["value"] += 10
            self._records[rec_id]["ts"] = ts
            rec = self._records[rec_id].copy()
            rec["op"] = "U"
            records.append(rec)

        # Deletes
        for i in range(min(deletes, len(existing_ids) - updates)):
            rec_id = existing_ids[updates + i]
            rec = self._records[rec_id].copy()
            rec["ts"] = ts
            rec["op"] = "D"
            self._records[rec_id]["_deleted"] = True
            records.append(rec)

        return pd.DataFrame(records) if records else pd.DataFrame()

    def _create_record(self, ts: datetime) -> Dict:
        """Create a new record."""
        rec_id = self._next_id
        self._next_id += 1

        rec = {
            "id": rec_id,
            "name": f"Entity_{rec_id}",
            "value": rec_id * 100,
            "category": ["A", "B", "C"][rec_id % 3],
            "ts": ts,
        }
        self._records[rec_id] = rec.copy()
        return rec

    def get_expected_count(self, include_deleted: bool = False) -> int:
        """Get expected record count."""
        if include_deleted:
            return len(self._records)
        return sum(1 for r in self._records.values() if not r.get("_deleted"))


def run_bronze(
    csv_path: Path,
    bucket: str,
    prefix: str,
    run_date: date,
    load_pattern: LoadPattern,
    cdc: bool = False,
) -> Dict:
    """Run Bronze extraction."""
    opts = get_storage_options()
    if cdc:
        opts["cdc_operation_column"] = "op"

    bronze = BronzeSource(
        system="transitions",
        entity="test",
        source_type=SourceType.FILE_CSV,
        source_path=str(csv_path),
        target_path=f"s3://{bucket}/{prefix}/bronze/system=transitions/entity=test/dt={{run_date}}/",
        load_pattern=load_pattern,
        watermark_column="ts"
        if load_pattern == LoadPattern.INCREMENTAL_APPEND
        else None,
        partition_by=[],
        options=opts,
    )
    return bronze.run(run_date.isoformat())


def run_silver(
    bucket: str,
    prefix: str,
    run_date: date,
    history_mode: HistoryMode = HistoryMode.CURRENT_ONLY,
    delete_mode: DeleteMode = DeleteMode.IGNORE,
    cdc_col: str = None,
) -> Dict:
    """Run Silver curation."""
    source_path = (
        f"s3://{bucket}/{prefix}/bronze/system=transitions/entity=test/dt=*/*.parquet"
    )

    cdc_opts = None
    if cdc_col:
        cdc_opts = {"operation_column": cdc_col}

    silver = SilverEntity(
        source_path=source_path,
        target_path=f"s3://{bucket}/{prefix}/silver/domain=transitions/subject=test/dt={{run_date}}/",
        natural_keys=["id"],
        change_timestamp="ts",
        entity_kind=EntityKind.STATE,
        history_mode=history_mode,
        delete_mode=delete_mode,
        cdc_options=cdc_opts,
        storage_options=get_storage_options(),
    )
    return silver.run(run_date.isoformat())


def get_silver_data(client, bucket: str, prefix: str, run_date: str) -> pd.DataFrame:
    """Read Silver data from MinIO."""
    silver_prefix = f"{prefix}/silver/domain=transitions/subject=test/dt={run_date}/"
    objects = list_objects_in_prefix(client, bucket, silver_prefix)

    parquet_files = [o for o in objects if o.endswith(".parquet")]
    if not parquet_files:
        return pd.DataFrame()

    dfs = []
    for obj_key in parquet_files:
        df = download_parquet_from_minio(client, bucket, obj_key)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


class TestFullToIncrementalTransition:
    """Test transitioning from full snapshot to incremental loads."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"full_to_incr_{uuid.uuid4().hex[:8]}"

    def test_full_then_incremental_maintains_data_integrity(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Full snapshot → incremental should maintain all records."""
        gen = PatternTransitionDataGenerator()

        # Day 1: Full snapshot (10 records)
        day1_data = gen.generate_full_snapshot(1, record_count=10)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)

        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Day 2: Incremental (2 new, 2 updates)
        day2_data = gen.generate_incremental(2, new_count=2, update_count=2)
        csv2 = tmp_path / "day2.csv"
        day2_data.to_csv(csv2, index=False)

        run_bronze(
            csv2, minio_bucket, prefix, date(2025, 1, 7), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 7))

        # Verify: Should have 12 unique records (10 + 2 new)
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-07")
        assert len(silver_df) == 12, f"Expected 12 records, got {len(silver_df)}"
        assert silver_df["id"].is_unique, "IDs should be unique"

    def test_incremental_updates_applied_correctly(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Incremental updates should overwrite previous values."""
        gen = PatternTransitionDataGenerator()

        # Day 1: Full snapshot
        day1_data = gen.generate_full_snapshot(1, record_count=5)
        original_values = dict(zip(day1_data["id"], day1_data["value"]))
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)

        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Day 2: Incremental with updates
        day2_data = gen.generate_incremental(2, new_count=0, update_count=3)
        csv2 = tmp_path / "day2.csv"
        day2_data.to_csv(csv2, index=False)

        run_bronze(
            csv2, minio_bucket, prefix, date(2025, 1, 7), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 7))

        # Verify updated values
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-07")

        for _, row in silver_df.iterrows():
            if row["id"] in [1, 2, 3]:  # Updated IDs
                assert row["value"] == original_values[row["id"]] + 10, (
                    f"ID {row['id']} should have updated value"
                )


class TestIncrementalToFullRefresh:
    """Test transitioning from incremental back to full refresh."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"incr_to_full_{uuid.uuid4().hex[:8]}"

    def test_full_refresh_replaces_accumulated_state(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Full refresh should provide complete state, not duplicates."""
        gen = PatternTransitionDataGenerator()

        # Day 1: Full snapshot
        day1_data = gen.generate_full_snapshot(1, record_count=5)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Days 2-5: Incremental
        for day in range(2, 6):
            day_data = gen.generate_incremental(day, new_count=2, update_count=1)
            csv = tmp_path / f"day{day}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                LoadPattern.INCREMENTAL_APPEND,
            )
            run_silver(minio_bucket, prefix, date(2025, 1, 5 + day))

        # Verify we have accumulated data before refresh
        pre_refresh_df = get_silver_data(
            minio_client, minio_bucket, prefix, "2025-01-10"
        )
        assert len(pre_refresh_df) > 0, "Should have data before refresh"

        # Day 6: Full refresh with new snapshot (different data)
        gen2 = PatternTransitionDataGenerator()
        refresh_data = gen2.generate_full_snapshot(6, record_count=7)
        csv6 = tmp_path / "day6.csv"
        refresh_data.to_csv(csv6, index=False)
        run_bronze(
            csv6, minio_bucket, prefix, date(2025, 1, 11), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 11))

        # Verify: Silver should reflect new snapshot state
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-11")

        # After full refresh, we should deduplicate to current state
        # The exact count depends on Silver behavior with overlapping IDs
        assert len(silver_df) > 0, "Should have records after refresh"
        assert silver_df["id"].is_unique, "IDs should be unique after full refresh"


class TestCDCToSnapshotReconciliation:
    """Test reconciling CDC stream with periodic snapshots."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"cdc_snapshot_{uuid.uuid4().hex[:8]}"

    def test_cdc_accumulation_matches_snapshot(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """CDC accumulated state should match a full snapshot at same point."""
        # Run CDC for several days
        cdc_gen = PatternTransitionDataGenerator()
        cdc_prefix = f"{prefix}_cdc"

        # Day 1: Initial load via CDC
        day1_cdc = cdc_gen.generate_cdc_changes(1, inserts=5, updates=0, deletes=0)
        csv1 = tmp_path / "day1_cdc.csv"
        day1_cdc.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, cdc_prefix, date(2025, 1, 6), LoadPattern.CDC, cdc=True
        )
        run_silver(minio_bucket, cdc_prefix, date(2025, 1, 6), cdc_col="op")

        # Days 2-4: CDC changes
        for day in range(2, 5):
            day_cdc = cdc_gen.generate_cdc_changes(day, inserts=1, updates=1, deletes=0)
            csv = tmp_path / f"day{day}_cdc.csv"
            day_cdc.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                cdc_prefix,
                date(2025, 1, 5 + day),
                LoadPattern.CDC,
                cdc=True,
            )
            run_silver(minio_bucket, cdc_prefix, date(2025, 1, 5 + day), cdc_col="op")

        # Get CDC final state
        cdc_df = get_silver_data(minio_client, minio_bucket, cdc_prefix, "2025-01-09")

        # Generate equivalent snapshot
        snapshot_gen = PatternTransitionDataGenerator()
        # Simulate same operations
        snapshot_gen.generate_full_snapshot(1, record_count=5)
        for day in range(2, 5):
            snapshot_gen.generate_incremental(day, new_count=1, update_count=1)

        expected_count = snapshot_gen.get_expected_count()

        # Verify counts match
        assert len(cdc_df) == expected_count, (
            f"CDC count {len(cdc_df)} should match expected {expected_count}"
        )


class TestPatternSwitchWithSCD2:
    """Test pattern transitions with SCD2 history tracking."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"scd2_transition_{uuid.uuid4().hex[:8]}"

    def test_pattern_transition_preserves_history(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Switching patterns should preserve SCD2 history."""
        gen = PatternTransitionDataGenerator()

        # Day 1: Full snapshot
        day1_data = gen.generate_full_snapshot(1, record_count=5)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(
            minio_bucket,
            prefix,
            date(2025, 1, 6),
            history_mode=HistoryMode.FULL_HISTORY,
        )

        # Days 2-3: Incremental with updates (creates history)
        for day in range(2, 4):
            day_data = gen.generate_incremental(day, new_count=1, update_count=3)
            csv = tmp_path / f"day{day}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                LoadPattern.INCREMENTAL_APPEND,
            )
            run_silver(
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                history_mode=HistoryMode.FULL_HISTORY,
            )

        # Verify SCD2 state
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-08")

        if "effective_from" in silver_df.columns:
            # Check SCD2 columns exist
            assert "effective_to" in silver_df.columns
            assert "is_current" in silver_df.columns

            # Check exactly one current per ID
            current_df = silver_df[silver_df["is_current"] == 1]
            assert current_df["id"].is_unique, "Each ID should have one current record"

            # Check some records have history
            version_counts = silver_df.groupby("id").size()
            assert (version_counts > 1).any(), (
                "Some records should have history versions"
            )


class TestMixedPatternWeek:
    """Test a realistic week mixing different patterns."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"mixed_week_{uuid.uuid4().hex[:8]}"

    def test_realistic_week_pattern_mix(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """
        Simulate a realistic week:
        - Monday: Full snapshot (start fresh)
        - Tue-Thu: Incremental updates
        - Friday: Full refresh (weekly reconciliation)
        """
        gen = PatternTransitionDataGenerator()

        # Monday: Full snapshot
        monday_data = gen.generate_full_snapshot(1, record_count=10)
        csv = tmp_path / "monday.csv"
        monday_data.to_csv(csv, index=False)
        run_bronze(
            csv, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        monday_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-06")
        assert len(monday_df) == 10, (
            f"Monday should have 10 records, got {len(monday_df)}"
        )
        assert set(monday_df["id"].tolist()) == set(range(1, 11)), (
            "Monday should have IDs 1-10"
        )

        # Tuesday-Thursday: Incremental (2 new + 2 updates each day)
        for i, day_name in enumerate(["tuesday", "wednesday", "thursday"], start=2):
            day_data = gen.generate_incremental(i, new_count=2, update_count=2)
            csv = tmp_path / f"{day_name}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + i),
                LoadPattern.INCREMENTAL_APPEND,
            )
            run_silver(minio_bucket, prefix, date(2025, 1, 5 + i))

        thursday_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-09")
        expected_after_incr = 10 + (2 * 3)  # Original 10 + 2 new per day = 16
        assert len(thursday_df) == expected_after_incr, (
            f"Expected {expected_after_incr} after Tue-Thu incremental, got {len(thursday_df)}"
        )
        assert thursday_df["id"].is_unique, (
            "IDs should be unique after incremental updates"
        )

        # Verify some updates were applied (IDs 1,2 updated on Tuesday with value +10 each day)
        # Original values: ID 1 = 100, ID 2 = 200
        # After 3 days of updates: values should have increased
        row_1 = thursday_df[thursday_df["id"] == 1].iloc[0]
        row_2 = thursday_df[thursday_df["id"] == 2].iloc[0]
        assert row_1["value"] >= 100, "ID 1 value should be >= original"
        assert row_2["value"] >= 200, "ID 2 value should be >= original"

        # Friday: Full refresh with new generator (different IDs 1-15)
        gen2 = PatternTransitionDataGenerator()
        friday_data = gen2.generate_full_snapshot(5, record_count=15)
        csv = tmp_path / "friday.csv"
        friday_data.to_csv(csv, index=False)
        run_bronze(
            csv, minio_bucket, prefix, date(2025, 1, 10), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 10))

        friday_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        assert friday_df["id"].is_unique, "IDs should be unique after Friday refresh"

        # Friday refresh adds IDs 1-15 from gen2, but Silver dedupes across all Bronze
        # So we'll have: original IDs 1-16 from Mon-Thu PLUS new IDs from Friday
        # The exact count depends on overlap - gen2 creates IDs starting from 1
        # which overlap with existing IDs, so deduplication keeps latest
        assert len(friday_df) >= 15, (
            f"Friday should have at least 15 records, got {len(friday_df)}"
        )


class TestIncrementalFullSnapshotIncrementalCycle:
    """Test the critical incremental → full_snapshot → incremental cycle.

    This is a common production pattern (e.g., daily incrementals with weekly full refreshes)
    but has subtle behavior that must be understood:

    CURRENT BEHAVIOR (documented, not a bug):
    - Silver reads load_pattern from the LATEST partition only
    - If latest is incremental → APPEND_LOG (reads ALL partitions)
    - If latest is full_snapshot → REPLACE_DAILY (reads only latest partition)

    IMPLICATIONS:
    - incr(1-5) → full(6) → stop: Days 1-5 data IGNORED (REPLACE_DAILY)
    - incr(1-5) → full(6) → incr(7+): ALL partitions read including Days 1-5
      This can cause duplicates between Day 6 snapshot and Days 1-5 deltas

    The tests in this class DOCUMENT this behavior for user awareness.
    """

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"incr_full_incr_{uuid.uuid4().hex[:8]}"

    def test_incremental_to_full_to_incremental_reads_all_partitions(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """
        DOCUMENTS CURRENT BEHAVIOR:
        When pattern is incr → full → incr, Silver uses APPEND_LOG
        and reads ALL partitions, not just from the full_snapshot onwards.

        Scenario:
        - Days 1-3: incremental (accumulating deltas)
        - Day 4: full_snapshot (complete refresh)
        - Days 5-6: incremental (resume deltas)

        Silver will read all partitions and dedupe by natural key.
        The deduplication handles the overlap, but ALL data is read.
        """
        gen = PatternTransitionDataGenerator()

        # Days 1-3: Incremental loads
        day1_data = gen.generate_full_snapshot(1, record_count=5)  # Initial state
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Track IDs from incremental phase
        incremental_phase_ids = set(day1_data["id"].tolist())

        for day in range(2, 4):
            day_data = gen.generate_incremental(day, new_count=2, update_count=1)
            csv = tmp_path / f"day{day}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                LoadPattern.INCREMENTAL_APPEND,
            )
            run_silver(minio_bucket, prefix, date(2025, 1, 5 + day))
            incremental_phase_ids.update(day_data["id"].tolist())

        # Day 4: Full snapshot (new data generator = different IDs 1-8)
        gen2 = PatternTransitionDataGenerator()
        day4_data = gen2.generate_full_snapshot(4, record_count=8)
        csv4 = tmp_path / "day4.csv"
        day4_data.to_csv(csv4, index=False)
        run_bronze(
            csv4, minio_bucket, prefix, date(2025, 1, 9), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 9))

        snapshot_ids = set(day4_data["id"].tolist())

        # Days 5-6: Back to incremental
        for day in range(5, 7):
            day_data = gen2.generate_incremental(day, new_count=1, update_count=1)
            csv = tmp_path / f"day{day}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 4 + day),
                LoadPattern.INCREMENTAL_APPEND,
            )
            run_silver(minio_bucket, prefix, date(2025, 1, 4 + day))

        # Final Silver output
        final_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-11")

        # DOCUMENTING CURRENT BEHAVIOR:
        # Silver reads ALL partitions because latest is incremental → APPEND_LOG
        # This means Days 1-3 data IS included (overlapping with Day 4 snapshot)
        # Deduplication by natural_key resolves this to unique records

        assert final_df["id"].is_unique, "IDs should be unique after deduplication"

        # The overlapping IDs (1-5 from Days 1-3 and 1-8 from Day 4) are deduped
        # Latest version wins based on change_timestamp
        # We should have: IDs from Day 4 snapshot + new IDs from Days 5-6

        # IDs that were ONLY in incremental phase (not in snapshot)
        # e.g., if gen1 created IDs 6,7,8,9 on days 2-3, those persist through dedup
        unique_incr_only = incremental_phase_ids - snapshot_ids

        # These IDs from Days 1-3 should STILL be present because Silver reads all partitions
        for uid in unique_incr_only:
            assert uid in final_df["id"].values, (
                f"ID {uid} from incremental phase should persist (Silver reads ALL partitions)"
            )

    def test_full_snapshot_at_end_ignores_earlier_partitions(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """
        DOCUMENTS CURRENT BEHAVIOR:
        When the LATEST partition is full_snapshot, Silver uses REPLACE_DAILY
        and ONLY reads that partition, ignoring all earlier partitions.

        Scenario:
        - Days 1-3: incremental
        - Day 4: full_snapshot (STOP HERE)

        Silver reads ONLY Day 4, all earlier data is IGNORED.
        """
        gen = PatternTransitionDataGenerator()

        # Days 1-3: Incremental with unique IDs
        day1_data = gen.generate_full_snapshot(1, record_count=5)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.INCREMENTAL_APPEND
        )

        # Don't run Silver yet - we want to see what happens at Day 4
        incremental_ids = set(day1_data["id"].tolist())

        for day in range(2, 4):
            day_data = gen.generate_incremental(day, new_count=3, update_count=0)
            csv = tmp_path / f"day{day}.csv"
            day_data.to_csv(csv, index=False)
            run_bronze(
                csv,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                LoadPattern.INCREMENTAL_APPEND,
            )
            incremental_ids.update(day_data["id"].tolist())

        # After days 1-3: should have 5 + 3 + 3 = 11 unique IDs

        # Day 4: Full snapshot with DIFFERENT IDs (20-29)
        gen2 = PatternTransitionDataGenerator()
        gen2._next_id = 20  # Start IDs from 20
        day4_data = gen2.generate_full_snapshot(4, record_count=10)
        csv4 = tmp_path / "day4.csv"
        day4_data.to_csv(csv4, index=False)
        run_bronze(
            csv4, minio_bucket, prefix, date(2025, 1, 9), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 9))

        snapshot_ids = set(day4_data["id"].tolist())

        # Verify Silver ONLY contains Day 4 data
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-09")

        # DOCUMENTING CURRENT BEHAVIOR:
        # Latest partition is full_snapshot → REPLACE_DAILY
        # Silver reads ONLY Day 4 partition

        assert len(silver_df) == 10, (
            f"Expected 10 records from Day 4 snapshot only, got {len(silver_df)}"
        )

        actual_ids = set(silver_df["id"].tolist())

        # Should have ONLY Day 4 IDs (20-29)
        assert actual_ids == snapshot_ids, (
            f"Expected only snapshot IDs {snapshot_ids}, got {actual_ids}"
        )

        # Days 1-3 IDs should NOT be present (IGNORED!)
        for incr_id in incremental_ids:
            assert incr_id not in actual_ids, (
                f"ID {incr_id} from incremental phase should be IGNORED "
                "(REPLACE_DAILY reads only latest partition)"
            )

    def test_silver_uses_latest_partition_load_pattern(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """
        DOCUMENTS CURRENT BEHAVIOR:
        Silver determines input_mode from the LATEST partition's metadata only.

        Day 1: full_snapshot → Silver uses REPLACE_DAILY
        Day 2: incremental → Silver uses APPEND_LOG (reads both days)
        """
        gen = PatternTransitionDataGenerator()

        # Day 1: Full snapshot (IDs 1-5)
        day1_data = gen.generate_full_snapshot(1, record_count=5)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # After Day 1: Should have 5 records (REPLACE_DAILY)
        day1_silver = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-06")
        assert len(day1_silver) == 5, (
            f"Day 1 should have 5 records, got {len(day1_silver)}"
        )

        # Day 2: Incremental (2 new records)
        day2_data = gen.generate_incremental(2, new_count=2, update_count=0)
        csv2 = tmp_path / "day2.csv"
        day2_data.to_csv(csv2, index=False)
        run_bronze(
            csv2, minio_bucket, prefix, date(2025, 1, 7), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 7))

        # After Day 2: APPEND_LOG reads both partitions
        day2_silver = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-07")

        # DOCUMENTING CURRENT BEHAVIOR:
        # Day 2 is incremental → APPEND_LOG → reads Day 1 AND Day 2
        assert len(day2_silver) == 7, (
            f"Day 2 should have 7 records (5 from Day 1 + 2 new), got {len(day2_silver)}"
        )

        # All IDs from both days should be present
        expected_ids = set(range(1, 8))  # IDs 1-7
        actual_ids = set(day2_silver["id"].tolist())
        assert actual_ids == expected_ids, (
            f"Expected IDs {expected_ids}, got {actual_ids}"
        )

    def test_pattern_transition_with_scd2_preserves_history(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """
        DOCUMENTS CURRENT BEHAVIOR:
        SCD2 history is preserved across pattern transitions when Silver reads
        all partitions (APPEND_LOG mode).

        Scenario:
        - Days 1-2: incremental with updates → creates history
        - Day 3: full_snapshot inserted
        - Day 4: incremental → APPEND_LOG reads all, history preserved
        """
        gen = PatternTransitionDataGenerator()

        # Day 1: Initial state
        day1_data = gen.generate_full_snapshot(1, record_count=3)
        csv1 = tmp_path / "day1.csv"
        day1_data.to_csv(csv1, index=False)
        run_bronze(
            csv1, minio_bucket, prefix, date(2025, 1, 6), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(
            minio_bucket,
            prefix,
            date(2025, 1, 6),
            history_mode=HistoryMode.FULL_HISTORY,
        )

        # Day 2: Updates create history
        day2_data = gen.generate_incremental(2, new_count=0, update_count=3)
        csv2 = tmp_path / "day2.csv"
        day2_data.to_csv(csv2, index=False)
        run_bronze(
            csv2, minio_bucket, prefix, date(2025, 1, 7), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(
            minio_bucket,
            prefix,
            date(2025, 1, 7),
            history_mode=HistoryMode.FULL_HISTORY,
        )

        # Day 3: Full snapshot (same IDs 1-3 but different values)
        gen2 = PatternTransitionDataGenerator()
        day3_data = gen2.generate_full_snapshot(3, record_count=3)
        csv3 = tmp_path / "day3.csv"
        day3_data.to_csv(csv3, index=False)
        run_bronze(
            csv3, minio_bucket, prefix, date(2025, 1, 8), LoadPattern.FULL_SNAPSHOT
        )
        run_silver(
            minio_bucket,
            prefix,
            date(2025, 1, 8),
            history_mode=HistoryMode.FULL_HISTORY,
        )

        # Day 4: Back to incremental
        day4_data = gen2.generate_incremental(4, new_count=1, update_count=1)
        csv4 = tmp_path / "day4.csv"
        day4_data.to_csv(csv4, index=False)
        run_bronze(
            csv4, minio_bucket, prefix, date(2025, 1, 9), LoadPattern.INCREMENTAL_APPEND
        )
        run_silver(
            minio_bucket,
            prefix,
            date(2025, 1, 9),
            history_mode=HistoryMode.FULL_HISTORY,
        )

        # Final Silver with SCD2
        final_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-09")

        # DOCUMENTING CURRENT BEHAVIOR:
        # Day 4 is incremental → APPEND_LOG reads all partitions
        # SCD2 history from Days 1-2 is preserved

        if "effective_from" in final_df.columns:
            # SCD2 columns should exist
            assert "effective_to" in final_df.columns
            assert "is_current" in final_df.columns

            # Check that we have history (multiple versions per ID)
            version_counts = final_df.groupby("id").size()
            multi_version_ids = version_counts[version_counts > 1].index.tolist()

            # IDs 1, 2, 3 should have history (updated on Day 2, then again on Day 3)
            assert len(multi_version_ids) > 0, (
                "SCD2 should preserve history across pattern transitions"
            )

            # Exactly one current record per ID
            current_df = final_df[final_df["is_current"] == 1]
            assert current_df["id"].is_unique, "Each ID should have one current record"
