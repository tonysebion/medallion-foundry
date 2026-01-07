"""Tests for failure recovery scenarios.

These tests verify pipeline behavior during and after failures:
- Bronze succeeds but Silver fails
- Gap days that need backfilling
- Duplicate/idempotent runs
- Recovery from partial failures
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set

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
from tests.integration.multiweek_data import FailureRecoveryScenario
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


class RecoveryTestDataGenerator:
    """Generate data for recovery testing scenarios."""

    def __init__(self, start_date: date = date(2025, 1, 6)):
        self.start_date = start_date
        self._records: Dict[int, Dict] = {}
        self._next_id = 1

    def generate_day_data(self, day: int, record_count: int = 3) -> pd.DataFrame:
        """Generate data for a specific day."""
        run_date = self.start_date + timedelta(days=day - 1)
        ts = datetime.combine(run_date, datetime.min.time().replace(hour=10))

        records = []

        # Add new records
        for _ in range(record_count):
            rec = self._create_record(ts, day)
            records.append(rec)

        # Add updates to existing records
        existing_ids = list(self._records.keys())
        for rec_id in existing_ids[: min(2, len(existing_ids))]:
            self._records[rec_id]["value"] += day * 10
            self._records[rec_id]["ts"] = ts
            records.append(self._records[rec_id].copy())

        return pd.DataFrame(records)

    def _create_record(self, ts: datetime, day: int) -> Dict:
        """Create a new record."""
        rec_id = self._next_id
        self._next_id += 1

        rec = {
            "id": rec_id,
            "name": f"Record_{rec_id}_Day{day}",
            "value": rec_id * 100,
            "created_day": day,
            "ts": ts,
        }
        self._records[rec_id] = rec.copy()
        return rec

    def get_records_created_on_day(self, day: int) -> List[int]:
        """Get IDs of records created on a specific day."""
        return [
            rec_id
            for rec_id, rec in self._records.items()
            if rec.get("created_day") == day
        ]

    def get_total_record_count(self) -> int:
        """Get total number of unique records."""
        return len(self._records)


def run_bronze(
    df: pd.DataFrame,
    tmp_path: Path,
    bucket: str,
    prefix: str,
    run_date: date,
    day_name: str,
) -> Dict:
    """Run Bronze extraction from DataFrame."""
    csv_path = tmp_path / f"{day_name}.csv"
    df.to_csv(csv_path, index=False)

    bronze = BronzeSource(
        system="recovery",
        entity="test",
        source_type=SourceType.FILE_CSV,
        source_path=str(csv_path),
        target_path=f"s3://{bucket}/{prefix}/bronze/system=recovery/entity=test/dt={{run_date}}/",
        load_pattern=LoadPattern.INCREMENTAL_APPEND,
        watermark_column="ts",
        partition_by=[],
        options=get_storage_options(),
    )
    return bronze.run(run_date.isoformat())


def run_silver(
    bucket: str,
    prefix: str,
    run_date: date,
    source_all_days: bool = True,
) -> Dict:
    """Run Silver curation."""
    if source_all_days:
        source_path = (
            f"s3://{bucket}/{prefix}/bronze/system=recovery/entity=test/dt=*/*.parquet"
        )
    else:
        source_path = f"s3://{bucket}/{prefix}/bronze/system=recovery/entity=test/dt={run_date.isoformat()}/*.parquet"

    silver = SilverEntity(
        source_path=source_path,
        target_path=f"s3://{bucket}/{prefix}/silver/system=recovery/entity=test/dt={{run_date}}/",
        natural_keys=["id"],
        change_timestamp="ts",
        entity_kind=EntityKind.STATE,
        history_mode=HistoryMode.CURRENT_ONLY,
        delete_mode=DeleteMode.IGNORE,
        storage_options=get_storage_options(),
    )
    return silver.run(run_date.isoformat())


def get_silver_data(client, bucket: str, prefix: str, run_date: str) -> pd.DataFrame:
    """Read Silver data from MinIO."""
    silver_prefix = f"{prefix}/silver/system=recovery/entity=test/dt={run_date}/"
    objects = list_objects_in_prefix(client, bucket, silver_prefix)

    parquet_files = [o for o in objects if o.endswith(".parquet")]
    if not parquet_files:
        return pd.DataFrame()

    dfs = []
    for obj_key in parquet_files:
        df = download_parquet_from_minio(client, bucket, obj_key)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def count_bronze_partitions(client, bucket: str, prefix: str) -> int:
    """Count number of Bronze date partitions."""
    bronze_prefix = f"{prefix}/bronze/system=recovery/entity=test/"
    objects = list_objects_in_prefix(client, bucket, bronze_prefix)

    # Extract unique date partitions
    dates = set()
    for obj in objects:
        parts = obj.split("/")
        for part in parts:
            if part.startswith("dt="):
                dates.add(part)
    return len(dates)


class TestSilverFailureBacklog:
    """Test scenarios where Bronze succeeds but Silver fails."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"silver_fail_{uuid.uuid4().hex[:8]}"

    def test_bronze_continues_during_silver_failure(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Bronze should continue running even when Silver 'fails'."""
        gen = RecoveryTestDataGenerator()

        # Days 1-3: Both Bronze and Silver run
        for day in range(1, 4):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")
            run_silver(minio_bucket, prefix, run_date)

        # Days 4-5: Bronze runs, Silver "fails" (we simulate by not running it)
        for day in range(4, 6):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")
            # Silver "fails" - not run

        # Verify Bronze has all 5 days of data
        bronze_dates = count_bronze_partitions(minio_client, minio_bucket, prefix)
        assert bronze_dates == 5, (
            f"Bronze should have 5 date partitions, got {bronze_dates}"
        )

        # Verify Silver only has 3 days
        day3_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-08")
        assert len(day3_df) > 0, "Day 3 Silver should have data"

        day5_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        assert len(day5_df) == 0, (
            "Day 5 Silver should not have data (simulated failure)"
        )

    def test_recovery_processes_backlog(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Recovery run should process all pending Bronze data."""
        gen = RecoveryTestDataGenerator()

        # Days 1-3: Normal operation
        for day in range(1, 4):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")
            run_silver(minio_bucket, prefix, run_date)

        # Days 4-5: Bronze only (Silver "fails")
        for day in range(4, 6):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")

        # Day 6: Recovery - Silver processes all pending data
        day6_data = gen.generate_day_data(6)
        run_date = date(2025, 1, 11)
        run_bronze(day6_data, tmp_path, minio_bucket, prefix, run_date, "day6")

        # Run Silver with source_all_days=True to process backlog
        run_silver(minio_bucket, prefix, run_date, source_all_days=True)

        # Verify recovery processed all data
        recovery_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-11")

        # Should have all unique IDs from all days
        expected_count = gen.get_total_record_count()
        assert len(recovery_df) == expected_count, (
            f"Recovery should have {expected_count} records, got {len(recovery_df)}"
        )

    def test_no_duplicates_after_recovery(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Recovery should not create duplicate records."""
        gen = RecoveryTestDataGenerator()

        # Days 1-2: Normal operation
        for day in range(1, 3):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")
            run_silver(minio_bucket, prefix, run_date)

        # Days 3-4: Bronze only
        for day in range(3, 5):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")

        # Day 5: Recovery
        day5_data = gen.generate_day_data(5)
        run_bronze(day5_data, tmp_path, minio_bucket, prefix, date(2025, 1, 10), "day5")
        run_silver(minio_bucket, prefix, date(2025, 1, 10), source_all_days=True)

        # Verify no duplicates
        recovery_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        assert recovery_df["id"].is_unique, (
            "Should not have duplicate IDs after recovery"
        )


class TestGapDayBackfill:
    """Test backfilling missing days due to outages."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"gap_fill_{uuid.uuid4().hex[:8]}"

    def test_backfill_missing_days(self, minio_client, minio_bucket, prefix, tmp_path):
        """Backfill should correctly process skipped days."""
        gen = RecoveryTestDataGenerator()

        # Day 1: Normal
        day1_data = gen.generate_day_data(1)
        run_bronze(day1_data, tmp_path, minio_bucket, prefix, date(2025, 1, 6), "day1")
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Days 2-4: Outage (no runs)
        # Generate data but don't run pipelines
        outage_data = {}
        for day in range(2, 5):
            outage_data[day] = gen.generate_day_data(day)

        # Day 5: System restored, backfill days 2-4
        for day in range(2, 5):
            run_date = date(2025, 1, 5 + day)
            run_bronze(
                outage_data[day], tmp_path, minio_bucket, prefix, run_date, f"day{day}"
            )

        # Then run day 5
        day5_data = gen.generate_day_data(5)
        run_bronze(day5_data, tmp_path, minio_bucket, prefix, date(2025, 1, 10), "day5")

        # Run Silver to process all backfilled data
        run_silver(minio_bucket, prefix, date(2025, 1, 10), source_all_days=True)

        # Verify all data is present
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        expected_count = gen.get_total_record_count()
        assert len(silver_df) == expected_count, (
            f"Backfill should produce {expected_count} records, got {len(silver_df)}"
        )

    def test_out_of_order_backfill(self, minio_client, minio_bucket, prefix, tmp_path):
        """Out-of-order backfill should still produce correct results."""
        gen = RecoveryTestDataGenerator()

        # Process days out of order: 1, 3, 2, 5, 4
        order = [1, 3, 2, 5, 4]
        day_data = {day: gen.generate_day_data(day) for day in range(1, 6)}

        for day in order:
            run_date = date(2025, 1, 5 + day)
            run_bronze(
                day_data[day], tmp_path, minio_bucket, prefix, run_date, f"day{day}"
            )

        # Run Silver after all Bronze is in place
        run_silver(minio_bucket, prefix, date(2025, 1, 10), source_all_days=True)

        # Verify data integrity
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        assert silver_df["id"].is_unique, (
            "Out-of-order backfill should produce unique records"
        )


class TestIdempotentRuns:
    """Test that duplicate runs don't create duplicates."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"idempotent_{uuid.uuid4().hex[:8]}"

    def test_duplicate_bronze_run_safe(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Running Bronze twice for same day should be safe."""
        gen = RecoveryTestDataGenerator()

        day1_data = gen.generate_day_data(1)
        run_date = date(2025, 1, 6)

        # Run Bronze twice
        run_bronze(
            day1_data, tmp_path, minio_bucket, prefix, run_date, "day1_first"
        )
        run_bronze(
            day1_data, tmp_path, minio_bucket, prefix, run_date, "day1_second"
        )

        # Run Silver
        run_silver(minio_bucket, prefix, run_date)

        # Verify no duplicates
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-06")
        assert silver_df["id"].is_unique, (
            "Duplicate Bronze runs should not create duplicates"
        )

    def test_duplicate_silver_run_idempotent(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Running Silver twice should produce identical results."""
        gen = RecoveryTestDataGenerator()

        # Setup Bronze data
        for day in range(1, 4):
            day_data = gen.generate_day_data(day)
            run_bronze(
                day_data,
                tmp_path,
                minio_bucket,
                prefix,
                date(2025, 1, 5 + day),
                f"day{day}",
            )

        # Run Silver first time
        run_silver(minio_bucket, prefix, date(2025, 1, 8))
        first_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-08")
        first_count = len(first_df)

        # Run Silver second time (same day, same data)
        run_silver(minio_bucket, prefix, date(2025, 1, 8))
        second_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-08")
        second_count = len(second_df)

        assert first_count == second_count, (
            "Duplicate Silver runs should produce same count"
        )
        assert second_df["id"].is_unique, (
            "Duplicate Silver runs should not create duplicates"
        )

    def test_rerun_after_partial_failure(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Re-running after partial failure should complete correctly."""
        gen = RecoveryTestDataGenerator()

        # Day 1: Complete run
        day1_data = gen.generate_day_data(1)
        run_bronze(day1_data, tmp_path, minio_bucket, prefix, date(2025, 1, 6), "day1")
        run_silver(minio_bucket, prefix, date(2025, 1, 6))

        # Day 2: Bronze succeeds, Silver "fails"
        day2_data = gen.generate_day_data(2)
        run_bronze(day2_data, tmp_path, minio_bucket, prefix, date(2025, 1, 7), "day2")
        # Silver fails (not run)

        # Day 2: Retry - re-run both (Bronze already exists)
        run_bronze(
            day2_data, tmp_path, minio_bucket, prefix, date(2025, 1, 7), "day2_retry"
        )
        run_silver(minio_bucket, prefix, date(2025, 1, 7))

        # Verify correct state
        silver_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-07")
        expected_count = gen.get_total_record_count()
        assert len(silver_df) == expected_count, (
            f"Rerun should produce {expected_count} records, got {len(silver_df)}"
        )
        assert silver_df["id"].is_unique, "Rerun should not create duplicates"


class TestWatermarkRecovery:
    """Test watermark handling during recovery scenarios."""

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"watermark_{uuid.uuid4().hex[:8]}"

    def test_watermark_advances_correctly_after_gap(
        self, minio_client, minio_bucket, prefix, tmp_path
    ):
        """Watermark should advance correctly after processing gap days."""
        gen = RecoveryTestDataGenerator()

        # Days 1-2: Normal operation
        for day in range(1, 3):
            day_data = gen.generate_day_data(day)
            run_date = date(2025, 1, 5 + day)
            run_bronze(day_data, tmp_path, minio_bucket, prefix, run_date, f"day{day}")
            run_silver(minio_bucket, prefix, run_date)

        # Get watermark/timestamp from day 2 data
        day2_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-07")
        if "ts" in day2_df.columns:
            day2_max_ts = pd.to_datetime(day2_df["ts"]).max()

        # Days 3-4: Gap (no runs)
        gap_data = {}
        for day in range(3, 5):
            gap_data[day] = gen.generate_day_data(day)

        # Day 5: Process backlog
        for day in range(3, 5):
            run_date = date(2025, 1, 5 + day)
            run_bronze(
                gap_data[day], tmp_path, minio_bucket, prefix, run_date, f"day{day}"
            )

        day5_data = gen.generate_day_data(5)
        run_bronze(day5_data, tmp_path, minio_bucket, prefix, date(2025, 1, 10), "day5")
        run_silver(minio_bucket, prefix, date(2025, 1, 10), source_all_days=True)

        # Verify all data processed
        final_df = get_silver_data(minio_client, minio_bucket, prefix, "2025-01-10")
        assert len(final_df) == gen.get_total_record_count()

        # Verify watermark advanced (latest ts should be from day 5)
        if "ts" in final_df.columns:
            final_max_ts = pd.to_datetime(final_df["ts"]).max()
            assert final_max_ts > day2_max_ts, (
                "Watermark should advance after gap processing"
            )


class TestFailureRecoveryScenarioIntegration:
    """Integration tests using the FailureRecoveryScenario class."""

    @pytest.fixture
    def scenario(self):
        return FailureRecoveryScenario(start_date=date(2025, 1, 6))

    @pytest.fixture
    def prefix(self):
        import uuid

        return f"scenario_{uuid.uuid4().hex[:8]}"

    def test_complete_failure_recovery_scenario(
        self, minio_client, minio_bucket, prefix, scenario, tmp_path
    ):
        """Run the complete failure recovery scenario."""
        gen = RecoveryTestDataGenerator(start_date=scenario.start_date)

        bronze_days_run: Set[int] = set()
        silver_days_run: Set[int] = set()

        # Process days according to scenario
        for day in range(1, 11):
            config = scenario.schedule[day - 1]

            if config.load_type.value == "skip":
                continue

            # Bronze always runs
            day_data = gen.generate_day_data(day)
            run_bronze(
                day_data, tmp_path, minio_bucket, prefix, config.run_date, f"day{day}"
            )
            bronze_days_run.add(day)

            # Silver only runs on allowed days
            if scenario.should_run_silver(day):
                # On recovery day, process all backlog
                if scenario.is_recovery_run(day):
                    run_silver(
                        minio_bucket, prefix, config.run_date, source_all_days=True
                    )
                else:
                    run_silver(minio_bucket, prefix, config.run_date)
                silver_days_run.add(day)

        # Verify Bronze ran for all non-skip days
        assert len(bronze_days_run) >= 8, "Bronze should run most days"

        # Verify Silver skipped failure days but ran recovery
        assert 3 not in silver_days_run, "Day 3 Silver should have 'failed'"
        assert 4 not in silver_days_run, "Day 4 Silver should have 'failed'"
        assert 5 in silver_days_run, "Day 5 Silver recovery should have run"

        # Verify final state is complete
        # Find last Silver run date (from our run, not the whole schedule)
        last_silver_day = max(silver_days_run)
        last_config = scenario.schedule[last_silver_day - 1]
        final_df = get_silver_data(
            minio_client, minio_bucket, prefix, last_config.run_date.isoformat()
        )

        expected_count = gen.get_total_record_count()
        assert len(final_df) == expected_count, (
            f"Final state should have {expected_count} records after recovery, "
            f"got {len(final_df)}"
        )
        assert final_df["id"].is_unique, "No duplicates after recovery"
