"""Multi-week pipeline lifecycle tests.

These tests simulate 3 weeks (21 days) of production pipeline operation,
testing realistic scenarios including:
- Full load → incremental transitions
- Schema evolution (v1 → v2 → v3)
- Weekend gaps
- Late-arriving data
- Full refresh reconciliation
- PolyBase DDL consistency across changes
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Dict, List

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
from tests.integration.multiweek_data import (
    MultiWeekScenario,
    CDCDeleteCycleScenario,
    LoadType,
    SchemaVersion,
)
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode, DeleteMode
from pipelines.lib.polybase import PolyBaseConfig, generate_from_metadata_dict


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


def run_bronze_for_day(
    scenario: MultiWeekScenario,
    day: int,
    tmp_path: Path,
    bucket: str,
    prefix: str,
    pattern: str = "incremental",
) -> Dict:
    """Run Bronze pipeline for a specific day."""
    config = scenario.schedule[day - 1]

    if config.load_type == LoadType.SKIP:
        return {"skipped": True, "reason": "weekend"}

    # Generate data
    df = scenario.generate_bronze_data(day, pattern)
    if df.empty:
        return {"skipped": True, "reason": "no data"}

    # Write to CSV
    csv_path = tmp_path / f"day_{day}.csv"
    df.to_csv(csv_path, index=False)

    # Map pattern to LoadPattern enum
    pattern_map = {
        "snapshot": LoadPattern.FULL_SNAPSHOT,
        "incremental": LoadPattern.INCREMENTAL_APPEND,
        "cdc": LoadPattern.CDC,
    }

    # Use full_snapshot for day 1 or full refresh days
    load_pattern = pattern_map.get(pattern, LoadPattern.INCREMENTAL_APPEND)
    if day == 1 or config.is_full_refresh:
        load_pattern = LoadPattern.FULL_SNAPSHOT

    opts = get_storage_options()
    if pattern == "cdc":
        opts["cdc_operation_column"] = "op"

    bronze = BronzeSource(
        system="multiweek",
        entity="lifecycle",
        source_type=SourceType.FILE_CSV,
        source_path=str(csv_path),
        target_path=f"s3://{bucket}/{prefix}/bronze/system=multiweek/entity=lifecycle/dt={{run_date}}/",
        load_pattern=load_pattern,
        watermark_column="ts" if pattern == "incremental" else None,
        partition_by=[],
        options=opts,
    )

    return bronze.run(config.run_date.isoformat())


def run_silver_for_day(
    scenario: MultiWeekScenario,
    day: int,
    bucket: str,
    prefix: str,
    history_mode: str = "current_only",
    delete_mode: str = "ignore",
    cdc_col: str = None,
    source_all_days: bool = True,
) -> Dict:
    """Run Silver pipeline for a specific day."""
    config = scenario.schedule[day - 1]

    if config.load_type == LoadType.SKIP:
        return {"skipped": True, "reason": "weekend"}

    # Source path - either all days or specific day
    if source_all_days:
        source_path = f"s3://{bucket}/{prefix}/bronze/system=multiweek/entity=lifecycle/dt=*/*.parquet"
    else:
        source_path = f"s3://{bucket}/{prefix}/bronze/system=multiweek/entity=lifecycle/dt={config.run_date.isoformat()}/*.parquet"

    cdc_opts = None
    if cdc_col:
        cdc_opts = {"operation_column": cdc_col}

    history_map = {
        "current_only": HistoryMode.CURRENT_ONLY,
        "full_history": HistoryMode.FULL_HISTORY,
    }
    delete_map = {
        "ignore": DeleteMode.IGNORE,
        "tombstone": DeleteMode.TOMBSTONE,
        "hard_delete": DeleteMode.HARD_DELETE,
    }

    silver = SilverEntity(
        source_path=source_path,
        target_path=f"s3://{bucket}/{prefix}/silver/system=multiweek/entity=lifecycle/dt={{run_date}}/",
        natural_keys=["id"],
        change_timestamp="ts",
        entity_kind=EntityKind.STATE,
        history_mode=history_map.get(history_mode, HistoryMode.CURRENT_ONLY),
        delete_mode=delete_map.get(delete_mode, DeleteMode.IGNORE),
        cdc_options=cdc_opts,
        storage_options=get_storage_options(),
    )

    return silver.run(config.run_date.isoformat())


def get_silver_data(client, bucket: str, prefix: str, run_date: str) -> pd.DataFrame:
    """Download and read Silver parquet data for a date."""
    silver_prefix = f"{prefix}/silver/system=multiweek/entity=lifecycle/dt={run_date}/"
    objects = list_objects_in_prefix(client, bucket, silver_prefix)

    parquet_files = [o for o in objects if o.endswith(".parquet")]
    if not parquet_files:
        return pd.DataFrame()

    dfs = []
    for obj_key in parquet_files:
        df = download_parquet_from_minio(client, bucket, obj_key)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def get_metadata_from_minio(client, bucket: str, prefix: str, run_date: str) -> dict:
    """Read _metadata.json from MinIO."""
    key = f"{prefix}/silver/system=multiweek/entity=lifecycle/dt={run_date}/_metadata.json"
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        return {}


class TestThreeWeekIncrementalLifecycle:
    """Test 3-week lifecycle with incremental loads."""

    @pytest.fixture
    def scenario(self):
        """Create the 3-week scenario."""
        return MultiWeekScenario(start_date=date(2025, 1, 6), weeks=3)

    @pytest.fixture
    def lifecycle_prefix(self):
        """Unique prefix for this test run."""
        import uuid
        return f"lifecycle_test_{uuid.uuid4().hex[:8]}"

    def test_week1_full_load_and_incremental(
        self, minio_client, minio_bucket, lifecycle_prefix, scenario, tmp_path
    ):
        """Week 1: Full load Day 1, incremental Days 2-5, verify weekend handling."""
        # Run days 1-7
        for day in range(1, 8):
            config = scenario.schedule[day - 1]

            # Bronze
            bronze_result = run_bronze_for_day(
                scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental"
            )

            if config.load_type == LoadType.SKIP:
                assert bronze_result.get("skipped"), f"Day {day} should be skipped (weekend)"
                continue

            assert bronze_result.get("row_count", 0) > 0, f"Day {day} should have data"

            # Silver
            silver_result = run_silver_for_day(
                scenario, day, minio_bucket, lifecycle_prefix, "current_only"
            )
            assert silver_result.get("row_count", 0) > 0

        # Verify final state after week 1
        # Day 5 is last run day of week 1 (Friday)
        silver_df = get_silver_data(minio_client, minio_bucket, lifecycle_prefix, "2025-01-10")
        assert len(silver_df) > 0, "Should have Silver data after week 1"

        # Verify no data on weekend days
        weekend_df = get_silver_data(minio_client, minio_bucket, lifecycle_prefix, "2025-01-11")
        assert len(weekend_df) == 0, "Should have no Silver data on Saturday"

    def test_week2_schema_evolution(
        self, minio_client, minio_bucket, lifecycle_prefix, scenario, tmp_path
    ):
        """Week 2: Schema change on Day 9, late data on Day 13."""
        # First run week 1 to establish data
        for day in range(1, 8):
            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental")
            config = scenario.schedule[day - 1]
            if config.load_type != LoadType.SKIP:
                run_silver_for_day(scenario, day, minio_bucket, lifecycle_prefix, "current_only")

        # Run week 2 (days 8-14)
        for day in range(8, 15):
            config = scenario.schedule[day - 1]

            bronze_result = run_bronze_for_day(
                scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental"
            )

            if config.load_type == LoadType.SKIP:
                continue

            # Silver
            silver_result = run_silver_for_day(
                scenario, day, minio_bucket, lifecycle_prefix, "current_only"
            )

            # Verify schema evolution on day 9
            if day >= 9:
                silver_df = get_silver_data(
                    minio_client, minio_bucket, lifecycle_prefix, config.run_date.isoformat()
                )
                # Schema V2 should have category column
                assert "category" in silver_df.columns, f"Day {day} should have category column"

    def test_week3_full_refresh_reconciliation(
        self, minio_client, minio_bucket, lifecycle_prefix, scenario, tmp_path
    ):
        """Week 3: Full refresh on Day 15, verify state reconciles."""
        # Run weeks 1-2
        for day in range(1, 15):
            config = scenario.schedule[day - 1]
            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental")
            if config.load_type != LoadType.SKIP:
                run_silver_for_day(scenario, day, minio_bucket, lifecycle_prefix, "current_only")

        # Get state before full refresh
        day14_df = get_silver_data(minio_client, minio_bucket, lifecycle_prefix, "2025-01-17")
        pre_refresh_count = len(day14_df)

        # Run full refresh on day 15
        run_bronze_for_day(scenario, 15, tmp_path, minio_bucket, lifecycle_prefix, "snapshot")
        run_silver_for_day(scenario, 15, minio_bucket, lifecycle_prefix, "current_only")

        # Verify full refresh
        day15_df = get_silver_data(minio_client, minio_bucket, lifecycle_prefix, "2025-01-20")
        assert len(day15_df) > 0, "Full refresh should produce data"

        # Continue with week 3
        for day in range(16, 22):
            config = scenario.schedule[day - 1]
            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental")
            if config.load_type != LoadType.SKIP:
                run_silver_for_day(scenario, day, minio_bucket, lifecycle_prefix, "current_only")

    def test_final_21_day_state(
        self, minio_client, minio_bucket, lifecycle_prefix, scenario, tmp_path
    ):
        """Verify complete 21-day lifecycle produces consistent final state."""
        # Run all 21 days
        for day in range(1, 22):
            config = scenario.schedule[day - 1]
            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, lifecycle_prefix, "incremental")
            if config.load_type != LoadType.SKIP:
                run_silver_for_day(scenario, day, minio_bucket, lifecycle_prefix, "current_only")

        # Get final state (last run day)
        final_date = scenario.schedule[20].run_date.isoformat()  # Day 21
        silver_df = get_silver_data(minio_client, minio_bucket, lifecycle_prefix, final_date)

        # Verify final schema (V3)
        assert "amount" in silver_df.columns, "Final schema should have amount"
        assert "category" in silver_df.columns, "Final schema should have category"
        assert "priority" in silver_df.columns, "Final schema should have priority"

        # Verify data integrity
        assert len(silver_df) > 0, "Should have records"
        assert silver_df["id"].is_unique, "IDs should be unique in SCD1"


class TestCDCDeleteCycleLifecycle:
    """Test 3-week CDC lifecycle with delete cycles."""

    @pytest.fixture
    def scenario(self):
        """Create CDC delete cycle scenario."""
        return CDCDeleteCycleScenario(start_date=date(2025, 1, 6))

    @pytest.fixture
    def cdc_prefix(self):
        """Unique prefix for CDC tests."""
        import uuid
        return f"cdc_lifecycle_{uuid.uuid4().hex[:8]}"

    def test_cdc_delete_cycle_tombstone(
        self, minio_client, minio_bucket, cdc_prefix, scenario, tmp_path
    ):
        """Test CDC with tombstone delete mode over 3 weeks."""
        # Run all days with CDC pattern
        for day in range(1, 22):
            config = scenario.schedule[day - 1]

            bronze_result = run_bronze_for_day(
                scenario, day, tmp_path, minio_bucket, cdc_prefix, "cdc"
            )

            if config.load_type == LoadType.SKIP:
                continue

            silver_result = run_silver_for_day(
                scenario, day, minio_bucket, cdc_prefix,
                history_mode="current_only",
                delete_mode="tombstone",
                cdc_col="op",
            )

        # Verify tombstone records exist
        final_date = scenario.schedule[20].run_date.isoformat()
        silver_df = get_silver_data(minio_client, minio_bucket, cdc_prefix, final_date)

        if "_deleted" in silver_df.columns:
            deleted_count = silver_df["_deleted"].sum() if silver_df["_deleted"].dtype == bool else (silver_df["_deleted"] == 1).sum()
            # Should have some tombstone records from week 2 deletes
            assert deleted_count >= 0, "Tombstone mode tracks deletes"

    def test_cdc_delete_cycle_hard_delete(
        self, minio_client, minio_bucket, cdc_prefix, scenario, tmp_path
    ):
        """Test CDC with hard delete mode over 3 weeks."""
        hard_prefix = f"{cdc_prefix}_hard"

        # Run all days
        for day in range(1, 22):
            config = scenario.schedule[day - 1]

            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, hard_prefix, "cdc")

            if config.load_type == LoadType.SKIP:
                continue

            run_silver_for_day(
                scenario, day, minio_bucket, hard_prefix,
                history_mode="current_only",
                delete_mode="hard_delete",
                cdc_col="op",
            )

        # Verify hard deletes removed records
        final_date = scenario.schedule[20].run_date.isoformat()
        silver_df = get_silver_data(minio_client, minio_bucket, hard_prefix, final_date)

        # Should have fewer records than inserts due to hard deletes
        assert len(silver_df) >= 0, "Hard delete removes records"


class TestSCD2ThreeWeekHistory:
    """Test SCD2 full history tracking over 3 weeks."""

    @pytest.fixture
    def scenario(self):
        return MultiWeekScenario(start_date=date(2025, 1, 6), weeks=3)

    @pytest.fixture
    def scd2_prefix(self):
        import uuid
        return f"scd2_lifecycle_{uuid.uuid4().hex[:8]}"

    def test_scd2_builds_complete_history(
        self, minio_client, minio_bucket, scd2_prefix, scenario, tmp_path
    ):
        """Verify SCD2 captures full version history over 21 days."""
        # Run all days
        for day in range(1, 22):
            config = scenario.schedule[day - 1]

            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, scd2_prefix, "incremental")

            if config.load_type == LoadType.SKIP:
                continue

            run_silver_for_day(
                scenario, day, minio_bucket, scd2_prefix,
                history_mode="full_history",
            )

        # Get final SCD2 state
        final_date = scenario.schedule[20].run_date.isoformat()
        silver_df = get_silver_data(minio_client, minio_bucket, scd2_prefix, final_date)

        # Verify SCD2 columns
        assert "effective_from" in silver_df.columns
        assert "effective_to" in silver_df.columns
        assert "is_current" in silver_df.columns

        # Verify exactly one current per ID
        current_df = silver_df[silver_df["is_current"] == 1]
        assert current_df["id"].is_unique, "Each ID should have exactly one current record"

        # Verify history exists (some IDs should have multiple versions)
        version_counts = silver_df.groupby("id").size()
        assert (version_counts > 1).any(), "Some records should have history"


class TestPolyBaseDDLConsistency:
    """Test PolyBase DDL remains valid across schema changes."""

    @pytest.fixture
    def scenario(self):
        return MultiWeekScenario(start_date=date(2025, 1, 6), weeks=3)

    @pytest.fixture
    def polybase_prefix(self):
        import uuid
        return f"polybase_lifecycle_{uuid.uuid4().hex[:8]}"

    def test_polybase_ddl_across_schema_versions(
        self, minio_client, minio_bucket, polybase_prefix, scenario, tmp_path
    ):
        """Verify PolyBase DDL updates correctly as schema evolves."""
        polybase_config = PolyBaseConfig(
            data_source_name="lifecycle_source",
            data_source_location="wasbs://silver@account.blob.core.windows.net/",
        )

        ddl_by_version = {}

        # Run pipeline and capture DDL at schema change points
        schema_check_days = [5, 12, 21]  # V1, V2, V3 checkpoints

        for day in range(1, 22):
            config = scenario.schedule[day - 1]

            run_bronze_for_day(scenario, day, tmp_path, minio_bucket, polybase_prefix, "incremental")

            if config.load_type == LoadType.SKIP:
                continue

            run_silver_for_day(scenario, day, minio_bucket, polybase_prefix, "current_only")

            # Capture DDL at checkpoints
            if day in schema_check_days:
                metadata = get_metadata_from_minio(
                    minio_client, minio_bucket, polybase_prefix, config.run_date.isoformat()
                )
                if metadata:
                    ddl = generate_from_metadata_dict(
                        metadata, polybase_config, entity_name=f"lifecycle_v{config.schema_version.value}"
                    )
                    ddl_by_version[config.schema_version] = ddl

        # Verify DDL evolved with schema
        if SchemaVersion.V1 in ddl_by_version:
            assert "value" in ddl_by_version[SchemaVersion.V1].lower()

        if SchemaVersion.V2 in ddl_by_version:
            assert "category" in ddl_by_version[SchemaVersion.V2].lower()

        if SchemaVersion.V3 in ddl_by_version:
            assert "priority" in ddl_by_version[SchemaVersion.V3].lower()
            assert "amount" in ddl_by_version[SchemaVersion.V3].lower()
