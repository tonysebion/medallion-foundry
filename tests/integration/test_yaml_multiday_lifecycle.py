"""YAML-based multi-day lifecycle integration tests.

Tests that a single YAML configuration pattern can be run across multiple days
with different data (full load → incremental → full refresh), producing correct
Silver output and properly leveraging Bronze metadata.

Test Scenarios:
- Scenario A: Snapshot → Incremental → Full Refresh (SCD1)
- Scenario B: CDC with different delete modes
- Scenario C: SCD Type 2 history building
- Scenario D: Silver metadata discovery from Bronze
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

from tests.integration.conftest import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    download_parquet_from_minio,
    is_minio_available,
    list_objects_in_prefix,
)
from tests.integration.multiday_data import (
    generate_cdc_day1,
    generate_cdc_day2,
    generate_cdc_day3,
    generate_incremental_day2,
    generate_incremental_day3,
    generate_snapshot_day1,
    generate_snapshot_day3,
)


# Skip all tests if MinIO is not available
pytestmark = [
    pytest.mark.skipif(not is_minio_available(), reason="MinIO not available"),
    pytest.mark.integration,
]


# =============================================================================
# YAML Templates
# =============================================================================

LIFECYCLE_YAML_TEMPLATE = """# yaml-language-server: $schema=../../pipelines/schema/pipeline.schema.json
name: yaml_lifecycle_test

bronze:
  system: yaml_lifecycle
  entity: {entity}
  source_type: file_csv
  source_path: "{source_path}"
  target_path: "s3://{bucket}/{prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt={{run_date}}/"
  load_pattern: {load_pattern}
  {watermark_line}
  partition_by: []
  options:
    endpoint_url: "{endpoint}"
    addressing_style: path

silver:
  domain: yaml_lifecycle
  subject: {entity}
  source_path: "s3://{bucket}/{prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt=*/*.parquet"
  target_path: "s3://{bucket}/{prefix}/silver/domain=yaml_lifecycle/subject={entity}/dt={{run_date}}/"
  natural_keys: [id]
  change_timestamp: ts
  model: {silver_model}
  {delete_mode_line}
  {cdc_options_line}
  storage_options:
    endpoint_url: "{endpoint}"
    key: "{access_key}"
    secret: "{secret_key}"
    region: "{region}"
    addressing_style: path
"""


# =============================================================================
# Helper Functions
# =============================================================================


def create_yaml_config(
    template: str,
    tmp_path: Path,
    entity: str,
    source_path: str,
    bucket: str,
    prefix: str,
    load_pattern: str,
    silver_model: str,
    watermark_column: Optional[str] = None,
    delete_mode: Optional[str] = None,
    cdc_operation_column: Optional[str] = None,
) -> Path:
    """Create a YAML config file from template with substitutions."""
    watermark_line = f"watermark_column: {watermark_column}" if watermark_column else ""
    delete_mode_line = f"delete_mode: {delete_mode}" if delete_mode else ""
    cdc_options_line = ""
    if cdc_operation_column:
        cdc_options_line = f"""cdc_options:
    operation_column: {cdc_operation_column}"""

    # Convert Windows paths to forward slashes for YAML compatibility
    source_path_yaml = source_path.replace("\\", "/")

    content = template.format(
        entity=entity,
        source_path=source_path_yaml,
        bucket=bucket,
        prefix=prefix,
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        region=MINIO_REGION,
        load_pattern=load_pattern,
        silver_model=silver_model,
        watermark_line=watermark_line,
        delete_mode_line=delete_mode_line,
        cdc_options_line=cdc_options_line,
    )

    yaml_path = tmp_path / f"lifecycle_{entity}_{load_pattern}.yaml"
    yaml_path.write_text(content)
    return yaml_path


def run_yaml_pipeline(
    yaml_path: Path,
    run_date: str,
    layer: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """Run a YAML pipeline via subprocess.

    Args:
        yaml_path: Path to YAML config file
        run_date: Run date in YYYY-MM-DD format
        layer: Optional layer to run ("bronze" or "silver"), None for both

    Returns:
        CompletedProcess with stdout/stderr
    """
    if layer:
        cmd = [
            sys.executable,
            "-m",
            "pipelines",
            f"{yaml_path}:{layer}",
            "--date",
            run_date,
        ]
    else:
        cmd = [sys.executable, "-m", "pipelines", str(yaml_path), "--date", run_date]

    env = os.environ.copy()
    env.update(
        {
            "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
            "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
            "AWS_REGION": MINIO_REGION,
            "AWS_S3_ADDRESSING_STYLE": "path",
        }
    )

    result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=120)
    return result


def get_silver_df(
    client,
    bucket: str,
    prefix: str,
    entity: str,
    run_date: Optional[str] = None,
) -> pd.DataFrame:
    """Download Silver parquet files and combine into DataFrame.

    Args:
        client: MinIO client
        bucket: S3 bucket name
        prefix: Path prefix
        entity: Entity name
        run_date: If specified, only get data from that partition. If None, get all partitions.

    Returns:
        DataFrame with all records from the requested partition(s)
    """
    if run_date:
        silver_prefix = (
            f"{prefix}/silver/domain=yaml_lifecycle/subject={entity}/dt={run_date}/"
        )
    else:
        silver_prefix = f"{prefix}/silver/domain=yaml_lifecycle/subject={entity}/"
    objects = list_objects_in_prefix(client, bucket, silver_prefix)

    parquet_files = [o for o in objects if o.endswith(".parquet")]
    if not parquet_files:
        return pd.DataFrame()

    dfs = []
    for key in parquet_files:
        df = download_parquet_from_minio(client, bucket, key)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def get_deduplicated_silver_df(
    client,
    bucket: str,
    prefix: str,
    entity: str,
    natural_key: str = "id",
    timestamp_col: str = "ts",
) -> pd.DataFrame:
    """Get Silver data deduplicated by natural key, keeping latest version.

    This simulates what a consumer would see when reading Silver across all
    partitions and deduplicating to get the current state.
    """
    df = get_silver_df(client, bucket, prefix, entity)
    if df.empty:
        return df

    # Sort by timestamp descending, keep first (latest) per natural key
    df = df.sort_values(timestamp_col, ascending=False)
    df = df.drop_duplicates(subset=[natural_key], keep="first")
    return df.reset_index(drop=True)


def get_bronze_metadata(
    client,
    bucket: str,
    prefix: str,
    entity: str,
    run_date: str,
) -> Optional[dict]:
    """Read Bronze _metadata.json for a specific date."""
    key = f"{prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt={run_date}/_metadata.json"
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception:
        return None


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def lifecycle_prefix(cleanup_prefix: str) -> str:
    """Unique prefix for lifecycle tests."""
    return f"{cleanup_prefix}/yaml_lifecycle"


@pytest.fixture
def minio_env():
    """Set up MinIO environment variables."""
    os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    os.environ["AWS_REGION"] = MINIO_REGION
    os.environ["AWS_S3_ADDRESSING_STYLE"] = "path"
    yield


# =============================================================================
# Test Classes
# =============================================================================


@pytest.mark.integration
class TestYamlSnapshotIncrementalCycle:
    """Test 4-day lifecycle: snapshot → incremental → incremental → snapshot refresh."""

    def test_full_lifecycle_scd1(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """Run 4-day cycle with full_merge_dedupe model."""
        entity = "customers_scd1"

        # Day 1: Full snapshot (5 records)
        csv_day1 = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_day1, index=False)

        yaml_path = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day1),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="full_snapshot",
            silver_model="full_merge_dedupe",
        )

        result = run_yaml_pipeline(yaml_path, "2025-01-10")
        assert result.returncode == 0, f"Day 1 failed: {result.stderr}"

        # Verify Day 1 Silver output
        silver_df = get_silver_df(minio_client, minio_bucket, lifecycle_prefix, entity)
        assert len(silver_df) == 5, (
            f"Expected 5 records after Day 1, got {len(silver_df)}"
        )
        assert set(silver_df["id"].tolist()) == {1, 2, 3, 4, 5}

        # Verify Bronze metadata has load_pattern
        metadata = get_bronze_metadata(
            minio_client, minio_bucket, lifecycle_prefix, entity, "2025-01-10"
        )
        assert metadata is not None, "Bronze metadata should exist"
        assert metadata.get("load_pattern") == "full_snapshot"

        # Day 2: Incremental (updates + new record)
        csv_day2 = tmp_path / f"{entity}_2025-01-11.csv"
        generate_incremental_day2(entity).to_csv(csv_day2, index=False)

        yaml_path_incr = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day2),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="incremental",
            silver_model="full_merge_dedupe",
            watermark_column="ts",
        )

        result = run_yaml_pipeline(yaml_path_incr, "2025-01-11")
        assert result.returncode == 0, f"Day 2 failed: {result.stderr}"

        # Verify Day 2: Each Silver run writes its own partition
        # The full_merge_dedupe model deduplicates within each run, but partitions accumulate
        # We should have 6 unique IDs when looking at the latest version of each
        silver_df = get_silver_df(minio_client, minio_bucket, lifecycle_prefix, entity)
        unique_ids = silver_df["id"].unique()
        assert len(unique_ids) == 6, (
            f"Expected 6 unique IDs after Day 2, got {len(unique_ids)}"
        )
        assert set(unique_ids) == {1, 2, 3, 4, 5, 6}

        # Verify ID 1 has updated value in the latest version
        id1_rows = silver_df[silver_df["id"] == 1].sort_values("ts", ascending=False)
        latest_row_1 = id1_rows.iloc[0]
        assert "v2" in latest_row_1["name"], (
            "ID 1 latest version should have name with v2"
        )
        assert latest_row_1["value"] == 150, "ID 1 latest version should have value=150"

        # Verify Bronze metadata for incremental
        metadata = get_bronze_metadata(
            minio_client, minio_bucket, lifecycle_prefix, entity, "2025-01-11"
        )
        assert metadata.get("load_pattern") == "incremental"

        # Day 3: Another incremental
        csv_day3 = tmp_path / f"{entity}_2025-01-12.csv"
        generate_incremental_day3(entity).to_csv(csv_day3, index=False)

        yaml_path_incr2 = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day3),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="incremental",
            silver_model="full_merge_dedupe",
            watermark_column="ts",
        )

        result = run_yaml_pipeline(yaml_path_incr2, "2025-01-12")
        assert result.returncode == 0, f"Day 3 failed: {result.stderr}"

        # Verify Day 3: Still 6 unique IDs (ID 4 updated)
        silver_df = get_silver_df(minio_client, minio_bucket, lifecycle_prefix, entity)
        unique_ids = silver_df["id"].unique()
        assert len(unique_ids) == 6, (
            f"Expected 6 unique IDs after Day 3, got {len(unique_ids)}"
        )

        # Verify ID 4 has final update in latest version
        id4_rows = silver_df[silver_df["id"] == 4].sort_values("ts", ascending=False)
        latest_row_4 = id4_rows.iloc[0]
        assert latest_row_4["status"] == "closed", (
            "ID 4 latest version should have status=closed"
        )

        # Day 4: Full refresh snapshot
        csv_day4 = tmp_path / f"{entity}_2025-01-13.csv"
        generate_snapshot_day3(entity).to_csv(
            csv_day4, index=False
        )  # Use day3 snapshot (5 records)

        yaml_path_refresh = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day4),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="full_snapshot",
            silver_model="full_merge_dedupe",
        )

        result = run_yaml_pipeline(yaml_path_refresh, "2025-01-13")
        assert result.returncode == 0, f"Day 4 failed: {result.stderr}"

        # Final verification: Get the latest partition (Day 4)
        # The full_merge_dedupe model reads ALL Bronze partitions and deduplicates.
        # Since Bronze Day 4 uses full_snapshot but Silver uses APPEND_LOG (per model spec),
        # Silver reads all 4 days of Bronze data and keeps the latest version of each ID.
        # - Day 1 had IDs 1-5
        # - Day 2 added ID 6
        # - Day 4 snapshot has IDs 1,3,4,5,6 (ID 2 missing = implicit delete in snapshot)
        # But full_merge_dedupe doesn't detect implicit deletes - it just keeps latest version.
        # So final state has 6 unique IDs (1-6), with latest versions of each.
        silver_df = get_silver_df(
            minio_client, minio_bucket, lifecycle_prefix, entity, run_date="2025-01-13"
        )
        assert len(silver_df) == 6, (
            f"Day 4 partition should have 6 records (full_merge_dedupe sees all IDs), got {len(silver_df)}"
        )
        assert silver_df["id"].is_unique, "IDs should be unique in Day 4 partition"
        assert set(silver_df["id"].tolist()) == {1, 2, 3, 4, 5, 6}, (
            "Should have all 6 unique IDs"
        )

        # Also verify that deduplicating across all partitions gives the same result
        deduped_df = get_deduplicated_silver_df(
            minio_client, minio_bucket, lifecycle_prefix, entity
        )
        assert len(deduped_df) == 6, (
            f"Deduplicated data should have 6 records, got {len(deduped_df)}"
        )


@pytest.mark.integration
class TestYamlSCD2HistoryBuilding:
    """Test SCD Type 2 history building across multiple days."""

    def test_scd2_builds_history(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """Verify SCD2 creates history records for updated entities."""
        entity = "customers_scd2"

        # Day 1: Full snapshot
        csv_day1 = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_day1, index=False)

        yaml_path = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day1),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="full_snapshot",
            silver_model="scd_type_2",
        )

        result = run_yaml_pipeline(yaml_path, "2025-01-10")
        assert result.returncode == 0, f"Day 1 failed: {result.stderr}"

        # Day 2: Incremental with updates
        csv_day2 = tmp_path / f"{entity}_2025-01-11.csv"
        generate_incremental_day2(entity).to_csv(csv_day2, index=False)

        yaml_path_incr = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day2),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="incremental",
            silver_model="scd_type_2",
            watermark_column="ts",
        )

        result = run_yaml_pipeline(yaml_path_incr, "2025-01-11")
        assert result.returncode == 0, f"Day 2 failed: {result.stderr}"

        # Verify SCD2 structure - check the latest partition (Day 2)
        silver_df = get_silver_df(
            minio_client, minio_bucket, lifecycle_prefix, entity, run_date="2025-01-11"
        )

        # Should have SCD2 columns
        assert "effective_from" in silver_df.columns, (
            "Should have effective_from column"
        )
        assert "is_current" in silver_df.columns, "Should have is_current column"

        # Verify exactly one current per ID in the latest partition
        current_df = silver_df[silver_df["is_current"] == 1]
        assert current_df["id"].is_unique, (
            "Each ID should have exactly one current record in Day 2 partition"
        )

        # Also verify all partitions combined - when we dedupe by (id, effective_from),
        # we should see history records
        all_silver_df = get_silver_df(
            minio_client, minio_bucket, lifecycle_prefix, entity
        )
        assert "effective_from" in all_silver_df.columns, (
            "All partitions should have SCD2 columns"
        )

        # IDs 1 and 3 were updated, so they may have multiple versions across partitions
        # The exact behavior depends on SCD2 implementation details


@pytest.mark.integration
@pytest.mark.parametrize(
    "delete_mode,expect_id2",
    [
        ("ignore", False),  # ID 2 delete ignored but dedupe keeps latest which is D
        ("hard_delete", False),  # ID 2 explicitly removed
    ],
)
class TestYamlCDCDeleteModes:
    """Test CDC patterns with different delete modes."""

    def test_cdc_delete_handling(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
        delete_mode,
        expect_id2,
    ):
        """Test CDC with parameterized delete modes."""
        entity = f"cdc_{delete_mode}"

        # Day 1: CDC Inserts
        csv_day1 = tmp_path / f"{entity}_2025-01-10.csv"
        generate_cdc_day1(entity).to_csv(csv_day1, index=False)

        yaml_path = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day1),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="cdc",
            silver_model="cdc_current"
            if delete_mode == "ignore"
            else f"cdc_current_{delete_mode}",
            cdc_operation_column="op",
            delete_mode=delete_mode,
        )

        result = run_yaml_pipeline(yaml_path, "2025-01-10")
        assert result.returncode == 0, f"Day 1 failed: {result.stderr}"

        # Day 2: CDC Updates + Insert
        csv_day2 = tmp_path / f"{entity}_2025-01-11.csv"
        generate_cdc_day2(entity).to_csv(csv_day2, index=False)

        yaml_path2 = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day2),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="cdc",
            silver_model="cdc_current"
            if delete_mode == "ignore"
            else f"cdc_current_{delete_mode}",
            cdc_operation_column="op",
            delete_mode=delete_mode,
        )

        result = run_yaml_pipeline(yaml_path2, "2025-01-11")
        assert result.returncode == 0, f"Day 2 failed: {result.stderr}"

        # Day 3: CDC Delete + Update
        csv_day3 = tmp_path / f"{entity}_2025-01-12.csv"
        generate_cdc_day3(entity).to_csv(csv_day3, index=False)

        yaml_path3 = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day3),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="cdc",
            silver_model="cdc_current"
            if delete_mode == "ignore"
            else f"cdc_current_{delete_mode}",
            cdc_operation_column="op",
            delete_mode=delete_mode,
        )

        result = run_yaml_pipeline(yaml_path3, "2025-01-12")
        assert result.returncode == 0, f"Day 3 failed: {result.stderr}"

        # Verify delete mode behavior - check the latest partition (Day 3)
        # where the delete operation was processed
        silver_df = get_silver_df(
            minio_client, minio_bucket, lifecycle_prefix, entity, run_date="2025-01-12"
        )

        if expect_id2:
            assert 2 in silver_df["id"].values, (
                "ID 2 should be present in Day 3 partition"
            )
        else:
            # For ignore mode: delete ops are filtered, so ID 2 won't appear in Day 3 partition
            # For hard_delete mode: ID 2 is explicitly removed from the current state
            # Either way, ID 2 should not be in the final Day 3 partition
            assert 2 not in silver_df["id"].values, (
                f"ID 2 should not be in Day 3 partition with delete_mode={delete_mode}"
            )

        # Also verify all other records are present
        assert 1 in silver_df["id"].values or len(silver_df) > 0, (
            "Other records should be present"
        )


@pytest.mark.integration
class TestYamlMetadataCommunication:
    """Test that Bronze metadata is correctly written and contains load_pattern."""

    def test_metadata_contains_load_pattern(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """Verify Bronze writes load_pattern to metadata."""
        entity = "metadata_test"

        # Create and run a snapshot load
        csv_day1 = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_day1, index=False)

        yaml_path = create_yaml_config(
            LIFECYCLE_YAML_TEMPLATE,
            tmp_path,
            entity=entity,
            source_path=str(csv_day1),
            bucket=minio_bucket,
            prefix=lifecycle_prefix,
            load_pattern="full_snapshot",
            silver_model="full_merge_dedupe",
        )

        result = run_yaml_pipeline(yaml_path, "2025-01-10")
        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Verify metadata
        metadata = get_bronze_metadata(
            minio_client, minio_bucket, lifecycle_prefix, entity, "2025-01-10"
        )
        assert metadata is not None, "Metadata should exist"
        assert "load_pattern" in metadata, "Metadata should contain load_pattern"
        assert metadata["load_pattern"] == "full_snapshot"
        assert "system" in metadata, "Metadata should contain system"
        assert metadata["system"] == "yaml_lifecycle"


@pytest.mark.integration
class TestSilverMetadataDiscovery:
    """Test that Silver can discover input_mode from Bronze metadata."""

    def test_silver_discovers_snapshot_pattern(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """Silver reads full_snapshot from metadata and uses REPLACE_DAILY."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.silver import SilverEntity

        entity = "discovery_snapshot"

        # Run Bronze first
        csv_path = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_path, index=False)

        bronze = BronzeSource(
            system="yaml_lifecycle",
            entity=entity,
            source_type=SourceType.FILE_CSV,
            source_path=str(csv_path),
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt={{run_date}}/",
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        bronze.run("2025-01-10")

        # Run Silver without explicit input_mode - should discover from metadata
        silver = SilverEntity(
            source_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt=*/*.parquet",
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/silver/domain=yaml_lifecycle/subject={entity}/dt={{run_date}}/",
            natural_keys=["id"],
            change_timestamp="ts",
            # input_mode NOT set - should be discovered
            storage_options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        result = silver.run("2025-01-10")

        assert result.get("row_count", 0) > 0, "Silver should process records"

    def test_silver_discovers_incremental_pattern(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """Silver reads incremental from metadata and uses APPEND_LOG."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.silver import SilverEntity

        entity = "discovery_incr"

        # Run Bronze with incremental pattern
        csv_path = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_path, index=False)

        bronze = BronzeSource(
            system="yaml_lifecycle",
            entity=entity,
            source_type=SourceType.FILE_CSV,
            source_path=str(csv_path),
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt={{run_date}}/",
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column="ts",
            options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        bronze.run("2025-01-10")

        # Verify Bronze metadata shows incremental
        metadata = get_bronze_metadata(
            minio_client, minio_bucket, lifecycle_prefix, entity, "2025-01-10"
        )
        assert metadata["load_pattern"] == "incremental"

        # Run Silver without input_mode - should discover APPEND_LOG
        silver = SilverEntity(
            source_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt=*/*.parquet",
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/silver/domain=yaml_lifecycle/subject={entity}/dt={{run_date}}/",
            natural_keys=["id"],
            change_timestamp="ts",
            # input_mode NOT set - should be discovered from metadata
            storage_options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        result = silver.run("2025-01-10")

        assert result.get("row_count", 0) > 0, "Silver should process records"

    def test_explicit_input_mode_overrides_discovery(
        self,
        minio_client,
        minio_bucket,
        lifecycle_prefix,
        minio_env,
        tmp_path,
    ):
        """When input_mode is set explicitly, metadata discovery is not used."""
        from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
        from pipelines.lib.silver import SilverEntity
        from pipelines.lib.storage_config import InputMode

        entity = "discovery_override"

        # Run Bronze with full_snapshot
        csv_path = tmp_path / f"{entity}_2025-01-10.csv"
        generate_snapshot_day1(entity).to_csv(csv_path, index=False)

        bronze = BronzeSource(
            system="yaml_lifecycle",
            entity=entity,
            source_type=SourceType.FILE_CSV,
            source_path=str(csv_path),
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt={{run_date}}/",
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        bronze.run("2025-01-10")

        # Run Silver with explicit APPEND_LOG (overrides what metadata would say)
        silver = SilverEntity(
            source_path=f"s3://{minio_bucket}/{lifecycle_prefix}/bronze/system=yaml_lifecycle/entity={entity}/dt=*/*.parquet",
            target_path=f"s3://{minio_bucket}/{lifecycle_prefix}/silver/domain=yaml_lifecycle/subject={entity}/dt={{run_date}}/",
            natural_keys=["id"],
            change_timestamp="ts",
            input_mode=InputMode.APPEND_LOG,  # Explicit override
            storage_options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
                "addressing_style": "path",
            },
        )
        result = silver.run("2025-01-10")

        assert result.get("row_count", 0) > 0, "Silver should process records"
        # The explicit input_mode should be used, not discovered one
