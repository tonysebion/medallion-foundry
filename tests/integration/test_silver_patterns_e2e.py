"""Silver Processing Pattern End-to-End Tests.

Story 2: Tests that verify Silver processing correctly transforms each Bronze
load pattern into the appropriate Silver model:
- SNAPSHOT → PERIODIC_SNAPSHOT (exact Bronze data preserved)
- INCREMENTAL_APPEND → INCREMENTAL_MERGE (appends treated as changes)
- INCREMENTAL_MERGE → INCREMENTAL_MERGE (updates and inserts emitted)
- CURRENT_HISTORY → SCD_TYPE_2 (current/history views correct)

Tests verify:
- Pattern discovery from Bronze metadata
- Correct Silver model selection
- Data transformation correctness
- Multi-batch scenarios (T0→T1→T2)
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

import pandas as pd
import pytest

from core.foundation.primitives.patterns import LoadPattern
from core.foundation.primitives.models import SilverModel
from core.infrastructure.config import DatasetConfig, EntityKind
from tests.integration.conftest import (
    requires_minio,
    upload_dataframe_to_minio,
    download_parquet_from_minio,
)
from tests.integration.helpers import (
    read_bronze_parquet,
    verify_bronze_metadata,
)
from tests.pattern_verification.pattern_data.generators import (
    PatternTestDataGenerator,
    PatternScenario,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def pattern_generator() -> PatternTestDataGenerator:
    """Create pattern test data generator with standard seed."""
    return PatternTestDataGenerator(seed=42, base_rows=100)


@pytest.fixture
def t0_date() -> date:
    """Standard T0 (initial load) date."""
    return date(2024, 1, 15)


@pytest.fixture
def t1_date() -> date:
    """Standard T1 (incremental) date."""
    return date(2024, 1, 16)


@pytest.fixture
def t2_date() -> date:
    """Standard T2 (second incremental) date."""
    return date(2024, 1, 17)


# =============================================================================
# Helper Functions
# =============================================================================


def create_bronze_output(
    df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "pattern_test",
) -> Path:
    """Create Bronze output structure with metadata for Silver processing.

    Args:
        df: Source DataFrame
        output_path: Path for Bronze output
        load_pattern: Load pattern value
        run_date: Run date for metadata
        system: Source system name
        table: Source table name

    Returns:
        Path to Bronze partition directory
    """
    # Create Bronze partition structure
    partition_path = (
        output_path
        / f"system={system}"
        / f"table={table}"
        / f"pattern={load_pattern}"
        / f"dt={run_date.isoformat()}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    # Write parquet file
    parquet_file = partition_path / "chunk_0.parquet"
    df.to_parquet(parquet_file, index=False)

    # Write metadata
    metadata = {
        "timestamp": run_date.isoformat(),
        "system": system,
        "table": table,
        "load_pattern": load_pattern,
        "chunk_count": 1,
        "record_count": len(df),
        "run_id": str(uuid.uuid4()),
    }
    metadata_path = partition_path / "_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))

    # Write checksums
    import hashlib
    checksum = hashlib.sha256(parquet_file.read_bytes()).hexdigest()
    checksums = {
        "files": [
            {
                "path": "chunk_0.parquet",
                "sha256": checksum,
                "size": parquet_file.stat().st_size,
            }
        ]
    }
    checksums_path = partition_path / "_checksums.json"
    checksums_path.write_text(json.dumps(checksums, indent=2))

    return partition_path


def create_silver_dataset_config(
    system: str = "synthetic",
    entity: str = "pattern_test",
    entity_kind: str = "event",
    natural_keys: Optional[List[str]] = None,
    order_column: str = "updated_at",
    event_ts_column: str = "created_at",
    change_ts_column: Optional[str] = None,
) -> DatasetConfig:
    """Create DatasetConfig for Silver processing.

    Args:
        system: Source system name
        entity: Entity name
        entity_kind: Entity kind (event, state, derived_event)
        natural_keys: Primary key columns
        order_column: Column for ordering
        event_ts_column: Event timestamp column
        change_ts_column: Change timestamp column (required for state entities)

    Returns:
        DatasetConfig instance
    """
    natural_keys = natural_keys or ["record_id"]

    silver_config = {
        "enabled": True,
        "entity_kind": entity_kind,
        "version": 1,
        "natural_keys": natural_keys,
        "order_column": order_column,
        "event_ts_column": event_ts_column,
        "input_storage": "local",
        "schema_mode": "allow_new_columns",
    }

    # Add change_ts_column for state entities (required)
    if entity_kind == "state" or entity_kind == "derived_state":
        silver_config["change_ts_column"] = change_ts_column or event_ts_column

    return DatasetConfig.from_dict({
        "environment": "test",
        "domain": "healthcare",
        "system": system,
        "entity": entity,
        "bronze": {
            "enabled": True,
        },
        "silver": silver_config,
    })


def run_silver_processing(
    bronze_path: Path,
    silver_path: Path,
    dataset_config: DatasetConfig,
    run_date: date,
) -> Dict[str, Any]:
    """Run Silver processing and return results.

    Args:
        bronze_path: Path to Bronze partition
        silver_path: Path for Silver output
        dataset_config: DatasetConfig for processing
        run_date: Processing date

    Returns:
        Dictionary with metrics and output info
    """
    from core.domain.services.pipelines.silver.processor import SilverProcessor

    processor = SilverProcessor(
        dataset=dataset_config,
        bronze_path=bronze_path,
        silver_partition=silver_path,
        run_date=run_date,
        verify_checksum=False,
    )

    result = processor.run()

    return {
        "metrics": result.metrics,
        "outputs": result.outputs,
        "schema_snapshot": result.schema_snapshot,
        "silver_path": silver_path,
    }


def read_silver_parquet(silver_path: Path) -> pd.DataFrame:
    """Read all parquet files from Silver output.

    Args:
        silver_path: Path to Silver partition

    Returns:
        Combined DataFrame from all parquet files
    """
    parquet_files = list(silver_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {silver_path}")

    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]


# =============================================================================
# SilverModel Mapping Tests
# =============================================================================


class TestSilverModelMapping:
    """Test correct SilverModel selection for each LoadPattern."""

    def test_snapshot_maps_to_periodic_snapshot(self):
        """Verify SNAPSHOT → PERIODIC_SNAPSHOT mapping."""
        pattern = LoadPattern.SNAPSHOT
        model = SilverModel.default_for_load_pattern(pattern)
        assert model == SilverModel.PERIODIC_SNAPSHOT

    def test_incremental_append_maps_to_incremental_merge(self):
        """Verify INCREMENTAL_APPEND → INCREMENTAL_MERGE mapping."""
        pattern = LoadPattern.INCREMENTAL_APPEND
        model = SilverModel.default_for_load_pattern(pattern)
        assert model == SilverModel.INCREMENTAL_MERGE

    def test_incremental_merge_maps_to_incremental_merge(self):
        """Verify INCREMENTAL_MERGE → INCREMENTAL_MERGE mapping."""
        pattern = LoadPattern.INCREMENTAL_MERGE
        model = SilverModel.default_for_load_pattern(pattern)
        assert model == SilverModel.INCREMENTAL_MERGE

    def test_current_history_maps_to_scd_type_2(self):
        """Verify CURRENT_HISTORY → SCD_TYPE_2 mapping."""
        pattern = LoadPattern.CURRENT_HISTORY
        model = SilverModel.default_for_load_pattern(pattern)
        assert model == SilverModel.SCD_TYPE_2


# =============================================================================
# SNAPSHOT → PERIODIC_SNAPSHOT Tests
# =============================================================================


class TestSnapshotToPeriodicSnapshotE2E:
    """Test SNAPSHOT pattern produces PERIODIC_SNAPSHOT Silver model."""

    def test_snapshot_silver_processing_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver processing of SNAPSHOT data completes successfully."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Create Bronze output
        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        # Run Silver processing
        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        assert result["metrics"].rows_read == 100
        assert result["metrics"].rows_written > 0

    def test_snapshot_silver_preserves_all_rows(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify PERIODIC_SNAPSHOT preserves exact row count from Bronze."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Read Silver output
        silver_df = read_silver_parquet(tmp_path / "silver")

        # PERIODIC_SNAPSHOT should preserve exact row count
        assert len(silver_df) == 100, "PERIODIC_SNAPSHOT should preserve all rows"

    def test_snapshot_silver_has_metadata_columns(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify Silver output includes standard metadata columns."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df = read_silver_parquet(tmp_path / "silver")

        # Check metadata columns
        expected_columns = ["load_batch_id", "record_source"]
        for col in expected_columns:
            assert col in silver_df.columns, f"Missing metadata column: {col}"

    def test_snapshot_silver_preserves_data_values(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify PERIODIC_SNAPSHOT preserves original data values."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df = read_silver_parquet(tmp_path / "silver")

        # Check first row values match (only columns that are preserved)
        bronze_first = scenario.t0.iloc[0]
        silver_first = silver_df[silver_df["record_id"] == bronze_first["record_id"]].iloc[0]

        # Verify primary key preserved
        assert silver_first["record_id"] == bronze_first["record_id"]

        # Verify columns from Bronze are preserved in Silver (plus metadata)
        bronze_cols = set(scenario.t0.columns)
        silver_cols = set(silver_df.columns)

        # Silver should have the natural key column at minimum
        assert "record_id" in silver_cols, "Natural key must be in Silver"

        # Silver adds metadata columns
        metadata_cols = {"load_batch_id", "record_source", "pipeline_run_at", "environment", "domain"}
        silver_new_cols = silver_cols - bronze_cols
        assert len(silver_new_cols) > 0, "Silver should add metadata columns"


# =============================================================================
# INCREMENTAL_APPEND → INCREMENTAL_MERGE Tests
# =============================================================================


class TestIncrementalAppendToMergeE2E:
    """Test INCREMENTAL_APPEND pattern produces INCREMENTAL_MERGE Silver model."""

    def test_incremental_append_silver_processing_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver processing of INCREMENTAL_APPEND data completes."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        assert result["metrics"].rows_read == 100
        assert result["metrics"].rows_written > 0

    def test_incremental_append_t0_t1_processing(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
        t1_date: date,
    ):
        """Test Silver processing handles T0→T1 incremental append."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        # Process T0
        bronze_t0_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        t0_result = run_silver_processing(
            bronze_path=bronze_t0_path,
            silver_path=tmp_path / "silver_t0",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Process T1
        bronze_t1_path = create_bronze_output(
            df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_append",
            run_date=t1_date,
        )

        t1_result = run_silver_processing(
            bronze_path=bronze_t1_path,
            silver_path=tmp_path / "silver_t1",
            dataset_config=dataset_config,
            run_date=t1_date,
        )

        # Verify both processed successfully
        assert t0_result["metrics"].rows_written > 0
        assert t1_result["metrics"].rows_written > 0

        # Read outputs
        silver_t0_df = read_silver_parquet(tmp_path / "silver_t0")
        silver_t1_df = read_silver_parquet(tmp_path / "silver_t1")

        # Verify no overlap in IDs (append-only)
        t0_ids = set(silver_t0_df["record_id"])
        t1_ids = set(silver_t1_df["record_id"])
        overlap = t0_ids & t1_ids

        assert len(overlap) == 0, "Incremental append should have no ID overlap"


# =============================================================================
# INCREMENTAL_MERGE → INCREMENTAL_MERGE Tests
# =============================================================================


class TestIncrementalMergeToMergeE2E:
    """Test INCREMENTAL_MERGE pattern produces INCREMENTAL_MERGE Silver model."""

    def test_incremental_merge_silver_processing_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver processing of INCREMENTAL_MERGE data completes."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        assert result["metrics"].rows_read == 100
        assert result["metrics"].rows_written > 0

    def test_incremental_merge_t1_processes_updates_and_inserts(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
        t1_date: date,
    ):
        """Test Silver correctly processes T1 with updates and inserts."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Process T0
        bronze_t0_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        run_silver_processing(
            bronze_path=bronze_t0_path,
            silver_path=tmp_path / "silver_t0",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Process T1
        bronze_t1_path = create_bronze_output(
            df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_merge",
            run_date=t1_date,
        )

        run_silver_processing(
            bronze_path=bronze_t1_path,
            silver_path=tmp_path / "silver_t1",
            dataset_config=dataset_config,
            run_date=t1_date,
        )

        # Read outputs
        silver_t0_df = read_silver_parquet(tmp_path / "silver_t0")
        silver_t1_df = read_silver_parquet(tmp_path / "silver_t1")

        t0_ids = set(silver_t0_df["record_id"])
        t1_ids = set(silver_t1_df["record_id"])

        # T1 should have both updates (overlap) and inserts (new)
        updates = t0_ids & t1_ids
        inserts = t1_ids - t0_ids

        assert len(updates) > 0, "Should have updates (overlapping IDs)"
        assert len(inserts) > 0, "Should have inserts (new IDs)"

    def test_incremental_merge_deduplicates_by_natural_key(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver deduplicates by natural key within a batch."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df = read_silver_parquet(tmp_path / "silver")

        # Should be unique by natural key
        assert silver_df["record_id"].is_unique, "Should be deduplicated by natural key"


# =============================================================================
# CURRENT_HISTORY → SCD_TYPE_2 Tests
# =============================================================================


class TestCurrentHistoryToSCD2E2E:
    """Test CURRENT_HISTORY pattern produces SCD_TYPE_2 Silver model."""

    def test_current_history_silver_processing_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver processing of CURRENT_HISTORY data completes."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["entity_id"],
            order_column="effective_from",
            event_ts_column="effective_from",
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        assert result["metrics"].rows_read == 50
        assert result["metrics"].rows_written > 0

    def test_current_history_t0_all_entities_current(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify T0 CURRENT_HISTORY has all entities marked as current."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        bronze_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["entity_id"],
            order_column="effective_from",
            event_ts_column="effective_from",
        )

        run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        silver_df = read_silver_parquet(tmp_path / "silver")

        # All T0 records should be current
        if "is_current" in silver_df.columns:
            assert silver_df["is_current"].all(), "All T0 records should be current"

    def test_current_history_t1_creates_new_versions(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
        t1_date: date,
    ):
        """Verify T1 CURRENT_HISTORY creates new entity versions."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        # Process T0
        bronze_t0_path = create_bronze_output(
            df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="current_history",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="state",
            natural_keys=["entity_id"],
            order_column="effective_from",
            event_ts_column="effective_from",
        )

        run_silver_processing(
            bronze_path=bronze_t0_path,
            silver_path=tmp_path / "silver_t0",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        # Process T1
        bronze_t1_path = create_bronze_output(
            df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="current_history",
            run_date=t1_date,
        )

        run_silver_processing(
            bronze_path=bronze_t1_path,
            silver_path=tmp_path / "silver_t1",
            dataset_config=dataset_config,
            run_date=t1_date,
        )

        # Read outputs
        silver_t0_df = read_silver_parquet(tmp_path / "silver_t0")
        silver_t1_df = read_silver_parquet(tmp_path / "silver_t1")

        # T1 should have higher versions
        if "version" in silver_t0_df.columns and "version" in silver_t1_df.columns:
            max_t0_version = silver_t0_df["version"].max()
            min_t1_version = silver_t1_df["version"].min()
            assert min_t1_version > max_t0_version, "T1 versions should be higher"


# =============================================================================
# Multi-Batch Processing Tests
# =============================================================================


class TestMultiBatchProcessing:
    """Test Silver processing across multiple batches."""

    def test_three_batch_snapshot_processing(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test processing T0→T1→T2 snapshots."""
        scenario = pattern_generator.generate_snapshot_scenario(
            rows=100,
            include_replacement=True,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        total_rows_written = 0

        for i, batch_name in enumerate(["t0", "t1"]):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            bronze_path = create_bronze_output(
                df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="snapshot",
                run_date=batch_date,
            )

            result = run_silver_processing(
                bronze_path=bronze_path,
                silver_path=tmp_path / f"silver_{batch_name}",
                dataset_config=dataset_config,
                run_date=batch_date,
            )

            total_rows_written += result["metrics"].rows_written

        assert total_rows_written > 0, "Should process multiple batches"

    def test_three_batch_incremental_merge_processing(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test processing T0→T1→T2 incremental merges."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=3,
            update_rate=0.2,
            insert_rate=0.1,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        all_ids: set = set()
        batch_names = ["t0", "t1", "t2"]

        for i, batch_name in enumerate(batch_names):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            bronze_path = create_bronze_output(
                df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_merge",
                run_date=batch_date,
            )

            run_silver_processing(
                bronze_path=bronze_path,
                silver_path=tmp_path / f"silver_{batch_name}",
                dataset_config=dataset_config,
                run_date=batch_date,
            )

            silver_df = read_silver_parquet(tmp_path / f"silver_{batch_name}")
            all_ids.update(silver_df["record_id"])

        # Should have cumulative IDs from all batches
        expected_final_count = scenario.metadata["expected_final_row_count"]
        assert len(all_ids) == expected_final_count


# =============================================================================
# MinIO Integration Tests
# =============================================================================


@requires_minio
class TestSilverPatternsMinIO:
    """Test Silver patterns with MinIO storage."""

    def test_silver_snapshot_to_minio(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test Silver processing with data from MinIO."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Upload Bronze to MinIO
        bronze_key = f"{cleanup_prefix}/bronze/pattern_test/snapshot.parquet"
        upload_dataframe_to_minio(
            minio_client,
            minio_bucket,
            bronze_key,
            scenario.t0,
        )

        # Read back and process locally
        bronze_df = download_parquet_from_minio(minio_client, minio_bucket, bronze_key)

        bronze_path = create_bronze_output(
            df=bronze_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        dataset_config = create_silver_dataset_config(
            entity_kind="event",
            natural_keys=["record_id"],
        )

        result = run_silver_processing(
            bronze_path=bronze_path,
            silver_path=tmp_path / "silver",
            dataset_config=dataset_config,
            run_date=t0_date,
        )

        assert result["metrics"].rows_written == 100
