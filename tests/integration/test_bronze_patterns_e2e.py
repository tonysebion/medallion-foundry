"""Bronze Pattern Compliance End-to-End Tests.

Story 1: Tests that verify Bronze extraction produces pattern-compliant output
for all load patterns (SNAPSHOT, INCREMENTAL_APPEND, INCREMENTAL_MERGE, CURRENT_HISTORY).

Tests run against MinIO (S3 backend) and use PatternTestDataGenerator for
deterministic multi-batch scenarios. Each pattern is tested with T0, T1, T2, T3
batches to verify compliance.

Key validations:
- Pattern-specific data characteristics (overlap, uniqueness, versioning)
- Metadata files (_metadata.json contains correct load_pattern)
- Checksum integrity (_checksums.json matches actual files)
- Row counts match generator metadata
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from tests.integration.conftest import (
    requires_minio,
    upload_dataframe_to_minio,
    download_parquet_from_minio,
)
from tests.integration.helpers import (
    read_bronze_parquet,
    verify_bronze_metadata,
    verify_checksum_integrity,
)
from tests.pattern_verification.pattern_data.generators import (
    PatternTestDataGenerator,
)
from tests.pattern_verification.pattern_data.assertions import (
    AssertionValidator,
    create_snapshot_assertions,
    create_incremental_append_assertions,
    create_incremental_merge_assertions,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def pattern_generator() -> PatternTestDataGenerator:
    """Create pattern test data generator with standard seed."""
    return PatternTestDataGenerator(seed=42, base_rows=1000)


@pytest.fixture
def t0_date() -> date:
    """Standard T0 (initial load) date."""
    return date(2024, 1, 15)


# =============================================================================
# Helper Functions
# =============================================================================


def run_bronze_extraction_for_pattern(
    input_df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "pattern_test",
) -> Dict[str, Any]:
    """Run Bronze extraction for a specific pattern and return results.

    Args:
        input_df: Source DataFrame to extract
        output_path: Path for Bronze output
        load_pattern: Load pattern (snapshot, incremental_append, etc.)
        run_date: Extraction date
        system: Source system name
        table: Source table name

    Returns:
        Dictionary with exit_code, bronze_path, created_files, metadata
    """
    import uuid
    from core.infrastructure.runtime.context import build_run_context
    from core.orchestration.runner import ExtractJob

    # Write input data to temp location
    input_path = output_path.parent / "input"
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f"{table}.parquet"
    input_df.to_parquet(input_file, index=False)

    # Use unique table name to avoid checkpoint conflicts
    unique_table = f"{table}_{uuid.uuid4().hex[:8]}"

    # Build config with checkpoint disabled
    config = {
        "environment": "test",
        "domain": "healthcare",
        "system": system,
        "entity": unique_table,
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(output_path.parent / "_checkpoints"),
                "output_defaults": {
                    "parquet": True,
                    "csv": False,
                },
            },
        },
        "source": {
            "system": system,
            "table": unique_table,
            "type": "file",
            "file": {
                "path": str(input_file),
                "format": "parquet",
            },
            "run": {
                "load_pattern": load_pattern,
                "local_output_dir": str(output_path),
                "storage_enabled": False,
                "max_rows_per_file": 0,
                "checkpoint_enabled": False,
                "cleanup_on_failure": True,
            },
        },
    }

    # Run extraction
    context = build_run_context(cfg=config, run_date=run_date)
    job = ExtractJob(context)
    exit_code = job.run()

    return {
        "exit_code": exit_code,
        "bronze_path": context.bronze_path,
        "created_files": job.created_files,
        "schema_snapshot": job.schema_snapshot,
        "context": context,
    }


def verify_pattern_metadata(
    bronze_path: Path,
    expected_pattern: str,
    expected_row_count: int,
) -> Dict[str, Any]:
    """Verify Bronze metadata matches expected pattern and row count.

    Args:
        bronze_path: Path to Bronze output
        expected_pattern: Expected load_pattern value
        expected_row_count: Expected record_count value

    Returns:
        Metadata dictionary
    """
    metadata_info = verify_bronze_metadata(bronze_path)
    metadata = metadata_info["metadata"]

    # Verify load_pattern in metadata
    actual_pattern = metadata.get("load_pattern", "").lower()
    assert actual_pattern == expected_pattern.lower(), (
        f"Expected load_pattern '{expected_pattern}', got '{actual_pattern}'"
    )

    # Verify row count
    actual_count = metadata.get("record_count", 0)
    assert actual_count == expected_row_count, (
        f"Expected record_count {expected_row_count}, got {actual_count}"
    )

    result: Dict[str, Any] = metadata
    return result


# =============================================================================
# SNAPSHOT Pattern Tests
# =============================================================================


class TestSnapshotPatternE2E:
    """Test SNAPSHOT pattern produces complete replacement data."""

    def test_snapshot_t0_extraction_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test SNAPSHOT T0 extraction completes successfully."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        assert result["exit_code"] == 0, "Extraction should succeed"
        assert result["bronze_path"].exists(), "Bronze path should exist"
        assert len(result["created_files"]) > 0, "Should create files"

    def test_snapshot_metadata_contains_pattern(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT metadata contains correct load_pattern."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        verify_pattern_metadata(
            result["bronze_path"],
            expected_pattern="snapshot",
            expected_row_count=100,
        )

    def test_snapshot_checksums_valid(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT checksums match actual file contents."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        assert verify_checksum_integrity(result["bronze_path"])

    def test_snapshot_data_has_unique_ids(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT data has unique record IDs."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        df = read_bronze_parquet(result["bronze_path"])

        assert df["record_id"].is_unique, "record_id should be unique"
        assert df["record_id"].notna().all(), "record_id should have no nulls"

    def test_snapshot_replacement_has_different_ids(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT replacement batch has different IDs (full replacement)."""
        scenario = pattern_generator.generate_snapshot_scenario(
            rows=100,
            include_replacement=True,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        # Run T1 (replacement)
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="snapshot",
            run_date=date(2024, 1, 16),
        )

        # Verify no overlap (complete replacement)
        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        t0_ids = set(t0_df["record_id"])
        t1_ids = set(t1_df["record_id"])
        overlap = t0_ids & t1_ids

        assert len(overlap) == 0, (
            f"SNAPSHOT replacement should have no ID overlap, got {len(overlap)}"
        )


# =============================================================================
# INCREMENTAL_APPEND Pattern Tests
# =============================================================================


class TestIncrementalAppendPatternE2E:
    """Test INCREMENTAL_APPEND pattern produces append-only data."""

    def test_incremental_append_t0_extraction_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test INCREMENTAL_APPEND T0 extraction completes successfully."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        assert result["exit_code"] == 0, "Extraction should succeed"
        assert result["bronze_path"].exists(), "Bronze path should exist"

    def test_incremental_append_metadata_contains_pattern(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND metadata contains correct load_pattern."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        verify_pattern_metadata(
            result["bronze_path"],
            expected_pattern="incremental_append",
            expected_row_count=100,
        )

    def test_incremental_append_t1_has_no_id_overlap(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND T1 contains only NEW records (zero overlap)."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_append",
            run_date=date(2024, 1, 16),
        )

        # Verify ZERO overlap (append-only)
        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        t0_ids = set(t0_df["record_id"])
        t1_ids = set(t1_df["record_id"])
        overlap = t0_ids & t1_ids

        assert len(overlap) == 0, (
            f"INCREMENTAL_APPEND should have zero ID overlap, got {len(overlap)}: {overlap}"
        )

    def test_incremental_append_t1_ids_are_sequential(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND T1 IDs are sequential after T0 max."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_append",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        # Get max T0 ID number
        max_t0_id = max(int(rid.replace("REC", "")) for rid in t0_df["record_id"])

        # All T1 IDs should be greater than max T0 ID
        for record_id in t1_df["record_id"]:
            id_num = int(record_id.replace("REC", ""))
            assert id_num > max_t0_id, (
                f"T1 ID {record_id} should be > max T0 ID REC{max_t0_id:08d}"
            )

    def test_incremental_append_multi_batch_cumulative(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND across T0→T1→T2→T3 maintains no overlap."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=4,
            insert_rate=0.1,
        )

        all_ids: set = set()
        batch_names = ["t0", "t1", "t2", "t3"]

        for i, batch_name in enumerate(batch_names):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            result = run_bronze_extraction_for_pattern(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_append",
                run_date=date(2024, 1, 15 + i),
            )

            df = read_bronze_parquet(result["bronze_path"])
            batch_ids = set(df["record_id"])

            # Verify no overlap with previous batches
            overlap = all_ids & batch_ids
            assert len(overlap) == 0, (
                f"Batch {batch_name} has {len(overlap)} overlapping IDs with previous batches"
            )

            all_ids.update(batch_ids)


# =============================================================================
# INCREMENTAL_MERGE Pattern Tests
# =============================================================================


class TestIncrementalMergePatternE2E:
    """Test INCREMENTAL_MERGE pattern produces updates + inserts."""

    def test_incremental_merge_t0_extraction_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test INCREMENTAL_MERGE T0 extraction completes successfully."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        assert result["exit_code"] == 0, "Extraction should succeed"
        assert result["bronze_path"].exists(), "Bronze path should exist"

    def test_incremental_merge_metadata_contains_pattern(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE metadata contains correct load_pattern."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        verify_pattern_metadata(
            result["bronze_path"],
            expected_pattern="incremental_merge",
            expected_row_count=100,
        )

    def test_incremental_merge_t1_has_updates_and_inserts(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE T1 contains both updates AND inserts."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_merge",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        t0_ids = set(t0_df["record_id"])
        t1_ids = set(t1_df["record_id"])

        # Updates = IDs that exist in both T0 and T1
        updates = t0_ids & t1_ids
        # Inserts = IDs only in T1
        inserts = t1_ids - t0_ids

        assert len(updates) > 0, "INCREMENTAL_MERGE should have updates"
        assert len(inserts) > 0, "INCREMENTAL_MERGE should have inserts"

        # Verify counts match scenario metadata
        expected_updates = scenario.metadata["changes"]["t1"]["update_count"]
        expected_inserts = scenario.metadata["changes"]["t1"]["insert_count"]

        assert len(updates) == expected_updates, (
            f"Expected {expected_updates} updates, got {len(updates)}"
        )
        assert len(inserts) == expected_inserts, (
            f"Expected {expected_inserts} inserts, got {len(inserts)}"
        )

    def test_incremental_merge_updates_have_changed_values(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE updated records have different field values."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_merge",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        # Get updated record IDs
        t0_ids = set(t0_df["record_id"])
        t1_ids = set(t1_df["record_id"])
        updated_ids = t0_ids & t1_ids

        # For each updated record, at least one value should change
        changes_detected = 0
        for rid in list(updated_ids)[:10]:  # Check first 10
            t0_row = t0_df[t0_df["record_id"] == rid].iloc[0]
            t1_row = t1_df[t1_df["record_id"] == rid].iloc[0]

            # Check if status or amount changed
            if t0_row["status"] != t1_row["status"]:
                changes_detected += 1
            elif abs(t0_row["amount"] - t1_row["amount"]) > 0.01:
                changes_detected += 1

        assert changes_detected > 0, "Updated records should have changed values"

    def test_incremental_merge_expected_final_row_count(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE final row count matches expected."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=4,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Run all batches and collect unique IDs
        all_ids: set = set()
        batch_names = ["t0", "t1", "t2", "t3"]

        for i, batch_name in enumerate(batch_names):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            result = run_bronze_extraction_for_pattern(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_merge",
                run_date=date(2024, 1, 15 + i),
            )

            df = read_bronze_parquet(result["bronze_path"])
            all_ids.update(df["record_id"])

        expected_final_count = scenario.metadata["expected_final_row_count"]
        assert len(all_ids) == expected_final_count, (
            f"Expected final row count {expected_final_count}, got {len(all_ids)}"
        )


# =============================================================================
# CURRENT_HISTORY (SCD2) Pattern Tests
# =============================================================================


class TestCurrentHistoryPatternE2E:
    """Test CURRENT_HISTORY pattern produces SCD2-compliant versioned data."""

    def test_current_history_t0_extraction_succeeds(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test CURRENT_HISTORY T0 extraction completes successfully."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        assert result["exit_code"] == 0, "Extraction should succeed"
        assert result["bronze_path"].exists(), "Bronze path should exist"

    def test_current_history_metadata_contains_pattern(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify CURRENT_HISTORY metadata contains correct load_pattern."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        verify_pattern_metadata(
            result["bronze_path"],
            expected_pattern="current_history",
            expected_row_count=50,  # Initial entities
        )

    def test_current_history_t0_all_version_1(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify CURRENT_HISTORY T0 records all have version=1."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        df = read_bronze_parquet(result["bronze_path"])

        assert (df["version"] == 1).all(), "All T0 records should have version=1"
        assert df["is_current"].all(), "All T0 records should be current"

    def test_current_history_t1_has_incremented_versions(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify CURRENT_HISTORY T1 records have incremented versions."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        # Run T0
        run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="current_history",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="current_history",
            run_date=date(2024, 1, 16),
        )

        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        # T1 records should have version > 1
        assert (t1_df["version"] > 1).all(), "T1 records should have version > 1"
        assert t1_df["is_current"].all(), "New versions should be current"

    def test_current_history_entity_ids_are_consistent(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify CURRENT_HISTORY entity_ids are consistent across versions."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="current_history",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="current_history",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        t0_entities = set(t0_df["entity_id"])
        t1_entities = set(t1_df["entity_id"])

        # T1 entities should be a subset of T0 (changes to existing entities)
        assert t1_entities.issubset(t0_entities), (
            "T1 entities should exist in T0"
        )

    def test_current_history_has_effective_dates(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify CURRENT_HISTORY records have effective_from timestamps."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50,
            changes_per_entity=2,
        )

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="current_history",
            run_date=t0_date,
        )

        df = read_bronze_parquet(result["bronze_path"])

        assert "effective_from" in df.columns, "Should have effective_from column"
        assert df["effective_from"].notna().all(), "effective_from should not be null"


# =============================================================================
# Cross-Pattern Validation Tests
# =============================================================================


class TestPatternAssertionFramework:
    """Test using PatternAssertions framework for validation."""

    def test_snapshot_assertions_pass(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT data passes assertion framework."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        df = read_bronze_parquet(result["bronze_path"])

        # Create assertions
        assertions = create_snapshot_assertions(
            row_count=100,
            columns=list(df.columns),
            first_row_values={"record_id": "REC00000001"},
        )

        # Validate
        validator = AssertionValidator(assertions)
        report = validator.validate_all(df)

        assert report.passed, f"Assertions failed:\n{report.summary()}"

    def test_incremental_append_assertions_pass(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND data passes assertion framework."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_append",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_append",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        # Create assertions for T1
        assertions = create_incremental_append_assertions(
            new_rows_count=len(t1_df),
            columns=list(t1_df.columns),
        )

        # Validate T1 against T0
        validator = AssertionValidator(assertions)
        report = validator.validate_all(t1_df, previous_df=t0_df)

        assert report.passed, f"Assertions failed:\n{report.summary()}"

    def test_incremental_merge_assertions_pass(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE data passes assertion framework."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Run T0
        t0_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_merge",
            run_date=t0_date,
        )

        # Run T1
        t1_result = run_bronze_extraction_for_pattern(
            input_df=scenario.t1,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_merge",
            run_date=date(2024, 1, 16),
        )

        t0_df = read_bronze_parquet(t0_result["bronze_path"])
        t1_df = read_bronze_parquet(t1_result["bronze_path"])

        # Get expected counts from scenario metadata
        expected_updates = scenario.metadata["changes"]["t1"]["update_count"]
        expected_inserts = scenario.metadata["changes"]["t1"]["insert_count"]

        # Create assertions for T1
        assertions = create_incremental_merge_assertions(
            updated_count=expected_updates,
            inserted_count=expected_inserts,
            columns=list(t1_df.columns),
        )

        # Validate T1 against T0
        validator = AssertionValidator(assertions)
        report = validator.validate_all(t1_df, previous_df=t0_df)

        assert report.passed, f"Assertions failed:\n{report.summary()}"


# =============================================================================
# MinIO Integration Tests
# =============================================================================


@requires_minio
class TestBronzePatternsMinIO:
    """Test Bronze patterns with MinIO storage backend."""

    def test_snapshot_to_minio(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify SNAPSHOT data can be written to and read from MinIO."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Upload to MinIO
        key = f"{cleanup_prefix}/bronze/pattern_test/snapshot.parquet"
        upload_dataframe_to_minio(
            minio_client,
            minio_bucket,
            key,
            scenario.t0,
        )

        # Read back
        df = download_parquet_from_minio(minio_client, minio_bucket, key)

        assert len(df) == 100, "Should have 100 rows"
        assert df["record_id"].is_unique, "record_id should be unique"

    def test_incremental_append_to_minio(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        t0_date: date,
    ):
        """Verify INCREMENTAL_APPEND batches maintain no overlap in MinIO."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100,
            batches=3,
            insert_rate=0.1,
        )

        all_ids: set = set()
        batch_names = ["t0", "t1", "t2"]

        for batch_name in batch_names:
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            # Upload to MinIO
            key = f"{cleanup_prefix}/bronze/pattern_test/{batch_name}.parquet"
            upload_dataframe_to_minio(
                minio_client,
                minio_bucket,
                key,
                batch_df,
            )

            # Read back and verify
            df = download_parquet_from_minio(minio_client, minio_bucket, key)
            batch_ids = set(df["record_id"])

            overlap = all_ids & batch_ids
            assert len(overlap) == 0, (
                f"Batch {batch_name} has overlap with previous: {overlap}"
            )

            all_ids.update(batch_ids)

    def test_incremental_merge_to_minio(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        t0_date: date,
    ):
        """Verify INCREMENTAL_MERGE batches maintain proper overlap in MinIO."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100,
            batches=2,
            update_rate=0.2,
            insert_rate=0.1,
        )

        # Upload T0
        t0_key = f"{cleanup_prefix}/bronze/pattern_test/t0.parquet"
        upload_dataframe_to_minio(
            minio_client,
            minio_bucket,
            t0_key,
            scenario.t0,
        )

        # Upload T1
        t1_key = f"{cleanup_prefix}/bronze/pattern_test/t1.parquet"
        upload_dataframe_to_minio(
            minio_client,
            minio_bucket,
            t1_key,
            scenario.t1,
        )

        # Read back and verify
        t0_df = download_parquet_from_minio(minio_client, minio_bucket, t0_key)
        t1_df = download_parquet_from_minio(minio_client, minio_bucket, t1_key)

        t0_ids = set(t0_df["record_id"])
        t1_ids = set(t1_df["record_id"])

        updates = t0_ids & t1_ids
        inserts = t1_ids - t0_ids

        assert len(updates) > 0, "Should have updates"
        assert len(inserts) > 0, "Should have inserts"
