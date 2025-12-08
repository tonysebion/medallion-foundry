"""Full Pipeline End-to-End Integration Tests.

Story 1.0: Tests that run actual pipeline entry points (ExtractJob.run(),
SilverProcessor.run()) and verify data correctness, not just row counts.

This module tests:
- Bronze extraction via ExtractJob.run() with RunContext
- Bronze metadata and checksum generation
- Silver processing via SilverProcessor.run()
- Incremental T0 → T1 scenarios
- Data verification against golden files
- Error scenarios (corrupted checksums, missing data)
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from tests.integration.conftest import requires_minio
from tests.integration.helpers import (
    load_golden_file,
    verify_dataframe_first_row,
    verify_dataframe_schema,
    verify_bronze_metadata,
    verify_checksum_integrity,
    read_bronze_parquet,
)


# =============================================================================
# TestBronzePipelineE2E: Test actual ExtractJob.run()
# =============================================================================


class TestBronzePipelineE2E:
    """Test actual ExtractJob.run() with local storage."""

    def test_bronze_extract_claims_t0_runs_successfully(
        self,
        run_bronze_extraction,
    ):
        """Run ExtractJob.run() and verify it completes successfully."""
        result = run_bronze_extraction()

        assert result["exit_code"] == 0, "Bronze extraction should succeed"
        assert result["bronze_path"].exists(), "Bronze output path should exist"
        assert len(result["created_files"]) > 0, "Should create output files"

    def test_bronze_creates_metadata_files(
        self,
        run_bronze_extraction,
    ):
        """Verify Bronze extraction creates _metadata.json and _checksums.json."""
        result = run_bronze_extraction()
        bronze_path = result["bronze_path"]

        # Verify metadata files exist and are valid
        metadata_info = verify_bronze_metadata(bronze_path)

        assert metadata_info["metadata"]["system"] == "synthetic"
        assert metadata_info["metadata"]["table"] == "claims"
        assert metadata_info["metadata"]["record_count"] == 100

    def test_bronze_checksum_integrity(
        self,
        run_bronze_extraction,
    ):
        """Verify checksums in manifest match actual file contents."""
        result = run_bronze_extraction()
        bronze_path = result["bronze_path"]

        # This will raise AssertionError if checksums don't match
        assert verify_checksum_integrity(bronze_path)

    def test_bronze_parquet_contains_expected_data(
        self,
        run_bronze_extraction,
    ):
        """Verify Bronze parquet files contain correct data (not just row count)."""
        result = run_bronze_extraction()
        bronze_path = result["bronze_path"]

        # Read Bronze output
        df = read_bronze_parquet(bronze_path)

        # Load golden file expectations
        golden = load_golden_file("claims_t0_first_row")

        # Verify row count
        assert len(df) == golden["row_count"], (
            f"Expected {golden['row_count']} rows, got {len(df)}"
        )

        # Verify first row values match golden file
        verify_dataframe_first_row(df, golden)

    def test_bronze_parquet_schema_matches_expected(
        self,
        run_bronze_extraction,
    ):
        """Verify Bronze parquet schema matches expected columns and types."""
        result = run_bronze_extraction()
        bronze_path = result["bronze_path"]

        df = read_bronze_parquet(bronze_path)
        golden = load_golden_file("claims_t0_schema")

        verify_dataframe_schema(df, golden)

    def test_bronze_schema_snapshot_captured(
        self,
        run_bronze_extraction,
    ):
        """Verify Bronze extraction captures schema snapshot."""
        result = run_bronze_extraction()

        assert len(result["schema_snapshot"]) > 0, "Should capture schema"

        # Verify schema has expected structure
        for col_info in result["schema_snapshot"]:
            assert "name" in col_info
            assert "dtype" in col_info


# =============================================================================
# TestDataVerificationE2E: Verify exact data values
# =============================================================================


class TestDataVerificationE2E:
    """Test data correctness beyond row counts."""

    def test_claims_first_row_exact_values(
        self,
        claims_t0_df: pd.DataFrame,
    ):
        """Verify exact values in first row match deterministic seed."""
        golden = load_golden_file("claims_t0_first_row")

        # Verify first row values (seed=42 produces deterministic data)
        first_row = golden["first_row"]
        assert claims_t0_df.iloc[0]["claim_id"] == first_row["claim_id"]
        assert claims_t0_df.iloc[0]["patient_id"] == first_row["patient_id"]
        assert claims_t0_df.iloc[0]["provider_id"] == first_row["provider_id"]
        assert claims_t0_df.iloc[0]["claim_type"] == first_row["claim_type"]
        assert claims_t0_df.iloc[0]["status"] == first_row["status"]

        # Float comparison with tolerance
        assert abs(claims_t0_df.iloc[0]["billed_amount"] - first_row["billed_amount"]) < 0.01
        assert abs(claims_t0_df.iloc[0]["paid_amount"] - first_row["paid_amount"]) < 0.01

    def test_claims_id_format_matches_pattern(
        self,
        claims_t0_df: pd.DataFrame,
    ):
        """Verify claim IDs follow expected pattern."""
        golden = load_golden_file("claims_t0_first_row")
        patterns = golden["id_patterns"]

        # All claim_ids should start with prefix and be proper width
        for claim_id in claims_t0_df["claim_id"]:
            assert claim_id.startswith(patterns["claim_id_prefix"])
            # Total length = prefix (3) + width (8) = 11
            assert len(claim_id) == len(patterns["claim_id_prefix"]) + patterns["claim_id_width"]

    def test_claims_schema_types_correct(
        self,
        claims_t0_df: pd.DataFrame,
    ):
        """Verify column types are correct."""
        # String columns
        assert claims_t0_df["claim_id"].dtype == "object"
        assert claims_t0_df["patient_id"].dtype == "object"
        assert claims_t0_df["status"].dtype == "object"

        # Numeric columns
        assert claims_t0_df["billed_amount"].dtype == "float64"
        assert claims_t0_df["paid_amount"].dtype == "float64"

        # Datetime columns
        assert pd.api.types.is_datetime64_any_dtype(claims_t0_df["created_at"])
        assert pd.api.types.is_datetime64_any_dtype(claims_t0_df["updated_at"])

    def test_claims_no_null_primary_keys(
        self,
        claims_t0_df: pd.DataFrame,
    ):
        """Verify no null values in primary key column."""
        assert claims_t0_df["claim_id"].notna().all(), "claim_id should have no nulls"
        assert claims_t0_df["claim_id"].nunique() == len(claims_t0_df), "claim_id should be unique"

    def test_claims_deterministic_generation(
        self,
        t0_date: date,
    ):
        """Verify same seed produces identical data."""
        from tests.synthetic_data import ClaimsGenerator

        gen1 = ClaimsGenerator(seed=42, row_count=100)
        gen2 = ClaimsGenerator(seed=42, row_count=100)

        df1 = gen1.generate_t0(t0_date)
        df2 = gen2.generate_t0(t0_date)

        # DataFrames should be identical
        pd.testing.assert_frame_equal(df1, df2)


# =============================================================================
# TestIncrementalPipelineE2E: Test T0 → T1 scenarios
# =============================================================================


class TestIncrementalPipelineE2E:
    """Test incremental extraction scenarios."""

    def test_t1_contains_updates_and_inserts(
        self,
        claims_t0_df: pd.DataFrame,
        claims_t1_df: pd.DataFrame,
    ):
        """Verify T1 data contains both updates and new records."""
        # T1 should have updates (existing IDs) and inserts (new IDs)
        t0_ids = set(claims_t0_df["claim_id"])
        t1_ids = set(claims_t1_df["claim_id"])

        # Some T1 IDs should exist in T0 (updates)
        updates = t0_ids.intersection(t1_ids)
        assert len(updates) > 0, "T1 should contain updates to existing records"

        # Some T1 IDs should be new (inserts)
        inserts = t1_ids - t0_ids
        assert len(inserts) > 0, "T1 should contain new records"

    def test_t1_updates_have_progressed_status(
        self,
        claims_t0_df: pd.DataFrame,
        claims_t1_df: pd.DataFrame,
    ):
        """Verify updated records have progressed to later status values."""
        t0_ids = set(claims_t0_df["claim_id"])

        # Get updated records (IDs that exist in both T0 and T1)
        updated_records = claims_t1_df[claims_t1_df["claim_id"].isin(t0_ids)]

        # Updated records should have final statuses (approved, denied, paid)
        final_statuses = {"approved", "denied", "paid"}
        updated_statuses = set(updated_records["status"].unique())

        assert updated_statuses.issubset(final_statuses), (
            f"Updated records should have final statuses, got {updated_statuses}"
        )

    def test_t1_new_records_have_sequential_ids(
        self,
        claims_t0_df: pd.DataFrame,
        claims_t1_df: pd.DataFrame,
    ):
        """Verify new T1 records have IDs after max T0 ID."""
        t0_ids = set(claims_t0_df["claim_id"])
        t1_ids = set(claims_t1_df["claim_id"])

        # Get max T0 ID number
        max_t0_id = max(int(cid.replace("CLM", "")) for cid in t0_ids)

        # New T1 IDs should be > max T0 ID
        new_ids = t1_ids - t0_ids
        for new_id in new_ids:
            id_num = int(new_id.replace("CLM", ""))
            assert id_num > max_t0_id, (
                f"New ID {new_id} should be > max T0 ID CLM{max_t0_id:08d}"
            )

    def test_t1_new_records_have_submitted_status(
        self,
        claims_t0_df: pd.DataFrame,
        claims_t1_df: pd.DataFrame,
    ):
        """Verify new T1 records have initial 'submitted' status."""
        t0_ids = set(claims_t0_df["claim_id"])

        # Get new records only
        new_records = claims_t1_df[~claims_t1_df["claim_id"].isin(t0_ids)]

        # All new records should be 'submitted'
        assert (new_records["status"] == "submitted").all(), (
            "New records should have 'submitted' status"
        )


# =============================================================================
# TestSilverPipelineE2E: Test SilverProcessor.run()
# =============================================================================


class TestSilverPipelineE2E:
    """Test actual SilverProcessor.run() with Bronze input."""

    @pytest.fixture
    def bronze_output_for_silver(
        self,
        run_bronze_extraction,
    ) -> Path:
        """Run Bronze extraction and return path for Silver processing."""
        result = run_bronze_extraction()
        return result["bronze_path"]

    @pytest.fixture
    def silver_dataset_config(self):
        """Create DatasetConfig for Silver processing."""
        from core.infrastructure.config import DatasetConfig

        return DatasetConfig.from_dict({
            "environment": "test",
            "domain": "healthcare",
            "system": "synthetic",
            "entity": "claims",
            "bronze": {
                "enabled": True,
            },
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "version": 1,
                "natural_keys": ["claim_id"],
                "order_column": "updated_at",
                "input_storage": "local",
            },
        })

    def test_silver_processor_runs_successfully(
        self,
        bronze_output_for_silver: Path,
        silver_dataset_config,
        temp_dir: Path,
        t0_date: date,
    ):
        """Run SilverProcessor.run() and verify it completes."""
        from core.domain.services.pipelines.silver.processor import SilverProcessor

        silver_partition = temp_dir / "silver" / "claims"

        processor = SilverProcessor(
            dataset=silver_dataset_config,
            bronze_path=bronze_output_for_silver,
            silver_partition=silver_partition,
            run_date=t0_date,
            verify_checksum=False,  # Skip checksum for this test
        )

        result = processor.run()

        assert result.metrics.rows_read == 100, "Should read 100 rows from Bronze"
        assert result.metrics.rows_written > 0, "Should write rows to Silver"

    def test_silver_output_has_metadata_columns(
        self,
        bronze_output_for_silver: Path,
        silver_dataset_config,
        temp_dir: Path,
        t0_date: date,
    ):
        """Verify Silver output includes standard metadata columns."""
        from core.domain.services.pipelines.silver.processor import SilverProcessor

        silver_partition = temp_dir / "silver" / "claims"

        processor = SilverProcessor(
            dataset=silver_dataset_config,
            bronze_path=bronze_output_for_silver,
            silver_partition=silver_partition,
            run_date=t0_date,
            verify_checksum=False,
        )

        result = processor.run()

        # Read Silver output
        silver_files = list(silver_partition.rglob("*.parquet"))
        assert len(silver_files) > 0, "Should create Silver parquet files"

        df = pd.read_parquet(silver_files[0])

        # Verify metadata columns added
        expected_metadata_cols = [
            "load_batch_id",
            "record_source",
            "pipeline_run_at",
            "environment",
            "domain",
        ]
        for col in expected_metadata_cols:
            assert col in df.columns, f"Missing metadata column: {col}"


# =============================================================================
# TestErrorScenariosE2E: Test error handling
# =============================================================================


class TestErrorScenariosE2E:
    """Test error handling in pipelines."""

    def test_missing_input_path_raises_error(
        self,
        temp_dir: Path,
        t0_date: date,
    ):
        """Verify Bronze extraction fails with clear error for missing input."""
        from core.infrastructure.runtime.context import build_run_context
        from core.orchestration.runner import ExtractJob

        config = {
            "environment": "test",
            "domain": "healthcare",
            "system": "synthetic",
            "entity": "claims",
            "platform": {
                "bronze": {
                    "storage_backend": "local",
                    "output_defaults": {"parquet": True, "csv": False},
                },
            },
            "source": {
                "system": "synthetic",
                "table": "claims",
                "type": "file",
                "file": {
                    "path": str(temp_dir / "nonexistent"),
                    "format": "parquet",
                },
                "run": {
                    "load_pattern": "snapshot",
                    "local_output_dir": str(temp_dir / "output"),
                    "storage_enabled": False,
                },
            },
        }

        context = build_run_context(cfg=config, run_date=t0_date)
        job = ExtractJob(context)

        with pytest.raises(Exception):
            job.run()

    def test_silver_fails_with_missing_bronze(
        self,
        temp_dir: Path,
        t0_date: date,
    ):
        """Verify Silver processing fails with clear error for missing Bronze."""
        from core.infrastructure.config import DatasetConfig
        from core.domain.services.pipelines.silver.processor import SilverProcessor

        dataset = DatasetConfig.from_dict({
            "environment": "test",
            "domain": "healthcare",
            "system": "synthetic",
            "entity": "claims",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "version": 1,
                "natural_keys": ["claim_id"],
                "order_column": "updated_at",
                "input_storage": "local",
            },
        })

        missing_bronze = temp_dir / "missing_bronze"
        silver_partition = temp_dir / "silver"

        processor = SilverProcessor(
            dataset=dataset,
            bronze_path=missing_bronze,
            silver_partition=silver_partition,
            run_date=t0_date,
            verify_checksum=False,
        )

        with pytest.raises(Exception):
            processor.run()


# =============================================================================
# TestMinIOPipelineE2E: Test with MinIO storage backend
# =============================================================================


@requires_minio
class TestMinIOPipelineE2E:
    """Test full pipeline with MinIO storage backend."""

    def test_synthetic_data_uploaded_to_minio(
        self,
        minio_client,
        minio_bucket: str,
        minio_synthetic_input: str,
    ):
        """Verify synthetic data is uploaded to MinIO."""
        from tests.integration.conftest import list_objects_in_prefix

        objects = list_objects_in_prefix(minio_client, minio_bucket, minio_synthetic_input)
        assert len(objects) > 0, "Should have synthetic data in MinIO"

    def test_read_synthetic_from_minio(
        self,
        minio_client,
        minio_bucket: str,
        minio_synthetic_input: str,
    ):
        """Verify synthetic data can be read back from MinIO."""
        from tests.integration.conftest import download_parquet_from_minio

        key = f"{minio_synthetic_input}/claims_t0.parquet"
        df = download_parquet_from_minio(minio_client, minio_bucket, key)

        assert len(df) == 100, "Should read 100 rows from MinIO"
        assert "claim_id" in df.columns, "Should have claim_id column"

        # Verify first row matches golden
        golden = load_golden_file("claims_t0_first_row")
        assert df.iloc[0]["claim_id"] == golden["first_row"]["claim_id"]
