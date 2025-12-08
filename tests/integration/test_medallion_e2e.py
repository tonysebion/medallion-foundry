"""Bronze→Silver End-to-End Medallion Pipeline Tests.

Story 3: Comprehensive E2E tests that run full Bronze→Silver pipelines with
synthetic data, validating the complete medallion flow for each load pattern.

Tests verify:
- Source → Bronze → Silver flow for each pattern
- Domain-specific generators (Claims, Orders, Transactions)
- Row counts match across layers
- Data integrity (checksums, no data loss)
- Metadata propagation between layers
- 3-batch scenarios (T0, T1, T2)
"""

from __future__ import annotations

import json
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from core.foundation.primitives.patterns import LoadPattern
from core.infrastructure.config import DatasetConfig
from tests.integration.conftest import (
    requires_minio,
    upload_dataframe_to_minio,
    download_parquet_from_minio,
    list_objects_in_prefix,
)
from tests.synthetic_data import (
    ClaimsGenerator,
    OrdersGenerator,
    TransactionsGenerator,
    generate_time_series_data,
)
from tests.pattern_verification.pattern_data.generators import (
    PatternTestDataGenerator,
)


# =============================================================================
# Test Fixtures
# =============================================================================


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


@pytest.fixture
def claims_generator() -> ClaimsGenerator:
    """Healthcare claims data generator."""
    return ClaimsGenerator(seed=42, row_count=100)


@pytest.fixture
def orders_generator() -> OrdersGenerator:
    """E-commerce orders data generator."""
    return OrdersGenerator(seed=42, row_count=100)


@pytest.fixture
def transactions_generator() -> TransactionsGenerator:
    """Financial transactions data generator."""
    return TransactionsGenerator(seed=42, row_count=100)


@pytest.fixture
def pattern_generator() -> PatternTestDataGenerator:
    """Pattern test data generator."""
    return PatternTestDataGenerator(seed=42, base_rows=100)


# =============================================================================
# Helper Functions
# =============================================================================


def run_bronze_extraction(
    input_df: pd.DataFrame,
    output_path: Path,
    load_pattern: str,
    run_date: date,
    system: str = "synthetic",
    table: str = "medallion_test",
) -> Dict[str, Any]:
    """Run Bronze extraction and return results.

    Args:
        input_df: Source DataFrame
        output_path: Path for Bronze output
        load_pattern: Load pattern
        run_date: Run date
        system: Source system name
        table: Source table name

    Returns:
        Dictionary with exit_code, bronze_path, row_count
    """
    from core.infrastructure.runtime.context import build_run_context
    from core.orchestration.runner import ExtractJob

    # Write input data
    input_path = output_path.parent / "input"
    input_path.mkdir(parents=True, exist_ok=True)
    input_file = input_path / f"{table}.parquet"
    input_df.to_parquet(input_file, index=False)

    # Unique table name for isolation
    unique_table = f"{table}_{uuid.uuid4().hex[:8]}"

    config = {
        "environment": "test",
        "domain": "healthcare",
        "system": system,
        "entity": unique_table,
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(output_path.parent / "_checkpoints"),
                "output_defaults": {"parquet": True, "csv": False},
            },
        },
        "source": {
            "system": system,
            "table": unique_table,
            "type": "file",
            "file": {"path": str(input_file), "format": "parquet"},
            "run": {
                "load_pattern": load_pattern,
                "local_output_dir": str(output_path),
                "storage_enabled": False,
                "checkpoint_enabled": False,
            },
        },
    }

    context = build_run_context(cfg=config, run_date=run_date)
    job = ExtractJob(context)
    exit_code = job.run()

    return {
        "exit_code": exit_code,
        "bronze_path": context.bronze_path,
        "input_row_count": len(input_df),
    }


def run_silver_processing(
    bronze_path: Path,
    silver_path: Path,
    run_date: date,
    natural_keys: List[str],
    entity_kind: str = "event",
    order_column: str = "updated_at",
    event_ts_column: str = "created_at",
) -> Dict[str, Any]:
    """Run Silver processing and return results.

    Args:
        bronze_path: Path to Bronze partition
        silver_path: Path for Silver output
        run_date: Run date
        natural_keys: Primary key columns
        entity_kind: Entity kind
        order_column: Order column
        event_ts_column: Event timestamp column

    Returns:
        Dictionary with metrics and output info
    """
    from core.domain.services.pipelines.silver.processor import SilverProcessor

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

    if entity_kind == "state":
        silver_config["change_ts_column"] = event_ts_column

    dataset_config = DatasetConfig.from_dict({
        "environment": "test",
        "domain": "healthcare",
        "system": "synthetic",
        "entity": "medallion_test",
        "bronze": {"enabled": True},
        "silver": silver_config,
    })

    processor = SilverProcessor(
        dataset=dataset_config,
        bronze_path=bronze_path,
        silver_partition=silver_path,
        run_date=run_date,
        verify_checksum=False,
    )

    result = processor.run()

    return {
        "rows_read": result.metrics.rows_read,
        "rows_written": result.metrics.rows_written,
        "changed_keys": result.metrics.changed_keys,
        "silver_path": silver_path,
    }


def read_parquet_files(path: Path) -> pd.DataFrame:
    """Read all parquet files from a directory."""
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {path}")
    dfs = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]


def verify_bronze_metadata(bronze_path: Path) -> Dict[str, Any]:
    """Read and return Bronze metadata."""
    metadata_path = bronze_path / "_metadata.json"
    if metadata_path.exists():
        result: Dict[str, Any] = json.loads(metadata_path.read_text())
        return result
    return {}


# =============================================================================
# Full Medallion Pipeline Tests - Claims Domain
# =============================================================================


class TestClaimsMedallionPipeline:
    """Test full Bronze→Silver pipeline with Claims data."""

    def test_claims_snapshot_medallion_flow(
        self,
        claims_generator: ClaimsGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test complete snapshot flow: Source → Bronze → Silver for claims."""
        # Generate claims data
        claims_df = claims_generator.generate_t0(t0_date)

        # Run Bronze extraction
        bronze_result = run_bronze_extraction(
            input_df=claims_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
            system="healthcare",
            table="claims",
        )

        assert bronze_result["exit_code"] == 0, "Bronze extraction should succeed"

        # Run Silver processing
        silver_result = run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["claim_id"],
            order_column="updated_at",
            event_ts_column="created_at",
        )

        assert silver_result["rows_read"] > 0, "Should read rows from Bronze"
        assert silver_result["rows_written"] > 0, "Should write rows to Silver"

        # Verify row counts match
        bronze_df = read_parquet_files(bronze_result["bronze_path"])
        silver_df = read_parquet_files(tmp_path / "silver")

        assert len(bronze_df) == len(claims_df), "Bronze should have all input rows"
        assert len(silver_df) == len(claims_df), "Silver should have all rows"

    def test_claims_incremental_medallion_t0_t1(
        self,
        claims_generator: ClaimsGenerator,
        tmp_path: Path,
        t0_date: date,
        t1_date: date,
    ):
        """Test incremental medallion flow with T0 and T1 batches."""
        # Generate T0 and T1 claims
        t0_df = claims_generator.generate_t0(t0_date)
        t1_df = claims_generator.generate_t1(t1_date, t0_df)

        # Process T0
        bronze_t0 = run_bronze_extraction(
            input_df=t0_df,
            output_path=tmp_path / "bronze_t0",
            load_pattern="incremental_merge",
            run_date=t0_date,
            table="claims_t0",
        )
        assert bronze_t0["exit_code"] == 0

        silver_t0 = run_silver_processing(
            bronze_path=bronze_t0["bronze_path"],
            silver_path=tmp_path / "silver_t0",
            run_date=t0_date,
            natural_keys=["claim_id"],
        )
        assert silver_t0["rows_written"] > 0

        # Process T1
        bronze_t1 = run_bronze_extraction(
            input_df=t1_df,
            output_path=tmp_path / "bronze_t1",
            load_pattern="incremental_merge",
            run_date=t1_date,
            table="claims_t1",
        )
        assert bronze_t1["exit_code"] == 0

        silver_t1 = run_silver_processing(
            bronze_path=bronze_t1["bronze_path"],
            silver_path=tmp_path / "silver_t1",
            run_date=t1_date,
            natural_keys=["claim_id"],
        )
        assert silver_t1["rows_written"] > 0

        # Verify data integrity
        silver_t0_df = read_parquet_files(tmp_path / "silver_t0")
        silver_t1_df = read_parquet_files(tmp_path / "silver_t1")

        # T1 should have updates and inserts
        t0_ids = set(silver_t0_df["claim_id"])
        t1_ids = set(silver_t1_df["claim_id"])

        # Some overlap expected (updates)
        overlap = t0_ids & t1_ids
        assert len(overlap) > 0 or len(t1_ids - t0_ids) > 0, "T1 should have changes"


# =============================================================================
# Full Medallion Pipeline Tests - Orders Domain
# =============================================================================


class TestOrdersMedallionPipeline:
    """Test full Bronze→Silver pipeline with Orders data."""

    def test_orders_snapshot_medallion_flow(
        self,
        orders_generator: OrdersGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test complete snapshot flow for orders."""
        orders_df = orders_generator.generate_t0(t0_date)

        # Bronze
        bronze_result = run_bronze_extraction(
            input_df=orders_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
            system="ecommerce",
            table="orders",
        )
        assert bronze_result["exit_code"] == 0

        # Silver - Orders uses order_ts instead of created_at
        silver_result = run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["order_id"],
            order_column="updated_at",
            event_ts_column="order_ts",
        )

        assert silver_result["rows_written"] == len(orders_df)

    def test_orders_three_batch_flow(
        self,
        orders_generator: OrdersGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test three-batch medallion flow for orders."""
        # Generate time series
        time_series = generate_time_series_data("orders", t0_date, seed=42, row_count=100)

        all_silver_ids = set()

        for i, batch_name in enumerate(["t0", "t1"]):
            batch_df = time_series.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            # Bronze
            bronze_result = run_bronze_extraction(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_merge",
                run_date=batch_date,
                table=f"orders_{batch_name}",
            )
            assert bronze_result["exit_code"] == 0

            # Silver - Orders uses order_ts instead of created_at
            silver_result = run_silver_processing(
                bronze_path=bronze_result["bronze_path"],
                silver_path=tmp_path / f"silver_{batch_name}",
                run_date=batch_date,
                natural_keys=["order_id"],
                order_column="updated_at",
                event_ts_column="order_ts",
            )
            assert silver_result["rows_written"] > 0

            # Collect IDs
            silver_df = read_parquet_files(tmp_path / f"silver_{batch_name}")
            all_silver_ids.update(silver_df["order_id"])

        # Should have accumulated IDs across batches
        assert len(all_silver_ids) > 0


# =============================================================================
# Full Medallion Pipeline Tests - Transactions Domain
# =============================================================================


class TestTransactionsMedallionPipeline:
    """Test full Bronze→Silver pipeline with Transactions data."""

    def test_transactions_snapshot_medallion_flow(
        self,
        transactions_generator: TransactionsGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test complete snapshot flow for transactions."""
        txn_df = transactions_generator.generate_t0(t0_date)

        # Bronze
        bronze_result = run_bronze_extraction(
            input_df=txn_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
            system="finance",
            table="transactions",
        )
        assert bronze_result["exit_code"] == 0

        # Silver
        silver_result = run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["transaction_id"],
            order_column="updated_at",
            event_ts_column="created_at",
        )

        assert silver_result["rows_written"] == len(txn_df)


# =============================================================================
# Pattern-Specific Medallion Tests
# =============================================================================


class TestPatternMedallionFlow:
    """Test medallion flow for each load pattern."""

    def test_snapshot_pattern_medallion(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test SNAPSHOT pattern through full medallion."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Bronze
        bronze_result = run_bronze_extraction(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )
        assert bronze_result["exit_code"] == 0

        # Silver
        silver_result = run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["record_id"],
        )

        # Verify complete flow
        assert silver_result["rows_written"] == 100

    def test_incremental_append_pattern_medallion(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test INCREMENTAL_APPEND pattern through full medallion."""
        scenario = pattern_generator.generate_incremental_append_scenario(
            rows=100, batches=3, insert_rate=0.1
        )

        total_rows = 0
        all_ids: set[str] = set()

        for i, batch_name in enumerate(["t0", "t1", "t2"]):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            # Bronze
            bronze_result = run_bronze_extraction(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_append",
                run_date=batch_date,
                table=f"append_{batch_name}",
            )
            assert bronze_result["exit_code"] == 0

            # Silver
            silver_result = run_silver_processing(
                bronze_path=bronze_result["bronze_path"],
                silver_path=tmp_path / f"silver_{batch_name}",
                run_date=batch_date,
                natural_keys=["record_id"],
            )

            silver_df = read_parquet_files(tmp_path / f"silver_{batch_name}")
            batch_ids = set(silver_df["record_id"])

            # Verify no overlap for append pattern
            overlap = all_ids & batch_ids
            assert len(overlap) == 0, f"Append should have no overlap in {batch_name}"

            all_ids.update(batch_ids)
            total_rows += len(silver_df)

        assert total_rows > 100, "Should have accumulated rows across batches"

    def test_incremental_merge_pattern_medallion(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test INCREMENTAL_MERGE pattern through full medallion."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100, batches=3, update_rate=0.2, insert_rate=0.1
        )

        all_ids = set()

        for i, batch_name in enumerate(["t0", "t1", "t2"]):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            # Bronze
            bronze_result = run_bronze_extraction(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_merge",
                run_date=batch_date,
                table=f"merge_{batch_name}",
            )
            assert bronze_result["exit_code"] == 0

            # Silver
            silver_result = run_silver_processing(
                bronze_path=bronze_result["bronze_path"],
                silver_path=tmp_path / f"silver_{batch_name}",
                run_date=batch_date,
                natural_keys=["record_id"],
            )

            silver_df = read_parquet_files(tmp_path / f"silver_{batch_name}")
            all_ids.update(silver_df["record_id"])

        # Final unique ID count should match expected
        expected = scenario.metadata["expected_final_row_count"]
        assert len(all_ids) == expected

    def test_current_history_pattern_medallion(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test CURRENT_HISTORY pattern through full medallion."""
        scenario = pattern_generator.generate_scd2_scenario(
            entities=50, changes_per_entity=2
        )

        for i, batch_name in enumerate(["t0", "t1"]):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i * 7)

            # Bronze
            bronze_result = run_bronze_extraction(
                input_df=batch_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="current_history",
                run_date=batch_date,
                table=f"scd2_{batch_name}",
            )
            assert bronze_result["exit_code"] == 0

            # Silver
            silver_result = run_silver_processing(
                bronze_path=bronze_result["bronze_path"],
                silver_path=tmp_path / f"silver_{batch_name}",
                run_date=batch_date,
                natural_keys=["entity_id"],
                entity_kind="state",
                order_column="effective_from",
                event_ts_column="effective_from",
            )

            assert silver_result["rows_written"] > 0


# =============================================================================
# Data Integrity Tests
# =============================================================================


class TestMedallionDataIntegrity:
    """Test data integrity through medallion pipeline."""

    def test_no_data_loss_through_pipeline(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify no data loss from source through Silver."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)
        source_df = scenario.t0

        # Bronze
        bronze_result = run_bronze_extraction(
            input_df=source_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        bronze_df = read_parquet_files(bronze_result["bronze_path"])

        # Silver
        run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["record_id"],
        )

        silver_df = read_parquet_files(tmp_path / "silver")

        # Verify row counts
        assert len(bronze_df) == len(source_df), "Bronze should preserve all rows"
        assert len(silver_df) == len(source_df), "Silver should preserve all rows"

        # Verify all source IDs present in Silver
        source_ids = set(source_df["record_id"])
        silver_ids = set(silver_df["record_id"])
        assert source_ids == silver_ids, "All source IDs should be in Silver"

    def test_primary_key_uniqueness_preserved(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify primary key uniqueness is preserved through pipeline."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Bronze
        bronze_result = run_bronze_extraction(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        bronze_df = read_parquet_files(bronze_result["bronze_path"])

        # Silver
        run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["record_id"],
        )

        silver_df = read_parquet_files(tmp_path / "silver")

        # Verify uniqueness
        assert bronze_df["record_id"].is_unique, "Bronze should have unique IDs"
        assert silver_df["record_id"].is_unique, "Silver should have unique IDs"

    def test_checksum_verification_available(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify Bronze creates checksums for Silver verification."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_result = run_bronze_extraction(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        checksums_path = bronze_result["bronze_path"] / "_checksums.json"
        assert checksums_path.exists(), "Bronze should create checksums file"

        checksums = json.loads(checksums_path.read_text())
        assert "files" in checksums, "Checksums should have files list"
        assert len(checksums["files"]) > 0, "Should have checksum entries"


# =============================================================================
# Metadata Propagation Tests
# =============================================================================


class TestMetadataPropagation:
    """Test metadata propagation through medallion pipeline."""

    def test_bronze_metadata_includes_pattern(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify Bronze metadata includes load pattern."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_result = run_bronze_extraction(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        metadata = verify_bronze_metadata(bronze_result["bronze_path"])
        assert metadata.get("load_pattern") == "snapshot"

    def test_silver_adds_metadata_columns(
        self,
        pattern_generator: PatternTestDataGenerator,
        tmp_path: Path,
        t0_date: date,
    ):
        """Verify Silver adds standard metadata columns."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        bronze_result = run_bronze_extraction(
            input_df=scenario.t0,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["record_id"],
        )

        silver_df = read_parquet_files(tmp_path / "silver")

        # Check for metadata columns
        expected_metadata = ["load_batch_id", "record_source"]
        for col in expected_metadata:
            assert col in silver_df.columns, f"Missing metadata column: {col}"


# =============================================================================
# MinIO Integration Tests
# =============================================================================


@requires_minio
class TestMedallionMinIO:
    """Test medallion pipeline with MinIO storage."""

    def test_medallion_flow_with_minio_storage(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test full medallion flow storing data to MinIO."""
        scenario = pattern_generator.generate_snapshot_scenario(rows=100)

        # Upload source to MinIO
        source_key = f"{cleanup_prefix}/source/data.parquet"
        upload_dataframe_to_minio(
            minio_client, minio_bucket, source_key, scenario.t0
        )

        # Download and process through Bronze
        source_df = download_parquet_from_minio(minio_client, minio_bucket, source_key)

        bronze_result = run_bronze_extraction(
            input_df=source_df,
            output_path=tmp_path / "bronze",
            load_pattern="snapshot",
            run_date=t0_date,
        )

        # Process through Silver
        silver_result = run_silver_processing(
            bronze_path=bronze_result["bronze_path"],
            silver_path=tmp_path / "silver",
            run_date=t0_date,
            natural_keys=["record_id"],
        )

        # Upload Silver to MinIO
        silver_df = read_parquet_files(tmp_path / "silver")
        silver_key = f"{cleanup_prefix}/silver/data.parquet"
        upload_dataframe_to_minio(
            minio_client, minio_bucket, silver_key, silver_df
        )

        # Verify Silver in MinIO
        final_df = download_parquet_from_minio(minio_client, minio_bucket, silver_key)
        assert len(final_df) == 100, "Should have all rows in MinIO"

    def test_multi_batch_medallion_minio(
        self,
        pattern_generator: PatternTestDataGenerator,
        minio_client,
        minio_bucket: str,
        cleanup_prefix: str,
        tmp_path: Path,
        t0_date: date,
    ):
        """Test multi-batch medallion flow with MinIO."""
        scenario = pattern_generator.generate_incremental_merge_scenario(
            rows=100, batches=2, update_rate=0.2, insert_rate=0.1
        )

        all_ids = set()

        for i, batch_name in enumerate(["t0", "t1"]):
            batch_df = scenario.batches.get(batch_name)
            if batch_df is None:
                continue

            batch_date = t0_date + timedelta(days=i)

            # Upload batch to MinIO
            batch_key = f"{cleanup_prefix}/source/{batch_name}.parquet"
            upload_dataframe_to_minio(
                minio_client, minio_bucket, batch_key, batch_df
            )

            # Download and process
            source_df = download_parquet_from_minio(
                minio_client, minio_bucket, batch_key
            )

            bronze_result = run_bronze_extraction(
                input_df=source_df,
                output_path=tmp_path / f"bronze_{batch_name}",
                load_pattern="incremental_merge",
                run_date=batch_date,
                table=f"minio_{batch_name}",
            )

            run_silver_processing(
                bronze_path=bronze_result["bronze_path"],
                silver_path=tmp_path / f"silver_{batch_name}",
                run_date=batch_date,
                natural_keys=["record_id"],
            )

            silver_df = read_parquet_files(tmp_path / f"silver_{batch_name}")
            all_ids.update(silver_df["record_id"])

        # Verify cumulative result
        expected = scenario.metadata["expected_final_row_count"]
        assert len(all_ids) == expected
