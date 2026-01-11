"""Tests for concurrent pipeline execution isolation.

Verifies that multiple pipeline instances can run simultaneously
without interfering with each other's:
- Output partitions
- Metadata files
- Checksum manifests

These tests ensure that pipelines are safe to run in parallel
(e.g., different entities, different dates) without data corruption
or state contamination.

Note: These tests run Bronze/Silver pipelines sequentially but verify
that their outputs are properly isolated and don't interfere with each other.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import SilverEntity


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    """Create sample orders data."""
    return pd.DataFrame(
        {
            "order_id": ["ORD001", "ORD002", "ORD003"],
            "customer_id": ["CUST01", "CUST02", "CUST01"],
            "order_total": [150.00, 89.99, 225.50],
            "status": ["completed", "pending", "shipped"],
            "updated_at": [
                "2025-01-15T10:00:00",
                "2025-01-15T11:30:00",
                "2025-01-15T14:00:00",
            ],
        }
    )


@pytest.fixture
def sample_customers_df() -> pd.DataFrame:
    """Create sample customers data."""
    return pd.DataFrame(
        {
            "customer_id": ["CUST01", "CUST02", "CUST03"],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "updated_at": [
                "2025-01-15T09:00:00",
                "2025-01-15T10:00:00",
                "2025-01-15T11:00:00",
            ],
        }
    )


@pytest.fixture
def orders_csv_files(tmp_path: Path, sample_orders_df: pd.DataFrame) -> Dict[str, Path]:
    """Create orders CSV files for multiple dates."""
    files = {}
    for run_date in ["2025-01-15", "2025-01-16", "2025-01-17"]:
        file_path = tmp_path / f"orders_{run_date}.csv"
        # Modify data slightly for each date
        df = sample_orders_df.copy()
        df["updated_at"] = df["updated_at"].str.replace("2025-01-15", run_date)
        df.to_csv(file_path, index=False)
        files[run_date] = file_path
    return files


@pytest.fixture
def customers_csv_files(
    tmp_path: Path, sample_customers_df: pd.DataFrame
) -> Dict[str, Path]:
    """Create customers CSV files for multiple dates."""
    files = {}
    for run_date in ["2025-01-15", "2025-01-16", "2025-01-17"]:
        file_path = tmp_path / f"customers_{run_date}.csv"
        # Modify data slightly for each date
        df = sample_customers_df.copy()
        df["updated_at"] = df["updated_at"].str.replace("2025-01-15", run_date)
        df.to_csv(file_path, index=False)
        files[run_date] = file_path
    return files


# =============================================================================
# Test: Isolation of Different Entities
# =============================================================================


class TestEntityIsolation:
    """Tests for isolation between different entities."""

    def test_different_entities_isolated(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
        customers_csv_files: Dict[str, Path],
    ):
        """Two pipelines for different entities produce isolated outputs.

        Runs orders and customers pipelines for the same date.
        Verifies both complete successfully with no cross-contamination.
        """
        run_date = "2025-01-15"
        bronze_base = tmp_path / "bronze"
        bronze_base.mkdir(parents=True, exist_ok=True)

        # Run orders pipeline
        orders_bronze = BronzeSource(
            system="sales",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(orders_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        orders_result = orders_bronze.run(run_date)
        assert orders_result.get("row_count", 0) > 0, "Orders pipeline failed"

        # Run customers pipeline
        customers_bronze = BronzeSource(
            system="crm",
            entity="customers",
            source_type=SourceType.FILE_CSV,
            source_path=str(customers_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        customers_result = customers_bronze.run(run_date)
        assert customers_result.get("row_count", 0) > 0, "Customers pipeline failed"

        # Verify output directories are separate
        orders_dir = bronze_base / "system=sales" / "entity=orders" / f"dt={run_date}"
        customers_dir = (
            bronze_base / "system=crm" / "entity=customers" / f"dt={run_date}"
        )

        assert orders_dir.exists(), "Orders output directory not created"
        assert customers_dir.exists(), "Customers output directory not created"

        # Verify data files exist and contain correct row counts
        orders_files = list(orders_dir.glob("*.parquet"))
        customers_files = list(customers_dir.glob("*.parquet"))

        assert len(orders_files) > 0, "No orders parquet files created"
        assert len(customers_files) > 0, "No customers parquet files created"

        # Verify row counts match expected
        orders_df = pd.read_parquet(orders_files[0])
        customers_df = pd.read_parquet(customers_files[0])

        assert len(orders_df) == 3, f"Wrong orders row count: {len(orders_df)}"
        assert len(customers_df) == 3, f"Wrong customers row count: {len(customers_df)}"

        # Verify metadata files are separate and have correct content
        orders_metadata = orders_dir / "_metadata.json"
        customers_metadata = customers_dir / "_metadata.json"

        assert orders_metadata.exists(), "Orders metadata not created"
        assert customers_metadata.exists(), "Customers metadata not created"

        with open(orders_metadata) as f:
            orders_meta = json.load(f)
        with open(customers_metadata) as f:
            customers_meta = json.load(f)

        assert orders_meta.get("entity") == "orders"
        assert customers_meta.get("entity") == "customers"
        assert orders_meta.get("system") == "sales"
        assert customers_meta.get("system") == "crm"


# =============================================================================
# Test: Isolation of Different Dates (Same Entity)
# =============================================================================


class TestDateIsolation:
    """Tests for isolation between different dates for the same entity."""

    def test_different_dates_isolated(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
    ):
        """Same entity, different dates produce isolated partitions.

        Runs orders pipeline for three different dates.
        Verifies all partitions are created correctly without interference.
        """
        bronze_base = tmp_path / "bronze"
        bronze_base.mkdir(parents=True, exist_ok=True)
        dates = ["2025-01-15", "2025-01-16", "2025-01-17"]

        # Run pipeline for each date
        for run_date in dates:
            bronze = BronzeSource(
                system="sales",
                entity="orders",
                source_type=SourceType.FILE_CSV,
                source_path=str(orders_csv_files[run_date]),
                target_path=str(
                    bronze_base
                    / "system={system}"
                    / "entity={entity}"
                    / "dt={run_date}"
                ),
            )
            result = bronze.run(run_date)
            assert result.get("row_count", 0) > 0, f"Pipeline failed for {run_date}"

        # Verify each date has its own partition
        for run_date in dates:
            partition_dir = (
                bronze_base / "system=sales" / "entity=orders" / f"dt={run_date}"
            )
            assert partition_dir.exists(), f"Partition not created for {run_date}"

            parquet_files = list(partition_dir.glob("*.parquet"))
            assert len(parquet_files) > 0, f"No parquet files for {run_date}"

            metadata_file = partition_dir / "_metadata.json"
            assert metadata_file.exists(), f"Metadata not created for {run_date}"

            # Verify metadata has correct date
            with open(metadata_file) as f:
                meta = json.load(f)
            # The metadata should indicate this is for this specific run
            assert (
                meta.get("written_at") is not None and meta.get("run_date") == run_date
            )

    def test_rerun_same_date_idempotent(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
    ):
        """Same entity, same date should be idempotent.

        Runs the same pipeline twice for the same date.
        Verifies second run produces identical output without duplicates.
        """
        run_date = "2025-01-15"
        bronze_base = tmp_path / "bronze"
        bronze_base.mkdir(parents=True, exist_ok=True)

        bronze = BronzeSource(
            system="sales",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(orders_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )

        # Run first time
        result1 = bronze.run(run_date)
        assert result1.get("row_count", 0) > 0

        # Read first output
        partition_dir = (
            bronze_base / "system=sales" / "entity=orders" / f"dt={run_date}"
        )
        first_files = sorted(partition_dir.glob("*.parquet"))
        first_df = pd.read_parquet(first_files[0])
        first_row_count = len(first_df)

        # Run second time (same date)
        result2 = bronze.run(run_date)
        assert result2.get("row_count", 0) > 0

        # Read second output
        second_files = sorted(partition_dir.glob("*.parquet"))
        second_df = pd.read_parquet(second_files[0])
        second_row_count = len(second_df)

        # Verify idempotency: same row count (no duplicates)
        assert first_row_count == second_row_count, (
            f"Row count changed: {first_row_count} -> {second_row_count}. "
            "Pipeline should be idempotent for same date."
        )


# =============================================================================
# Test: Checksum and Metadata Isolation
# =============================================================================


class TestArtifactIsolation:
    """Tests for output artifact isolation (checksums, metadata)."""

    def test_checksum_isolation(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
    ):
        """Checksums are written correctly per partition.

        Runs two dates and verifies each has its own correct checksums.
        """
        bronze_base = tmp_path / "bronze"
        bronze_base.mkdir(parents=True, exist_ok=True)
        dates = ["2025-01-15", "2025-01-16"]

        for run_date in dates:
            bronze = BronzeSource(
                system="sales",
                entity="orders",
                source_type=SourceType.FILE_CSV,
                source_path=str(orders_csv_files[run_date]),
                target_path=str(
                    bronze_base
                    / "system={system}"
                    / "entity={entity}"
                    / "dt={run_date}"
                ),
            )
            result = bronze.run(run_date)
            assert result.get("row_count", 0) > 0

        # Verify each partition has its own checksum file
        for run_date in dates:
            partition_dir = (
                bronze_base / "system=sales" / "entity=orders" / f"dt={run_date}"
            )
            checksums_file = partition_dir / "_checksums.json"

            assert checksums_file.exists(), f"Checksums not created for {run_date}"

            # Read and verify checksums exist
            with open(checksums_file) as f:
                checksums = json.load(f)

            # Should have files entry
            assert "files" in checksums or len(checksums) > 0, (
                f"Empty checksums for {run_date}"
            )

    def test_metadata_isolation(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
        customers_csv_files: Dict[str, Path],
    ):
        """Metadata files are isolated per entity and partition."""
        bronze_base = tmp_path / "bronze"
        bronze_base.mkdir(parents=True, exist_ok=True)
        run_date = "2025-01-15"

        # Run both pipelines
        orders_bronze = BronzeSource(
            system="sales",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(orders_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        orders_result = orders_bronze.run(run_date)
        assert orders_result.get("row_count", 0) > 0

        customers_bronze = BronzeSource(
            system="crm",
            entity="customers",
            source_type=SourceType.FILE_CSV,
            source_path=str(customers_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        customers_result = customers_bronze.run(run_date)
        assert customers_result.get("row_count", 0) > 0

        # Verify metadata files exist and have correct content
        orders_metadata_path = (
            bronze_base
            / "system=sales"
            / "entity=orders"
            / f"dt={run_date}"
            / "_metadata.json"
        )
        customers_metadata_path = (
            bronze_base
            / "system=crm"
            / "entity=customers"
            / f"dt={run_date}"
            / "_metadata.json"
        )

        with open(orders_metadata_path) as f:
            orders_metadata = json.load(f)
        with open(customers_metadata_path) as f:
            customers_metadata = json.load(f)

        # Verify metadata references correct entity
        assert orders_metadata.get("entity") == "orders"
        assert customers_metadata.get("entity") == "customers"
        assert orders_metadata.get("system") == "sales"
        assert customers_metadata.get("system") == "crm"


# =============================================================================
# Test: Silver Layer Isolation
# =============================================================================


class TestSilverLayerIsolation:
    """Tests for Silver layer output isolation."""

    def test_silver_different_entities_isolated(
        self,
        tmp_path: Path,
        orders_csv_files: Dict[str, Path],
        customers_csv_files: Dict[str, Path],
    ):
        """Silver pipelines for different entities produce isolated outputs.

        Creates Bronze data first, then runs Silver for orders and customers.
        Verifies both complete without interference.
        """
        run_date = "2025-01-15"
        bronze_base = tmp_path / "bronze"
        silver_base = tmp_path / "silver"

        # Create Bronze data first
        orders_bronze = BronzeSource(
            system="sales",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(orders_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        orders_bronze.run(run_date)

        customers_bronze = BronzeSource(
            system="crm",
            entity="customers",
            source_type=SourceType.FILE_CSV,
            source_path=str(customers_csv_files[run_date]),
            target_path=str(
                bronze_base / "system={system}" / "entity={entity}" / "dt={run_date}"
            ),
        )
        customers_bronze.run(run_date)

        # Run Silver for orders
        orders_silver = SilverEntity(
            source_path=str(
                bronze_base
                / "system=sales"
                / "entity=orders"
                / f"dt={run_date}"
                / "*.parquet"
            ),
            target_path=str(
                silver_base / "domain=sales" / "subject=orders" / "dt={run_date}"
            ),
            domain="sales",
            subject="orders",
            unique_columns=["order_id"],
            last_updated_column="updated_at",
        )
        orders_silver_result = orders_silver.run(run_date)
        assert orders_silver_result.get("row_count", 0) > 0, "Orders Silver failed"

        # Run Silver for customers
        customers_silver = SilverEntity(
            source_path=str(
                bronze_base
                / "system=crm"
                / "entity=customers"
                / f"dt={run_date}"
                / "*.parquet"
            ),
            target_path=str(
                silver_base / "domain=crm" / "subject=customers" / "dt={run_date}"
            ),
            domain="crm",
            subject="customers",
            unique_columns=["customer_id"],
            last_updated_column="updated_at",
        )
        customers_silver_result = customers_silver.run(run_date)
        assert customers_silver_result.get("row_count", 0) > 0, (
            "Customers Silver failed"
        )

        # Verify output directories are separate
        orders_silver_dir = (
            silver_base / "domain=sales" / "subject=orders" / f"dt={run_date}"
        )
        customers_silver_dir = (
            silver_base / "domain=crm" / "subject=customers" / f"dt={run_date}"
        )

        assert orders_silver_dir.exists(), "Orders Silver output not created"
        assert customers_silver_dir.exists(), "Customers Silver output not created"

        # Verify data isolation
        orders_files = list(orders_silver_dir.glob("*.parquet"))
        customers_files = list(customers_silver_dir.glob("*.parquet"))

        assert len(orders_files) > 0, "No orders Silver parquet files"
        assert len(customers_files) > 0, "No customers Silver parquet files"

        # Verify metadata isolation
        orders_metadata = orders_silver_dir / "_metadata.json"
        customers_metadata = customers_silver_dir / "_metadata.json"

        assert orders_metadata.exists(), "Orders Silver metadata not created"
        assert customers_metadata.exists(), "Customers Silver metadata not created"

        with open(orders_metadata) as f:
            orders_meta = json.load(f)
        with open(customers_metadata) as f:
            customers_meta = json.load(f)

        # Verify each metadata references correct entity
        assert (
            orders_meta.get("subject") == "orders"
            or orders_meta.get("entity") == "orders"
        )
        assert (
            customers_meta.get("subject") == "customers"
            or customers_meta.get("entity") == "customers"
        )
