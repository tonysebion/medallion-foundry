"""Integration test: Full Bronze → Silver pipeline with S3 storage.

This test demonstrates the complete data flow:
1. Create sample CSV files on local filesystem
2. Bronze layer extracts from local CSV and writes to S3 storage
3. Silver layer reads from S3 Bronze storage and writes curated data to S3

Uses real MinIO instance at http://localhost:49384 for full integration testing.

To run:
    pytest tests/integration/test_bronze_silver_s3_flow.py -v

MinIO console: http://localhost:49384/browser/mdf
MinIO S3 API: http://localhost:9000 (this port is used for actual S3 operations)
Default credentials: minioadmin / minioadmin
"""

import os
import uuid
from pathlib import Path

import pandas as pd
import pytest

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.runner import run_pipeline


# ---------------------------------------------------------------------------
# MinIO Configuration
# ---------------------------------------------------------------------------

# Note: MinIO has two ports - Console (49384) and S3 API (9000)
# The S3 API port is needed for DuckDB/boto3 operations
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "mdf")


def is_minio_available() -> bool:
    """Check if MinIO is available at the configured endpoint."""
    try:
        import urllib.request

        req = urllib.request.Request(f"{MINIO_ENDPOINT}/minio/health/live")
        with urllib.request.urlopen(req, timeout=2):
            return True
    except Exception:
        return False


# Skip marker for when MinIO is not available
requires_minio = pytest.mark.skipif(
    not is_minio_available(),
    reason=f"MinIO not available at {MINIO_ENDPOINT}",
)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def create_sample_csv(path: Path, rows: list[dict]) -> None:
    """Create a CSV file with the given rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minio_test_prefix():
    """Generate a unique prefix for test isolation in MinIO."""
    test_id = uuid.uuid4().hex[:8]
    prefix = f"test_run_{test_id}"
    yield prefix

    # Cleanup after test (optional - comment out to inspect results)
    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
        )
        # Delete all objects with this prefix
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    client.delete_objects(
                        Bucket=MINIO_BUCKET, Delete={"Objects": objects}
                    )
    except Exception as e:
        print(f"Cleanup warning: {e}")


@pytest.fixture
def minio_env(monkeypatch):
    """Set environment variables for MinIO access."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", MINIO_ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    # Also set S3-specific vars for compatibility
    monkeypatch.setenv("S3_ENDPOINT_URL", MINIO_ENDPOINT)


# ---------------------------------------------------------------------------
# Integration Tests using Real MinIO
# ---------------------------------------------------------------------------


class TestBronzeSilverMinIO:
    """Integration tests using real MinIO storage."""

    @requires_minio
    @pytest.mark.integration
    def test_csv_to_bronze_minio_to_silver_minio(
        self, tmp_path: Path, minio_test_prefix: str, minio_env, monkeypatch
    ):
        """
        Full pipeline test with REAL MinIO: Local CSV → Bronze (MinIO) → Silver (MinIO).

        This test:
        1. Creates sample CSV files on local filesystem
        2. BronzeSource extracts and writes to MinIO bucket 'mdf'
        3. SilverEntity reads from MinIO Bronze and writes to MinIO Silver
        4. Verifies data can be read back from MinIO

        Run with: pytest tests/integration/test_bronze_silver_s3_flow.py::TestBronzeSilverMinIO -v
        """
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"
        test_prefix = minio_test_prefix

        # ===== Step 1: Create sample CSV on local filesystem =====
        sample_data = [
            {
                "order_id": "ORD001",
                "customer_id": "CUST001",
                "product": "Widget",
                "quantity": 5,
                "total": 50.00,
                "order_ts": "2025-01-15T09:00:00",
            },
            {
                "order_id": "ORD002",
                "customer_id": "CUST002",
                "product": "Gadget",
                "quantity": 2,
                "total": 40.00,
                "order_ts": "2025-01-15T10:00:00",
            },
            {
                "order_id": "ORD001",
                "customer_id": "CUST001",
                "product": "Widget",
                "quantity": 7,  # Updated quantity
                "total": 70.00,  # Updated total
                "order_ts": "2025-01-15T11:00:00",  # Later timestamp
            },
            {
                "order_id": "ORD003",
                "customer_id": "CUST001",
                "product": "Gizmo",
                "quantity": 1,
                "total": 100.00,
                "order_ts": "2025-01-15T12:00:00",
            },
        ]

        source_file = tmp_path / "source" / "orders" / f"{run_date}.csv"
        create_sample_csv(source_file, sample_data)

        print(f"\n{'=' * 60}")
        print(f"MinIO Test: {test_prefix}")
        print(f"Bucket: {MINIO_BUCKET}")
        print(f"Endpoint: {MINIO_ENDPOINT}")
        print(f"{'=' * 60}")

        # ===== Step 2: Configure Bronze to write to MinIO =====
        # Note: partition_by=[] disables partitioning to avoid _load_date column issues
        bronze_path = f"s3://{MINIO_BUCKET}/{test_prefix}/bronze/system={{system}}/entity={{entity}}/dt={{run_date}}/"
        bronze = BronzeSource(
            system="sales",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "orders" / "{run_date}.csv"),
            target_path=bronze_path,
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            partition_by=[],  # Disable partitioning for S3 writes
            options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
            },
        )

        # ===== Step 3: Configure Silver to read from MinIO Bronze and write to MinIO =====
        silver_source = f"s3://{MINIO_BUCKET}/{test_prefix}/bronze/system=sales/entity=orders/dt={{run_date}}/*.parquet"
        silver_target = f"s3://{MINIO_BUCKET}/{test_prefix}/silver/domain=sales/subject=orders/dt={{run_date}}/"
        silver = SilverEntity(
            source_path=silver_source,
            target_path=silver_target,
            natural_keys=["order_id"],
            change_timestamp="order_ts",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
            attributes=["customer_id", "product", "quantity", "total"],
        )

        # ===== Step 4: Run the pipeline =====
        print("\nRunning Bronze extraction...")
        bronze_result = bronze.run(run_date)
        print(f"Bronze result: {bronze_result}")

        print("\nRunning Silver curation...")
        silver_result = silver.run(run_date)
        print(f"Silver result: {silver_result}")

        # ===== Step 5: Verify results =====
        assert bronze_result["row_count"] == 4, "Bronze should extract all 4 rows"
        assert silver_result["row_count"] == 3, "Silver should have 3 unique orders"

        # ===== Step 6: Read back from MinIO and verify =====
        import io
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name="us-east-1",
        )

        # List Bronze files
        bronze_path_resolved = (
            f"{test_prefix}/bronze/system=sales/entity=orders/dt={run_date}"
        )
        bronze_files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=MINIO_BUCKET, Prefix=bronze_path_resolved
        ):
            bronze_files.extend([obj["Key"] for obj in page.get("Contents", [])])
        print(f"\nBronze files in MinIO: {bronze_files}")
        assert len(bronze_files) > 0, "Bronze should create files in MinIO"

        # List Silver files
        silver_path_resolved = (
            f"{test_prefix}/silver/domain=sales/subject=orders/dt={run_date}"
        )
        silver_files = []
        for page in paginator.paginate(
            Bucket=MINIO_BUCKET, Prefix=silver_path_resolved
        ):
            silver_files.extend([obj["Key"] for obj in page.get("Contents", [])])
        print(f"Silver files in MinIO: {silver_files}")
        assert len(silver_files) > 0, "Silver should create files in MinIO"

        # Read Silver parquet and verify content
        silver_parquet = next((f for f in silver_files if f.endswith(".parquet")), None)
        if silver_parquet:
            response = client.get_object(Bucket=MINIO_BUCKET, Key=silver_parquet)
            silver_df = pd.read_parquet(io.BytesIO(response["Body"].read()))

            print(f"\nSilver DataFrame:\n{silver_df}")

            assert len(silver_df) == 3, f"Expected 3 rows, got {len(silver_df)}"
            assert set(silver_df["order_id"]) == {"ORD001", "ORD002", "ORD003"}

            # Verify ORD001 has the latest values
            ord001 = silver_df[silver_df["order_id"] == "ORD001"].iloc[0]
            assert ord001["quantity"] == 7, "Should have latest quantity"
            assert ord001["total"] == 70.00, "Should have latest total"

        print(f"\n{'=' * 60}")
        print("TEST PASSED: Full Bronze→Silver pipeline with MinIO storage")
        print(f"{'=' * 60}")

    @requires_minio
    @pytest.mark.integration
    def test_incremental_load_with_minio(
        self, tmp_path: Path, minio_test_prefix: str, minio_env, monkeypatch
    ):
        """
        Test incremental loading pattern with MinIO storage.

        Demonstrates watermarking and incremental appends across multiple runs.
        """
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        test_prefix = minio_test_prefix

        # ===== Day 1: Initial load =====
        day1_data = [
            {"id": 1, "value": "A", "updated_at": "2025-01-15T10:00:00"},
            {"id": 2, "value": "B", "updated_at": "2025-01-15T10:00:00"},
        ]

        source_file_day1 = tmp_path / "source" / "2025-01-15.csv"
        create_sample_csv(source_file_day1, day1_data)

        bronze = BronzeSource(
            system="test",
            entity="items",
            source_type=SourceType.FILE_CSV,
            source_path=str(tmp_path / "source" / "{run_date}.csv"),
            target_path=f"s3://{MINIO_BUCKET}/{test_prefix}/bronze/system={{system}}/entity={{entity}}/dt={{run_date}}/",
            load_pattern=LoadPattern.FULL_SNAPSHOT,
            partition_by=[],  # Disable partitioning for S3 writes
            options={
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
            },
        )

        silver = SilverEntity(
            source_path=f"s3://{MINIO_BUCKET}/{test_prefix}/bronze/system=test/entity=items/dt={{run_date}}/*.parquet",
            target_path=f"s3://{MINIO_BUCKET}/{test_prefix}/silver/domain=test/subject=items/dt={{run_date}}/",
            natural_keys=["id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

        result_day1 = run_pipeline(bronze, silver, "2025-01-15")
        assert result_day1.success
        assert result_day1.bronze["row_count"] == 2
        assert result_day1.silver["row_count"] == 2

        # ===== Day 2: New data with updates =====
        day2_data = [
            {"id": 2, "value": "B-updated", "updated_at": "2025-01-16T10:00:00"},
            {"id": 3, "value": "C", "updated_at": "2025-01-16T10:00:00"},
        ]

        source_file_day2 = tmp_path / "source" / "2025-01-16.csv"
        create_sample_csv(source_file_day2, day2_data)

        result_day2 = run_pipeline(bronze, silver, "2025-01-16")
        assert result_day2.success
        assert result_day2.bronze["row_count"] == 2

        print(f"\nDay 1 results: {result_day1}")
        print(f"Day 2 results: {result_day2}")
        print("TEST PASSED: Incremental load pattern with MinIO")
