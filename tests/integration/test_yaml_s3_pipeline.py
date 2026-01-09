"""Integration test: YAML-driven Bronze → Silver pipeline with S3 storage.

This test demonstrates the recommended YAML-first approach:
1. Creates sample CSV files on local filesystem
2. Defines a pipeline YAML file dynamically
3. Runs the YAML pipeline which writes to S3 storage
4. Verifies data in S3

Uses real MinIO instance at http://localhost:9000 for full integration testing.

To run:
    pytest tests/integration/test_yaml_s3_pipeline.py -v

MinIO console: http://localhost:49384/browser/mdf
MinIO S3 API: http://localhost:9000
Default credentials: minioadmin / minioadmin
"""

import os
import uuid
from pathlib import Path

import pandas as pd
import pytest
import yaml

from pipelines.lib.config_loader import load_pipeline
from pipelines.lib.runner import run_pipeline


# ---------------------------------------------------------------------------
# MinIO Configuration
# ---------------------------------------------------------------------------

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


def create_pipeline_yaml(
    yaml_path: Path,
    source_path: str,
    bronze_target: str,
    silver_target: str,
) -> None:
    """Create a pipeline YAML file."""
    pipeline_config = {
        "name": "test_yaml_s3_pipeline",
        "description": "Integration test: YAML pipeline writing to S3",
        "bronze": {
            "system": "retail",
            "entity": "orders",
            "source_type": "file_csv",
            "source_path": source_path,
            "target_path": bronze_target,
            "load_pattern": "full_snapshot",
            "partition_by": [],  # Disable partitioning for S3
            "options": {
                "endpoint_url": MINIO_ENDPOINT,
                "key": MINIO_ACCESS_KEY,
                "secret": MINIO_SECRET_KEY,
            },
        },
        "silver": {
            "domain": "retail",
            "subject": "orders",
            "source_path": bronze_target.replace("{run_date}/", "{run_date}/*.parquet").replace(
                "{system}", "retail"
            ).replace("{entity}", "orders"),
            "target_path": silver_target,
            "natural_keys": ["order_id"],
            "change_timestamp": "order_ts",
            "entity_kind": "state",
            "history_mode": "current_only",
            "attributes": ["customer_id", "product", "quantity", "total"],
        },
    }

    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with open(yaml_path, "w") as f:
        # Add schema comment for IDE support
        f.write("# yaml-language-server: $schema=../../pipelines/schema/pipeline.schema.json\n")
        yaml.dump(pipeline_config, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minio_test_prefix():
    """Generate a unique prefix for test isolation in MinIO."""
    test_id = uuid.uuid4().hex[:8]
    prefix = f"yaml_test_{test_id}"
    yield prefix

    # Cleanup after test
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
                    client.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": objects})
    except Exception as e:
        print(f"Cleanup warning: {e}")


@pytest.fixture
def minio_env(monkeypatch):
    """Set environment variables for MinIO access."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", MINIO_ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", MINIO_ENDPOINT)


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------


class TestYamlPipelineS3:
    """Integration tests using YAML-defined pipelines with S3 storage."""

    @requires_minio
    @pytest.mark.integration
    def test_yaml_pipeline_to_s3(
        self, tmp_path: Path, minio_test_prefix: str, minio_env, monkeypatch
    ):
        """
        Full pipeline test using YAML configuration: Local CSV → Bronze (S3) → Silver (S3).

        This test demonstrates the recommended YAML-first approach:
        1. Creates a pipeline YAML file with S3 targets
        2. Creates sample CSV data
        3. Loads the YAML config and runs the pipeline
        4. Verifies data was written to S3

        Run with: pytest tests/integration/test_yaml_s3_pipeline.py -v
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
        ]

        source_file = tmp_path / "source" / "orders" / f"{run_date}.csv"
        create_sample_csv(source_file, sample_data)

        # ===== Step 2: Create pipeline YAML =====
        bronze_target = f"s3://{MINIO_BUCKET}/{test_prefix}/bronze/system={{system}}/entity={{entity}}/dt={{run_date}}/"
        silver_target = f"s3://{MINIO_BUCKET}/{test_prefix}/silver/domain=retail/subject=orders/dt={{run_date}}/"

        yaml_file = tmp_path / "pipelines" / "test_pipeline.yaml"
        create_pipeline_yaml(
            yaml_file,
            source_path=str(tmp_path / "source" / "orders" / "{run_date}.csv"),
            bronze_target=bronze_target,
            silver_target=silver_target,
        )

        print(f"\n{'='*60}")
        print(f"YAML Pipeline Test: {test_prefix}")
        print(f"YAML file: {yaml_file}")
        print(f"{'='*60}")

        # Print the generated YAML for debugging
        with open(yaml_file) as f:
            print(f"Pipeline YAML:\n{f.read()}")

        # ===== Step 3: Load and run the YAML pipeline =====
        config = load_pipeline(str(yaml_file))
        bronze_source = config.bronze
        silver_entity = config.silver

        print("\nRunning pipeline from YAML configuration...")
        result = run_pipeline(bronze_source, silver_entity, run_date)

        print(f"Pipeline result: {result}")
        print(f"  Success: {result.success}")
        print(f"  Bronze rows: {result.bronze.get('row_count', 0)}")
        print(f"  Silver rows: {result.silver.get('row_count', 0)}")

        # ===== Step 4: Verify results =====
        assert result.success, f"Pipeline should succeed: {result.error}"
        assert result.bronze["row_count"] == 3, "Bronze should extract all 3 rows"
        assert result.silver["row_count"] == 2, "Silver should have 2 unique orders"

        # ===== Step 5: Read back from MinIO and verify =====
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
        bronze_path = f"{test_prefix}/bronze/system=retail/entity=orders/dt={run_date}"
        bronze_files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=bronze_path):
            bronze_files.extend([obj["Key"] for obj in page.get("Contents", [])])
        print(f"\nBronze files in S3: {bronze_files}")
        assert len(bronze_files) > 0, "Bronze should create files in S3"
        assert any(f.endswith(".parquet") for f in bronze_files), "Bronze should write parquet file"

        # List Silver files
        silver_path = f"{test_prefix}/silver/domain=retail/subject=orders/dt={run_date}"
        silver_files = []
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=silver_path):
            silver_files.extend([obj["Key"] for obj in page.get("Contents", [])])
        print(f"Silver files in S3: {silver_files}")
        assert len(silver_files) > 0, "Silver should create files in S3"

        # Read Silver parquet and verify content
        silver_parquet = next((f for f in silver_files if f.endswith(".parquet")), None)
        if silver_parquet:
            response = client.get_object(Bucket=MINIO_BUCKET, Key=silver_parquet)
            silver_df = pd.read_parquet(io.BytesIO(response["Body"].read()))

            print(f"\nSilver DataFrame:\n{silver_df}")

            assert len(silver_df) == 2, f"Expected 2 rows, got {len(silver_df)}"
            assert set(silver_df["order_id"]) == {"ORD001", "ORD002"}

            # Verify ORD001 has the latest values
            ord001 = silver_df[silver_df["order_id"] == "ORD001"].iloc[0]
            assert ord001["quantity"] == 7, "Should have latest quantity"
            assert ord001["total"] == 70.00, "Should have latest total"

        print(f"\n{'='*60}")
        print("TEST PASSED: YAML-driven Bronze→Silver pipeline with S3 storage")
        print(f"{'='*60}")
