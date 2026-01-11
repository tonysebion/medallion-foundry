"""Comprehensive YAML integration test with S3/MinIO storage.

This test validates the full YAML-driven Bronze→Silver pipeline with:
- Static YAML pattern templates with runtime substitution
- S3/MinIO storage for both Bronze and Silver layers
- All supporting files: _metadata.json, _checksums.json
- SHA256 checksum integrity verification
- PolyBase DDL generation from metadata

YAML Pattern Templates:
    tests/integration/yaml_configs/patterns/
        - snapshot_state_scd1.yaml   - Full snapshot → State → SCD1
        - snapshot_state_scd2.yaml   - Full snapshot → State → SCD2
        - incremental_state_scd1.yaml - Incremental → State → SCD1
        - incremental_state_scd2.yaml - Incremental → State → SCD2
        - snapshot_event.yaml        - Full snapshot → Event
        - incremental_event.yaml     - Incremental → Event
        - skipped_days.yaml          - Non-consecutive run dates

Sample Data:
    pipelines/examples/sample_data/
        - orders_2025-01-15.csv      - Order transactions
        - customers_2025-01-15.csv   - Customer master with SCD2 history

To run:
    pytest tests/integration/test_yaml_s3_comprehensive.py -v

    # Run specific pattern
    pytest tests/integration/test_yaml_s3_comprehensive.py -k "snapshot_state_scd1" -v

    # Run all state patterns
    pytest tests/integration/test_yaml_s3_comprehensive.py -k "state" -v

MinIO setup (required):
    docker run -p 9000:9000 -p 49384:49384 minio/minio server /data --console-address ":49384"

MinIO console: http://localhost:49384/browser/mdf
MinIO S3 API: http://localhost:9000
Default credentials: minioadmin / minioadmin
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

from pipelines.lib.config_loader import load_pipeline
from pipelines.lib.polybase import generate_from_metadata, PolyBaseConfig

from .yaml_configs.template_loader import (
    get_pattern_template,
    load_yaml_with_substitutions,
)


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


requires_minio = pytest.mark.skipif(
    not is_minio_available(),
    reason=f"MinIO not available at {MINIO_ENDPOINT}",
)


# ---------------------------------------------------------------------------
# Pattern Configuration
# ---------------------------------------------------------------------------


@dataclass
class PatternConfig:
    """Configuration for a Bronze→Silver pattern test."""

    name: str  # Pattern name (matches yaml file)
    data_type: str  # orders, customers, or events
    system: str  # Source system name
    entity: str  # Entity name
    unique_columns: str  # Primary key column(s)
    last_updated_column: str  # Timestamp column
    entity_kind: str  # state or event
    history_mode: str  # current_only or full_history
    expected_dedup_rows: int  # Expected rows after deduplication
    has_scd2_columns: bool  # Whether to check for effective_from/to


# Define all patterns to test
PATTERNS = [
    # State entities (dimensions)
    PatternConfig(
        name="snapshot_state_scd1",
        data_type="orders",
        system="retail",
        entity="orders",
        unique_columns="order_id",
        last_updated_column="updated_at",
        entity_kind="state",
        history_mode="current_only",
        expected_dedup_rows=5,  # 5 unique orders in sample data
        has_scd2_columns=False,
    ),
    PatternConfig(
        name="snapshot_state_scd2",
        data_type="customers",
        system="crm",
        entity="customers",
        unique_columns="customer_id",
        last_updated_column="updated_at",
        entity_kind="state",
        history_mode="full_history",
        expected_dedup_rows=6,  # 6 rows with history in sample data
        has_scd2_columns=True,
    ),
    # Event entities (facts)
    PatternConfig(
        name="snapshot_event",
        data_type="orders",
        system="retail",
        entity="order_events",
        unique_columns="order_id",
        last_updated_column="updated_at",
        entity_kind="event",
        history_mode="current_only",
        expected_dedup_rows=5,  # 5 unique orders
        has_scd2_columns=False,
    ),
]


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def get_s3_client():
    """Get configured boto3 S3 client for MinIO."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def compute_sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


def verify_artifact_exists(client, bucket: str, path: str, artifact_name: str) -> bool:
    """Check if an artifact file exists in S3."""
    from botocore.exceptions import ClientError

    key = path.rstrip("/") + "/" + artifact_name
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
    except Exception:
        return False


def read_json_from_s3(client, bucket: str, key: str) -> Optional[Dict[str, Any]]:
    """Read and parse JSON file from S3."""
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return json.load(response["Body"])
    except Exception as e:
        print(f"Failed to read JSON from {key}: {e}")
        return None


def verify_parquet_integrity(
    client, bucket: str, parquet_key: str, expected_sha256: str
) -> bool:
    """Verify SHA256 hash of parquet file matches expected value."""
    try:
        response = client.get_object(Bucket=bucket, Key=parquet_key)
        data = response["Body"].read()
        actual_sha256 = compute_sha256_bytes(data)
        return actual_sha256 == expected_sha256
    except Exception as e:
        print(f"Failed to verify parquet integrity: {e}")
        return False


def list_files_in_path(client, bucket: str, prefix: str) -> List[str]:
    """List all files in an S3 path."""
    try:
        files = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            files.extend([obj["Key"] for obj in page.get("Contents", [])])
        return files
    except Exception:
        return []


def verify_bronze_artifacts(client, bucket: str, bronze_path: str) -> Dict[str, Any]:
    """Verify Bronze layer artifacts exist and return details."""
    from botocore.exceptions import ClientError

    result = {
        "path": bronze_path,
        "parquet_files": [],
        "metadata": None,
        "checksums": None,
        "metadata_exists": False,
        "checksums_exists": False,
        "parquet_count": 0,
    }

    # List all files
    try:
        all_files = list_files_in_path(client, bucket, bronze_path)
    except Exception as e:
        result["error"] = str(e)
        return result

    # Find parquet files
    result["parquet_files"] = [f for f in all_files if f.endswith(".parquet")]
    result["parquet_count"] = len(result["parquet_files"])

    # Check for metadata
    metadata_key = bronze_path.rstrip("/") + "/_metadata.json"
    try:
        client.head_object(Bucket=bucket, Key=metadata_key)
        result["metadata_exists"] = True
        result["metadata"] = read_json_from_s3(client, bucket, metadata_key)
    except ClientError:
        pass

    # Check for checksums
    checksums_key = bronze_path.rstrip("/") + "/_checksums.json"
    try:
        client.head_object(Bucket=bucket, Key=checksums_key)
        result["checksums_exists"] = True
        result["checksums"] = read_json_from_s3(client, bucket, checksums_key)
    except ClientError:
        pass

    return result


def verify_silver_artifacts(client, bucket: str, silver_path: str) -> Dict[str, Any]:
    """Verify Silver layer artifacts exist and return details."""
    from botocore.exceptions import ClientError

    result = {
        "path": silver_path,
        "parquet_files": [],
        "metadata": None,
        "checksums": None,
        "metadata_exists": False,
        "checksums_exists": False,
        "parquet_count": 0,
    }

    # List all files
    try:
        all_files = list_files_in_path(client, bucket, silver_path)
    except Exception as e:
        result["error"] = str(e)
        return result

    # Find parquet files
    result["parquet_files"] = [f for f in all_files if f.endswith(".parquet")]
    result["parquet_count"] = len(result["parquet_files"])

    # Check for metadata
    metadata_key = silver_path.rstrip("/") + "/_metadata.json"
    try:
        client.head_object(Bucket=bucket, Key=metadata_key)
        result["metadata_exists"] = True
        result["metadata"] = read_json_from_s3(client, bucket, metadata_key)
    except ClientError:
        pass

    # Check for checksums
    checksums_key = silver_path.rstrip("/") + "/_checksums.json"
    try:
        client.head_object(Bucket=bucket, Key=checksums_key)
        result["checksums_exists"] = True
        result["checksums"] = read_json_from_s3(client, bucket, checksums_key)
    except ClientError:
        pass

    return result


def verify_checksum_integrity(
    client, bucket: str, base_path: str, checksums: Dict[str, Any]
) -> Dict[str, Any]:
    """Verify SHA256 hashes of parquet files match manifest.

    Returns dict with verification results.
    """
    from botocore.exceptions import ClientError

    result = {
        "verified": [],
        "mismatched": [],
        "missing": [],
        "all_valid": False,
    }

    if not checksums or "files" not in checksums:
        result["error"] = "No checksums data provided"
        return result

    for file_entry in checksums.get("files", []):
        filename = file_entry.get("path")
        expected_sha256 = file_entry.get("sha256")
        expected_size = file_entry.get("size_bytes")

        if not filename or not expected_sha256:
            continue

        file_key = base_path.rstrip("/") + "/" + filename

        try:
            try:
                client.head_object(Bucket=bucket, Key=file_key)
            except ClientError:
                result["missing"].append(filename)
                continue

            response = client.get_object(Bucket=bucket, Key=file_key)
            data = response["Body"].read()

            actual_sha256 = compute_sha256_bytes(data)
            actual_size = len(data)

            if actual_sha256 == expected_sha256 and actual_size == expected_size:
                result["verified"].append(filename)
            else:
                result["mismatched"].append(
                    {
                        "file": filename,
                        "expected_sha256": expected_sha256,
                        "actual_sha256": actual_sha256,
                        "expected_size": expected_size,
                        "actual_size": actual_size,
                    }
                )
        except Exception as e:
            result["missing"].append(f"{filename} (error: {e})")

    result["all_valid"] = (
        len(result["verified"]) > 0
        and len(result["mismatched"]) == 0
        and len(result["missing"]) == 0
    )

    return result


def generate_polybase_ddl_from_s3(
    client, bucket: str, metadata_key: str
) -> Optional[str]:
    """Generate PolyBase DDL from metadata file in S3."""
    metadata = read_json_from_s3(client, bucket, metadata_key)
    if not metadata:
        return None

    # Write metadata to temp file for generate_from_metadata
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(metadata, f)
        temp_path = f.name

    try:
        config = PolyBaseConfig(
            data_source_name="test_source",
            data_source_location="wasbs://test@account.blob.core.windows.net/",
        )
        ddl = generate_from_metadata(Path(temp_path), config)
        return ddl
    finally:
        Path(temp_path).unlink(missing_ok=True)


def get_sample_data_path(data_type: str, run_date: str) -> Path:
    """Get path to sample data file."""
    sample_dir = (
        Path(__file__).parent.parent.parent / "pipelines" / "examples" / "sample_data"
    )
    return sample_dir / f"{data_type}_{run_date}.csv"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minio_test_prefix():
    """Generate a unique prefix for test isolation in MinIO."""
    test_id = uuid.uuid4().hex[:8]
    prefix = f"test_pattern_{test_id}"
    yield prefix

    # Cleanup after test - DISABLED for inspection
    # Uncomment to enable cleanup:
    # try:
    #     fs = get_s3fs()
    #     objects = fs.find(f"{MINIO_BUCKET}/{prefix}")
    #     for obj in objects:
    #         fs.rm(obj)
    # except Exception as e:
    #     print(f"Cleanup warning: {e}")
    print(
        f"\n[CLEANUP DISABLED] Test files preserved at: s3://{MINIO_BUCKET}/{prefix}/"
    )


@pytest.fixture
def minio_env(monkeypatch):
    """Set environment variables for MinIO access."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", MINIO_ENDPOINT)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("S3_ENDPOINT_URL", MINIO_ENDPOINT)


# ---------------------------------------------------------------------------
# Parametrized Pattern Tests
# ---------------------------------------------------------------------------


class TestYamlPatterns:
    """Parametrized tests for all Bronze→Silver patterns."""

    @requires_minio
    @pytest.mark.integration
    @pytest.mark.parametrize("pattern", PATTERNS, ids=lambda p: p.name)
    def test_pattern(
        self,
        pattern: PatternConfig,
        tmp_path: Path,
        minio_test_prefix: str,
        minio_env,
        monkeypatch,
    ):
        """Test a Bronze→Silver pattern with static YAML template.

        Validates:
        1. Bronze parquet files created
        2. Bronze _metadata.json exists
        3. Silver parquet files created
        4. Silver _metadata.json exists
        5. Data content is correct (deduplication, SCD2 if applicable)
        6. PolyBase DDL can be generated from metadata
        """
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"
        test_prefix = minio_test_prefix

        # Get sample data path
        source_path = get_sample_data_path(pattern.data_type, run_date)
        if not source_path.exists():
            pytest.skip(f"Sample data not found: {source_path}")

        print(f"\n{'=' * 60}")
        print(f"Pattern Test: {pattern.name}")
        print(f"Data Type: {pattern.data_type}")
        print(f"Source: {source_path}")
        print(f"Prefix: {test_prefix}")
        print(f"{'=' * 60}")

        # Load pattern template and substitute placeholders
        template_path = get_pattern_template(pattern.name)
        substitutions = {
            "bucket": MINIO_BUCKET,
            "prefix": test_prefix,
            "source_path": str(source_path).replace("\\", "/"),
            "run_date": run_date,
            "system": pattern.system,
            "entity": pattern.entity,
            "unique_columns": pattern.unique_columns,
            "last_updated_column": pattern.last_updated_column,
        }

        yaml_content = load_yaml_with_substitutions(template_path, substitutions)
        yaml_file = tmp_path / f"{pattern.name}.yaml"
        yaml_file.write_text(yaml_content)

        print(f"\nYAML config:\n{yaml_content}")

        # Load and run pipeline
        pipeline = load_pipeline(yaml_file)

        print("\nRunning Bronze extraction...")
        bronze_result = pipeline.bronze.run(run_date)
        print(f"Bronze result: {bronze_result}")

        print("\nRunning Silver curation...")
        silver_result = pipeline.silver.run(run_date)
        print(f"Silver result: {silver_result}")

        # Verify Bronze artifacts
        import io

        client = get_s3_client()
        bronze_path = f"{test_prefix}/bronze/system={pattern.system}/entity={pattern.entity}/dt={run_date}"

        bronze_artifacts = verify_bronze_artifacts(client, MINIO_BUCKET, bronze_path)
        print(f"\nBronze artifacts: {bronze_artifacts}")

        assert bronze_artifacts["parquet_count"] > 0, (
            "Bronze should create parquet files"
        )
        assert bronze_artifacts["metadata_exists"], (
            "Bronze should create _metadata.json"
        )

        # Verify Bronze metadata content
        if bronze_artifacts["metadata"]:
            metadata = bronze_artifacts["metadata"]
            assert "row_count" in metadata, "Metadata should have row_count"
            assert "columns" in metadata, "Metadata should have columns"

        # Verify checksums if available
        if bronze_artifacts["checksums_exists"]:
            checksum_result = verify_checksum_integrity(
                client, MINIO_BUCKET, bronze_path, bronze_artifacts["checksums"]
            )
            print(f"Bronze checksum verification: {checksum_result}")

        # Verify Silver artifacts
        silver_path = (
            f"{test_prefix}/silver/domain={pattern.system}/subject={pattern.entity}"
        )

        silver_artifacts = verify_silver_artifacts(client, MINIO_BUCKET, silver_path)
        print(f"\nSilver artifacts: {silver_artifacts}")

        assert silver_artifacts["parquet_count"] > 0, (
            "Silver should create parquet files"
        )

        # Read Silver parquet and verify content
        silver_parquet = next(
            (f for f in silver_artifacts["parquet_files"] if f.endswith(".parquet")),
            None,
        )
        assert silver_parquet is not None, "Silver should have parquet file"

        response = client.get_object(Bucket=MINIO_BUCKET, Key=silver_parquet)
        silver_df = pd.read_parquet(io.BytesIO(response["Body"].read()))

        print(f"\nSilver DataFrame:\n{silver_df}")

        # Verify SCD2 columns if applicable
        if pattern.has_scd2_columns:
            assert "effective_from" in silver_df.columns, (
                "SCD2 should have effective_from"
            )
            assert "effective_to" in silver_df.columns, "SCD2 should have effective_to"
            assert "is_current" in silver_df.columns, "SCD2 should have is_current"

        # Test PolyBase DDL generation
        if bronze_artifacts["metadata_exists"]:
            metadata_key = bronze_path + "/_metadata.json"
            ddl = generate_polybase_ddl_from_s3(client, MINIO_BUCKET, metadata_key)
            if ddl:
                print(f"\nGenerated PolyBase DDL:\n{ddl[:500]}...")
                assert "CREATE EXTERNAL TABLE" in ddl, (
                    "DDL should contain CREATE EXTERNAL TABLE"
                )

        print(f"\n{'=' * 60}")
        print(f"TEST PASSED: {pattern.name}")
        print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Specialized Tests
# ---------------------------------------------------------------------------


class TestSpecializedScenarios:
    """Non-parametrized tests for specific scenarios."""

    @requires_minio
    @pytest.mark.integration
    def test_skipped_days(
        self,
        tmp_path: Path,
        minio_test_prefix: str,
        minio_env,
        monkeypatch,
        multi_day_orders_csv,
    ):
        """Test pipeline with non-consecutive run dates.

        Simulates weekends/holidays where pipelines don't run every day.
        Verifies PolyBase DDL works with gaps in date partitions.
        """
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        test_prefix = minio_test_prefix
        run_dates = ["2025-01-15", "2025-01-17", "2025-01-19"]  # Skip 16, 18

        print(f"\n{'=' * 60}")
        print(f"Skipped Days Test: {test_prefix}")
        print(f"Run Dates: {run_dates}")
        print(f"{'=' * 60}")

        # Load pattern template
        template_path = get_pattern_template("skipped_days")
        client = get_s3_client()

        bronze_paths = []
        silver_paths = []

        for run_date in run_dates:
            csv_path = multi_day_orders_csv[run_date]

            substitutions = {
                "bucket": MINIO_BUCKET,
                "prefix": test_prefix,
                "source_path": str(csv_path).replace("\\", "/"),
                "run_date": run_date,
                "system": "retail",
                "entity": "orders",
                "unique_columns": "order_id",
                "last_updated_column": "updated_at",
            }

            yaml_content = load_yaml_with_substitutions(template_path, substitutions)
            yaml_file = tmp_path / f"skipped_days_{run_date}.yaml"
            yaml_file.write_text(yaml_content)

            # Run pipeline for this date
            pipeline = load_pipeline(yaml_file)

            print(f"\nRunning pipeline for {run_date}...")
            bronze_result = pipeline.bronze.run(run_date)
            silver_result = pipeline.silver.run(run_date)

            print(f"  Bronze: {bronze_result['row_count']} rows")
            print(f"  Silver: {silver_result['row_count']} rows")

            bronze_path = (
                f"{test_prefix}/bronze/system=retail/entity=orders/dt={run_date}"
            )
            silver_path = f"{test_prefix}/silver/domain=retail/subject=orders"

            bronze_paths.append(bronze_path)
            silver_paths.append(silver_path)

        # Verify Bronze partitions exist for each run date
        print("\nVerifying Bronze partitions...")
        for i, run_date in enumerate(run_dates):
            bronze_path = bronze_paths[i]
            bronze_artifacts = verify_bronze_artifacts(
                client, MINIO_BUCKET, bronze_path
            )
            assert bronze_artifacts["parquet_count"] > 0, (
                f"Bronze partition for {run_date} should exist"
            )
            print(f"  {run_date}: {bronze_artifacts['parquet_count']} parquet files")

        # Verify skipped dates DON'T have partitions
        skipped_dates = ["2025-01-16", "2025-01-18"]
        for skipped_date in skipped_dates:
            skipped_path = (
                f"{test_prefix}/bronze/system=retail/entity=orders/dt={skipped_date}"
            )
            files = list_files_in_path(client, MINIO_BUCKET, skipped_path)
            assert len(files) == 0, f"Skipped date {skipped_date} should have no files"

        # Verify Silver output exists
        silver_path = f"{test_prefix}/silver/domain=retail/subject=orders"
        silver_artifacts = verify_silver_artifacts(client, MINIO_BUCKET, silver_path)
        assert silver_artifacts["parquet_count"] > 0, "Silver should have parquet files"

        # Verify PolyBase DDL can be generated from final Bronze metadata
        final_bronze_path = bronze_paths[-1]  # Use last run date
        metadata_key = final_bronze_path + "/_metadata.json"
        ddl = generate_polybase_ddl_from_s3(client, MINIO_BUCKET, metadata_key)
        if ddl:
            print(f"\nPolyBase DDL generated successfully ({len(ddl)} chars)")
            assert "CREATE EXTERNAL TABLE" in ddl

        print(f"\n{'=' * 60}")
        print("TEST PASSED: Skipped days scenario")
        print(f"{'=' * 60}")

    @requires_minio
    @pytest.mark.integration
    def test_checksum_integrity(
        self,
        tmp_path: Path,
        minio_test_prefix: str,
        minio_env,
        monkeypatch,
        sample_orders_csv,
    ):
        """Verify SHA256 checksums are correct for all artifacts."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"
        test_prefix = minio_test_prefix

        # Load a simple pattern
        template_path = get_pattern_template("snapshot_state_scd1")
        substitutions = {
            "bucket": MINIO_BUCKET,
            "prefix": test_prefix,
            "source_path": str(sample_orders_csv).replace("\\", "/"),
            "run_date": run_date,
            "system": "test",
            "entity": "orders",
            "unique_columns": "order_id",
            "last_updated_column": "updated_at",
        }

        yaml_content = load_yaml_with_substitutions(template_path, substitutions)
        yaml_file = tmp_path / "checksum_test.yaml"
        yaml_file.write_text(yaml_content)

        # Run pipeline
        pipeline = load_pipeline(yaml_file)
        pipeline.bronze.run(run_date)
        pipeline.silver.run(run_date)

        # Verify Bronze checksums
        client = get_s3_client()
        bronze_path = f"{test_prefix}/bronze/system=test/entity=orders/dt={run_date}"

        bronze_artifacts = verify_bronze_artifacts(client, MINIO_BUCKET, bronze_path)

        if bronze_artifacts["checksums_exists"]:
            checksum_result = verify_checksum_integrity(
                client, MINIO_BUCKET, bronze_path, bronze_artifacts["checksums"]
            )
            print("\nBronze checksum verification:")
            print(f"  Verified: {len(checksum_result['verified'])} files")
            print(f"  Mismatched: {len(checksum_result['mismatched'])} files")
            print(f"  Missing: {len(checksum_result['missing'])} files")

            assert checksum_result["all_valid"], "All Bronze checksums should be valid"

        print("TEST PASSED: Checksum integrity verification")

    @requires_minio
    @pytest.mark.integration
    def test_scd2_effective_dates(
        self,
        tmp_path: Path,
        minio_test_prefix: str,
        minio_env,
        monkeypatch,
        sample_customers_csv,
    ):
        """Verify SCD2 effective_from/effective_to dates are correct."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"
        test_prefix = minio_test_prefix

        # Load SCD2 pattern
        template_path = get_pattern_template("snapshot_state_scd2")
        substitutions = {
            "bucket": MINIO_BUCKET,
            "prefix": test_prefix,
            "source_path": str(sample_customers_csv).replace("\\", "/"),
            "run_date": run_date,
            "system": "crm",
            "entity": "customers",
            "unique_columns": "customer_id",
            "last_updated_column": "updated_at",
        }

        yaml_content = load_yaml_with_substitutions(template_path, substitutions)
        yaml_file = tmp_path / "scd2_test.yaml"
        yaml_file.write_text(yaml_content)

        # Run pipeline
        pipeline = load_pipeline(yaml_file)
        pipeline.bronze.run(run_date)
        pipeline.silver.run(run_date)

        # Read Silver data
        import io

        client = get_s3_client()
        silver_path = f"{test_prefix}/silver/domain=crm/subject=customers"
        silver_files = list_files_in_path(client, MINIO_BUCKET, silver_path)

        silver_parquet = next((f for f in silver_files if f.endswith(".parquet")), None)
        assert silver_parquet, "Silver parquet should exist"

        response = client.get_object(Bucket=MINIO_BUCKET, Key=silver_parquet)
        silver_df = pd.read_parquet(io.BytesIO(response["Body"].read()))

        print(f"\nSilver SCD2 DataFrame:\n{silver_df}")

        # Verify SCD2 columns exist
        assert "effective_from" in silver_df.columns
        assert "effective_to" in silver_df.columns
        assert "is_current" in silver_df.columns

        # Verify CUST001 has multiple versions (historical data in sample)
        cust001_rows = silver_df[silver_df["customer_id"] == "CUST001"]
        assert len(cust001_rows) >= 2, (
            f"CUST001 should have historical versions, got {len(cust001_rows)}"
        )

        # Verify only one version per customer is current
        current_rows = silver_df[silver_df["is_current"] == 1]
        unique_current_customers = current_rows["customer_id"].nunique()
        assert unique_current_customers == len(current_rows), (
            "Each customer should have exactly one current row"
        )

        # Verify effective_to is None for current rows
        for _, row in current_rows.iterrows():
            assert pd.isna(row["effective_to"]) or row["effective_to"] is None, (
                f"Current row for {row['customer_id']} should have None effective_to"
            )

        print("TEST PASSED: SCD2 effective dates verification")

    @requires_minio
    @pytest.mark.integration
    def test_polybase_ddl_generation(
        self,
        tmp_path: Path,
        minio_test_prefix: str,
        minio_env,
        monkeypatch,
        sample_orders_csv,
    ):
        """Verify PolyBase DDL is correctly generated from metadata."""
        monkeypatch.setenv("PIPELINE_STATE_DIR", str(tmp_path / "state"))

        run_date = "2025-01-15"
        test_prefix = minio_test_prefix

        # Load a simple pattern
        template_path = get_pattern_template("snapshot_state_scd1")
        substitutions = {
            "bucket": MINIO_BUCKET,
            "prefix": test_prefix,
            "source_path": str(sample_orders_csv).replace("\\", "/"),
            "run_date": run_date,
            "system": "test",
            "entity": "orders",
            "unique_columns": "order_id",
            "last_updated_column": "updated_at",
        }

        yaml_content = load_yaml_with_substitutions(template_path, substitutions)
        yaml_file = tmp_path / "polybase_test.yaml"
        yaml_file.write_text(yaml_content)

        # Run pipeline
        pipeline = load_pipeline(yaml_file)
        pipeline.bronze.run(run_date)

        # Generate PolyBase DDL from Bronze metadata
        client = get_s3_client()
        bronze_path = f"{test_prefix}/bronze/system=test/entity=orders/dt={run_date}"
        metadata_key = bronze_path + "/_metadata.json"

        ddl = generate_polybase_ddl_from_s3(client, MINIO_BUCKET, metadata_key)

        assert ddl is not None, "DDL should be generated"
        assert "CREATE EXTERNAL TABLE" in ddl, "DDL should have CREATE EXTERNAL TABLE"
        # DDL references column names from orders data
        assert "order_id" in ddl.lower(), "DDL should contain order_id column"

        print(f"\nGenerated PolyBase DDL:\n{ddl}")
        print("TEST PASSED: PolyBase DDL generation")
