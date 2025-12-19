"""Integration test fixtures for MinIO-backed end-to-end testing.

Provides fixtures for:
- MinIO S3-compatible storage connections
- Synthetic data generation and upload
- Bronze/Silver pipeline testing
- Test bucket management and cleanup
"""

from __future__ import annotations

import os
import uuid
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator

import boto3
import pandas as pd
import pytest
from botocore.exceptions import ClientError

from tests.synthetic_data import (
    ClaimsGenerator,
    OrdersGenerator,
    TransactionsGenerator,
    generate_time_series_data,
)


# =============================================================================
# MinIO Configuration
# =============================================================================

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "mdf")
MINIO_REGION = "us-east-1"


def is_minio_available() -> bool:
    """Check if MinIO is reachable."""
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
        )
        client.list_buckets()
        return True
    except Exception:
        return False


# Skip marker for tests requiring MinIO
requires_minio = pytest.mark.skipif(
    not is_minio_available(),
    reason=f"MinIO not available at {MINIO_ENDPOINT}",
)


# =============================================================================
# MinIO Client Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def minio_client():
    """Create a boto3 S3 client configured for MinIO.

    Session-scoped to reuse connection across all tests.
    """
    if not is_minio_available():
        pytest.skip(f"MinIO not available at {MINIO_ENDPOINT}")

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    return client


@pytest.fixture(scope="session")
def minio_bucket(minio_client) -> str:
    """Ensure the test bucket exists.

    Creates bucket if it doesn't exist. Session-scoped.
    """
    try:
        minio_client.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        minio_client.create_bucket(Bucket=MINIO_BUCKET)
    return MINIO_BUCKET


@pytest.fixture
def test_prefix() -> str:
    """Generate a unique prefix for test isolation.

    Each test gets its own prefix to avoid conflicts.
    """
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cleanup_prefix(minio_client, minio_bucket, test_prefix) -> Generator[str, None, None]:
    """Yield test prefix and cleanup all objects under it after test."""
    yield test_prefix

    # Cleanup: delete all objects with this prefix
    try:
        paginator = minio_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=minio_bucket, Prefix=test_prefix):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if objects:
                    minio_client.delete_objects(
                        Bucket=minio_bucket,
                        Delete={"Objects": objects},
                    )
    except Exception:
        pass  # Best effort cleanup


# =============================================================================
# Platform Configuration Fixtures
# =============================================================================


@pytest.fixture
def minio_platform_config(minio_bucket, cleanup_prefix) -> Dict[str, Any]:
    """Platform configuration for MinIO storage backend.

    Sets environment variables and returns config dict.
    """
    # Set environment variables for S3Storage
    os.environ["MINIO_ENDPOINT_URL"] = MINIO_ENDPOINT
    os.environ["MINIO_ACCESS_KEY"] = MINIO_ACCESS_KEY
    os.environ["MINIO_SECRET_KEY"] = MINIO_SECRET_KEY

    return {
        "bronze": {
            "storage_backend": "s3",
            "s3_bucket": minio_bucket,
            "s3_prefix": f"{cleanup_prefix}/bronze",
        },
        "silver": {
            "storage_backend": "s3",
            "s3_bucket": minio_bucket,
            "s3_prefix": f"{cleanup_prefix}/silver",
        },
        "s3_connection": {
            "endpoint_url_env": "MINIO_ENDPOINT_URL",
            "access_key_env": "MINIO_ACCESS_KEY",
            "secret_key_env": "MINIO_SECRET_KEY",
            "region": MINIO_REGION,
        },
    }


@pytest.fixture
def minio_bronze_config(minio_platform_config, temp_dir) -> Dict[str, Any]:
    """Complete Bronze extraction config for MinIO.

    Uses file source type for testing with synthetic data files.
    """
    return {
        "config_version": 1,
        "pipeline_id": "integration_test",
        "layer": "bronze",
        "domain": "test",
        "environment": "test",
        "data_classification": "internal",
        "platform": minio_platform_config,
        "source": {
            "system": "synthetic",
            "table": "claims",
            "type": "file",
            "file": {
                "path": str(temp_dir / "input"),
                "format": "parquet",
            },
            "run": {
                "load_pattern": "snapshot",
            },
        },
    }


# =============================================================================
# Synthetic Data Fixtures
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
    """Standard T2 (late data) date."""
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
def claims_t0_df(claims_generator, t0_date) -> pd.DataFrame:
    """T0 claims dataset."""
    return claims_generator.generate_t0(t0_date)


@pytest.fixture
def claims_t1_df(claims_generator, t0_date, t1_date, claims_t0_df) -> pd.DataFrame:
    """T1 claims dataset (incremental from T0)."""
    return claims_generator.generate_t1(t1_date, claims_t0_df)


@pytest.fixture
def orders_t0_df(orders_generator, t0_date) -> pd.DataFrame:
    """T0 orders dataset."""
    return orders_generator.generate_t0(t0_date)


@pytest.fixture
def transactions_t0_df(transactions_generator, t0_date) -> pd.DataFrame:
    """T0 transactions dataset."""
    return transactions_generator.generate_t0(t0_date)


@pytest.fixture
def claims_time_series(t0_date) -> Dict[str, pd.DataFrame]:
    """Complete T0/T1/T2 claims time series."""
    return generate_time_series_data("claims", t0_date, seed=42, row_count=100)


@pytest.fixture
def orders_time_series(t0_date) -> Dict[str, pd.DataFrame]:
    """Complete T0/T1 orders time series."""
    return generate_time_series_data("orders", t0_date, seed=42, row_count=100)


# =============================================================================
# Helper Functions
# =============================================================================


def upload_dataframe_to_minio(
    client,
    bucket: str,
    key: str,
    df: pd.DataFrame,
    format: str = "parquet",
) -> str:
    """Upload a DataFrame to MinIO as parquet or CSV.

    Args:
        client: boto3 S3 client
        bucket: Target bucket name
        key: Object key (path)
        df: DataFrame to upload
        format: Output format ('parquet' or 'csv')

    Returns:
        Full S3 path (s3://bucket/key)
    """
    import io

    if format == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        content_type = "application/octet-stream"
    else:
        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        content_type = "text/csv"

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType=content_type,
    )

    return f"s3://{bucket}/{key}"


def list_objects_in_prefix(client, bucket: str, prefix: str) -> list:
    """List all objects under a prefix.

    Returns list of object keys.
    """
    objects = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" in page:
            objects.extend([obj["Key"] for obj in page["Contents"]])
    return objects


def download_parquet_from_minio(client, bucket: str, key: str) -> pd.DataFrame:
    """Download a parquet file from MinIO as DataFrame."""
    import io

    response = client.get_object(Bucket=bucket, Key=key)
    buffer = io.BytesIO(response["Body"].read())
    return pd.read_parquet(buffer)


# =============================================================================
# Temp Directory Fixture (overrides conftest.py for integration tests)
# =============================================================================


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Temporary directory for integration test files."""
    return tmp_path


# =============================================================================
# Full Pipeline Testing Fixtures
# =============================================================================


@pytest.fixture
def synthetic_input_path(temp_dir: Path, claims_t0_df: pd.DataFrame) -> Path:
    """Write synthetic claims data to temp directory as parquet.

    Returns path to the parquet file (not directory).
    """
    input_dir = temp_dir / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = input_dir / "claims_t0.parquet"
    claims_t0_df.to_parquet(parquet_file, index=False)
    return parquet_file


@pytest.fixture
def bronze_output_path(temp_dir: Path) -> Path:
    """Path for Bronze extraction output."""
    output_dir = temp_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


@pytest.fixture
def bronze_pipeline_config(
    synthetic_input_path: Path,
    bronze_output_path: Path,
) -> Dict[str, Any]:
    """Complete Bronze pipeline configuration for testing.

    Uses local file source and local output (no S3 required).
    """
    return {
        "environment": "test",
        "domain": "healthcare",
        "system": "synthetic",
        "entity": "claims",
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(bronze_output_path.parent / "_checkpoints"),
                "output_defaults": {
                    "parquet": True,
                    "csv": False,
                },
            },
        },
        "source": {
            "system": "synthetic",
            "table": "claims",
            "type": "file",
            "file": {
                "path": str(synthetic_input_path),
                "format": "parquet",
            },
        "run": {
            "load_pattern": "snapshot",
            "local_output_dir": str(bronze_output_path),
            "storage_enabled": False,
            "max_rows_per_file": 0,
            "checkpoint_enabled": False,
            "cleanup_on_failure": True,
        },
        },
    }


@pytest.fixture
def minio_synthetic_input(
    minio_client,
    minio_bucket: str,
    cleanup_prefix: str,
    claims_t0_df: pd.DataFrame,
) -> str:
    """Upload synthetic data to MinIO and return S3 path.

    Returns the S3 key prefix where data was uploaded.
    """
    input_key = f"{cleanup_prefix}/synthetic/claims/claims_t0.parquet"
    upload_dataframe_to_minio(
        minio_client,
        minio_bucket,
        input_key,
        claims_t0_df,
        format="parquet",
    )
    return f"{cleanup_prefix}/synthetic/claims"


@pytest.fixture
def bronze_s3_pipeline_config(
    minio_bucket: str,
    cleanup_prefix: str,
    minio_synthetic_input: str,
    temp_dir: Path,
) -> Dict[str, Any]:
    """Bronze pipeline configuration with S3/MinIO storage backend.

    Uses MinIO for source data and Bronze output.
    """
    # Set environment variables for S3 access
    os.environ["MINIO_ENDPOINT_URL"] = MINIO_ENDPOINT
    os.environ["MINIO_ACCESS_KEY"] = MINIO_ACCESS_KEY
    os.environ["MINIO_SECRET_KEY"] = MINIO_SECRET_KEY

    return {
        "environment": "test",
        "domain": "healthcare",
        "system": "synthetic",
        "entity": "claims",
        "platform": {
            "bronze": {
                "storage_backend": "s3",
                "s3_bucket": minio_bucket,
                "s3_prefix": f"{cleanup_prefix}/bronze",
                "output_defaults": {
                    "parquet": True,
                    "csv": False,
                },
            },
            "s3_connection": {
                "endpoint_url": MINIO_ENDPOINT,
                "access_key": MINIO_ACCESS_KEY,
                "secret_key": MINIO_SECRET_KEY,
                "region": MINIO_REGION,
            },
        },
        "source": {
            "system": "synthetic",
            "table": "claims",
            "type": "file",
            "file": {
                "path": str(temp_dir / "input"),
                "format": "parquet",
            },
        "run": {
            "load_pattern": "snapshot",
            "local_output_dir": str(temp_dir / "output"),
            "storage_enabled": True,
            "max_rows_per_file": 0,
            "cleanup_on_failure": True,
            "checkpoint_enabled": False,
        },
        },
    }


# Note: run_bronze_extraction fixture removed - use pipelines.lib.bronze.BronzeSource instead
# Example:
#   from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
#   bronze = BronzeSource(system="test", entity="test", source_type=SourceType.FILE_PARQUET, ...)
#   result = bronze.run(run_date)
