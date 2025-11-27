"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path
from core.samples.bootstrap import bootstrap as bootstrap_samples
from datetime import date


@pytest.fixture(scope="session", autouse=True)
def ensure_bootstrap_samples(tmp_path_factory):
    """Ensure Bronze sample data exists before tests run.

    Uses today's date for deterministic partition names already referenced
    in example configs. If data already exists, no changes are made.
    """
    root = Path("sampledata/source_samples")
    if not root.exists() or not any(root.rglob("*.csv")):
        bootstrap_samples([date.today().isoformat()])


# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_config():
    """Provide a sample valid configuration."""
    return {
        "platform": {
            "bronze": {
                "s3_bucket": "test-bronze-bucket",
                "s3_prefix": "bronze",
                "partitioning": {"use_dt_partition": True},
                "output_defaults": {
                    "allow_csv": True,
                    "allow_parquet": True,
                    "parquet_compression": "snappy",
                },
            },
            "s3_connection": {
                "endpoint_url_env": "TEST_S3_ENDPOINT",
                "access_key_env": "TEST_S3_KEY",
                "secret_key_env": "TEST_S3_SECRET",
            },
        },
        "source": {
            "type": "api",
            "system": "test_system",
            "table": "test_table",
            "api": {
                "base_url": "https://api.example.com",
                "endpoint": "/v1/test",
                "auth_type": "none",
            },
            "run": {
                "max_rows_per_file": 10000,
                "write_csv": True,
                "write_parquet": True,
                "s3_enabled": False,
                "local_output_dir": "./output",
                "timeout_seconds": 30,
            },
        },
    }


@pytest.fixture
def sample_records():
    """Provide sample records for testing."""
    return [
        {"id": 1, "name": "Record 1", "value": 100, "status": "active"},
        {"id": 2, "name": "Record 2", "value": 200, "status": "active"},
        {"id": 3, "name": "Record 3", "value": 300, "status": "inactive"},
        {"id": 4, "name": "Record 4", "value": 400, "status": "active"},
        {"id": 5, "name": "Record 5", "value": 500, "status": "pending"},
    ]
