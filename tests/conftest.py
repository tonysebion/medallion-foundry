"""Pytest configuration and fixtures."""

import os
import subprocess
import pytest
from core.config.loader import load_config_with_env
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem
import sys
from pathlib import Path


@pytest.fixture(scope="session", autouse=True)
def ensure_sample_data_available(pytestconfig) -> None:
    """Fail early if Bronze sample data is missing so tests don't run blind."""
    project_root = Path(__file__).resolve().parents[1]
    root = project_root / "sampledata" / "source_samples"

    if os.environ.get("SKIP_SAMPLE_DATA_CHECK"):
        return

    # If source_samples are missing, attempt to generate them automatically
    # However, skip sample generation when only running style checks to avoid long runs
    args = getattr(pytestconfig, "args", []) or []
    is_style_only = (
        all(
            any(s in str(arg) for s in ["test_style_black.py", "test_style_flake8.py"])
            for arg in args
        )
        if args
        else False
    )

    if is_style_only:
        # We are running style tests only; don't try to generate sample data
        return
    # Check for either local or S3 sample data presence. For S3, we try to
    # resolve at least one file via the storage config in the pattern YAMLs.
    sampledata_found = False
    # 1) Check local sampledata
    if root.exists() and (any(root.rglob("*.csv")) or any(root.rglob("*.parquet"))):
        sampledata_found = True

    # 2) Check S3 sample data configured by pattern configs (opt-in via `environment` in pattern YAML)
    if not sampledata_found:
        patterns_dir = project_root / "docs" / "examples" / "configs" / "patterns"
        for cfg_file in patterns_dir.glob("pattern_*.yaml"):
            try:
                dataset, env_cfg = load_config_with_env(cfg_file)
            except Exception:
                continue
            if dataset.bronze.source_type != "file":
                continue
            path_pattern = dataset.bronze.path_pattern or ""
            uri = StorageURI.parse(path_pattern)
            if uri.backend == "s3" and env_cfg and env_cfg.s3:
                try:
                    fs = create_filesystem(uri, env_cfg)
                    _ = fs.ls(uri.to_fsspec_path(env_cfg), detail=False)
                    sampledata_found = True
                    break
                except Exception:
                    # Try the next config
                    continue

    if not sampledata_found:
        pytest.exit(
            "Bronze sample data missing; populate `sampledata/source_samples` or ensure S3 is available"
        )


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
