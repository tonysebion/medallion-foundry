"""Tests for enhanced features: file size control, partitioning, metadata."""

import pytest
from datetime import date
from core.io import chunk_records, write_batch_metadata
from core.config import build_relative_path


def test_chunk_records_by_size():
    """Test size-based chunking."""
    # Create sample records
    records = [{"id": i, "data": "x" * 1000} for i in range(100)]

    # Chunk with size limit
    chunks = chunk_records(records, max_rows=0, max_size_mb=0.01)  # 10KB limit

    # Should create multiple chunks due to size limit
    assert len(chunks) > 1

    # Each chunk should be relatively small
    for chunk in chunks:
        assert len(chunk) > 0


def test_chunk_records_by_rows():
    """Test row-based chunking."""
    records = [{"id": i} for i in range(100)]

    # Chunk with row limit
    chunks = chunk_records(records, max_rows=25, max_size_mb=None)

    # Should create 4 chunks of 25 records each
    assert len(chunks) == 4
    assert all(len(chunk) == 25 for chunk in chunks)


def test_chunk_records_combined():
    """Test combined row and size limits."""
    records = [{"id": i, "data": "x" * 100} for i in range(50)]

    # Both limits
    chunks = chunk_records(records, max_rows=20, max_size_mb=0.002)  # 2KB limit

    # Should chunk based on whichever limit is hit first
    assert len(chunks) >= 3
    assert all(len(chunk) <= 20 for chunk in chunks)


def test_partition_strategy_date():
    """Test date partitioning strategy."""
    cfg = {
        "platform": {
            "bronze": {
                "partitioning": {"use_dt_partition": True, "partition_strategy": "date"}
            }
        },
        "source": {"system": "test_system", "table": "test_table", "run": {}},
    }

    run_date = date(2025, 1, 12)
    path = build_relative_path(cfg, run_date)

    assert path == "system=test_system/table=test_table/pattern=full/dt=2025-01-12/"


def test_partition_strategy_hourly():
    """Test hourly partitioning strategy."""
    cfg = {
        "platform": {
            "bronze": {
                "partitioning": {
                    "use_dt_partition": True,
                    "partition_strategy": "hourly",
                }
            }
        },
        "source": {"system": "test_system", "table": "test_table", "run": {}},
    }

    run_date = date(2025, 1, 12)
    path = build_relative_path(cfg, run_date)

    # Should include hour partition
    assert path.startswith(
        "system=test_system/table=test_table/pattern=full/dt=2025-01-12/hour="
    )
    assert path.endswith("/")


def test_partition_strategy_batch_id():
    """Test batch_id partitioning strategy."""
    cfg = {
        "platform": {
            "bronze": {
                "partitioning": {
                    "use_dt_partition": True,
                    "partition_strategy": "batch_id",
                }
            }
        },
        "source": {
            "system": "test_system",
            "table": "test_table",
            "run": {"batch_id": "test_batch_123"},
        },
    }

    run_date = date(2025, 1, 12)
    path = build_relative_path(cfg, run_date)

    assert (
        path
        == "system=test_system/table=test_table/pattern=full/dt=2025-01-12/batch_id=test_batch_123/"
    )


def test_write_batch_metadata(tmp_path):
    """Test batch metadata writing."""

    # Write metadata with correct function signature (record_count, chunk_count)
    metadata_path = write_batch_metadata(
        tmp_path,
        record_count=1000,
        chunk_count=5,
        cursor="test-cursor",
        performance_metrics={"duration_seconds": 45.2},
        quality_metrics={"null_count": 0},
    )

    # Verify file created
    assert metadata_path.exists()
    assert metadata_path.name == "_metadata.json"

    # Verify contents
    import json

    with open(metadata_path) as f:
        loaded = json.load(f)

    assert loaded["record_count"] == 1000
    assert loaded["chunk_count"] == 5
    assert loaded["cursor"] == "test-cursor"
    assert "performance" in loaded
    assert "quality" in loaded


def test_partition_no_dt():
    """Test partitioning when use_dt_partition is False."""
    cfg = {
        "platform": {"bronze": {"partitioning": {"use_dt_partition": False}}},
        "source": {"system": "test_system", "table": "test_table", "run": {}},
    }

    run_date = date(2025, 1, 12)
    path = build_relative_path(cfg, run_date)

    # Should not include dt partition
    assert path == "system=test_system/table=test_table/pattern=full/"
    assert "dt=" not in path


def test_parallel_workers_validation():
    """Test validation of parallel_workers configuration."""
    from core.config import load_config
    import tempfile
    import yaml

    # Valid config with parallel_workers
    valid_config = {
        "platform": {
            "bronze": {
                "s3_bucket": "test",
                "s3_prefix": "bronze",
                "partitioning": {"use_dt_partition": True},
                "output_defaults": {
                    "allow_csv": True,
                    "allow_parquet": True,
                    "parquet_compression": "snappy",
                },
            },
            "s3_connection": {
                "endpoint_url_env": "S3_ENDPOINT",
                "access_key_env": "S3_KEY",
                "secret_key_env": "S3_SECRET",
            },
        },
        "source": {
            "type": "api",
            "system": "test",
            "table": "test",
            "api": {"base_url": "https://api.test.com", "endpoint": "/data"},
            "run": {"parallel_workers": 4, "local_output_dir": "./output"},
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(valid_config, f)
        temp_path = f.name

    try:
        # Should load successfully
        cfg = load_config(temp_path)
        assert cfg["source"]["run"]["parallel_workers"] == 4
    finally:
        import os

        os.unlink(temp_path)


def test_parallel_workers_invalid():
    """Test validation rejects invalid parallel_workers."""
    from core.config import load_config
    import tempfile
    import yaml

    # Invalid: negative value
    invalid_config = {
        "platform": {
            "bronze": {
                "s3_bucket": "test",
                "s3_prefix": "bronze",
                "partitioning": {"use_dt_partition": True},
                "output_defaults": {
                    "allow_csv": True,
                    "allow_parquet": True,
                    "parquet_compression": "snappy",
                },
            },
            "s3_connection": {
                "endpoint_url_env": "S3_ENDPOINT",
                "access_key_env": "S3_KEY",
                "secret_key_env": "S3_SECRET",
            },
        },
        "source": {
            "type": "api",
            "system": "test",
            "table": "test",
            "api": {"base_url": "https://api.test.com", "endpoint": "/data"},
            "run": {
                "parallel_workers": 0,  # Invalid: must be >= 1
                "local_output_dir": "./output",
            },
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(invalid_config, f)
        temp_path = f.name

    try:
        with pytest.raises(
            ValueError, match="parallel_workers must be a positive integer"
        ):
            load_config(temp_path)
    finally:
        import os

        os.unlink(temp_path)
