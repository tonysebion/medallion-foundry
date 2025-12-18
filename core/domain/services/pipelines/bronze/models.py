"""Bronze pipeline models and configuration defaults."""

from __future__ import annotations

from typing import Any, Dict

from core.foundation.primitives.patterns import LoadPattern

DEFAULT_BRONZE_OUTPUT_DEFAULTS: Dict[str, Any] = {
    "allow_csv": True,
    "allow_parquet": True,
    "parquet_compression": "snappy",
}

DEFAULT_BRONZE_PARTITIONING: Dict[str, Any] = {
    "use_dt_partition": True,
    "partition_strategy": "date",
}

DEFAULT_BRONZE_RUN_OPTIONS: Dict[str, Any] = {
    "load_pattern": LoadPattern.SNAPSHOT.value,
    "max_rows_per_file": 0,
    "max_file_size_mb": None,
    "parallel_workers": 1,
    "write_csv": True,
    "write_parquet": True,
    "cleanup_on_failure": True,
    "local_output_dir": "./output",
}


def default_bronze_platform_config() -> Dict[str, Any]:
    """Return defaults for the platform.bronze section."""
    return {
        "storage_backend": "s3",
        "output_defaults": DEFAULT_BRONZE_OUTPUT_DEFAULTS.copy(),
        "partitioning": DEFAULT_BRONZE_PARTITIONING.copy(),
    }


def default_bronze_run_config() -> Dict[str, Any]:
    """Return defaults for source.run.*."""
    return DEFAULT_BRONZE_RUN_OPTIONS.copy()


__all__ = [
    "LoadPattern",
    "DEFAULT_BRONZE_OUTPUT_DEFAULTS",
    "DEFAULT_BRONZE_PARTITIONING",
    "DEFAULT_BRONZE_RUN_OPTIONS",
    "default_bronze_platform_config",
    "default_bronze_run_config",
]
