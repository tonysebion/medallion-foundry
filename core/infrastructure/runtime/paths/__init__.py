"""Path helpers package (kept for compatibility)."""

from __future__ import annotations

from datetime import datetime as _datetime

from .partition_builder import (
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_bronze_relative_path,
    build_silver_partition,
    build_silver_partition_path,
    reset_datetime_provider,
    set_datetime_provider,
)

datetime = _datetime

__all__ = [
    "BronzePartition",
    "SilverPartition",
    "build_bronze_partition",
    "build_bronze_relative_path",
    "build_silver_partition",
    "build_silver_partition_path",
    "reset_datetime_provider",
    "set_datetime_provider",
    "datetime",
]
