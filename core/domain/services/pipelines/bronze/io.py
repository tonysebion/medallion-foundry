"""Functions for chunking records and writing CSV/Parquet.

This module provides Bronze-layer I/O utilities. Many utilities are now
shared with Silver via the runtime layer. This module re-exports them
for backward compatibility and adds Bronze-specific functions.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from core.infrastructure.runtime.file_io import (
    ChunkSizer as _RuntimeChunkSizer,
    chunk_records as _runtime_chunk_records,
    DataFrameMerger,
)
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata as _write_batch_metadata,
    write_checksum_manifest as _write_checksum_manifest,
)
from core.infrastructure.io.storage import compute_file_sha256 as _runtime_compute_file_sha256
from core.infrastructure.io.storage.checksum import (
    verify_checksum_manifest as _verify_checksum_manifest,
)

import pandas as pd

ChunkSizer = _RuntimeChunkSizer
chunk_records = _runtime_chunk_records
compute_file_sha256 = _runtime_compute_file_sha256
write_batch_metadata = _write_batch_metadata
write_checksum_manifest = _write_checksum_manifest
verify_checksum_manifest = _verify_checksum_manifest

logger = logging.getLogger(__name__)


def write_csv_chunk(chunk: List[Any], out_path: Path) -> None:
    if not chunk:
        return

    first = chunk[0]
    if not isinstance(first, dict):
        with out_path.open("w", encoding="utf-8") as f:
            for r in chunk:
                f.write(json.dumps(r) + "\n")
        logger.info("Wrote %d JSON lines to %s", len(chunk), out_path)
        return

    fieldnames = sorted(first.keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk)

    logger.info("Wrote %d rows to CSV at %s", len(chunk), out_path)


def write_parquet_chunk(
    chunk: List[Any], out_path: Path, compression: str = "snappy"
) -> None:
    if not chunk:
        return

    first = chunk[0]
    if not isinstance(first, dict):
        logger.warning(
            f"Records are not dict-like; skipping Parquet for {out_path.name}"
        )
        return

    df = pd.DataFrame.from_records(chunk)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression=compression)
    logger.info("Wrote %d rows to Parquet at %s", len(chunk), out_path)


def _merge_records_impl(
    existing_path: Path,
    new_records: List[Dict[str, Any]],
    primary_keys: List[str],
    out_path: Optional[Path],
    read_existing: Callable[[Path], pd.DataFrame],
    write_new: Callable[[pd.DataFrame, Path], None],
    write_merged: Callable[[pd.DataFrame, Path, int, int], None],
    count_existing: Callable[[Path], int],
    format_name: str,
) -> int:
    """Common merge implementation template for CSV and Parquet.

    Args:
        existing_path: Path to existing file
        new_records: New records to merge
        primary_keys: List of columns forming the primary key
        out_path: Output path (defaults to existing_path)
        read_existing: Callable to read existing file into DataFrame
        write_new: Callable(df, target) to write new DataFrame
        write_merged: Callable(df, target) to write merged DataFrame
        count_existing: Callable(path) to count rows in existing file
        format_name: Format name for logging (e.g., "Parquet", "CSV")

    Returns:
        Total record count after merge
    """
    if not new_records:
        logger.info("No new records to merge")
        if existing_path.exists():
            return count_existing(existing_path)
        return 0

    new_df = pd.DataFrame.from_records(new_records)
    target = out_path or existing_path

    if not existing_path.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        write_new(new_df, target)
        logger.info("Created new %s with %d records at %s", format_name, len(new_df), target)
        return len(new_df)

    existing_df = read_existing(existing_path)
    merged_df, updated_count, inserted_count = DataFrameMerger.merge_upsert(
        existing_df, new_df, primary_keys
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    write_merged(merged_df, target, updated_count, inserted_count)

    return len(merged_df)


def merge_parquet_records(
    existing_path: Path,
    new_records: List[Dict[str, Any]],
    primary_keys: List[str],
    out_path: Optional[Path] = None,
    compression: str = "snappy",
) -> int:
    """Merge new records with existing Parquet file using primary keys.

    Implements INCREMENTAL_MERGE pattern: upsert semantics where new records
    replace existing records with matching primary keys.

    Args:
        existing_path: Path to existing Parquet file
        new_records: New records to merge
        primary_keys: List of columns that form the primary key
        out_path: Output path (defaults to existing_path, overwriting)
        compression: Parquet compression codec

    Returns:
        Total record count after merge
    """
    def write_new(df: pd.DataFrame, target: Path) -> None:
        df.to_parquet(target, index=False, compression=compression)

    def write_merged(df: pd.DataFrame, target: Path, updated: int, inserted: int) -> None:
        df.to_parquet(target, index=False, compression=compression)
        logger.info(
            "Merged Parquet: %d total records (%d updated, %d inserted) at %s",
            len(df), updated, inserted, target,
        )

    def count_existing(path: Path) -> int:
        return len(pd.read_parquet(path))

    return _merge_records_impl(
        existing_path=existing_path,
        new_records=new_records,
        primary_keys=primary_keys,
        out_path=out_path,
        read_existing=pd.read_parquet,
        write_new=write_new,
        write_merged=write_merged,
        count_existing=count_existing,
        format_name="Parquet",
    )


def merge_csv_records(
    existing_path: Path,
    new_records: List[Dict[str, Any]],
    primary_keys: List[str],
    out_path: Optional[Path] = None,
) -> int:
    """Merge new records with existing CSV file using primary keys.

    Implements INCREMENTAL_MERGE pattern for CSV files.

    Args:
        existing_path: Path to existing CSV file
        new_records: New records to merge
        primary_keys: List of columns that form the primary key
        out_path: Output path (defaults to existing_path, overwriting)

    Returns:
        Total record count after merge
    """
    def write_new(df: pd.DataFrame, target: Path) -> None:
        df.to_csv(target, index=False)

    def write_merged(df: pd.DataFrame, target: Path, updated: int, inserted: int) -> None:
        df.to_csv(target, index=False)
        logger.info("Merged CSV: %d total records at %s", len(df), target)

    def count_existing(path: Path) -> int:
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1  # Subtract header

    return _merge_records_impl(
        existing_path=existing_path,
        new_records=new_records,
        primary_keys=primary_keys,
        out_path=out_path,
        read_existing=pd.read_csv,
        write_new=write_new,
        write_merged=write_merged,
        count_existing=count_existing,
        format_name="CSV",
    )
