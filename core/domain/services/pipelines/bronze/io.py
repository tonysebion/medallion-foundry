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
from typing import Any, Dict, List, Optional, cast

from core.foundation.time_utils import utc_isoformat as _utc_isoformat
from core.infrastructure.runtime.file_io import (
    ChunkSizer,
    chunk_records,
    DataFrameMerger,
)
from core.infrastructure.io.storage import (
    compute_file_sha256,
    write_checksum_manifest as _infra_write_checksum_manifest,
    verify_checksum_manifest as _infra_verify_checksum_manifest,
)

import pandas as pd

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


def write_batch_metadata(
    out_dir: Path,
    record_count: int,
    chunk_count: int,
    cursor: Optional[str] = None,
    performance_metrics: Optional[Dict[str, Any]] = None,
    quality_metrics: Optional[Dict[str, Any]] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Path:
    """
    Write metadata file for batch tracking and monitoring.

    Args:
        out_dir: Directory where metadata file will be written
        record_count: Total number of records processed
        chunk_count: Number of chunk files created
        cursor: Optional cursor value for incremental loads
        performance_metrics: Optional performance metrics (duration, records/sec, etc.)
        quality_metrics: Optional data quality metrics (nulls, duplicates, etc.)
    """
    import json

    metadata = {
        "timestamp": _utc_isoformat(),
        "record_count": record_count,
        "chunk_count": chunk_count,
    }
    if cursor:
        metadata["cursor"] = cursor

    if performance_metrics:
        metadata["performance"] = performance_metrics

    if quality_metrics:
        metadata["quality"] = quality_metrics

    if extra_metadata:
        metadata.update(extra_metadata)

    metadata_path = out_dir / "_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    logger.info("Wrote metadata to %s", metadata_path)
    return metadata_path


def write_checksum_manifest(
    out_dir: Path,
    files: List[Path],
    load_pattern: str,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Path:
    """Write a checksum manifest containing hashes of produced files.

    Delegates to infrastructure.storage.checksum.write_checksum_manifest().
    """
    return _infra_write_checksum_manifest(out_dir, files, load_pattern, extra_metadata)


def verify_checksum_manifest(
    bronze_dir: Path,
    manifest_name: str = "_checksums.json",
    expected_pattern: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate that files in a Bronze partition match recorded checksums.

    Delegates to infrastructure.storage.checksum.verify_checksum_manifest().
    """
    return _infra_verify_checksum_manifest(bronze_dir, manifest_name, expected_pattern)


# Backward compatibility aliases for internal merge functions
# Now delegating to the shared DataFrameMerger in runtime layer
def _validate_primary_keys(
    existing_df: pd.DataFrame, new_df: pd.DataFrame, primary_keys: List[str]
) -> None:
    """Validate primary keys exist in both DataFrames (delegates to DataFrameMerger)."""
    DataFrameMerger.validate_keys(existing_df, primary_keys, "existing data")
    DataFrameMerger.validate_keys(new_df, primary_keys, "new data")


def _build_key_series(df: pd.DataFrame, primary_keys: List[str]) -> pd.Series:
    """Build composite key series (delegates to DataFrameMerger)."""
    return DataFrameMerger.build_composite_key(df, primary_keys)


def _merge_dataframes(
    existing_df: pd.DataFrame, new_df: pd.DataFrame, primary_keys: List[str]
) -> tuple[pd.DataFrame, int, int]:
    """Return merged DataFrame along with counts of updated and inserted keys.

    Delegates to DataFrameMerger.merge_upsert().
    """
    return DataFrameMerger.merge_upsert(existing_df, new_df, primary_keys)


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
    if not new_records:
        logger.info("No new records to merge")
        if existing_path.exists():
            return len(pd.read_parquet(existing_path))
        return 0

    new_df = pd.DataFrame.from_records(new_records)

    if not existing_path.exists():
        # No existing data, just write new records
        target = out_path or existing_path
        target.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_parquet(target, index=False, compression=compression)
        logger.info("Created new Parquet with %d records at %s", len(new_df), target)
        return len(new_df)

    existing_df = pd.read_parquet(existing_path)
    merged_df, updated_count, inserted_count = DataFrameMerger.merge_upsert(
        existing_df, new_df, primary_keys
    )

    target = out_path or existing_path
    target.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_parquet(target, index=False, compression=compression)
    logger.info(
        "Merged Parquet: %d total records (%d updated, %d inserted) at %s",
        len(merged_df),
        updated_count,
        inserted_count,
        target,
    )

    return len(merged_df)


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
    if not new_records:
        logger.info("No new records to merge")
        if existing_path.exists():
            with existing_path.open("r", encoding="utf-8") as f:
                return sum(1 for _ in f) - 1  # Subtract header
        return 0

    new_df = pd.DataFrame.from_records(new_records)

    if not existing_path.exists():
        # No existing data, just write new records
        target = out_path or existing_path
        target.parent.mkdir(parents=True, exist_ok=True)
        new_df.to_csv(target, index=False)
        logger.info("Created new CSV with %d records at %s", len(new_df), target)
        return len(new_df)

    existing_df = pd.read_csv(existing_path)
    merged_df, _, _ = DataFrameMerger.merge_upsert(existing_df, new_df, primary_keys)

    target = out_path or existing_path
    target.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(target, index=False)

    logger.info("Merged CSV: %d total records at %s", len(merged_df), target)
    return len(merged_df)
