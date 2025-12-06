"""Functions for chunking records and writing CSV/Parquet."""

import logging
from typing import List, Dict, Any, Optional, cast
from pathlib import Path
import csv
import json
import sys
import hashlib

import pandas as pd

logger = logging.getLogger(__name__)


def estimate_record_size(record: Any) -> int:
    """Estimate the memory size of a single record in bytes."""
    if record is None:
        return 0

    if isinstance(record, bytes):
        return len(record)

    if isinstance(record, str):
        return len(record.encode("utf-8"))

    if isinstance(record, dict):
        try:
            payload = json.dumps(record, ensure_ascii=False)
        except TypeError:
            payload = str(record)
        return len(payload.encode("utf-8"))

    if isinstance(record, (list, tuple)):
        return sum(estimate_record_size(item) for item in record) + len(record)

    return sys.getsizeof(record)


def chunk_records(
    records: List[Dict[str, Any]],
    max_rows: int = 0,
    max_size_mb: Optional[float] = None,
) -> List[List[Dict[str, Any]]]:
    """
    Chunk records based on row count and/or file size.

    Args:
        records: List of records to chunk
        max_rows: Maximum rows per chunk (0 = no limit)
        max_size_mb: Maximum size per chunk in MB (None = no limit)

    Returns:
        List of record chunks
    """
    if not records:
        return []

    # Simple row-based chunking if no size limit
    if max_size_mb is None or max_size_mb <= 0:
        if max_rows <= 0:
            return [records]
        return [records[i : i + max_rows] for i in range(0, len(records), max_rows)]

    # Size-aware chunking
    max_size_bytes = max_size_mb * 1024 * 1024
    chunks = []
    current_chunk: List[Dict[str, Any]] = []
    current_size = 0

    for record in records:
        record_size = estimate_record_size(record)

        # Check if adding this record would exceed limits
        would_exceed_size = (current_size + record_size) > max_size_bytes
        would_exceed_rows = max_rows > 0 and len(current_chunk) >= max_rows

        if current_chunk and (would_exceed_size or would_exceed_rows):
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0

        current_chunk.append(record)
        current_size += record_size

    # Add final chunk
    if current_chunk:
        chunks.append(current_chunk)

    logger.info(
        f"Chunked {len(records)} records into {len(chunks)} chunks "
        f"(max_rows={max_rows}, max_size_mb={max_size_mb})"
    )
    return chunks


def write_csv_chunk(chunk: List[Any], out_path: Path) -> None:
    if not chunk:
        return

    first = chunk[0]
    if not isinstance(first, dict):
        with out_path.open("w", encoding="utf-8") as f:
            for r in chunk:
                f.write(json.dumps(r) + "\n")
        logger.info(f"Wrote {len(chunk)} JSON lines to {out_path}")
        return

    fieldnames = sorted(first.keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk)

    logger.info(f"Wrote {len(chunk)} rows to CSV at {out_path}")


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
    logger.info(f"Wrote {len(chunk)} rows to Parquet at {out_path}")


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
    from datetime import datetime
    import json

    metadata = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
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

    logger.info(f"Wrote metadata to {metadata_path}")
    return metadata_path


def write_checksum_manifest(
    out_dir: Path,
    files: List[Path],
    load_pattern: str,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Path:
    """
    Write a checksum manifest containing hashes of produced files.
    """
    import json
    from datetime import datetime

    manifest: Dict[str, Any] = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "load_pattern": load_pattern,
        "files": [],
    }

    for file_path in files:
        if not file_path.exists():
            continue
        hasher = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        manifest["files"].append(
            {
                "path": file_path.name,
                "size_bytes": file_path.stat().st_size,
                "sha256": hasher.hexdigest(),
            }
        )

    if extra_metadata:
        manifest.update(extra_metadata)

    manifest_path = out_dir / "_checksums.json"
    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    logger.info(f"Wrote checksum manifest to {manifest_path}")
    return manifest_path


def verify_checksum_manifest(
    bronze_dir: Path,
    manifest_name: str = "_checksums.json",
    expected_pattern: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validate that the files in a Bronze partition match the recorded checksums.

    Args:
        bronze_dir: The Bronze partition directory to verify.
        manifest_name: Name of the manifest file (defaults to '_checksums.json').
        expected_pattern: Optional load pattern that must match the manifest.

    Returns:
        Parsed manifest dictionary if verification succeeds.

    Raises:
        FileNotFoundError: If the manifest is missing.
        ValueError: If the manifest is malformed or verification fails.
    """
    manifest_path = bronze_dir / manifest_name
    if not manifest_path.exists():
        raise FileNotFoundError(f"Checksum manifest not found at {manifest_path}")

    with manifest_path.open("r", encoding="utf-8") as handle:
        manifest: Dict[str, Any] = cast(Dict[str, Any], json.load(handle))

    if expected_pattern and manifest.get("load_pattern") != expected_pattern:
        raise ValueError(
            f"Manifest load_pattern {manifest.get('load_pattern')} does not match expected {expected_pattern}"
        )

    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError(
            f"Checksum manifest at {manifest_path} does not list any files to validate"
        )

    missing_files = []
    mismatched_files = []

    for entry in files:
        rel_name = entry.get("path")
        if not rel_name:
            raise ValueError(f"Malformed entry in checksum manifest: {entry}")
        target = bronze_dir / rel_name
        if not target.exists():
            missing_files.append(rel_name)
            continue

        expected_size = entry.get("size_bytes")
        expected_hash = entry.get("sha256")

        actual_size = target.stat().st_size
        hasher = hashlib.sha256()
        with target.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        actual_hash = hasher.hexdigest()

        if actual_size != expected_size or actual_hash != expected_hash:
            mismatched_files.append(rel_name)

    if missing_files or mismatched_files:
        issues = []
        if missing_files:
            issues.append(f"missing files: {missing_files}")
        if mismatched_files:
            issues.append(f"checksum mismatches: {mismatched_files}")
        raise ValueError(
            f"Checksum verification failed for {manifest_path}: {', '.join(issues)}"
        )

    return manifest


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
        logger.info(f"Created new Parquet with {len(new_df)} records at {target}")
        return len(new_df)

    # Load existing data
    existing_df = pd.read_parquet(existing_path)

    # Verify primary keys exist in both DataFrames
    missing_in_existing = [k for k in primary_keys if k not in existing_df.columns]
    missing_in_new = [k for k in primary_keys if k not in new_df.columns]

    if missing_in_existing:
        raise ValueError(f"Primary keys missing in existing data: {missing_in_existing}")
    if missing_in_new:
        raise ValueError(f"Primary keys missing in new data: {missing_in_new}")

    # Create composite key for matching
    if len(primary_keys) == 1:
        pk_col = primary_keys[0]
        existing_keys = set(existing_df[pk_col].astype(str))
        new_keys = set(new_df[pk_col].astype(str))
    else:
        # Multi-column key: create tuple index
        existing_keys = set(
            existing_df[primary_keys].apply(lambda r: tuple(str(v) for v in r), axis=1)
        )
        new_keys = set(
            new_df[primary_keys].apply(lambda r: tuple(str(v) for v in r), axis=1)
        )

    # Identify records to keep from existing (those not in new)
    if len(primary_keys) == 1:
        pk_col = primary_keys[0]
        keep_mask = ~existing_df[pk_col].astype(str).isin(new_keys)
    else:
        keep_mask = ~existing_df[primary_keys].apply(
            lambda r: tuple(str(v) for v in r), axis=1
        ).isin(new_keys)

    kept_existing = existing_df[keep_mask]

    # Concatenate kept existing with new records
    merged_df = pd.concat([kept_existing, new_df], ignore_index=True)

    # Write merged result
    target = out_path or existing_path
    target.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_parquet(target, index=False, compression=compression)

    updated_count = len(existing_keys & new_keys)
    inserted_count = len(new_keys - existing_keys)
    logger.info(
        f"Merged Parquet: {len(merged_df)} total records "
        f"({updated_count} updated, {inserted_count} inserted) at {target}"
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
        logger.info(f"Created new CSV with {len(new_df)} records at {target}")
        return len(new_df)

    # Load existing data
    existing_df = pd.read_csv(existing_path)

    # Verify primary keys exist
    missing_in_existing = [k for k in primary_keys if k not in existing_df.columns]
    missing_in_new = [k for k in primary_keys if k not in new_df.columns]

    if missing_in_existing:
        raise ValueError(f"Primary keys missing in existing data: {missing_in_existing}")
    if missing_in_new:
        raise ValueError(f"Primary keys missing in new data: {missing_in_new}")

    # Create key set from new records
    if len(primary_keys) == 1:
        pk_col = primary_keys[0]
        new_keys = set(new_df[pk_col].astype(str))
        keep_mask = ~existing_df[pk_col].astype(str).isin(new_keys)
    else:
        new_keys = set(
            new_df[primary_keys].apply(lambda r: tuple(str(v) for v in r), axis=1)
        )
        keep_mask = ~existing_df[primary_keys].apply(
            lambda r: tuple(str(v) for v in r), axis=1
        ).isin(new_keys)

    kept_existing = existing_df[keep_mask]

    # Concatenate and write
    merged_df = pd.concat([kept_existing, new_df], ignore_index=True)
    target = out_path or existing_path
    target.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(target, index=False)

    logger.info(f"Merged CSV: {len(merged_df)} total records at {target}")
    return len(merged_df)
