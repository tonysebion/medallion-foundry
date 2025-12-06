"""Unified file I/O utilities for Bronze and Silver layers.

This module provides shared DataFrame reading and writing functionality
used by both the Bronze and Silver pipeline layers, eliminating code
duplication and ensuring consistent I/O patterns.

Key classes:
- DataFrameLoader: Load DataFrames from CSV/Parquet files (local or S3)
- DataFrameWriter: Write DataFrames to CSV/Parquet with atomic operations
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

# Re-export compute_file_sha256 from infrastructure for backward compatibility
from core.infrastructure.storage.checksum import compute_file_sha256

logger = logging.getLogger(__name__)


class DataFrameLoader:
    """Unified DataFrame loading from files and directories.

    Supports loading CSV and Parquet files from:
    - Single files
    - Directories (recursive or non-recursive)
    - S3 via fsspec (when filesystem is provided)

    Example:
        # Load from directory
        df = DataFrameLoader.from_directory(Path("data/bronze/partition"))

        # Load from directory with recursive search
        df = DataFrameLoader.from_directory(Path("data/bronze"), recursive=True)

        # Load from S3
        df = DataFrameLoader.from_s3("s3://bucket/prefix/", fs)
    """

    @staticmethod
    def from_directory(
        path: Path,
        recursive: bool = False,
        include_csv: bool = True,
        include_parquet: bool = True,
    ) -> pd.DataFrame:
        """Load all CSV/Parquet files from a directory.

        Args:
            path: Directory path to read from
            recursive: If True, search subdirectories (uses **/*.ext pattern)
            include_csv: Whether to include CSV files
            include_parquet: Whether to include Parquet files

        Returns:
            Concatenated DataFrame from all matching files

        Raises:
            FileNotFoundError: If no data files found at path
        """
        if path.is_file():
            return DataFrameLoader._read_single_file(path)

        pattern = "**/*" if recursive else "*"
        frames: list[pd.DataFrame] = []

        if include_parquet:
            parquet_files = sorted(path.glob(f"{pattern}.parquet"))
            for pf in parquet_files:
                logger.debug("Reading Parquet: %s", pf.name)
                frames.append(pd.read_parquet(pf))

        if include_csv:
            csv_files = sorted(path.glob(f"{pattern}.csv"))
            for cf in csv_files:
                logger.debug("Reading CSV: %s", cf.name)
                frames.append(pd.read_csv(cf))

        if not frames:
            raise FileNotFoundError(f"No data files found at {path}")

        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def from_s3(
        fsspec_path: str,
        fs: Any,
        include_csv: bool = True,
        include_parquet: bool = True,
    ) -> pd.DataFrame:
        """Load all CSV/Parquet files from an S3 path.

        Args:
            fsspec_path: S3 path in fsspec format (e.g., "s3://bucket/key")
            fs: fsspec-compatible filesystem object
            include_csv: Whether to include CSV files
            include_parquet: Whether to include Parquet files

        Returns:
            Concatenated DataFrame from all matching files

        Raises:
            FileNotFoundError: If no data files found at path
        """
        try:
            all_files = fs.ls(fsspec_path, detail=False)
        except FileNotFoundError:
            raise FileNotFoundError(f"No partition found at {fsspec_path}")

        csv_files = [f for f in all_files if f.endswith(".csv")] if include_csv else []
        parquet_files = (
            [f for f in all_files if f.endswith(".parquet")]
            if include_parquet
            else []
        )

        if not csv_files and not parquet_files:
            raise FileNotFoundError(f"No data files found at {fsspec_path}")

        frames: list[pd.DataFrame] = []

        for csv_file in csv_files:
            logger.debug("Reading CSV from S3: %s", csv_file)
            with fs.open(csv_file, "r") as f:
                frames.append(pd.read_csv(f))

        for parquet_file in parquet_files:
            logger.debug("Reading Parquet from S3: %s", parquet_file)
            with fs.open(parquet_file, "rb") as f:
                frames.append(pd.read_parquet(f))

        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _read_single_file(path: Path) -> pd.DataFrame:
        """Read a single file based on extension."""
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        elif path.suffix == ".csv":
            return pd.read_csv(path)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")


class DataFrameWriter:
    """Unified DataFrame writing with format support and atomic operations.

    Supports writing to CSV and/or Parquet with optional atomic replacement
    (write to temp file then rename) for crash safety.

    Example:
        writer = DataFrameWriter(write_parquet=True, write_csv=False)
        files = writer.write(df, Path("output/data"))

        # Write with chunk suffix
        files = writer.write_chunk(df, Path("output/data"), chunk_index=1)

        # Atomic write (temp file + rename)
        files = writer.write_atomic(df, Path("output/data"))
    """

    def __init__(
        self,
        write_parquet: bool = True,
        write_csv: bool = False,
        compression: str = "snappy",
    ) -> None:
        """Initialize writer with output format settings.

        Args:
            write_parquet: Whether to write Parquet files
            write_csv: Whether to write CSV files
            compression: Parquet compression codec (snappy, gzip, etc.)
        """
        self.write_parquet = write_parquet
        self.write_csv = write_csv
        self.compression = compression

    def write(
        self,
        df: pd.DataFrame,
        path: Path,
        base_name: str | None = None,
    ) -> list[Path]:
        """Write DataFrame to configured formats.

        Args:
            df: DataFrame to write
            path: Directory path for output
            base_name: Base filename (without extension). If None, uses directory name.

        Returns:
            List of created file paths
        """
        if df.empty:
            return []

        path.mkdir(parents=True, exist_ok=True)
        name = base_name or path.name
        files: list[Path] = []

        if self.write_parquet:
            parquet_path = path / f"{name}.parquet"
            df.to_parquet(parquet_path, index=False, compression=self.compression)
            logger.info("Wrote %d rows to Parquet at %s", len(df), parquet_path)
            files.append(parquet_path)

        if self.write_csv:
            csv_path = path / f"{name}.csv"
            df.to_csv(csv_path, index=False)
            logger.info("Wrote %d rows to CSV at %s", len(df), csv_path)
            files.append(csv_path)

        return files

    def write_chunk(
        self,
        df: pd.DataFrame,
        path: Path,
        chunk_index: int,
        prefix: str = "part",
    ) -> list[Path]:
        """Write DataFrame chunk with indexed suffix.

        Args:
            df: DataFrame to write
            path: Directory path for output
            chunk_index: Chunk number (0-indexed)
            prefix: Filename prefix

        Returns:
            List of created file paths
        """
        base_name = f"{prefix}-{chunk_index:04d}"
        return self.write(df, path, base_name)

    def write_atomic(
        self,
        df: pd.DataFrame,
        path: Path,
        base_name: str | None = None,
    ) -> list[Path]:
        """Write DataFrame with atomic replacement (temp file + rename).

        This ensures crash safety by writing to a temporary file first,
        then atomically renaming to the final path.

        Args:
            df: DataFrame to write
            path: Directory path for output
            base_name: Base filename (without extension)

        Returns:
            List of created file paths
        """
        if df.empty:
            return []

        path.mkdir(parents=True, exist_ok=True)
        name = base_name or path.name
        files: list[Path] = []

        if self.write_parquet:
            final_path = path / f"{name}.parquet"
            tmp_path = path / f".{name}.parquet.tmp"
            df.to_parquet(tmp_path, index=False, compression=self.compression)
            tmp_path.replace(final_path)
            logger.info("Wrote %d rows to Parquet at %s", len(df), final_path)
            files.append(final_path)

        if self.write_csv:
            final_path = path / f"{name}.csv"
            tmp_path = path / f".{name}.csv.tmp"
            df.to_csv(tmp_path, index=False)
            tmp_path.replace(final_path)
            logger.info("Wrote %d rows to CSV at %s", len(df), final_path)
            files.append(final_path)

        return files


def write_records_to_csv(records: list[dict[str, Any]], out_path: Path) -> None:
    """Write list of records to CSV file.

    Handles both dict records (written as CSV with header) and non-dict
    records (written as JSON lines).

    Args:
        records: List of records to write
        out_path: Output file path
    """
    if not records:
        return

    first = records[0]
    if not isinstance(first, dict):
        with out_path.open("w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        logger.info("Wrote %d JSON lines to %s", len(records), out_path)
        return

    fieldnames = sorted(first.keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logger.info("Wrote %d rows to CSV at %s", len(records), out_path)


def write_records_to_parquet(
    records: list[dict[str, Any]],
    out_path: Path,
    compression: str = "snappy",
) -> None:
    """Write list of records to Parquet file.

    Args:
        records: List of dict records to write
        out_path: Output file path
        compression: Parquet compression codec
    """
    if not records:
        return

    first = records[0]
    if not isinstance(first, dict):
        logger.warning(
            "Records are not dict-like; skipping Parquet for %s", out_path.name
        )
        return

    df = pd.DataFrame.from_records(records)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression=compression)
    logger.info("Wrote %d rows to Parquet at %s", len(records), out_path)


# =============================================================================
# Chunk Sizing and Record Chunking
# =============================================================================


class ChunkSizer:
    """Estimates memory size of records for chunk sizing heuristics.

    Used to create memory-aware chunks that respect both row count
    and approximate memory limits.

    Example:
        size = ChunkSizer.size_of({"name": "test", "value": 123})
    """

    @staticmethod
    def size_of(record: Any) -> int:
        """Estimate the memory size of a record in bytes.

        Args:
            record: Any Python object (dict, str, bytes, list, etc.)

        Returns:
            Estimated size in bytes
        """
        import sys

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
            return sum(ChunkSizer.size_of(item) for item in record) + len(record)

        return sys.getsizeof(record)


def chunk_records(
    records: list[dict[str, Any]],
    max_rows: int = 0,
    max_size_mb: float | None = None,
) -> list[list[dict[str, Any]]]:
    """Chunk records based on row count and/or memory size.

    Creates chunks that respect both maximum row count and approximate
    memory size limits. Useful for processing large datasets in manageable
    pieces.

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
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_size = 0

    for record in records:
        record_size = ChunkSizer.size_of(record)

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
        "Chunked %d records into %d chunks (max_rows=%d, max_size_mb=%s)",
        len(records),
        len(chunks),
        max_rows,
        max_size_mb,
    )
    return chunks


# =============================================================================
# DataFrame Merge and Key Operations
# =============================================================================


class DataFrameMerger:
    """Primary key operations for DataFrame merging and deduplication.

    Provides utilities for validating primary keys, building composite keys,
    and performing upsert/merge operations on DataFrames.

    Example:
        # Merge with upsert semantics
        merged, updated, inserted = DataFrameMerger.merge_upsert(
            existing_df, new_df, ["id"]
        )

        # Deduplicate by key
        deduped = DataFrameMerger.deduplicate(df, ["id"], order_by="updated_at")
    """

    @staticmethod
    def validate_keys(
        df: pd.DataFrame,
        primary_keys: list[str],
        context: str = "data",
    ) -> None:
        """Validate that primary key columns exist in DataFrame.

        Args:
            df: DataFrame to validate
            primary_keys: List of column names that form the primary key
            context: Description for error messages

        Raises:
            ValueError: If any primary key columns are missing
        """
        missing = [k for k in primary_keys if k not in df.columns]
        if missing:
            raise ValueError(f"Primary keys missing in {context}: {missing}")

    @staticmethod
    def build_composite_key(df: pd.DataFrame, primary_keys: list[str]) -> pd.Series:
        """Build a composite key series from multiple columns.

        Args:
            df: DataFrame containing key columns
            primary_keys: List of column names to combine

        Returns:
            Series of string or tuple keys
        """
        if len(primary_keys) == 1:
            return df[primary_keys[0]].astype(str)
        return df[primary_keys].astype(str).apply(lambda row: tuple(row), axis=1)

    @staticmethod
    def merge_upsert(
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame,
        primary_keys: list[str],
    ) -> tuple[pd.DataFrame, int, int]:
        """Merge DataFrames with upsert semantics.

        New records replace existing records with matching primary keys.
        Records in existing that don't match are preserved.

        Args:
            existing_df: Existing DataFrame
            new_df: New records to merge
            primary_keys: List of columns forming the primary key

        Returns:
            Tuple of (merged DataFrame, updated count, inserted count)

        Raises:
            ValueError: If primary keys are missing in either DataFrame
        """
        DataFrameMerger.validate_keys(existing_df, primary_keys, "existing data")
        DataFrameMerger.validate_keys(new_df, primary_keys, "new data")

        existing_series = DataFrameMerger.build_composite_key(existing_df, primary_keys)
        new_series = DataFrameMerger.build_composite_key(new_df, primary_keys)

        existing_keys = set(existing_series)
        new_keys = set(new_series)

        keep_mask = ~existing_series.isin(new_keys)
        kept_existing = existing_df[keep_mask]

        merged_df = pd.concat([kept_existing, new_df], ignore_index=True)
        updated_count = len(existing_keys & new_keys)
        inserted_count = len(new_keys - existing_keys)

        return merged_df, updated_count, inserted_count

    @staticmethod
    def deduplicate(
        df: pd.DataFrame,
        primary_keys: list[str],
        order_by: str | None = None,
        keep: str = "last",
    ) -> pd.DataFrame:
        """Deduplicate DataFrame by primary keys.

        Args:
            df: DataFrame to deduplicate
            primary_keys: List of columns forming the primary key
            order_by: Optional column to sort by before deduplication
            keep: Which duplicate to keep ('first' or 'last')

        Returns:
            Deduplicated DataFrame
        """
        if not primary_keys or any(pk not in df.columns for pk in primary_keys):
            logger.warning(
                "Primary keys missing or not present; returning original DataFrame"
            )
            return df

        working = df.copy()
        if order_by and order_by in df.columns:
            working = working.sort_values(order_by)
        else:
            working = working.reset_index()

        result = working.drop_duplicates(subset=primary_keys, keep=keep)
        return result.drop(columns=["index"], errors="ignore")


# =============================================================================
# DataFrame Normalization
# =============================================================================


def normalize_dataframe(
    df: pd.DataFrame,
    trim_strings: bool = False,
    empty_strings_as_null: bool = False,
) -> pd.DataFrame:
    """Normalize DataFrame string columns.

    Applies common string normalization operations:
    - Trim leading/trailing whitespace
    - Convert empty strings to null

    Args:
        df: DataFrame to normalize
        trim_strings: Whether to trim whitespace from string columns
        empty_strings_as_null: Whether to convert empty strings to None

    Returns:
        Normalized DataFrame (copy)
    """
    result = df.copy()
    if trim_strings or empty_strings_as_null:
        object_cols = result.select_dtypes(include="object").columns
        for col in object_cols:
            if trim_strings:
                result[col] = result[col].apply(
                    lambda val: val.strip() if isinstance(val, str) else val
                )
            if empty_strings_as_null:
                result[col] = result[col].apply(
                    lambda val: None if isinstance(val, str) and val == "" else val
                )
    return result


def sanitize_partition_value(value: Any) -> str:
    """Sanitize a value for use in partition paths.

    Replaces any characters that are not alphanumeric, dot, underscore,
    or hyphen with underscores.

    Args:
        value: Value to sanitize

    Returns:
        Sanitized string safe for filesystem paths
    """
    import re

    return re.sub(r"[^0-9A-Za-z._-]", "_", str(value))
