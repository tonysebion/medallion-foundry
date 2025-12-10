"""Memory-aware record chunking utilities.

This module provides utilities for splitting large record sets into
manageable chunks based on row count and/or memory size limits.

Key classes:
- ChunkSizer: Estimate memory size of records
- chunk_records: Split records into chunks respecting limits
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List

import pandas as pd

from core.infrastructure.io.storage.plan import ChunkWriterConfig

logger = logging.getLogger(__name__)


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

    Example:
        # Chunk by row count only
        chunks = chunk_records(records, max_rows=1000)

        # Chunk by memory size
        chunks = chunk_records(records, max_size_mb=10.0)

        # Chunk by both (whichever limit hits first)
        chunks = chunk_records(records, max_rows=1000, max_size_mb=10.0)
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


class ChunkWriter:
    """Write CSV/Parquet chunks and push to storage as needed."""

    def __init__(self, config: ChunkWriterConfig) -> None:
        self.config = config

    def write(self, chunk_index: int, chunk: List[dict[str, Any]]) -> List[Path]:
        created_files: List[Path] = []
        suffix = f"{self.config.chunk_prefix}-part-{chunk_index:04d}"

        try:
            if self.config.write_csv:
                csv_path = self.config.out_dir / f"{suffix}.csv"
                write_csv_chunk(chunk, csv_path)
                created_files.append(csv_path)
                self.config.storage_plan.upload(csv_path)

            if self.config.write_parquet:
                parquet_path = self.config.out_dir / f"{suffix}.parquet"
                write_parquet_chunk(
                    chunk, parquet_path, compression=self.config.parquet_compression
                )
                created_files.append(parquet_path)
                self.config.storage_plan.upload(parquet_path)

            logger.debug("Completed processing chunk %d", chunk_index)
            return created_files
        except Exception as exc:
            logger.error("Failed to process chunk %d: %s", chunk_index, exc)
            raise


class ChunkProcessor:
    """Coordinate sequential/parallel chunk execution."""

    def __init__(self, writer: ChunkWriter, parallel_workers: int) -> None:
        self.writer = writer
        self.parallel_workers = max(1, parallel_workers)

    def process(self, chunks: List[List[dict[str, Any]]]) -> List[Path]:
        if not chunks:
            return []

        created_files: List[Path] = []
        if self.parallel_workers > 1 and len(chunks) > 1:
            logger.info(
                "Processing %d chunks with %d workers", len(chunks), self.parallel_workers
            )
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                futures = {
                    executor.submit(self.writer.write, idx, chunk): idx
                    for idx, chunk in enumerate(chunks, start=1)
                }
                for future in as_completed(futures):
                    created_files.extend(future.result())
        else:
            for idx, chunk in enumerate(chunks, start=1):
                created_files.extend(self.writer.write(idx, chunk))

        return created_files


__all__ = [
    "ChunkSizer",
    "chunk_records",
    "write_csv_chunk",
    "write_parquet_chunk",
    "ChunkWriter",
    "ChunkProcessor",
]
