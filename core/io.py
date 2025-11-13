"""Functions for chunking records and writing CSV/Parquet."""

import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import csv
import json
import sys

import pandas as pd

logger = logging.getLogger(__name__)


def estimate_record_size(record: Dict[str, Any]) -> int:
    """Estimate the memory size of a single record in bytes."""
    return sys.getsizeof(str(record))


def chunk_records(
    records: List[Dict[str, Any]], 
    max_rows: int = 0,
    max_size_mb: Optional[float] = None
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
    current_chunk = []
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
    
    logger.info(f"Chunked {len(records)} records into {len(chunks)} chunks "
                f"(max_rows={max_rows}, max_size_mb={max_size_mb})")
    return chunks


def write_csv_chunk(chunk: List[Dict[str, Any]], out_path: Path) -> None:
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


def write_parquet_chunk(chunk: List[Dict[str, Any]], out_path: Path, compression: str = "snappy") -> None:
    if not chunk:
        return

    first = chunk[0]
    if not isinstance(first, dict):
        logger.warning(f"Records are not dict-like; skipping Parquet for {out_path.name}")
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
    quality_metrics: Optional[Dict[str, Any]] = None
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

    metadata_path = out_dir / "_metadata.json"
    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Wrote metadata to {metadata_path}")
    return metadata_path
