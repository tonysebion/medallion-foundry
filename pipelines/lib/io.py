"""Unified I/O utilities for pipelines.

Provides consistent read/write operations with metadata tracking
across different storage backends and file formats.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "ReadResult",
    "WriteMetadata",
    "get_latest_partition",
    "list_partitions",
    "read_bronze",
    "write_partitioned",
    "write_silver",
]


@dataclass
class WriteMetadata:
    """Metadata written alongside data files."""

    row_count: int
    columns: List[str]
    written_at: str
    source_path: Optional[str] = None
    pipeline_name: Optional[str] = None
    run_date: Optional[str] = None
    format: str = "parquet"
    compression: Optional[str] = None
    partition_by: Optional[List[str]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WriteMetadata":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class ReadResult:
    """Result of a read operation with metadata."""

    table: "ibis.Table"
    row_count: int
    columns: List[str]
    source_path: str
    metadata: Optional[WriteMetadata] = None


def read_bronze(
    path: str,
    *,
    run_date: Optional[str] = None,
) -> ReadResult:
    """Read Bronze layer data with metadata.

    Args:
        path: Path to Bronze data (supports {run_date} template)
        run_date: Run date for path substitution

    Returns:
        ReadResult with table and metadata
    """
    import ibis

    resolved_path = path.format(run_date=run_date) if run_date else path

    # Connect and read
    con = ibis.duckdb.connect()

    if resolved_path.endswith(".csv"):
        t = con.read_csv(resolved_path)
    else:
        t = con.read_parquet(resolved_path)

    row_count = t.count().execute()

    # Try to read metadata if it exists
    metadata = None
    metadata_path = _get_metadata_path(resolved_path)
    if metadata_path and Path(metadata_path).exists():
        try:
            with open(metadata_path) as f:
                metadata = WriteMetadata.from_dict(json.load(f))
        except Exception as e:
            logger.debug("Could not read metadata from %s: %s", metadata_path, e)

    return ReadResult(
        table=t,
        row_count=row_count,
        columns=list(t.columns),
        source_path=resolved_path,
        metadata=metadata,
    )


def write_silver(
    t: "ibis.Table",
    path: str,
    *,
    format: str = "parquet",
    compression: str = "snappy",
    partition_by: Optional[List[str]] = None,
    write_metadata: bool = True,
    pipeline_name: Optional[str] = None,
    run_date: Optional[str] = None,
    source_path: Optional[str] = None,
) -> WriteMetadata:
    """Write Silver layer data with metadata.

    Args:
        t: Ibis table to write
        path: Output path
        format: Output format ("parquet" or "csv")
        compression: Compression codec for parquet
        partition_by: Columns to partition by
        write_metadata: Whether to write _metadata.json
        pipeline_name: Name of the pipeline for metadata
        run_date: Run date for metadata
        source_path: Source path for lineage tracking

    Returns:
        WriteMetadata with details about what was written
    """
    # Execute count before writing (Ibis is lazy)
    row_count = t.count().execute()
    columns = list(t.columns)
    now = datetime.now(timezone.utc).isoformat()

    # Prepare output directory
    output_path = Path(path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Write data
    if format == "parquet":
        data_file = output_path / "data.parquet"
        t.to_parquet(str(data_file))
    elif format == "csv":
        data_file = output_path / "data.csv"
        t.execute().to_csv(str(data_file), index=False)
    else:
        raise ValueError(f"Unsupported format: {format}")

    # Create metadata
    metadata = WriteMetadata(
        row_count=row_count,
        columns=columns,
        written_at=now,
        source_path=source_path,
        pipeline_name=pipeline_name,
        run_date=run_date,
        format=format,
        compression=compression if format == "parquet" else None,
        partition_by=partition_by,
    )

    # Write metadata file
    if write_metadata:
        metadata_file = output_path / "_metadata.json"
        metadata_file.write_text(metadata.to_json())
        logger.debug("Wrote metadata to %s", metadata_file)

    logger.info("Wrote %d rows to %s", row_count, path)

    return metadata


def write_partitioned(
    t: "ibis.Table",
    path: str,
    partition_by: List[str],
    *,
    format: str = "parquet",
    compression: str = "snappy",
) -> Dict[str, WriteMetadata]:
    """Write data partitioned by specified columns.

    Args:
        t: Ibis table to write
        path: Base output path
        partition_by: Columns to partition by
        format: Output format
        compression: Compression codec

    Returns:
        Dictionary mapping partition paths to their metadata
    """
    import ibis

    # Get distinct partition values
    partition_values = t.select(*partition_by).distinct().execute()

    results: Dict[str, WriteMetadata] = {}

    for _, row in partition_values.iterrows():
        # Build partition path
        partition_parts = [f"{col}={row[col]}" for col in partition_by]
        partition_path = Path(path) / "/".join(partition_parts)

        # Filter to this partition
        filter_expr = ibis.literal(True)
        for col in partition_by:
            filter_expr = filter_expr & (t[col] == row[col])

        partition_data = t.filter(filter_expr)

        # Write partition
        metadata = write_silver(
            partition_data,
            str(partition_path),
            format=format,
            compression=compression,
            partition_by=None,  # Already partitioned
        )

        results[str(partition_path)] = metadata

    return results


def _get_metadata_path(data_path: str) -> Optional[str]:
    """Get the metadata file path for a data path."""
    path = Path(data_path)

    if path.is_file():
        # Data file -> metadata in same directory
        return str(path.parent / "_metadata.json")
    elif path.is_dir():
        # Directory -> metadata inside
        return str(path / "_metadata.json")
    elif "*" in data_path:
        # Glob pattern -> metadata in parent directory
        base = data_path.split("*")[0].rstrip("/")
        return str(Path(base) / "_metadata.json")

    return None


def list_partitions(
    base_path: str,
    partition_columns: List[str],
) -> List[Dict[str, str]]:
    """List available partitions under a base path.

    Args:
        base_path: Base path to search
        partition_columns: Expected partition column names

    Returns:
        List of partition value dictionaries

    Example:
        >>> list_partitions("s3://silver/orders/", ["year", "month"])
        [{"year": "2024", "month": "01"}, {"year": "2024", "month": "02"}, ...]
    """
    base = Path(base_path)
    partitions: List[Dict[str, str]] = []

    if not base.exists():
        return partitions

    def extract_partition(path: Path, depth: int = 0) -> Optional[Dict[str, str]]:
        """Recursively extract partition values."""
        if depth >= len(partition_columns):
            return {}

        result: Dict[str, str] = {}

        for child in path.iterdir():
            if child.is_dir() and "=" in child.name:
                col, val = child.name.split("=", 1)
                if col == partition_columns[depth]:
                    result[col] = val
                    sub_result = extract_partition(child, depth + 1)
                    if sub_result is not None:
                        result.update(sub_result)
                        partitions.append(result.copy())

        return result if result else None

    extract_partition(base)
    return partitions


def get_latest_partition(
    base_path: str,
    partition_column: str = "dt",
) -> Optional[str]:
    """Get the most recent partition value.

    Args:
        base_path: Base path to search
        partition_column: Partition column name (default "dt")

    Returns:
        Latest partition value or None
    """
    base = Path(base_path)

    if not base.exists():
        return None

    values = []
    for child in base.iterdir():
        if child.is_dir() and child.name.startswith(f"{partition_column}="):
            val = child.name.split("=", 1)[1]
            values.append(val)

    if not values:
        return None

    return sorted(values)[-1]
