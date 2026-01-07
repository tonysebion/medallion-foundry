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
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import ibis

logger = logging.getLogger(__name__)

__all__ = [
    "OutputMetadata",
    "ReadResult",
    "SilverOutputMetadata",
    "WriteMetadata",
    "get_latest_partition",
    "infer_column_types",
    "list_partitions",
    "maybe_dry_run",
    "maybe_skip_if_exists",
    "read_bronze",
    "write_partitioned",
    "write_silver",
    "write_silver_with_artifacts",
]


@dataclass
class OutputMetadata:
    """Unified metadata for Bronze, Silver, and API outputs.

    Common fields are defined as dataclass attributes. Source-specific fields
    (system, entity, watermarks, etc.) are stored in the 'extra' dict.

    Examples:
        # Bronze output
        metadata = OutputMetadata(
            row_count=1000,
            columns=[{"name": "id", "type": "int64"}],
            written_at="2025-01-15T10:00:00Z",
            run_date="2025-01-15",
            extra={
                "system": "claims_dw",
                "entity": "claims_header",
                "source_type": "database_mssql",
                "load_pattern": "full_snapshot",
            },
        )

        # Silver output
        metadata = OutputMetadata(
            row_count=1000,
            columns=[{"name": "id", "type": "int64"}],
            written_at="2025-01-15T10:00:00Z",
            run_date="2025-01-15",
            extra={
                "entity_kind": "state",
                "history_mode": "current_only",
                "natural_keys": ["id"],
                "change_timestamp": "updated_at",
            },
        )
    """

    # Core fields (present in all outputs)
    row_count: int
    columns: List[Dict[str, Any]]  # List of {name, type, nullable, ...}
    written_at: str

    # Common optional fields
    run_date: Optional[str] = None
    format: str = "parquet"
    compression: Optional[str] = None
    data_files: List[str] = field(default_factory=list)

    # Source-specific fields go here
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary (extra fields merged at top level)."""
        result = asdict(self)
        # Merge extra into top level for backwards compatibility
        extra = result.pop("extra", {})
        result.update(extra)
        return result

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OutputMetadata":
        """Create from dictionary, putting unknown fields in extra."""
        known_fields = {k for k in cls.__dataclass_fields__}
        known = {k: v for k, v in data.items() if k in known_fields and k != "extra"}
        extra = data.get("extra", {})
        # Also collect unknown fields into extra
        for k, v in data.items():
            if k not in known_fields:
                extra[k] = v
        known["extra"] = extra
        return cls(**known)

    @classmethod
    def from_file(cls, path: Path) -> "OutputMetadata":
        """Load from file."""
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


# Backwards compatibility alias
WriteMetadata = OutputMetadata


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

    # Create metadata - use OutputMetadata with extra for optional fields
    metadata = OutputMetadata(
        row_count=row_count,
        columns=[{"name": c} for c in columns],  # Convert to list of dicts
        written_at=now,
        run_date=run_date,
        format=format,
        compression=compression if format == "parquet" else None,
        extra={
            "source_path": source_path,
            "pipeline_name": pipeline_name,
            "partition_by": partition_by,
        },
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


@dataclass
class SilverOutputMetadata:
    """Comprehensive metadata for Silver layer outputs.

    Includes schema information for PolyBase external table generation.
    """

    # Basic metadata
    row_count: int
    columns: List[Dict[str, Any]]  # List of {name, type, nullable}
    written_at: str

    # Entity information
    entity_kind: str  # "state" or "event"
    history_mode: str  # "current_only" or "full_history"
    natural_keys: List[str]
    change_timestamp: str

    # Partitioning
    partition_by: Optional[List[str]] = None

    # Source tracking
    source_path: Optional[str] = None
    pipeline_name: Optional[str] = None
    run_date: Optional[str] = None

    # Format info
    format: str = "parquet"
    compression: str = "snappy"

    # Files written
    data_files: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SilverOutputMetadata":
        """Create from dictionary."""
        fields = {k for k in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in fields})

    @classmethod
    def from_file(cls, path: Path) -> "SilverOutputMetadata":
        """Load from file."""
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))


def infer_column_types(
    source: Any,
    *,
    include_sql_types: bool = True,
) -> List[Dict[str, Any]]:
    """Infer column types from an Ibis table or list of records.

    This is the unified column inference function used by Bronze, Silver, and API.

    Args:
        source: Either an Ibis table or a list of dicts (records)
        include_sql_types: Whether to include SQL Server type mappings

    Returns:
        List of column info dicts with name, type, nullable, and optionally sql_type

    Examples:
        # From Ibis table
        columns = infer_column_types(ibis_table)

        # From list of records (API data)
        columns = infer_column_types([{"id": 1, "name": "foo"}])
    """
    # Check if it's an Ibis table by looking for schema method
    if hasattr(source, "schema"):
        return _infer_column_types_from_ibis(source, include_sql_types)
    elif isinstance(source, list):
        return _infer_column_types_from_records(source)
    else:
        raise TypeError(f"Cannot infer columns from {type(source)}")


def _infer_column_types_from_ibis(
    t: "ibis.Table",
    include_sql_types: bool = True,
) -> List[Dict[str, Any]]:
    """Infer column types from an Ibis table schema."""
    columns = []
    schema = t.schema()

    for name in schema.names:
        dtype = schema[name]
        type_str = str(dtype)

        col_info: Dict[str, Any] = {
            "name": name,
            "ibis_type": type_str,
            "nullable": dtype.nullable if hasattr(dtype, "nullable") else True,
        }

        if include_sql_types:
            col_info["sql_type"] = _map_ibis_type_to_sql(type_str)

        columns.append(col_info)

    return columns


def _infer_column_types_from_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Infer column types from a list of records (dicts).

    Column order is preserved based on first appearance across records,
    rather than being sorted alphabetically.
    """
    if not records:
        return []

    # Collect all keys while preserving insertion order (first-seen order)
    # Using dict.fromkeys() to dedupe while preserving order
    all_keys: Dict[str, None] = {}
    for record in records:
        for key in record.keys():
            if key not in all_keys:
                all_keys[key] = None

    # Infer types from first non-null value
    columns = []
    for key in all_keys:
        sample_value = None
        for record in records:
            if key in record and record[key] is not None:
                sample_value = record[key]
                break

        if sample_value is None:
            type_name = "string"
        else:
            type_name = type(sample_value).__name__

        columns.append({
            "name": key,
            "type": type_name,
            "nullable": True,
        })

    return columns


# Keep the old name as an alias for backwards compatibility
def _infer_column_types(t: "ibis.Table") -> List[Dict[str, Any]]:
    """Infer column types from an Ibis table schema.

    Deprecated: Use infer_column_types() instead.
    """
    return infer_column_types(t, include_sql_types=True)


def _map_ibis_type_to_sql(ibis_type: str) -> str:
    """Map Ibis type string to SQL Server type.

    Args:
        ibis_type: Ibis type name

    Returns:
        SQL Server type string
    """
    type_lower = ibis_type.lower()

    # Handle parametric types
    if "string" in type_lower or "utf8" in type_lower:
        return "NVARCHAR(4000)"
    elif "int64" in type_lower or "bigint" in type_lower:
        return "BIGINT"
    elif "int32" in type_lower or "int" in type_lower:
        return "INT"
    elif "int16" in type_lower or "smallint" in type_lower:
        return "SMALLINT"
    elif "float64" in type_lower or "double" in type_lower:
        return "FLOAT"
    elif "float32" in type_lower or "float" in type_lower:
        return "REAL"
    elif "decimal" in type_lower:
        # Try to extract precision/scale
        return "DECIMAL(18,2)"
    elif "bool" in type_lower:
        return "BIT"
    elif "date" in type_lower and "time" not in type_lower:
        return "DATE"
    elif "timestamp" in type_lower or "datetime" in type_lower:
        return "DATETIME2"
    elif "time" in type_lower:
        return "TIME"
    elif "binary" in type_lower or "bytes" in type_lower:
        return "VARBINARY(MAX)"
    else:
        return "NVARCHAR(4000)"


def write_silver_with_artifacts(
    t: "ibis.Table",
    path: str,
    *,
    entity_kind: str,
    history_mode: str,
    natural_keys: List[str],
    change_timestamp: str,
    format: str = "parquet",
    compression: str = "snappy",
    partition_by: Optional[List[str]] = None,
    pipeline_name: Optional[str] = None,
    run_date: Optional[str] = None,
    source_path: Optional[str] = None,
    write_checksums: bool = True,
    entity_name: Optional[str] = None,
) -> SilverOutputMetadata:
    """Write Silver layer data with metadata and checksums.

    This is the preferred method for writing Silver outputs as it generates
    all necessary artifacts for data integrity and PolyBase integration.

    Args:
        t: Ibis table to write
        path: Output directory path
        entity_kind: "state" or "event"
        history_mode: "current_only" or "full_history"
        natural_keys: Primary key columns
        change_timestamp: Timestamp column name
        format: Output format ("parquet" or "csv")
        compression: Compression codec for parquet
        partition_by: Columns to partition by
        pipeline_name: Name of the pipeline for metadata
        run_date: Run date for metadata
        source_path: Source path for lineage tracking
        write_checksums: Whether to write _checksums.json
        entity_name: Entity name for filename (defaults to 'data' for backwards compat)

    Returns:
        SilverOutputMetadata with comprehensive details

    Example:
        >>> metadata = write_silver_with_artifacts(
        ...     table,
        ...     "./silver/orders/",
        ...     entity_kind="state",
        ...     history_mode="current_only",
        ...     natural_keys=["order_id"],
        ...     change_timestamp="updated_at",
        ...     entity_name="orders",  # Creates orders.parquet
        ... )
    """
    from pipelines.lib.checksum import write_checksum_manifest

    # Execute count before writing (Ibis is lazy)
    row_count = t.count().execute()
    columns = _infer_column_types(t)
    now = datetime.now(timezone.utc).isoformat()

    # Prepare output directory
    output_path = Path(path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Use entity name for filename if provided, otherwise fall back to 'data'
    base_filename = entity_name if entity_name else "data"

    # Write data files
    data_files = []
    if format == "parquet":
        data_file = output_path / f"{base_filename}.parquet"
        t.to_parquet(str(data_file))
        data_files.append(str(data_file))
    elif format == "csv":
        data_file = output_path / f"{base_filename}.csv"
        t.execute().to_csv(str(data_file), index=False)
        data_files.append(str(data_file))
    else:
        raise ValueError(f"Unsupported format: {format}")

    # Create comprehensive metadata
    metadata = SilverOutputMetadata(
        row_count=row_count,
        columns=columns,
        written_at=now,
        entity_kind=entity_kind,
        history_mode=history_mode,
        natural_keys=natural_keys,
        change_timestamp=change_timestamp,
        partition_by=partition_by,
        source_path=source_path,
        pipeline_name=pipeline_name,
        run_date=run_date,
        format=format,
        compression=compression if format == "parquet" else None,
        data_files=[Path(f).name for f in data_files],
    )

    # Write metadata file
    metadata_file = output_path / "_metadata.json"
    metadata_file.write_text(metadata.to_json(), encoding="utf-8")
    logger.debug("Wrote metadata to %s", metadata_file)

    # Write checksums
    if write_checksums:
        write_checksum_manifest(
            output_path,
            [Path(f) for f in data_files],
            entity_kind=entity_kind,
            history_mode=history_mode,
            row_count=row_count,
        )

    logger.info("Wrote %d rows to %s with artifacts", row_count, path)

    return metadata


# --- Run helpers (consolidated from run_helpers.py) ---

ExistsFn = Callable[[], bool]


def maybe_skip_if_exists(
    *,
    skip_if_exists: bool,
    exists_fn: ExistsFn,
    target: str,
    logger: logging.Logger,
    context: Optional[str] = None,
    reason: str = "already_exists",
) -> Optional[Dict[str, Any]]:
    """Return skip metadata when data already exists.

    Args:
        skip_if_exists: Whether skip-if-exists check is enabled
        exists_fn: Callable that returns True if target data exists
        target: Target path for logging
        logger: Logger instance
        context: Optional context label for logging (default: "pipeline")
        reason: Skip reason for metadata (default: "already_exists")

    Returns:
        Skip metadata dict if skipped, None otherwise
    """
    if not skip_if_exists:
        return None

    if exists_fn():
        label = context or "pipeline"
        logger.info("Skipping %s - data already exists at %s", label, target)
        return {"skipped": True, "reason": reason, "target": target}

    return None


def maybe_dry_run(
    *,
    dry_run: bool,
    logger: logging.Logger,
    message: str,
    message_args: Tuple[Any, ...] = (),
    message_kwargs: Optional[Dict[str, Any]] = None,
    target: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Return dry-run metadata when dry_run flag is set.

    Args:
        dry_run: Whether dry-run mode is enabled
        logger: Logger instance
        message: Log message format string
        message_args: Positional args for message formatting
        message_kwargs: Keyword args for message formatting
        target: Target path for metadata
        extra: Extra fields to include in metadata

    Returns:
        Dry-run metadata dict if dry-run mode, None otherwise
    """
    if not dry_run:
        return None

    logger.info(message, *message_args, **(message_kwargs or {}))
    result: Dict[str, Any] = {"dry_run": True, "target": target}
    if extra:
        result.update(extra)
    return result
