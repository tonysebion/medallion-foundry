"""Unified artifact writing utilities for Bronze/Silver/API pipelines.

This module consolidates common write logic to reduce code duplication:
- Parquet writing (local and cloud)
- Metadata JSON writing
- Checksum manifest writing

Usage:
    from pipelines.lib.artifact_writer import write_artifacts

    result = write_artifacts(
        table=t,
        target="s3://bucket/bronze/entity/dt=2025-01-01/",
        entity_name="orders",
        row_count=1000,
        columns=columns,
        run_date="2025-01-01",
        extra_metadata={"system": "retail", "entity": "orders"},
        storage_options={"endpoint_url": "http://localhost:9000"},
        write_metadata=True,
        write_checksums=True,
    )
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple, Union

import pyarrow.parquet as pq

from pipelines.lib.checksum import (
    compute_bytes_sha256,
    write_checksum_manifest,
    write_checksum_manifest_s3,
)
from pipelines.lib.io import OutputMetadata, utc_now_iso
from pipelines.lib.observability import get_structlog_logger
from pipelines.lib.storage import get_storage, parse_uri
from pipelines.lib.storage_config import _configure_duckdb_s3, _extract_storage_options

if TYPE_CHECKING:
    import ibis  # type: ignore[import-untyped]

logger = get_structlog_logger(__name__)

__all__ = ["write_artifacts", "WriteResult"]


# Helper wrappers to avoid mypy false positives around Ibis bindings


def _duck_to_parquet(
    duck_table: Any, target: str, partition_cols: Optional[Union[str, Tuple[str, ...]]]
) -> List[str]:
    """Write parquet via DuckDB Ibis table and return generated path list."""
    duck_table.to_parquet(target, partition_by=partition_cols)
    return [target]


def _table_to_parquet_local(
    table: Any,
    output_dir: Path,
    output_file: Path,
    partition_cols: Optional[Union[str, Tuple[str, ...]]] = None,
) -> List[str]:
    """Write parquet locally, optionally partitioned."""
    if partition_cols is None:
        table.to_parquet(str(output_file))
    else:
        table.to_parquet(str(output_dir), partition_by=partition_cols)
    return [str(output_file)]


@dataclass
class WriteResult:
    """Result of artifact write operation."""

    row_count: int
    target: str
    data_files: List[str]
    metadata_file: Optional[str] = None
    checksums_file: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to result dictionary."""
        result: Dict[str, Any] = {
            "row_count": self.row_count,
            "target": self.target,
            "files": self.data_files,
        }
        if self.metadata_file:
            result["metadata_file"] = self.metadata_file
        if self.checksums_file:
            result["checksums_file"] = self.checksums_file
        return result


def write_artifacts(
    table: "ibis.Table",
    target: str,
    entity_name: str,
    columns: List[Dict[str, Any]],
    run_date: str,
    *,
    extra_metadata: Optional[Dict[str, Any]] = None,
    storage_options: Optional[Dict[str, Any]] = None,
    write_metadata: bool = True,
    write_checksums: bool = True,
    checksum_extra: Optional[Dict[str, Any]] = None,
    partition_by: Optional[List[str]] = None,
    compression: str = "snappy",
) -> WriteResult:
    """Write table with metadata and checksum artifacts.

    This unified function handles both cloud (S3/ADLS) and local filesystem writes,
    with consistent artifact generation.

    Args:
        table: Ibis table to write
        target: Target directory path (local or s3://...)
        entity_name: Name for the parquet file (e.g., "orders" -> orders.parquet)
        columns: Column metadata list from infer_column_types()
        run_date: Run date string (YYYY-MM-DD)
        extra_metadata: Additional fields for _metadata.json extra dict
        storage_options: S3/ADLS options (endpoint_url, key, secret, etc.)
        write_metadata: Whether to write _metadata.json
        write_checksums: Whether to write _checksums.json
        checksum_extra: Additional fields for checksum manifest
        partition_by: Columns to partition by (optional)
        compression: Parquet compression codec

    Returns:
        WriteResult with file paths and row count
    """
    # Execute count before writing (Ibis is lazy)
    count_result = table.count().execute()
    row_count = int(
        count_result.iloc[0] if hasattr(count_result, "iloc") else count_result
    )

    if row_count == 0:
        logger.warning("write_artifacts_no_rows", target=target)
        return WriteResult(row_count=0, target=target, data_files=[])

    # Determine if cloud or local storage
    scheme, _ = parse_uri(target)
    is_cloud = scheme in ("s3", "abfs")

    parquet_filename = f"{entity_name}.parquet"
    data_files: List[str] = []
    now = utc_now_iso()

    if is_cloud:
        data_files = _write_cloud(
            table=table,
            target=target,
            parquet_filename=parquet_filename,
            storage_options=storage_options,
            partition_by=partition_by,
            compression=compression,
        )
    else:
        data_files = _write_local(
            table=table,
            target=target,
            parquet_filename=parquet_filename,
            partition_by=partition_by,
        )

    # Create storage backend for artifact writes
    storage_opts = _extract_storage_options(storage_options) if storage_options else {}
    storage = get_storage(target, **storage_opts)

    metadata_file = None
    checksums_file = None

    # Write metadata
    if write_metadata:
        metadata = OutputMetadata(
            row_count=row_count,
            columns=columns,
            written_at=now,
            run_date=run_date,
            data_files=[parquet_filename],
            extra=extra_metadata or {},
        )
        storage.write_text("_metadata.json", metadata.to_json())
        metadata_file = "_metadata.json"
        logger.debug("artifact_metadata_written", target=target)

    # Write checksums
    if write_checksums:
        if is_cloud:
            _write_checksums_cloud(
                storage=storage,
                parquet_filename=parquet_filename,
                row_count=row_count,
                extra_metadata=checksum_extra,
            )
        else:
            write_checksum_manifest(
                Path(target),
                [Path(target) / parquet_filename],
                row_count=row_count,
                extra_metadata=checksum_extra or {},
            )
        checksums_file = "_checksums.json"
        logger.debug("artifact_checksums_written", target=target)

    return WriteResult(
        row_count=row_count,
        target=target,
        data_files=data_files if not is_cloud else [parquet_filename],
        metadata_file=metadata_file,
        checksums_file=checksums_file,
    )


def _write_cloud(
    table: "ibis.Table",
    target: str,
    parquet_filename: str,
    storage_options: Optional[Dict[str, Any]],
    partition_by: Optional[List[str]],
    compression: str,
) -> List[str]:
    """Write parquet to cloud storage (S3/ADLS)."""
    import ibis

    storage_opts = _extract_storage_options(storage_options) if storage_options else {}
    storage = get_storage(target, **storage_opts)
    storage.makedirs("")

    # Non-partitioned - write via boto3 for reliable S3 endpoint handling
    if partition_by is None or not partition_by:
        arrow_table = table.to_pyarrow()
        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer, compression=compression)
        parquet_bytes = buffer.getvalue()

        result = storage.write_bytes(parquet_filename, parquet_bytes)
        if not result.success:
            raise RuntimeError(f"Failed to write parquet to cloud: {result.error}")
        return [f"{target.rstrip('/')}/{parquet_filename}"]

    # Partitioned writes need DuckDB with S3 configured
    con = ibis.duckdb.connect()
    _configure_duckdb_s3(con, storage_options)
    arrow_table = table.to_pyarrow()
    duck_table = con.create_table("_temp_write", arrow_table)
    # Ibis 11.0.0 has a bug with list syntax for partition_by
    # Single column: pass as string; multiple columns: pass as tuple
    if isinstance(partition_by, list):
        partition_cols = (
            partition_by[0] if len(partition_by) == 1 else tuple(partition_by)
        )
    else:
        partition_cols = partition_by  # type: ignore[unreachable]

    return _duck_to_parquet(duck_table, target, partition_cols)


def _write_local(
    table: "ibis.Table",
    target: str,
    parquet_filename: str,
    partition_by: Optional[List[str]],
) -> List[str]:
    """Write parquet to local filesystem."""
    output_dir = Path(target)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / parquet_filename

    if partition_by is None or not partition_by:
        table.to_parquet(str(output_file))
        return [str(output_file)]

    # Ibis 11.0.0 has a bug with list syntax for partition_by
    # Single column: pass as string; multiple columns: pass as tuple
    if isinstance(partition_by, list):
        partition_cols = (
            partition_by[0] if len(partition_by) == 1 else tuple(partition_by)
        )
    else:
        partition_cols = partition_by  # type: ignore[unreachable]
    return _table_to_parquet_local(table, output_dir, output_file, partition_cols)


def _write_checksums_cloud(
    storage: Any,
    parquet_filename: str,
    row_count: int,
    extra_metadata: Optional[Dict[str, Any]],
) -> None:
    """Write checksum manifest to cloud storage."""
    try:
        parquet_data = storage.read_bytes(parquet_filename)
        file_checksum_data = [
            {
                "path": parquet_filename,
                "size_bytes": len(parquet_data),
                "sha256": compute_bytes_sha256(parquet_data),
            }
        ]
        write_checksum_manifest_s3(
            storage,
            file_checksum_data,
            row_count=row_count,
            extra_metadata=extra_metadata or {},
        )
    except Exception as e:
        logger.warning("checksum_write_failed: %s", str(e))
