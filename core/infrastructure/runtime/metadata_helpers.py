"""Shared metadata helpers for Bronze/Silver pipelines moved to infrastructure layer."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Sequence

from core.foundation.time_utils import utc_isoformat
from core.infrastructure.io.storage.checksum import write_checksum_manifest as _write_checksum_manifest

logger = logging.getLogger(__name__)


def _maybe_add(payload: Dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        payload[key] = value


def build_batch_metadata_extra(
    *,
    load_pattern: str,
    bronze_path: str,
    domain: str,
    entity: str,
    primary_keys: Sequence[str] | None = None,
    order_column: str | None = None,
    version: int | None = None,
    load_partition_name: str | None = None,
    include_pattern_folder: bool | None = None,
    partition_columns: Sequence[str] | None = None,
    write_parquet: bool | None = None,
    write_csv: bool | None = None,
    parquet_compression: str | None = None,
    normalization: Dict[str, Any] | None = None,
    schema: Dict[str, Any] | None = None,
    error_handling: Dict[str, Any] | None = None,
    require_checksum: bool | None = None,
    silver_model: str | None = None,
    artifacts: Dict[str, Sequence[str]] | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build shared metadata payload used across layers."""
    payload: Dict[str, Any] = {
        "load_pattern": load_pattern,
        "bronze_path": bronze_path,
        "domain": domain,
        "entity": entity,
    }

    if primary_keys:
        payload["primary_keys"] = list(primary_keys)
    _maybe_add(payload, "order_column", order_column)
    _maybe_add(payload, "version", version)
    _maybe_add(payload, "load_partition_name", load_partition_name)
    _maybe_add(payload, "include_pattern_folder", include_pattern_folder)
    if partition_columns:
        payload["partition_columns"] = list(partition_columns)
    _maybe_add(payload, "write_parquet", write_parquet)
    _maybe_add(payload, "write_csv", write_csv)
    _maybe_add(payload, "parquet_compression", parquet_compression)
    _maybe_add(payload, "normalization", normalization)
    _maybe_add(payload, "schema", schema)
    _maybe_add(payload, "error_handling", error_handling)
    _maybe_add(payload, "artifacts", artifacts)
    _maybe_add(payload, "require_checksum", require_checksum)
    _maybe_add(payload, "silver_model", silver_model)

    if extra:
        payload.update(extra)

    return payload


def build_checksum_metadata_extra(
    *,
    load_pattern: str,
    bronze_path: str,
    schema_snapshot: Sequence[Dict[str, str]],
    stats: Dict[str, Any],
    runtime_seconds: float | None = None,
) -> Dict[str, Any]:
    """Build checksum metadata payload shared by Bronze/Silver."""
    payload: Dict[str, Any] = {
        "load_pattern": load_pattern,
        "bronze_path": bronze_path,
        "schema": list(schema_snapshot),
        "stats": stats,
    }
    _maybe_add(payload, "runtime_seconds", runtime_seconds)
    return payload


def write_batch_metadata(
    out_dir: Path,
    record_count: int,
    chunk_count: int,
    cursor: str | None = None,
    performance_metrics: Dict[str, Any] | None = None,
    quality_metrics: Dict[str, Any] | None = None,
    extra_metadata: Dict[str, Any] | None = None,
) -> Path:
    """Write per-batch metadata for monitoring and reliability."""
    metadata: Dict[str, Any] = {
        "timestamp": utc_isoformat(),
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
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)

    logger.info("Wrote metadata to %s", metadata_path)
    return metadata_path


def write_checksum_manifest(
    out_dir: Path,
    files: list[Path],
    load_pattern: str,
    extra_metadata: Dict[str, Any] | None = None,
) -> Path:
    """Delegate checksum manifest creation to the storage helper."""
    return _write_checksum_manifest(out_dir, files, load_pattern, extra_metadata)


__all__ = [
    "build_batch_metadata_extra",
    "build_checksum_metadata_extra",
    "write_batch_metadata",
    "write_checksum_manifest",
]
