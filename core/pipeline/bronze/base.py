from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.bronze.io import write_batch_metadata, write_checksum_manifest
from core.primitives.foundations.patterns import LoadPattern


def infer_schema(records: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    if not records:
        return []
    keys = sorted(
        {key for record in records if isinstance(record, dict) for key in record.keys()}
    )
    schema_snapshot: List[Dict[str, str]] = []
    for key in keys:
        value = next(
            (
                record.get(key)
                for record in records
                if isinstance(record, dict) and key in record
            ),
            None,
        )
        schema_snapshot.append(
            {
                "name": key,
                "dtype": type(value).__name__ if value is not None else "unknown",
            }
        )
    return schema_snapshot


def build_reference_info(
    reference_mode: Dict[str, Any] | None, out_dir: Path
) -> Optional[Dict[str, Any]]:
    if not reference_mode or not reference_mode.get("enabled"):
        return None
    return {
        "role": reference_mode.get("role"),
        "cadence_days": reference_mode.get("cadence_days"),
        "delta_patterns": reference_mode.get("delta_patterns"),
        "reference_path": str(out_dir),
        "reference_type": "reference"
        if reference_mode.get("role") == "reference"
        else "delta",
    }


def emit_bronze_metadata(
    out_dir: Path,
    run_date: datetime,
    system: str,
    table: str,
    relative_path: str,
    formats: Dict[str, bool],
    load_pattern: LoadPattern,
    reference_mode: Dict[str, Any] | None,
    schema_snapshot: List[Dict[str, str]],
    chunk_count: int,
    record_count: int,
    cursor: Optional[str],
    created_files: List[Path],
) -> Path:
    run_date_str = run_date.date().isoformat()
    metadata_payload = {
        "batch_timestamp": datetime.utcnow().isoformat() + "Z",
        "run_date": run_date_str,
        "system": system,
        "table": table,
        "partition_path": relative_path,
        "file_formats": formats,
        "load_pattern": load_pattern.value,
        "reference_mode": build_reference_info(reference_mode, out_dir),
    }
    metadata_path = write_batch_metadata(
        out_dir,
        record_count=record_count,
        chunk_count=chunk_count,
        cursor=cursor,
        extra_metadata=metadata_payload,
    )
    checksum_metadata = {
        "system": system,
        "table": table,
        "run_date": run_date_str,
        "config_name": reference_mode and reference_mode.get("config_name"),
        "schema": schema_snapshot,
        "stats": {
            "record_count": record_count,
            "chunk_count": chunk_count,
            "artifact_count": len(created_files) + 1,
        },
        "load_pattern": load_pattern.value,
    }
    write_checksum_manifest(
        out_dir,
        created_files + [metadata_path],
        load_pattern.value,
        checksum_metadata,
    )
    return metadata_path
