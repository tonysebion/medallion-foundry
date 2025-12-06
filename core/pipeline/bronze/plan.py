"""Bronze pipeline planning: dataclasses and helpers for storage/chunk config."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from core.primitives.foundations.patterns import LoadPattern
from core.infrastructure.storage.backend import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class StoragePlan:
    """Configuration for uploading chunks to remote storage."""

    enabled: bool
    backend: Optional[StorageBackend]
    relative_path: str

    def upload(self, file_path: Path) -> None:
        if not (self.enabled and self.backend):
            return
        remote_path = f"{self.relative_path}{file_path.name}"
        self.backend.upload_file(str(file_path), remote_path)


@dataclass
class ChunkWriterConfig:
    """Configuration for writing Bronze data chunks."""

    out_dir: Path
    write_csv: bool
    write_parquet: bool
    parquet_compression: str
    storage_plan: StoragePlan
    chunk_prefix: str


def resolve_load_pattern(run_cfg: Dict[str, Any]) -> LoadPattern:
    return LoadPattern.normalize(run_cfg.get("load_pattern"))


def compute_output_formats(
    run_cfg: Dict[str, Any], bronze_output: Dict[str, Any]
) -> Dict[str, bool]:
    write_csv = run_cfg.get("write_csv", True) and bronze_output.get("allow_csv", True)
    write_parquet = run_cfg.get("write_parquet", True) and bronze_output.get(
        "allow_parquet", True
    )
    return {"csv": write_csv, "parquet": write_parquet}


def build_chunk_writer_config(
    bronze_output: Dict[str, Any],
    run_cfg: Dict[str, Any],
    out_dir: Path,
    relative_path: str,
    load_pattern: LoadPattern,
    storage_backend: StorageBackend | None,
    formats: Dict[str, bool],
) -> Tuple[ChunkWriterConfig, StoragePlan]:
    parquet_compression = run_cfg.get(
        "parquet_compression",
        bronze_output.get("default_parquet_compression", "snappy"),
    )
    storage_enabled = run_cfg.get("storage_enabled", run_cfg.get("s3_enabled", False))
    storage_plan = StoragePlan(
        enabled=storage_enabled,
        backend=storage_backend,
        relative_path=relative_path,
    )
    writer_config = ChunkWriterConfig(
        out_dir=out_dir,
        write_csv=formats["csv"],
        write_parquet=formats["parquet"],
        parquet_compression=parquet_compression,
        storage_plan=storage_plan,
        chunk_prefix=load_pattern.chunk_prefix,
    )
    return writer_config, storage_plan
