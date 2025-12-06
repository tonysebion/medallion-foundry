from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Tuple

from core.primitives.foundations.patterns import LoadPattern
from core.orchestration.runner.chunks import ChunkWriterConfig, StoragePlan
from core.infrastructure.storage.backend import StorageBackend


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
