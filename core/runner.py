"""Core runner that wires config, extractor, IO, and storage together."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.extractors.base import BaseExtractor
from core.extractors.api_extractor import ApiExtractor
from core.extractors.db_extractor import DbExtractor
from core.extractors.file_extractor import FileExtractor

from core.io import (
    chunk_records,
    write_csv_chunk,
    write_parquet_chunk,
    write_batch_metadata,
    write_checksum_manifest,
)
from core.storage import StorageBackend, get_storage_backend
from core.patterns import LoadPattern
from core.catalog import report_schema_snapshot, report_quality_snapshot, report_run_metadata

logger = logging.getLogger(__name__)


def build_extractor(cfg: Dict[str, Any]) -> BaseExtractor:
    src = cfg["source"]
    src_type = src.get("type", "api")

    if src_type == "api":
        return ApiExtractor()
    if src_type == "db":
        return DbExtractor()
    if src_type == "custom":
        custom_cfg = src.get("custom_extractor", {})
        module_name = custom_cfg["module"]
        class_name = custom_cfg["class_name"]

        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls()
    if src_type == "file":
        return FileExtractor()

    raise ValueError(f"Unknown source.type: {src_type}")


@dataclass
class StoragePlan:
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
    out_dir: Path
    write_csv: bool
    write_parquet: bool
    parquet_compression: str
    storage_plan: StoragePlan
    chunk_prefix: str


class ChunkWriter:
    """Write CSV/Parquet chunks and push to storage as needed."""

    def __init__(self, config: ChunkWriterConfig) -> None:
        self.config = config

    def write(self, chunk_index: int, chunk: List[Dict[str, Any]]) -> List[Path]:
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
                write_parquet_chunk(chunk, parquet_path, compression=self.config.parquet_compression)
                created_files.append(parquet_path)
                self.config.storage_plan.upload(parquet_path)

            logger.debug(f"Completed processing chunk {chunk_index}")
            return created_files
        except Exception as exc:
            logger.error(f"Failed to process chunk {chunk_index}: {exc}")
            raise


class ChunkProcessor:
    """Coordinate sequential/parallel chunk execution."""

    def __init__(self, writer: ChunkWriter, parallel_workers: int) -> None:
        self.writer = writer
        self.parallel_workers = max(1, parallel_workers)

    def process(self, chunks: List[List[Dict[str, Any]]]) -> List[Path]:
        if not chunks:
            return []

        created_files: List[Path] = []
        if self.parallel_workers > 1 and len(chunks) > 1:
            logger.info(f"Processing {len(chunks)} chunks with {self.parallel_workers} workers")
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                futures = {
                    executor.submit(self.writer.write, idx, chunk): idx
                    for idx, chunk in enumerate(chunks, start=1)
                }
                for future in as_completed(futures):
                    chunk_index = futures[future]
                    chunk_files = future.result()
                    logger.debug(f"Chunk {chunk_index} completed successfully")
                    created_files.extend(chunk_files)
        else:
            logger.info(f"Processing {len(chunks)} chunks sequentially")
            for idx, chunk in enumerate(chunks, start=1):
                created_files.extend(self.writer.write(idx, chunk))

        return created_files


class ExtractJob:
    """Encapsulates lifecycle of a Bronze extract for a single config."""

    def __init__(
        self,
        cfg: Dict[str, Any],
        run_date: date,
        local_output_base: Path,
        relative_path: str,
    ) -> None:
        self.cfg = cfg
        self.run_date = run_date
        self.local_output_base = local_output_base
        self.relative_path = relative_path
        self.created_files: List[Path] = []
        self.storage_plan: Optional[StoragePlan] = None
        self.load_pattern: Optional[LoadPattern] = None
        self.output_formats: Dict[str, bool] = {"csv": True, "parquet": True}
        self._out_dir = self.local_output_base / self.relative_path
        self.schema_snapshot: List[Dict[str, str]] = []

    @property
    def source_cfg(self) -> Dict[str, Any]:
        return self.cfg["source"]

    def run(self) -> int:
        try:
            return self._run()
        except Exception:
            self._cleanup_on_failure()
            raise

    def _run(self) -> int:
        extractor = build_extractor(self.cfg)
        logger.info(
            f"Starting extract for {self.source_cfg['system']}.{self.source_cfg['table']} on {self.run_date}"
        )
        records, new_cursor = extractor.fetch_records(self.cfg, self.run_date)
        logger.info(f"Retrieved {len(records)} records from extractor")
        if not records:
            logger.warning("No records returned from extractor")
            return 0

        self.schema_snapshot = self._infer_schema(records)

        chunk_count, chunk_files = self._process_chunks(records)
        self.created_files.extend(chunk_files)
        self._emit_metadata(record_count=len(records), chunk_count=chunk_count, cursor=new_cursor)

        logger.info("Finished Bronze extract run successfully")
        return 0

    def _process_chunks(self, records: List[Dict[str, Any]]) -> tuple[int, List[Path]]:
        run_cfg = self.source_cfg["run"]
        self.load_pattern = LoadPattern.normalize(run_cfg.get("load_pattern"))
        logger.info(f"Load pattern: {self.load_pattern.value} ({self.load_pattern.describe()})")

        max_rows_per_file = int(run_cfg.get("max_rows_per_file", 0))
        max_file_size_mb = run_cfg.get("max_file_size_mb")
        chunks = chunk_records(records, max_rows_per_file, max_file_size_mb)

        platform_cfg = self.cfg["platform"]
        bronze_output = platform_cfg["bronze"]["output_defaults"]
        write_csv = run_cfg.get("write_csv", True) and bronze_output.get("allow_csv", True)
        write_parquet = run_cfg.get("write_parquet", True) and bronze_output.get("allow_parquet", True)
        parquet_compression = run_cfg.get(
            "parquet_compression", bronze_output.get("default_parquet_compression", "snappy")
        )
        self.output_formats = {"csv": write_csv, "parquet": write_parquet}

        storage_enabled = run_cfg.get("storage_enabled", run_cfg.get("s3_enabled", False))
        storage_backend = get_storage_backend(platform_cfg) if storage_enabled else None
        if storage_backend:
            logger.info(f"Initialized {storage_backend.get_backend_type()} storage backend")

        self._out_dir.mkdir(parents=True, exist_ok=True)

        self.storage_plan = StoragePlan(
            enabled=storage_enabled,
            backend=storage_backend,
            relative_path=self.relative_path,
        )
        writer_config = ChunkWriterConfig(
            out_dir=self._out_dir,
            write_csv=write_csv,
            write_parquet=write_parquet,
            parquet_compression=parquet_compression,
            storage_plan=self.storage_plan,
            chunk_prefix=self.load_pattern.chunk_prefix,
        )

        parallel_workers = int(run_cfg.get("parallel_workers", 1))
        processor = ChunkProcessor(ChunkWriter(writer_config), parallel_workers)
        chunk_files = processor.process(chunks)
        return len(chunks), chunk_files

    def _infer_schema(self, records: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        if not records:
            return []
        keys = sorted(
            {key for record in records if isinstance(record, dict) for key in record.keys()}
        )
        schema_snapshot: List[Dict[str, str]] = []
        for key in keys:
            value = next(
                (record.get(key) for record in records if isinstance(record, dict) and key in record),
                None,
            )
            schema_snapshot.append({"name": key, "dtype": type(value).__name__ if value is not None else "unknown"})
        return schema_snapshot

    def _emit_metadata(self, record_count: int, chunk_count: int, cursor: Optional[str]) -> None:
        reference_mode = self.source_cfg["run"].get("reference_mode")
        reference_info = None
        if reference_mode and reference_mode.get("enabled"):
            reference_info = {
                "role": reference_mode.get("role"),
                "cadence_days": reference_mode.get("cadence_days"),
                "delta_patterns": reference_mode.get("delta_patterns"),
                "reference_path": str(self._out_dir),
                "reference_type": "reference" if reference_mode.get("role") == "reference" else "delta",
            }

        metadata_path = write_batch_metadata(
            self._out_dir,
            record_count=record_count,
            chunk_count=chunk_count,
            cursor=cursor,
            extra_metadata={
                "batch_timestamp": datetime.now().isoformat(),
                "run_date": self.run_date.isoformat(),
                "system": self.source_cfg["system"],
                "table": self.source_cfg["table"],
                "partition_path": self.relative_path,
                "file_formats": self.output_formats,
                "load_pattern": self.load_pattern.value if self.load_pattern else LoadPattern.FULL.value,
                "reference_mode": reference_info,
            },
        )
        self.created_files.append(metadata_path)

        checksum_metadata = {
            "system": self.source_cfg["system"],
            "table": self.source_cfg["table"],
            "run_date": self.run_date.isoformat(),
            "config_name": self.source_cfg.get("config_name"),
        }
        stats = {
            "record_count": record_count,
            "chunk_count": chunk_count,
            "artifact_count": len(self.created_files),
        }
        extra_meta = {
            "schema": self.schema_snapshot,
            "stats": stats,
            "load_pattern": self.load_pattern.value if self.load_pattern else LoadPattern.FULL.value,
        }
        checksum_metadata.update(extra_meta)
        write_checksum_manifest(
            self._out_dir,
            self.created_files,
            self.load_pattern.value if self.load_pattern else LoadPattern.FULL.value,
            checksum_metadata,
        )

        if self.storage_plan:
            self.storage_plan.upload(metadata_path)

        dataset_id = f"bronze:{self.source_cfg['system']}.{self.source_cfg['table']}"
        report_schema_snapshot(dataset_id, self.schema_snapshot)
        report_quality_snapshot(dataset_id, stats)
        report_run_metadata(
            dataset_id,
            {
                "run_date": self.run_date.isoformat(),
                "load_pattern": self.load_pattern.value if self.load_pattern else LoadPattern.FULL.value,
                "chunk_count": chunk_count,
                "record_count": record_count,
                "relative_path": self.relative_path,
                "status": "success",
            },
        )
        if cursor:
            logger.info(f"Extractor returned new_cursor={cursor!r}")

    def _cleanup_on_failure(self) -> None:
        run_cfg = self.source_cfg.get("run", {})
        cleanup_on_failure = run_cfg.get("cleanup_on_failure", True)
        if not (cleanup_on_failure and self.created_files):
            return

        logger.info(f"Cleaning up {len(self.created_files)} files due to failure")
        for file_path in self.created_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(f"Deleted {file_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup {file_path}: {cleanup_error}")


def run_extract(
    cfg: Dict[str, Any],
    run_date: date,
    local_output_base: Path,
    relative_path: str,
) -> int:
    """Run extraction with error handling and cleanup."""
    job = ExtractJob(cfg, run_date, local_output_base, relative_path)
    try:
        return job.run()
    except Exception as exc:
        logger.error(f"Extract failed: {exc}", exc_info=True)
        raise
