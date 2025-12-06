"""Core runner that wires config, extractor, IO, and storage together."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, cast

from core.pipeline.bronze.base import emit_bronze_metadata, infer_schema
from core.pipeline.bronze.plan import (
    build_chunk_writer_config,
    compute_output_formats,
    resolve_load_pattern,
)
from core.primitives.catalog.hooks import (
    report_quality_snapshot,
    report_run_metadata,
    report_schema_snapshot,
)
from core.pipeline.runtime.context import RunContext
from core.adapters.extractors.api_extractor import ApiExtractor
from core.adapters.extractors.base import BaseExtractor
from core.adapters.extractors.db_extractor import DbExtractor
from core.adapters.extractors.db_multi_extractor import DbMultiExtractor
from core.infrastructure.config.environment import EnvironmentConfig
from core.adapters.extractors.file_extractor import FileExtractor
from core.pipeline.bronze.io import chunk_records
from core.primitives.foundations.patterns import LoadPattern
from core.orchestration.runner.chunks import ChunkProcessor, ChunkWriter
from core.infrastructure.storage import get_storage_backend

logger = logging.getLogger(__name__)


def build_extractor(
    cfg: Dict[str, Any], env_config: Optional[EnvironmentConfig] = None
) -> BaseExtractor:
    src = cfg["source"]
    src_type = src.get("type", "api")

    if src_type == "api":
        return ApiExtractor()
    if src_type == "db":
        return DbExtractor()
    if src_type == "db_multi":
        max_workers = src.get("run", {}).get("parallel_workers", 4)
        return DbMultiExtractor(max_workers=max_workers)
    if src_type == "custom":
        custom_cfg = src.get("custom_extractor", {})
        module_name = custom_cfg["module"]
        class_name = custom_cfg["class_name"]

        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        # Assume user-supplied custom extractor classes subclass BaseExtractor; cast for mypy
        cls_typed: Type[BaseExtractor] = cast(Type[BaseExtractor], cls)
        return cls_typed()
    if src_type == "file":
        return FileExtractor(env_config=env_config)

    raise ValueError(f"Unknown source.type: {src_type}")


class ExtractJob:
    def __init__(self, context: RunContext) -> None:
        self.ctx = context
        self.cfg = context.cfg
        self.run_date = context.run_date
        self.relative_path = context.relative_path
        self._out_dir = context.bronze_path
        self.created_files: List[Path] = []
        self.load_pattern: Optional[LoadPattern] = context.load_pattern
        self.output_formats: Dict[str, bool] = {}
        from core.pipeline.bronze.plan import StoragePlan

        self.storage_plan: Optional[StoragePlan] = None
        self.schema_snapshot: List[Dict[str, str]] = []

    @property
    def source_cfg(self) -> Dict[str, Any]:
        from typing import cast

        return cast(Dict[str, Any], self.cfg["source"])

    def run(self) -> int:
        try:
            return self._run()
        except Exception:
            self._cleanup_on_failure()
            raise

    def _run(self) -> int:
        extractor = build_extractor(self.cfg, self.ctx.env_config)
        logger.info(
            "Starting extract for %s.%s on %s",
            self.source_cfg["system"],
            self.source_cfg["table"],
            self.run_date,
        )
        records, new_cursor = extractor.fetch_records(self.cfg, self.run_date)
        logger.info("Retrieved %s records from extractor", len(records))
        if not records:
            logger.warning("No records returned from extractor")
            return 0

        self.schema_snapshot = infer_schema(records)

        chunk_count, chunk_files = self._process_chunks(records)
        self.created_files.extend(chunk_files)
        self._emit_metadata(
            record_count=len(records), chunk_count=chunk_count, cursor=new_cursor
        )

        logger.info("Finished Bronze extract run successfully")
        return 0

    def _process_chunks(self, records: List[Dict[str, Any]]) -> tuple[int, List[Path]]:
        run_cfg = self.source_cfg["run"]
        self.load_pattern = self.load_pattern or resolve_load_pattern(run_cfg)
        logger.info(
            "Load pattern: %s (%s)",
            self.load_pattern.value,
            self.load_pattern.describe(),
        )

        max_rows_per_file = int(run_cfg.get("max_rows_per_file", 0))
        max_file_size_mb = run_cfg.get("max_file_size_mb")
        chunks = chunk_records(records, max_rows_per_file, max_file_size_mb)

        platform_cfg = self.cfg["platform"]
        bronze_output = platform_cfg["bronze"]["output_defaults"]
        self.output_formats = compute_output_formats(run_cfg, bronze_output)

        storage_enabled = run_cfg.get(
            "storage_enabled", run_cfg.get("s3_enabled", False)
        )
        storage_backend = get_storage_backend(platform_cfg) if storage_enabled else None
        if storage_backend:
            logger.info(
                "Initialized %s storage backend", storage_backend.get_backend_type()
            )

        self._out_dir.mkdir(parents=True, exist_ok=True)

        writer_config, self.storage_plan = build_chunk_writer_config(
            bronze_output,
            run_cfg,
            self._out_dir,
            self.relative_path,
            self.load_pattern,
            storage_backend,
            self.output_formats,
        )

        parallel_workers = int(run_cfg.get("parallel_workers", 1))
        writer = ChunkWriter(writer_config)
        processor = ChunkProcessor(writer, parallel_workers)
        chunk_files = processor.process(chunks)
        return len(chunks), chunk_files

    def _emit_metadata(
        self, record_count: int, chunk_count: int, cursor: Optional[str]
    ) -> None:
        reference_mode = self.source_cfg["run"].get("reference_mode")
        from datetime import datetime

        run_datetime = datetime.combine(self.run_date, datetime.min.time())
        p_load_pattern = self.load_pattern or LoadPattern.SNAPSHOT

        metadata_path = emit_bronze_metadata(
            self._out_dir,
            run_datetime,
            self.source_cfg["system"],
            self.source_cfg["table"],
            self.relative_path,
            self.output_formats,
            p_load_pattern,
            reference_mode,
            self.schema_snapshot,
            chunk_count,
            record_count,
            cursor,
            self.created_files,
        )
        self.created_files.append(metadata_path)

        stats = {
            "record_count": record_count,
            "chunk_count": chunk_count,
            "artifact_count": len(self.created_files),
        }
        dataset_id = f"bronze:{self.source_cfg['system']}.{self.source_cfg['table']}"
        report_schema_snapshot(dataset_id, self.schema_snapshot)
        report_quality_snapshot(dataset_id, stats)
        report_run_metadata(
            dataset_id,
            {
                "run_date": self.run_date.isoformat(),
                "load_pattern": p_load_pattern.value,
                "chunk_count": chunk_count,
                "record_count": record_count,
                "relative_path": self.relative_path,
                "status": "success",
            },
        )

    def _cleanup_on_failure(self) -> None:
        run_cfg = self.source_cfg.get("run", {})
        cleanup_on_failure = run_cfg.get("cleanup_on_failure", True)
        if not (cleanup_on_failure and self.created_files):
            return

        logger.info("Cleaning up %s files due to failure", len(self.created_files))
        for file_path in self.created_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug("Deleted %s", file_path)
            except Exception as cleanup_error:  # pragma: no cover - best effort
                logger.warning("Failed to cleanup %s: %s", file_path, cleanup_error)


def run_extract(context: RunContext) -> int:
    job = ExtractJob(context)
    try:
        return job.run()
    except Exception as exc:
        logger.error("Extract failed: %s", exc, exc_info=True)
        raise
