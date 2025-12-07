"""Core runner that wires config, extractor, IO, and storage together."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from core.domain.services.pipelines.bronze.base import emit_bronze_metadata, infer_schema
from core.domain.services.pipelines.bronze.models import (
    build_chunk_writer_config,
    compute_output_formats,
    resolve_load_pattern,
)
from core.infrastructure.runtime.context import RunContext
from core.infrastructure.runtime.metadata import (
    Layer,
    RunStatus,
    build_run_metadata,
    write_run_metadata,
)
from core.infrastructure.io.extractors.base import BaseExtractor
from core.domain.adapters.extractors.factory import (
    ensure_extractors_loaded,
    get_extractor,
)
from core.domain.services.pipelines.bronze.io import chunk_records, verify_checksum_manifest
from core.foundation.primitives.patterns import LoadPattern
from core.domain.services.processing.chunk_processor import ChunkProcessor, ChunkWriter
from core.infrastructure.io.storage import get_storage_backend
from core.domain.adapters.schema.evolution import (
    EvolutionConfig,
    SchemaEvolution,
    SchemaEvolutionMode,
)
from core.domain.adapters.schema.types import ColumnSpec, DataType, SchemaSpec
from core.foundation.catalog.hooks import (
    report_quality_snapshot,
    report_run_metadata,
    report_schema_snapshot,
    report_lineage,
)

logger = logging.getLogger(__name__)
ensure_extractors_loaded()


def _load_extractors() -> None:
    """Expose extractor loading for tests and legacy callers."""
    ensure_extractors_loaded()


def build_extractor(
    cfg: Dict[str, Any],
    env_config: Optional[Any] = None,
) -> BaseExtractor:
    """Create an extractor following existing factory semantics."""
    source_cfg = cfg.setdefault("source", {})
    api_cfg = source_cfg.get("api")
    if isinstance(api_cfg, dict):
        api_cfg.setdefault("endpoint", "/")
    return get_extractor(cfg, env_config)


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
        from core.domain.services.pipelines.bronze.models import StoragePlan

        self.storage_plan: Optional[StoragePlan] = None
        self.schema_snapshot: List[Dict[str, str]] = []
        self.previous_manifest_schema: Optional[List[Dict[str, Any]]] = None

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
        self._inspect_existing_manifest()

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
        return len(chunk_files), chunk_files

    def _inspect_existing_manifest(self) -> None:
        manifest_path = self._out_dir / "_checksums.json"
        if not manifest_path.exists():
            return

        expected_pattern = self.load_pattern.value if self.load_pattern else None
        try:
            manifest = verify_checksum_manifest(
                self._out_dir,
                expected_pattern=expected_pattern,
            )
            self._store_previous_manifest_schema(manifest)
            self._assert_schema_compatible()
        except (ValueError, FileNotFoundError) as exc:
            manifest = self._load_manifest_json()
            if manifest:
                self._store_previous_manifest_schema(manifest)
            logger.warning(
                "Detected incomplete Bronze partition at %s: %s; resetting artifacts for a clean rerun.",
                self._out_dir,
                exc,
            )
            self._cleanup_existing_artifacts()
            return

        raise RuntimeError(
            f"Bronze partition {self._out_dir} already contains a verified checksum manifest; aborting to avoid duplicates."
        )

    def _cleanup_existing_artifacts(self) -> None:
        if not self._out_dir.exists():
            return
        logger.info("Clearing existing Bronze artifacts at %s before rerun", self._out_dir)
        shutil.rmtree(self._out_dir, ignore_errors=True)
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self.created_files = []

    def _load_manifest_json(self) -> Optional[Dict[str, Any]]:
        manifest_path = self._out_dir / "_checksums.json"
        if not manifest_path.exists():
            return None
        try:
            text = manifest_path.read_text(encoding="utf-8")
            return json.loads(text)
        except Exception:
            return None

    def _store_previous_manifest_schema(self, manifest: Optional[Dict[str, Any]]) -> None:
        if not manifest:
            return
        schema = manifest.get("schema")
        if isinstance(schema, list):
            self.previous_manifest_schema = schema

    def _schema_spec_from_snapshot(
        self, snapshot: List[Dict[str, Any]]
    ) -> SchemaSpec:
        columns: List[ColumnSpec] = []
        for entry in snapshot:
            name = entry.get("name")
            if not name:
                continue
            dtype = entry.get("dtype", "any")
            columns.append(
                ColumnSpec(
                    name=name,
                    type=DataType.from_string(dtype),
                    nullable=entry.get("nullable", True),
                )
            )
        return SchemaSpec(columns=columns)

    def _resolve_schema_evolution_config(self) -> EvolutionConfig:
        schema_cfg = self.source_cfg.get("run", {}).get("schema_evolution")
        if not schema_cfg or not isinstance(schema_cfg, dict):
            schema_cfg = self.cfg.get("schema_evolution") or {}
        if not isinstance(schema_cfg, dict):
            schema_cfg = {}

        mode_str = schema_cfg.get("mode", "strict")
        try:
            mode = SchemaEvolutionMode(mode_str)
        except ValueError:
            logger.warning(
                "Unknown schema_evolution.mode '%s', defaulting to strict", mode_str
            )
            mode = SchemaEvolutionMode.STRICT

        protected_columns = schema_cfg.get("protected_columns", [])
        if not isinstance(protected_columns, list):
            protected_columns = []

        return EvolutionConfig(
            mode=mode,
            allow_type_relaxation=schema_cfg.get("allow_type_relaxation", False),
            allow_precision_increase=schema_cfg.get("allow_precision_increase", True),
            protected_columns=protected_columns,
        )

    def _assert_schema_compatible(self) -> None:
        if not self.previous_manifest_schema or not self.schema_snapshot:
            return

        config = self._resolve_schema_evolution_config()
        evolution = SchemaEvolution(config)
        previous_spec = self._schema_spec_from_snapshot(self.previous_manifest_schema)
        current_spec = self._schema_spec_from_snapshot(self.schema_snapshot)
        result = evolution.check_evolution(previous_spec, current_spec)

        if not result.compatible:
            logger.error(
                "Schema drift blocked by evolution rules: %s", result.to_dict()
            )
            raise RuntimeError(
                "Schema drift detected and blocked by configured evolution rules. "
                f"Details: {result.to_dict()}"
            )

    def _metadata_config(self) -> Dict[str, Any]:
        """Build a config dict that includes the user-specified schema evolution block."""
        metadata_cfg: Dict[str, Any] = dict(self.cfg)
        run_schema = self.source_cfg.get("run", {}).get("schema_evolution")
        if run_schema:
            metadata_cfg["schema_evolution"] = run_schema
        return metadata_cfg

    def _emit_metadata(
        self, record_count: int, chunk_count: int, cursor: Optional[str]
    ) -> None:
        self._assert_schema_compatible()
        reference_mode = self.source_cfg["run"].get("reference_mode")
        from datetime import datetime

        run_datetime = datetime.combine(self.run_date, datetime.min.time())
        p_load_pattern = self.load_pattern or LoadPattern.SNAPSHOT
        chunk_artifacts = list(self.created_files)
        metadata_path, manifest_path = emit_bronze_metadata(
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

        metadata_cfg = self._metadata_config()
        run_metadata = build_run_metadata(
            metadata_cfg, Layer.BRONZE, run_id=self.ctx.run_id
        )
        run_metadata.complete(
            row_count_in=record_count,
            row_count_out=record_count,
            status=RunStatus.SUCCESS,
        )
        run_metadata_path = write_run_metadata(
            run_metadata, self.ctx.local_output_dir
        )
        self.created_files.append(run_metadata_path)

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

        source_name = self.source_cfg.get("config_name")
        if not source_name:
            source_name = f"{self.source_cfg['system']}.{self.source_cfg['table']}"
        source_dataset = f"source:{source_name}"
        lineage_metadata = {
            "partition_path": self.relative_path,
            "record_count": record_count,
            "chunk_count": chunk_count,
            "load_pattern": p_load_pattern.value,
            "files": [path.name for path in chunk_artifacts],
            "metadata": metadata_path.name,
            "manifest": manifest_path.name,
        }
        if cursor:
            lineage_metadata["cursor"] = cursor
        if reference_mode:
            lineage_metadata["reference_mode"] = reference_mode
        report_lineage(source_dataset, dataset_id, lineage_metadata)

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

            plan = self.storage_plan
            if plan:
                remote_path = plan.remote_path_for(file_path)
                try:
                    if plan.delete(file_path):
                        logger.info("Deleted remote artifact %s", remote_path)
                    else:
                        logger.debug(
                            "Remote artifact %s was not deleted (disabled or missing)",
                            remote_path,
                        )
                except Exception as remote_error:  # pragma: no cover
                    logger.warning(
                        "Failed to delete remote artifact %s: %s",
                        remote_path,
                        remote_error,
                    )


def run_extract(context: RunContext) -> int:
    job = ExtractJob(context)
    try:
        return job.run()
    except Exception as exc:
        logger.error("Extract failed: %s", exc, exc_info=True)
        raise
