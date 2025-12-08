"""Core runner that wires config, extractor, IO, and storage together."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from core.domain.services.pipelines.bronze.base import emit_bronze_metadata, infer_schema
from core.domain.services.processing.chunk_config import (
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
from core.domain.services.pipelines.bronze.io import chunk_records
from core.foundation.primitives.patterns import LoadPattern
from core.domain.services.processing.chunk_processor import ChunkProcessor, ChunkWriter
from core.infrastructure.io.storage import get_storage_backend
from core.domain.services.schema import SchemaEvolutionChecker
from core.orchestration.runner.artifact_cleanup import ArtifactCleanup
from core.orchestration.runner.manifest_inspector import ManifestInspector
from core.foundation.catalog.hooks import (
    report_quality_snapshot,
    report_run_metadata,
    report_schema_snapshot,
    report_lineage,
)
from core.foundation.state import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    build_watermark_store,
    compute_max_watermark,
    CheckpointStore,
    CheckpointConflictError,
    build_checkpoint_store,
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
        from core.domain.services.processing.chunk_config import StoragePlan

        self.storage_plan: Optional[StoragePlan] = None
        self.schema_snapshot: List[Dict[str, str]] = []
        self.previous_manifest_schema: Optional[List[Dict[str, Any]]] = None
        self._schema_checker = SchemaEvolutionChecker(context.cfg)

        # Checkpoint and watermark state
        self._watermark_store: Optional[WatermarkStore] = None
        self._checkpoint_store: Optional[CheckpointStore] = None
        self._watermark: Optional[Watermark] = None
        self._checkpoint_enabled = self._get_checkpoint_enabled()

    @property
    def source_cfg(self) -> Dict[str, Any]:

        return cast(Dict[str, Any], self.cfg["source"])

    def _get_checkpoint_enabled(self) -> bool:
        """Check if checkpoint tracking is enabled."""
        run_cfg = self.cfg.get("source", {}).get("run", {})
        return run_cfg.get("checkpoint_enabled", True)

    def _get_watermark_config(self) -> tuple[Optional[str], WatermarkType]:
        """Get watermark column and type from config."""
        run_cfg = self.source_cfg.get("run", {})
        watermark_column = run_cfg.get("watermark_column")
        watermark_type_str = run_cfg.get("watermark_type", "timestamp")
        try:
            watermark_type = WatermarkType(watermark_type_str)
        except ValueError:
            watermark_type = WatermarkType.TIMESTAMP
        return watermark_column, watermark_type

    def _is_incremental_pattern(self) -> bool:
        """Check if current load pattern requires watermark tracking."""
        if not self.load_pattern:
            run_cfg = self.source_cfg.get("run", {})
            self.load_pattern = resolve_load_pattern(run_cfg)
        return self.load_pattern in (
            LoadPattern.INCREMENTAL_APPEND,
            LoadPattern.INCREMENTAL_MERGE,
        )

    def _load_watermark(self) -> Optional[Watermark]:
        """Load watermark for incremental patterns."""
        if not self._is_incremental_pattern():
            return None

        watermark_column, watermark_type = self._get_watermark_config()
        if not watermark_column:
            logger.warning(
                "Incremental pattern %s requires watermark_column in config",
                self.load_pattern.value if self.load_pattern else "unknown",
            )
            return None

        self._watermark_store = build_watermark_store(self.cfg)
        watermark = self._watermark_store.get(
            self.source_cfg["system"],
            self.source_cfg["table"],
            watermark_column,
            watermark_type,
        )
        if watermark.watermark_value:
            logger.info(
                "Loaded watermark for %s.%s: %s=%s",
                self.source_cfg["system"],
                self.source_cfg["table"],
                watermark_column,
                watermark.watermark_value,
            )
        else:
            logger.info(
                "No existing watermark for %s.%s (first run)",
                self.source_cfg["system"],
                self.source_cfg["table"],
            )
        return watermark

    def _save_watermark(
        self, records: List[Dict[str, Any]], cursor: Optional[str]
    ) -> Optional[str]:
        """Save updated watermark after successful extraction."""
        if not self._watermark or not self._watermark_store:
            return cursor

        watermark_column = self._watermark.watermark_column
        new_watermark_value = cursor or compute_max_watermark(
            records, watermark_column, self._watermark.watermark_value
        )

        if new_watermark_value and new_watermark_value != self._watermark.watermark_value:
            self._watermark.update(
                new_value=new_watermark_value,
                run_id=self.ctx.run_id,
                run_date=self.run_date,
                record_count=len(records),
            )
            self._watermark_store.save(self._watermark)
            logger.info(
                "Updated watermark for %s.%s: %s=%s",
                self.source_cfg["system"],
                self.source_cfg["table"],
                watermark_column,
                new_watermark_value,
            )
        return new_watermark_value

    def _acquire_checkpoint(self) -> None:
        """Acquire checkpoint lock for idempotency."""
        if not self._checkpoint_enabled:
            return

        self._checkpoint_store = build_checkpoint_store(self.cfg)
        watermark_column, watermark_type = self._get_watermark_config()
        source_key = f"{self.source_cfg['system']}.{self.source_cfg['table']}"

        try:
            checkpoint = self._checkpoint_store.acquire_lock(
                partition_path=self.relative_path,
                source_key=source_key,
                run_id=self.ctx.run_id,
                run_date=self.run_date.isoformat(),
                watermark_column=watermark_column,
                watermark_type=watermark_type.value if watermark_type else "timestamp",
            )
            logger.info(
                "Acquired checkpoint lock for %s (checkpoint_id=%s)",
                self.relative_path,
                checkpoint.checkpoint_id,
            )
        except CheckpointConflictError as e:
            logger.warning("Checkpoint conflict: %s", e)
            raise

    def _release_checkpoint(
        self,
        success: bool,
        record_count: int = 0,
        chunk_count: int = 0,
        watermark_value: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Release checkpoint lock and update status."""
        if not self._checkpoint_enabled or not self._checkpoint_store:
            return

        self._checkpoint_store.release_lock(
            partition_path=self.relative_path,
            run_id=self.ctx.run_id,
            success=success,
            record_count=record_count,
            chunk_count=chunk_count,
            artifact_count=len(self.created_files),
            watermark_value=watermark_value,
            error_message=error_message,
        )
        logger.info(
            "Released checkpoint for %s (success=%s)",
            self.relative_path,
            success,
        )

    def run(self) -> int:
        try:
            return self._run()
        except CheckpointConflictError as exc:
            logger.info(
                "Skipping Bronze run for %s due to checkpoint conflict: %s",
                self.relative_path,
                exc,
            )
            return 0
        except Exception as e:
            self._release_checkpoint(
                success=False,
                error_message=str(e),
            )
            self._cleanup_on_failure()
            raise

    def _run(self) -> int:
        # Acquire checkpoint lock for idempotency
        self._acquire_checkpoint()

        # Load watermark for incremental patterns
        self._watermark = self._load_watermark()

        # Inject watermark value into config for extractor to use
        if self._watermark and self._watermark.watermark_value:
            self.cfg.setdefault("_runtime", {})["watermark_value"] = self._watermark.watermark_value

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
            self._release_checkpoint(success=True, record_count=0, chunk_count=0)
            return 0

        self.schema_snapshot = infer_schema(records)

        chunk_count, chunk_files = self._process_chunks(records)
        self.created_files.extend(chunk_files)

        # Save updated watermark
        final_watermark = self._save_watermark(records, new_cursor)

        self._emit_metadata(
            record_count=len(records), chunk_count=chunk_count, cursor=new_cursor
        )

        # Release checkpoint with success
        self._release_checkpoint(
            success=True,
            record_count=len(records),
            chunk_count=chunk_count,
            watermark_value=final_watermark,
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
        """Inspect existing manifest and handle accordingly.

        Uses ManifestInspector for cleaner separation of concerns.
        """
        inspector = ManifestInspector(self._out_dir, self.load_pattern)
        result = inspector.inspect()

        if not result.exists:
            return

        # Store schema from previous manifest (if any)
        if result.previous_schema:
            self.previous_manifest_schema = result.previous_schema

        if result.needs_cleanup:
            logger.warning(
                "Detected incomplete Bronze partition at %s: %s; resetting artifacts for a clean rerun.",
                self._out_dir,
                result.error_message,
            )
            self._cleanup_existing_artifacts()
            return

        # Valid manifest - check schema compatibility before aborting
        self._assert_schema_compatible()

        raise RuntimeError(
            f"Bronze partition {self._out_dir} already contains a verified checksum manifest; aborting to avoid duplicates."
        )

    def _cleanup_existing_artifacts(self) -> None:
        """Clean up existing artifacts for a fresh rerun.

        Uses ArtifactCleanup for consistent cleanup logic.
        """
        cleanup = ArtifactCleanup(self._out_dir, self.storage_plan)
        cleanup.cleanup_directory()
        self.created_files = []

    def _assert_schema_compatible(self) -> None:
        """Check schema compatibility using SchemaEvolutionChecker."""
        self._schema_checker.check_compatibility(
            self.previous_manifest_schema, self.schema_snapshot
        )

    def _metadata_config(self) -> Dict[str, Any]:
        """Build a config dict that includes the user-specified schema evolution block."""
        return self._schema_checker.get_config_for_metadata()

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
        """Clean up created files on failure.

        Uses ArtifactCleanup for consistent cleanup logic.
        """
        run_cfg = self.source_cfg.get("run", {})
        cleanup_enabled = run_cfg.get("cleanup_on_failure", True)
        cleanup = ArtifactCleanup(self._out_dir, self.storage_plan)
        cleanup.cleanup_files(self.created_files, cleanup_enabled)


def run_extract(context: RunContext) -> int:
    job = ExtractJob(context)
    try:
        return job.run()
    except Exception as exc:
        logger.error("Extract failed: %s", exc, exc_info=True)
        raise
