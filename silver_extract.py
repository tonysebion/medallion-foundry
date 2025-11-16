"""CLI utility for promoting Bronze data to the Silver layer."""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.config import build_relative_path, load_configs
from core.io import write_batch_metadata, verify_checksum_manifest, write_checksum_manifest
from core.logging_config import setup_logging
from core.patterns import LoadPattern
from core.paths import build_silver_partition_path
from core.catalog import (
    notify_catalog,
    report_schema_snapshot,
    report_quality_snapshot,
    report_run_metadata,
    report_lineage,
)
from core.hooks import fire_webhooks
from core.run_options import RunOptions
from core.storage.policy import enforce_storage_scope
from silver_stream import stream_silver_promotion
from core.silver.artifacts import (
    apply_schema_settings,
    build_current_view,
    normalize_dataframe,
    write_silver_outputs,
)
from core.silver.models import SilverModel

logger = logging.getLogger(__name__)


def parse_primary_keys(raw: str | None) -> List[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def discover_load_pattern(bronze_path: Path) -> Optional[LoadPattern]:
    metadata_path = bronze_path / "_metadata.json"
    if metadata_path.exists():
        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
            pattern_value = data.get("load_pattern")
            if pattern_value:
                return LoadPattern.normalize(pattern_value)
        except Exception as exc:
            logger.warning("Failed to read Bronze metadata at %s: %s", metadata_path, exc)
    return None


def _iter_bronze_frames(bronze_path: Path):
    csv_files = sorted(bronze_path.glob("*.csv"))
    parquet_files = sorted(bronze_path.glob("*.parquet"))

    for csv_path in csv_files:
        logger.debug("Reading CSV chunk %s", csv_path.name)
        yield pd.read_csv(csv_path)
    for parquet_path in parquet_files:
        logger.debug("Reading Parquet chunk %s", parquet_path.name)
        yield pd.read_parquet(parquet_path)


def load_bronze_records(bronze_path: Path) -> pd.DataFrame:
    frame_iter = _iter_bronze_frames(bronze_path)
    try:
        combined = next(frame_iter)
    except StopIteration:
        raise FileNotFoundError(f"No chunk files found in {bronze_path}")

    for frame in frame_iter:
        combined = pd.concat([combined, frame], ignore_index=True)
        del frame

    return combined


def derive_relative_partition(bronze_path: Path) -> Path:
    parts = list(bronze_path.parts)
    for idx, part in enumerate(parts):
        if part.startswith("system="):
            return Path(*parts[idx:])
    return Path(bronze_path.name)


def _default_silver_cfg() -> Dict[str, Any]:
    return {
        "schema": {"rename_map": {}, "column_order": None},
        "normalization": {"trim_strings": False, "empty_strings_as_null": False},
        "partitioning": {"columns": []},
        "error_handling": {"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0},
        "primary_keys": [],
        "order_column": None,
        "write_parquet": True,
        "write_csv": False,
        "parquet_compression": "snappy",
        "full_output_name": "full_snapshot",
        "cdc_output_name": "cdc_changes",
        "current_output_name": "current",
        "history_output_name": "history",
        "domain": "default",
        "entity": "dataset",
        "version": 1,
        "load_partition_name": "load_date",
        "include_pattern_folder": False,
        "require_checksum": False,
        "model": SilverModel.PERIODIC_SNAPSHOT.value,
    }


def _derive_bronze_path_from_config(cfg: Dict[str, Any], run_date: dt.date) -> Path:
    local_output_dir = Path(cfg["source"]["run"].get("local_output_dir", "./output"))
    relative_path = build_relative_path(cfg, run_date)
    return (local_output_dir / relative_path).resolve()


def _select_config(cfgs: List[Dict[str, Any]], source_name: Optional[str]) -> Dict[str, Any]:
    if source_name:
        matches = [cfg for cfg in cfgs if cfg["source"].get("config_name") == source_name]
        if not matches:
            raise ValueError(f"No source named '{source_name}' found in config")
        return matches[0]
    if len(cfgs) == 1:
        return cfgs[0]
    raise ValueError("Config contains multiple sources; specify --source-name to select one.")


@dataclass
class PromotionOptions:
    run_options: RunOptions
    schema_cfg: Dict[str, Any]
    normalization_cfg: Dict[str, Any]
    error_cfg: Dict[str, Any]
    silver_model: SilverModel

    @classmethod
    def from_inputs(
        cls,
        silver_cfg: Dict[str, Any],
        args: argparse.Namespace,
        load_pattern: LoadPattern,
    ) -> "PromotionOptions":
        schema_cfg = silver_cfg.get("schema", {"rename_map": {}, "column_order": None})
        normalization_cfg = silver_cfg.get("normalization", {"trim_strings": False, "empty_strings_as_null": False})
        error_cfg = silver_cfg.get("error_handling", {"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0})

        rename_map = schema_cfg.get("rename_map") or {}

        cli_primary = parse_primary_keys(args.primary_key) if args.primary_key is not None else None
        primary_keys_raw = list(cli_primary if cli_primary is not None else silver_cfg.get("primary_keys", []))
        primary_keys = [rename_map.get(pk, pk) for pk in primary_keys_raw]

        cli_order_column = args.order_column if args.order_column is not None else None
        order_column_raw = cli_order_column if cli_order_column is not None else silver_cfg.get("order_column")
        order_column = rename_map.get(order_column_raw, order_column_raw) if order_column_raw else None

        partition_columns = [rename_map.get(col, col) for col in silver_cfg.get("partitioning", {}).get("columns", [])]

        write_parquet = args.write_parquet if args.write_parquet is not None else silver_cfg.get("write_parquet", True)
        write_csv = args.write_csv if args.write_csv is not None else silver_cfg.get("write_csv", False)
        if not write_parquet and not write_csv:
            raise ValueError("At least one output format (Parquet or CSV) must be enabled")

        parquet_compression = args.parquet_compression if args.parquet_compression else silver_cfg.get("parquet_compression", "snappy")

        artifact_names = {
            "full_snapshot": args.full_output_name or silver_cfg.get("full_output_name", "full_snapshot"),
            "cdc": args.cdc_output_name or silver_cfg.get("cdc_output_name", "cdc_changes"),
            "current": args.current_output_name or silver_cfg.get("current_output_name", "current"),
            "history": args.history_output_name or silver_cfg.get("history_output_name", "history"),
        }

        run_options = RunOptions(
            load_pattern=load_pattern,
            require_checksum=args.require_checksum if args.require_checksum is not None else silver_cfg.get("require_checksum", False),
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
            primary_keys=primary_keys,
            order_column=order_column,
            partition_columns=partition_columns,
            artifact_names=artifact_names,
            on_success_webhooks=args.on_success_webhook or [],
            on_failure_webhooks=args.on_failure_webhook or [],
        )

        model_override = args.silver_model or silver_cfg.get("model")
        if model_override is not None:
            silver_model = SilverModel.normalize(model_override)
        else:
            silver_model = SilverModel.default_for_load_pattern(load_pattern)

        if silver_model.requires_dedupe:
            if not primary_keys:
                raise ValueError(f"{silver_model.describe()} requires silver.primary_keys to be defined")
            if not order_column:
                raise ValueError(f"{silver_model.describe()} requires silver.order_column to be defined")

        return cls(
            run_options=run_options,
            schema_cfg=schema_cfg,
            normalization_cfg=normalization_cfg,
            error_cfg=error_cfg,
            silver_model=silver_model,
        )


@dataclass
class PromotionContext:
    cfg: Optional[Dict[str, Any]]
    run_date: dt.date
    bronze_path: Path
    silver_partition: Path
    load_pattern: LoadPattern
    options: PromotionOptions
    domain: Any
    entity: Any
    version: int
    load_partition_name: str
    include_pattern_folder: bool
    silver_model: SilverModel


class SilverPromotionService:
    """High-level orchestrator for Silver CLI operations."""

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
        self.parser = parser
        self.args = args
        self.cfg_list = load_configs(args.config) if args.config else None
        self._silver_identity: Optional[Tuple[Any, Any, int, str, bool]] = None
        self._hook_context: Dict[str, Any] = {"layer": "silver"}
        self._run_options: Optional[RunOptions] = None

    def execute(self) -> int:
        try:
            code = self._run()
        except Exception as exc:
            self._fire_hooks(success=False, extra={"error": str(exc)})
            raise
        self._fire_hooks(success=(code == 0))
        return code

    def _run(self) -> int:
        if self.args.validate_only:
            self._handle_validate_only()
            return 0

        cfg = self._select_config()
        enforce_storage_scope(cfg["platform"], self.args.storage_scope)
        run_date = self._resolve_run_date()
        bronze_path = self._resolve_bronze_path(cfg, run_date)
        self._update_hook_context(bronze_path=str(bronze_path), run_date=run_date.isoformat())
        if not bronze_path.exists() or not bronze_path.is_dir():
            raise FileNotFoundError(f"Bronze path '{bronze_path}' does not exist or is not a directory")

        load_pattern = self._resolve_load_pattern(cfg, bronze_path)
        self._update_hook_context(load_pattern=load_pattern.value)
        silver_partition = self._resolve_silver_partition(cfg, bronze_path, load_pattern, run_date)
        logger.info("Bronze partition: %s", bronze_path)
        logger.info("Silver partition: %s", silver_partition)
        logger.info("Load pattern: %s", load_pattern.value)
        context = self._build_context(cfg, bronze_path, silver_partition, load_pattern, run_date)
        self._update_hook_context(
            silver_partition=str(silver_partition),
            config_name=context.cfg["source"].get("config_name") if context.cfg else None,
            domain=context.domain,
            entity=context.entity,
            silver_model=context.silver_model.value,
        )
        self._run_options = context.options.run_options
        self._maybe_verify_checksum(context)

        if self.args.dry_run:
            logger.info("Dry run complete; no files written")
            return 0

        bronze_size_bytes = self._calculate_directory_size(bronze_path)
        run_start = time.perf_counter()
        run_opts = context.options.run_options
        if self.args.stream_mode:
            outputs, chunk_count, record_count, schema_snapshot = stream_silver_promotion(
                bronze_path,
                silver_partition,
                run_opts,
                context.options.schema_cfg,
                context.options.normalization_cfg,
                context.options.error_cfg,
                context.silver_model,
            )
            logger.info("Streamed %s records from Bronze path %s", record_count, bronze_path)
        else:
            df = load_bronze_records(bronze_path)
            normalized_df = apply_schema_settings(df, context.options.schema_cfg)
            normalized_df = normalize_dataframe(normalized_df, context.options.normalization_cfg)
            logger.info("Loaded %s records from Bronze path %s", len(normalized_df), bronze_path)
            outputs = write_silver_outputs(
                normalized_df,
                run_opts.primary_keys,
                run_opts.order_column,
                run_opts.write_parquet,
                run_opts.write_csv,
                run_opts.parquet_compression,
                run_opts.artifact_names,
                run_opts.partition_columns,
                context.options.error_cfg,
                context.silver_model,
                silver_partition,
            )
            schema_snapshot = [
                {"name": col, "dtype": str(dtype)} for col, dtype in normalized_df.dtypes.items()
            ]
            record_count = len(normalized_df)
            chunk_count = len(outputs)

        self._write_metadata(record_count, chunk_count, context, outputs)
        runtime_seconds = time.perf_counter() - run_start
        self._write_checksum_manifest(
            outputs,
            context,
            schema_snapshot,
            record_count,
            chunk_count,
            bronze_size_bytes=bronze_size_bytes,
            runtime_seconds=runtime_seconds,
        )

        for label, paths in outputs.items():
            for path in paths:
                logger.info("Created Silver artifact '%s': %s", label, path)

        logger.info("Silver promotion complete")
        return 0

    def _handle_validate_only(self) -> None:
        if not self.cfg_list:
            self.parser.error("--config is required when using --validate-only")
        if self.args.source_name:
            cfg = _select_config(self.cfg_list, self.args.source_name)
            logger.info("Silver configuration valid for %s", cfg["source"]["config_name"])
        else:
            for cfg in self.cfg_list:
                logger.info("Silver configuration valid for %s", cfg["source"]["config_name"])

    def _select_config(self) -> Optional[Dict[str, Any]]:
        if not self.cfg_list:
            return None
        try:
            return _select_config(self.cfg_list, self.args.source_name)
        except ValueError as exc:
            self.parser.error(str(exc))

    def _resolve_run_date(self) -> dt.date:
        return dt.date.fromisoformat(self.args.date) if self.args.date else dt.date.today()

    def _resolve_bronze_path(self, cfg: Optional[Dict[str, Any]], run_date: dt.date) -> Path:
        if self.args.bronze_path:
            return Path(self.args.bronze_path).resolve()
        if not cfg:
            self.parser.error("Either --bronze-path or --config must be supplied")
        return _derive_bronze_path_from_config(cfg, run_date)

    def _resolve_load_pattern(self, cfg: Optional[Dict[str, Any]], bronze_path: Path) -> LoadPattern:
        metadata_pattern = discover_load_pattern(bronze_path) if self.args.pattern == "auto" else None
        if self.args.pattern != "auto":
            return LoadPattern.normalize(self.args.pattern)

        config_pattern = LoadPattern.normalize(cfg["source"]["run"].get("load_pattern")) if cfg else LoadPattern.FULL
        if metadata_pattern and cfg and metadata_pattern != config_pattern:
            logger.warning(
                "Config load_pattern (%s) differs from Bronze metadata (%s); using metadata value",
                config_pattern.value,
                metadata_pattern.value,
            )
            return metadata_pattern
        return metadata_pattern or config_pattern

    def _resolve_silver_partition(
        self,
        cfg: Optional[Dict[str, Any]],
        bronze_path: Path,
        load_pattern: LoadPattern,
        run_date: dt.date,
    ) -> Path:
        silver_base = Path(self.args.silver_base).resolve() if self.args.silver_base else None
        silver_cfg = cfg["silver"] if cfg else _default_silver_cfg()

        domain, entity, version, load_partition_name, include_pattern_folder = self._extract_identity(cfg, silver_cfg)
        self._silver_identity = (domain, entity, version, load_partition_name, include_pattern_folder)

        if not silver_base:
            if cfg:
                silver_base = Path(silver_cfg.get("output_dir", "./silver_output")).resolve()
            else:
                silver_base = Path("./silver_output").resolve()

        if cfg:
            partition = build_silver_partition_path(
                silver_base,
                domain,
                entity,
                version,
                load_partition_name,
                include_pattern_folder,
                load_pattern,
                run_date,
            )
        else:
            partition = silver_base / derive_relative_partition(bronze_path)

        return partition

    def _extract_identity(
        self,
        cfg: Optional[Dict[str, Any]],
        silver_cfg: Dict[str, Any],
    ) -> Tuple[Any, Any, int, str, bool]:
        if cfg:
            domain = silver_cfg.get("domain", cfg["source"]["system"])
            entity = silver_cfg.get("entity", cfg["source"]["table"])
        else:
            domain = silver_cfg.get("domain", "default")
            entity = silver_cfg.get("entity", "dataset")
        version = silver_cfg.get("version", 1)
        load_partition_name = silver_cfg.get("load_partition_name", "load_date")
        include_pattern_folder = silver_cfg.get("include_pattern_folder", False)
        return domain, entity, version, load_partition_name, include_pattern_folder

    def _build_context(
        self,
        cfg: Optional[Dict[str, Any]],
        bronze_path: Path,
        silver_partition: Path,
        load_pattern: LoadPattern,
        run_date: dt.date,
    ) -> PromotionContext:
        silver_cfg = cfg["silver"] if cfg else _default_silver_cfg()
        try:
            options = PromotionOptions.from_inputs(silver_cfg, self.args, load_pattern)
        except ValueError as exc:
            self.parser.error(str(exc))

        domain, entity, version, load_partition_name, include_pattern_folder = (
            self._silver_identity
            if self._silver_identity
            else self._extract_identity(cfg, silver_cfg)
        )

        silver_partition.mkdir(parents=True, exist_ok=True)

        return PromotionContext(
            cfg=cfg,
            run_date=run_date,
            bronze_path=bronze_path,
            silver_partition=silver_partition,
            load_pattern=load_pattern,
            options=options,
            domain=domain,
            entity=entity,
            version=version,
            load_partition_name=load_partition_name,
            include_pattern_folder=include_pattern_folder,
            silver_model=options.silver_model,
        )

    def _maybe_verify_checksum(self, context: PromotionContext) -> None:
        if not context.options.run_options.require_checksum:
            return
        manifest = verify_checksum_manifest(context.bronze_path, expected_pattern=context.load_pattern.value)
        manifest_path = context.bronze_path / "_checksums.json"
        logger.info(
            "Verified %s checksum entries from %s",
            len(manifest.get("files", [])),
            manifest_path,
        )

    def _write_metadata(
        self,
        record_count: int,
        chunk_count: int,
        context: PromotionContext,
        outputs: Dict[str, List[Path]],
    ) -> None:
        metadata_path = write_batch_metadata(
            context.silver_partition,
            record_count=record_count,
            chunk_count=chunk_count,
            extra_metadata={
                "load_pattern": context.load_pattern.value,
                "bronze_path": str(context.bronze_path),
                "primary_keys": context.options.run_options.primary_keys,
                "order_column": context.options.run_options.order_column,
                "domain": context.domain,
                "entity": context.entity,
                "version": context.version,
                "load_partition_name": context.load_partition_name,
                "include_pattern_folder": context.include_pattern_folder,
                "partition_columns": context.options.run_options.partition_columns,
                "write_parquet": context.options.run_options.write_parquet,
                "write_csv": context.options.run_options.write_csv,
                "parquet_compression": context.options.run_options.parquet_compression,
                "normalization": context.options.normalization_cfg,
                "schema": context.options.schema_cfg,
                "error_handling": context.options.error_cfg,
                "artifacts": {label: [p.name for p in paths] for label, paths in outputs.items()},
                "require_checksum": context.options.run_options.require_checksum,
                "silver_model": context.silver_model.value,
            },
        )
        logger.debug("Wrote Silver metadata to %s", metadata_path)

    def _write_checksum_manifest(
        self,
        outputs: Dict[str, List[Path]],
        context: PromotionContext,
        schema_snapshot: List[Dict[str, str]],
        record_count: int,
        chunk_count: int,
        bronze_size_bytes: int,
        runtime_seconds: float,
    ) -> None:
        files = [path for paths in outputs.values() for path in paths]
        if not files:
            logger.warning("No Silver artifacts produced; skipping checksum manifest")
            return

        dataset_id = f"silver:{context.domain}.{context.entity}"
        artifact_size_bytes = sum(path.stat().st_size for path in files)
        stats = {
            "record_count": record_count,
            "chunk_count": chunk_count,
            "primary_key_count": len(context.options.run_options.primary_keys),
            "bronze_size_bytes": bronze_size_bytes,
            "artifact_size_bytes": artifact_size_bytes,
        }

        extra = {
            "load_pattern": context.load_pattern.value,
            "bronze_path": str(context.bronze_path),
            "schema": schema_snapshot,
            "stats": stats,
            "runtime_seconds": runtime_seconds,
        }
        manifest_path = write_checksum_manifest(
            context.silver_partition,
            files,
            context.load_pattern.value,
            extra_metadata=extra,
        )
        logger.info("Wrote Silver checksum manifest to %s", manifest_path)
        report_schema_snapshot(dataset_id, schema_snapshot)
        report_quality_snapshot(dataset_id, {"record_count": stats["record_count"], "artifact_count": len(files)})
        run_metadata = {
            "load_pattern": context.load_pattern.value,
            "silver_partition": str(context.silver_partition),
            "artifact_names": list(outputs.keys()),
            "require_checksum": context.options.run_options.require_checksum,
            "manifest_path": manifest_path.name,
        }
        report_run_metadata(dataset_id, run_metadata)
        bronze_dataset = (
            f"bronze:{context.cfg['source']['system']}.{context.cfg['source']['table']}"
            if context.cfg
            else f"bronze:{context.bronze_path}"
        )
        lineage_metadata = {
            "manifest": manifest_path.name,
            "files": [p.name for p in files],
            "load_pattern": context.load_pattern.value,
        }
        report_lineage(bronze_dataset, dataset_id, lineage_metadata)

    def _fire_hooks(self, success: bool, extra: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {
            **self._hook_context,
            "status": "success" if success else "failure",
        }
        if extra:
            payload.update(extra)

        if self._run_options:
            urls = (
                self._run_options.on_success_webhooks if success else self._run_options.on_failure_webhooks
            )
        else:
            urls = self.args.on_success_webhook if success else self.args.on_failure_webhook
        fire_webhooks(urls, payload)

        event = "silver_promotion_completed" if success else "silver_promotion_failed"
        notify_catalog(event, payload)

    def _update_hook_context(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if value is not None:
                self._hook_context[key] = value

    def _calculate_directory_size(self, directory: Path) -> int:
        total = 0
        for path in directory.rglob("*"):
            if not path.is_file():
                continue
            try:
                total += path.stat().st_size
            except OSError:
                continue
        return total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote Bronze data to Silver layer with configurable load patterns",
    )
    parser.add_argument("--config", help="Shared YAML config (same as bronze_extract)")
    parser.add_argument(
        "--bronze-path",
        help="Path to the Bronze partition containing chunk files (overrides --config derived path)",
    )
    parser.add_argument(
        "--silver-base",
        help="Base directory for Silver outputs (default: silver.output_dir or ./silver_output)",
    )
    parser.add_argument(
        "--date",
        help="Logical run date (YYYY-MM-DD). Required when deriving bronze path from --config.",
    )
    parser.add_argument(
        "--source-name",
        help="Name of the source entry to run when config contains multiple sources",
    )
    parser.add_argument(
        "--pattern",
        choices=["auto"] + LoadPattern.choices(),
        default="auto",
        help="Load pattern to apply. 'auto' uses metadata if present, otherwise falls back to config/default.",
    )
    parser.add_argument(
        "--primary-key",
        help="Comma-separated list of primary key columns for current_history consolidation",
    )
    parser.add_argument(
        "--order-column",
        help="Column used to identify the latest record when building current views",
    )
    parser.add_argument(
        "--silver-model",
        choices=SilverModel.choices(),
        help="Silver asset model to produce (overrides defaults derived from Bronze load pattern)",
    )
    parser.add_argument(
        "--write-parquet",
        dest="write_parquet",
        action="store_true",
        help="Enable Parquet output (default: true unless disabled in config)",
    )
    parser.add_argument(
        "--no-write-parquet",
        dest="write_parquet",
        action="store_false",
        help="Disable Parquet output",
    )
    parser.set_defaults(write_parquet=None)
    parser.add_argument(
        "--write-csv",
        dest="write_csv",
        action="store_true",
        help="Enable CSV output (default: false unless enabled in config)",
    )
    parser.add_argument(
        "--no-write-csv",
        dest="write_csv",
        action="store_false",
        help="Disable CSV output",
    )
    parser.set_defaults(write_csv=None)
    parser.add_argument("--parquet-compression", help="Parquet compression codec (default: snappy)")
    parser.add_argument(
        "--stream",
        dest="stream_mode",
        action="store_true",
        help="Use the streaming writer that processes Bronze chunks incrementally",
    )
    parser.add_argument("--full-output-name", help="Base name for full snapshot files")
    parser.add_argument("--current-output-name", help="Base name for current view files")
    parser.add_argument("--history-output-name", help="Base name for history view files")
    parser.add_argument("--cdc-output-name", help="Base name for CDC output files")
    parser.add_argument(
        "--require-checksum",
        dest="require_checksum",
        action="store_true",
        help="Require Bronze checksum manifest verification before Silver runs",
    )
    parser.add_argument(
        "--no-require-checksum",
        dest="require_checksum",
        action="store_false",
        help="Disable checksum verification even if the config enables it",
    )
    parser.set_defaults(require_checksum=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and derived paths without writing Silver outputs",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration file (requires --config)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--log-format",
        choices=["human", "json", "simple"],
        default=None,
        help="Log format (default: human)",
    )
    parser.add_argument(
        "--storage-scope",
        choices=["any", "onprem"],
        default="any",
        help="Enforce storage classification policy (onprem rejects cloud storage endpoints).",
    )
    parser.add_argument(
        "--onprem-only",
        dest="storage_scope",
        action="store_const",
        const="onprem",
        help="Alias for `--storage-scope onprem` to enforce on-prem-only storage.",
    )
    parser.add_argument(
        "--on-success-webhook",
        action="append",
        dest="on_success_webhook",
        help="URL to POST a JSON payload to when the Silver promotion succeeds (can be specified multiple times)",
    )
    parser.add_argument(
        "--on-failure-webhook",
        action="append",
        dest="on_failure_webhook",
        help="URL to POST a JSON payload to when the Silver promotion fails (can be specified multiple times)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO, format_type=args.log_format)

    service = SilverPromotionService(parser, args)
    return service.execute()


if __name__ == "__main__":
    raise SystemExit(main())
