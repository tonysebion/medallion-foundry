"""CLI entrypoint for medallion-foundry.

This file wires together:

- Config loading and validation
- Extractor selection
- Bronze writing
- Storage backend upload
- Incremental cursor update
- Parallel execution (optional)

The implementations in `core` and `extractors`
are intentionally modular so they can be extended or customized.
"""

import sys
import argparse
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, cast

from core.config import load_configs, ensure_root_config
from core.runner import run_extract
from core.parallel import run_parallel_extracts
from core.logging_config import setup_logging
from core.storage import get_storage_backend
from core.patterns import LoadPattern
from core.catalog import notify_catalog, report_run_metadata
from core.context import build_run_context
from core.hooks import fire_webhooks
from core.run_options import RunOptions
from core.config.typed_models import RootConfig
from core.storage.policy import enforce_storage_scope

# Version is managed by setuptools_scm and written to core/_version.py
try:
    from core._version import __version__
except ImportError:
    __version__ = "unknown (not installed)"

logger = logging.getLogger(__name__)


def list_storage_backends() -> List[str]:
    """Return list of available storage backends."""
    return ["s3", "azure", "local"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract data from APIs or databases to Bronze layer",
        epilog="For detailed documentation, see: https://github.com/tonysebion/medallion-foundry",
    )

    # Core arguments
    parser.add_argument(
        "--config",
        help="Path to YAML config file(s). Use comma-separated list for multiple configs.",
    )
    parser.add_argument(
        "--date",
        help="Logical run date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--parallel-workers",
        type=int,
        default=1,
        help="Number of parallel workers for multi-config extraction (default: 1 = sequential)",
    )
    parser.add_argument(
        "--load-pattern",
        choices=LoadPattern.choices(),
        help="Override load pattern for all configs (full, cdc, current_history)",
    )

    # New flags
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and connections without running extraction",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration files (no connection tests)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (DEBUG level) logging",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Suppress all output except errors"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"medallion-foundry {__version__}",
        help="Show version and exit",
    )
    parser.add_argument(
        "--list-backends",
        action="store_true",
        help="List available storage backends and exit",
    )
    parser.add_argument(
        "--log-format",
        choices=["human", "json", "simple"],
        default=None,
        help="Log format (default: human). Can also set via BRONZE_LOG_FORMAT env var",
    )
    # Silver pipeline tuning (forwarded via RunOptions / RunContext for downstream use)
    parser.add_argument(
        "--artifact-writer",
        choices=["default", "transactional"],
        default="default",
        help="Select artifact writer implementation for downstream Silver (default|transactional)",
    )
    parser.add_argument(
        "--storage-scope",
        choices=["any", "onprem"],
        default="any",
        help="Enforce storage classification policy (onprem rejects cloud backends).",
    )
    parser.add_argument(
        "--onprem-only",
        dest="storage_scope",
        action="store_const",
        const="onprem",
        help="Shortcut for `--storage-scope onprem` to enforce on-prem-only storage.",
    )
    parser.add_argument(
        "--on-success-webhook",
        action="append",
        dest="on_success_webhook",
        help="URL to POST a JSON payload to when the Bronze run succeeds (can be specified multiple times)",
    )
    parser.add_argument(
        "--on-failure-webhook",
        action="append",
        dest="on_failure_webhook",
        help="URL to POST a JSON payload to when the Bronze run fails (can be specified multiple times)",
    )

    args = parser.parse_args()

    # Handle simple info commands
    if args.list_backends:
        backends = list_storage_backends()
        print("Available storage backends:")
        for backend in backends:
            print(f"  - {backend}")
        print(
            "\nNote: Azure requires additional dependencies. See docs/setup/INSTALLATION.md"
        )
        return 0

    # Configure logging based on flags
    log_level = (
        logging.DEBUG if args.verbose else logging.ERROR if args.quiet else logging.INFO
    )
    setup_logging(level=log_level, format_type=args.log_format, use_colors=True)

    orchestrator = BronzeOrchestrator(parser, args)
    return orchestrator.execute()


class BronzeOrchestrator:
    """Encapsulates Bronze CLI workflows (validation, dry-run, execution)."""

    def __init__(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        self.parser = parser
        self.args = args
        self.config_paths = (
            [p.strip() for p in args.config.split(",")] if args.config else []
        )
        self._hook_context: Dict[str, Any] = {
            "layer": "bronze",
            "config_paths": list(self.config_paths),
        }
        self._run_options: Optional[RunOptions] = None
        self._configs_info: List[Dict[str, Any]] = []

    def execute(self) -> int:
        try:
            code = self._run()
        except Exception as exc:
            self._dispatch_hooks(success=False, extra={"error": str(exc)})
            raise

        self._dispatch_hooks(success=(code == 0))
        return code

    def _run(self) -> int:
        self._require_config()

        if self.args.validate_only:
            logger.info("Running configuration validation (no connection tests)")
            return self._validate_configs(dry_run=False)

        if self.args.dry_run:
            logger.info(
                "Running in dry-run mode (validation + connection tests, no extraction)"
            )
            return self._validate_configs(dry_run=True)

        configs = self._load_all_configs()
        if configs is None:
            return 1
        if not configs:
            logger.error("No valid configs loaded")
            return 1
        for cfg in configs:
            enforce_storage_scope(cfg.platform, self.args.storage_scope)

        run_date = (
            dt.date.fromisoformat(self.args.date) if self.args.date else dt.date.today()
        )
        self._record_configs_info(configs, run_date)
        self._run_options = self._build_run_options(configs, run_date)
        self._update_hook_context(run_date=run_date.isoformat())
        self._update_hook_context(
            config_names=[cfg.model_dump()["source"].get("config_name") for cfg in configs],
            tables=[f"{cfg.source.system}.{cfg.source.table}" for cfg in configs],
        )

        contexts = [
            build_run_context(
                cfg,
                run_date,
                load_pattern_override=self.args.load_pattern,
            )
            for cfg in configs
        ]

        self._update_hook_context(
            config_names=[ctx.config_name for ctx in contexts],
            tables=[ctx.dataset_id for ctx in contexts],
        )

        if len(contexts) == 1:
            relative_path = contexts[0].relative_path
            self._update_hook_context(relative_path=relative_path)
            for info in self._configs_info:
                info["relative_path"] = relative_path
            return run_extract(contexts[0])

        logger.info(
            f"Running {len(contexts)} configs with {self.args.parallel_workers} workers"
        )
        results = run_parallel_extracts(contexts, self.args.parallel_workers)
        failed = sum(1 for _, status, _ in results if status != 0)
        self._update_hook_context(
            parallel_results=[
                {"config_name": name, "status": status} for name, status, _ in results
            ]
        )
        return 1 if failed > 0 else 0

    def _require_config(self) -> None:
        if not self.args.config:
            self.parser.error(
                "--config is required (unless using --list-backends or --version)"
            )

    def _validate_configs(self, dry_run: bool) -> int:
        all_valid = True
        for config_path in self.config_paths:
            try:
                logger.info(f"Validating config: {config_path}")
                cfgs = load_configs(config_path)
                cfgs = self._apply_load_pattern_override(cfgs)
                for cfg in cfgs:
                    tcfg = cfg
                    source = tcfg.source
                    cfg_dict = cfg.model_dump()
                    logger.info(
                        f"  ✓ System: {tcfg.source.system}, Table: {tcfg.source.table}, Type: {cfg_dict.get('source', {}).get('type', 'api')}, Config: {cfg_dict.get('source', {}).get('config_name')}"
                    )
                    logger.info(
                        f"  ✓ Load pattern: {tcfg.source.run.load_pattern.value if getattr(tcfg.source, 'run', None) else LoadPattern.FULL.value}"
                    )
                    platform = cfg.model_dump()["platform"]
                    storage_backend = platform["bronze"].get("storage_backend", "s3")
                    logger.info(f"  ✓ Storage backend: {storage_backend}")

                    if dry_run:
                        try:
                            backend = get_storage_backend(platform)
                            logger.info(
                                f"  ✓ Storage backend initialized: {backend.get_backend_type()}"
                            )
                        except Exception as exc:
                            logger.warning(
                                f"  ⚠ Storage backend validation failed: {exc}"
                            )

                logger.info(f"  ✓ Config valid: {config_path} ({len(cfgs)} source(s))")
            except Exception as exc:
                logger.error(f"  ✗ Config invalid: {config_path}")
                logger.error(f"    Error: {exc}")
                all_valid = False

        return 0 if all_valid else 1

    def _load_all_configs(self) -> Optional[List[RootConfig]]:
        configs: List[RootConfig] = []
        for config_path in self.config_paths:
            try:
                cfgs = load_configs(config_path)
                cfgs = self._apply_load_pattern_override(cfgs)
                configs.extend(cfgs)
            except Exception as exc:
                logger.error(
                    f"Failed to load config {config_path}: {exc}", exc_info=True
                )
                return None
        return configs

    def _apply_load_pattern_override(
        self, cfgs: List[RootConfig]
    ) -> List[RootConfig]:
        if not self.args.load_pattern:
            return cfgs
        normalized = LoadPattern.normalize(self.args.load_pattern).value
        updated: List[RootConfig] = []
        for cfg in cfgs:
            cfg_dict = cfg.model_dump()
            cfg_dict.setdefault("source", {}).setdefault("run", {})["load_pattern"] = normalized
            updated.append(ensure_root_config(cfg_dict))
        return updated

    def _dispatch_hooks(
        self, success: bool, extra: Optional[Dict[str, Any]] = None
    ) -> None:
        payload: Dict[str, Any] = {
            **self._hook_context,
            "status": "success" if success else "failure",
        }
        if extra:
            payload.update(extra)

        if self._run_options:
            urls = (
                self._run_options.on_success_webhooks
                if success
                else self._run_options.on_failure_webhooks
            )
        else:
            urls = (
                self.args.on_success_webhook
                if success
                else self.args.on_failure_webhook
            )
        fire_webhooks(urls, payload)

        event = "bronze_run_completed" if success else "bronze_run_failed"
        notify_catalog(event, payload)
        self._report_run_metadata(success, extra or {})

    def _update_hook_context(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if value is not None:
                self._hook_context[key] = value

    def _build_run_options(
        self, configs: List[RootConfig], run_date: dt.date
    ) -> RunOptions:
        # Prefer typed model if available to reduce dict key errors.
        typed: RootConfig | None = configs[0]
        if typed and typed.silver:
            run_cfg = typed.source.run
            load_pattern = LoadPattern.normalize(
                run_cfg.load_pattern.value
                if hasattr(run_cfg.load_pattern, "value")
                else run_cfg.load_pattern
            )
            silver_cfg = typed.silver
            return RunOptions(
                load_pattern=load_pattern,
                require_checksum=False,  # placeholder, legacy field not yet in typed model
                write_parquet=silver_cfg.write_parquet,
                write_csv=silver_cfg.write_csv,
                parquet_compression=silver_cfg.parquet_compression,
                primary_keys=silver_cfg.primary_keys,
                order_column=silver_cfg.order_column,
                partition_columns=silver_cfg.partitioning.columns,
                artifact_names={
                    "current": silver_cfg.current_output_name,
                    "history": silver_cfg.history_output_name,
                    "cdc": silver_cfg.cdc_output_name,
                    "full_snapshot": silver_cfg.full_output_name,
                },
                on_success_webhooks=self.args.on_success_webhook or [],
                on_failure_webhooks=self.args.on_failure_webhook or [],
                artifact_writer_kind=getattr(self.args, "artifact_writer", "default"),
            )

        # Fallback to dict-based extraction if typed model missing.
        cfg_dict: Dict[str, Any] = configs[0].model_dump()
        source_cfg = cast(Dict[str, Any], cfg_dict["source"])
        run_cfg = cast(Dict[str, Any], source_cfg["run"])
        silver_cfg = cast(Dict[str, Any], cfg_dict.get("silver", {}))
        load_pattern = LoadPattern.normalize(run_cfg.get("load_pattern"))
        write_parquet = run_cfg.get("write_parquet", True)
        write_csv = run_cfg.get("write_csv", False)
        parquet_compression = run_cfg.get("parquet_compression", "snappy")
        primary_keys = silver_cfg.get("primary_keys", []) or []
        order_column = silver_cfg.get("order_column")
        partition_columns = silver_cfg.get("partitioning", {}).get("columns", [])

        return RunOptions(
            load_pattern=load_pattern,
            require_checksum=silver_cfg.get("require_checksum", False),
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
            primary_keys=primary_keys,
            order_column=order_column,
            partition_columns=partition_columns,
            artifact_names=RunOptions.default_artifacts(),
            on_success_webhooks=self.args.on_success_webhook or [],
            on_failure_webhooks=self.args.on_failure_webhook or [],
            artifact_writer_kind=getattr(self.args, "artifact_writer", "default"),
        )

    def _record_configs_info(
        self, configs: List[RootConfig], run_date: dt.date
    ) -> None:
        self._configs_info = []
        for cfg in configs:
            dataset_id = f"bronze:{cfg.source.system}.{cfg.source.table}"
            cfg_dict = cfg.model_dump()
            self._configs_info.append(
                {
                    "dataset_id": dataset_id,
                    "config_name": cfg_dict["source"].get("config_name"),
                    "run_date": run_date.isoformat(),
                    "system": cfg.source.system,
                    "table": cfg.source.table,
                }
            )
        self._update_hook_context(
            datasets=[info["dataset_id"] for info in self._configs_info]
        )

    def _report_run_metadata(self, success: bool, extra: Dict[str, Any]) -> None:
        if not self._configs_info:
            return
        status = "success" if success else "failure"
        for info in self._configs_info:
            metadata = {
                "config_name": info["config_name"],
                "system": info["system"],
                "table": info["table"],
                "run_date": info["run_date"],
                "relative_path": info.get("relative_path"),
                "status": status,
            }
            metadata.update(extra)
            report_run_metadata(info["dataset_id"], metadata)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        logger.error(f"Fatal error: {exc}", exc_info=True)
        sys.exit(1)
