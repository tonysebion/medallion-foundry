"""CLI utility for promoting Bronze data to the Silver layer."""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.config import build_relative_path, load_configs
from core.io import write_batch_metadata, verify_checksum_manifest
from core.logging_config import setup_logging
from core.patterns import LoadPattern
from core.paths import build_silver_partition_path

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
        first_frame = next(frame_iter)
    except StopIteration:
        raise FileNotFoundError(f"No chunk files found in {bronze_path}")

    return pd.concat(itertools.chain([first_frame], frame_iter), ignore_index=True)


def derive_relative_partition(bronze_path: Path) -> Path:
    parts = list(bronze_path.parts)
    for idx, part in enumerate(parts):
        if part.startswith("system="):
            return Path(*parts[idx:])
    return Path(bronze_path.name)


def build_current_view(
    df: pd.DataFrame,
    primary_keys: List[str],
    order_column: str | None,
) -> pd.DataFrame:
    if not primary_keys or any(pk not in df.columns for pk in primary_keys):
        logger.warning(
            "Primary keys missing or not present in data; using entire dataset for current view"
        )
        return df

    working = df.copy()
    if order_column and order_column in df.columns:
        working = working.sort_values(order_column)
    else:
        working = working.reset_index()
    return working.drop_duplicates(subset=primary_keys, keep="last").drop(
        columns=["index"], errors="ignore"
    )


def apply_schema_settings(df: pd.DataFrame, schema_cfg: Dict[str, Any]) -> pd.DataFrame:
    rename_map = schema_cfg.get("rename_map") or {}
    column_order = schema_cfg.get("column_order")

    result = df.copy()
    if rename_map:
        missing = [col for col in rename_map if col not in result.columns]
        if missing:
            logger.warning("schema.rename_map references missing columns: %s", missing)
        result = result.rename(columns=rename_map)

    if column_order:
        missing_order_cols = [col for col in column_order if col not in result.columns]
        if missing_order_cols:
            logger.warning("schema.column_order missing columns: %s", missing_order_cols)
        ordered = [col for col in column_order if col in result.columns]
        remaining = [col for col in result.columns if col not in ordered]
        result = result[ordered + remaining]

    return result


def normalize_dataframe(df: pd.DataFrame, normalization_cfg: Dict[str, Any]) -> pd.DataFrame:
    trim_strings = normalization_cfg.get("trim_strings", False)
    empty_as_null = normalization_cfg.get("empty_strings_as_null", False)

    result = df.copy()
    if trim_strings or empty_as_null:
        object_cols = result.select_dtypes(include="object").columns
        for col in object_cols:
            if trim_strings:
                result[col] = result[col].apply(lambda val: val.strip() if isinstance(val, str) else val)
            if empty_as_null:
                result[col] = result[col].apply(lambda val: None if isinstance(val, str) and val == "" else val)
    return result


def _sanitize_partition_value(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-z._-]", "_", str(value))


def partition_dataframe(df: pd.DataFrame, partition_columns: List[str]) -> List[Tuple[List[str], pd.DataFrame]]:
    if not partition_columns:
        return [([], df)]

    partitions = [([], df)]
    for column in partition_columns:
        if column not in df.columns:
            logger.warning("Partition column '%s' not found; skipping this column", column)
            continue
        new_partitions: List[Tuple[List[str], pd.DataFrame]] = []
        for path_parts, subset in partitions:
            for value, group in subset.groupby(column):
                safe_value = _sanitize_partition_value(value)
                new_partitions.append((path_parts + [f"{column}={safe_value}"], group))
        partitions = new_partitions

    return partitions


def handle_error_rows(
    df: pd.DataFrame,
    primary_keys: List[str],
    error_cfg: Dict[str, Any],
    dataset_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    if not error_cfg.get("enabled") or not primary_keys:
        if error_cfg.get("enabled") and not primary_keys:
            logger.warning("Error handling enabled but no primary_keys specified; skipping validation")
        return df

    missing_cols = [col for col in primary_keys if col not in df.columns]
    if missing_cols:
        logger.warning("Primary key columns %s not found; skipping error handling", missing_cols)
        return df

    invalid_mask = df[primary_keys].isnull().any(axis=1)
    invalid_count = int(invalid_mask.sum())
    if invalid_count == 0:
        return df

    total_rows = len(df)
    percent = (invalid_count / total_rows) * 100 if total_rows else 0

    error_dir = output_dir / "_errors"
    error_dir.mkdir(parents=True, exist_ok=True)
    error_path = error_dir / f"{dataset_name}.csv"
    df.loc[invalid_mask].to_csv(error_path, index=False)
    logger.warning("Wrote %s invalid rows to %s", invalid_count, error_path)

    max_records = error_cfg.get("max_bad_records", 0)
    max_percent = error_cfg.get("max_bad_percent", 0.0)

    if (max_records == 0 and invalid_count > 0) or (invalid_count > max_records and percent > max_percent):
        raise ValueError(
            f"Error threshold exceeded for {dataset_name}: {invalid_count} invalid rows ({percent:.2f}%)"
        )

    return df.loc[~invalid_mask].copy()


def _write_dataset(
    df: pd.DataFrame,
    base_name: str,
    output_dir: Path,
    write_parquet: bool,
    write_csv: bool,
    parquet_compression: str,
) -> List[Path]:
    files: List[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    if write_parquet:
        parquet_path = output_dir / f"{base_name}.parquet"
        df.to_parquet(parquet_path, index=False, compression=parquet_compression)
        files.append(parquet_path)
    if write_csv:
        csv_path = output_dir / f"{base_name}.csv"
        df.to_csv(csv_path, index=False)
        files.append(csv_path)
    return files


class DatasetWriter:
    """Write datasets with partition/error policies."""

    def __init__(
        self,
        base_dir: Path,
        primary_keys: List[str],
        partition_columns: List[str],
        error_cfg: Dict[str, Any],
        write_parquet: bool,
        write_csv: bool,
        parquet_compression: str,
    ) -> None:
        self.base_dir = base_dir
        self.primary_keys = primary_keys
        self.partition_columns = partition_columns
        self.error_cfg = error_cfg
        self.write_parquet = write_parquet
        self.write_csv = write_csv
        self.parquet_compression = parquet_compression

    def write_dataset(self, dataset_name: str, dataset_df: pd.DataFrame) -> List[Path]:
        partitions = partition_dataframe(dataset_df, self.partition_columns) or [([], dataset_df)]
        written_files: List[Path] = []

        for path_parts, partition_df in partitions:
            target_dir = self.base_dir
            for part in path_parts:
                target_dir = target_dir / part
            cleaned_df = handle_error_rows(partition_df, self.primary_keys, self.error_cfg, dataset_name, target_dir)
            written_files.extend(
                _write_dataset(
                    cleaned_df,
                    dataset_name,
                    target_dir,
                    self.write_parquet,
                    self.write_csv,
                    self.parquet_compression,
                )
            )
            if path_parts:
                suffix = "/".join(path_parts)
                logger.info("Written partition %s for %s", suffix, dataset_name)

        return written_files


class SilverOutputPlanner:
    """Generate Silver artifacts for a given load pattern."""

    def __init__(self, writer: DatasetWriter, primary_keys: List[str], order_column: str | None) -> None:
        self.writer = writer
        self.primary_keys = primary_keys
        self.order_column = order_column

    def render(
        self,
        df: pd.DataFrame,
        pattern: LoadPattern,
        artifact_names: Dict[str, str],
    ) -> Dict[str, List[Path]]:
        outputs: Dict[str, List[Path]] = {}

        if pattern == LoadPattern.FULL:
            name = artifact_names.get("full_snapshot", "full_snapshot")
            outputs[name] = self.writer.write_dataset(name, df)
        elif pattern == LoadPattern.CDC:
            name = artifact_names.get("cdc", "cdc_changes")
            outputs[name] = self.writer.write_dataset(name, df)
        elif pattern == LoadPattern.CURRENT_HISTORY:
            history_name = artifact_names.get("history", "history")
            outputs[history_name] = self.writer.write_dataset(history_name, df)

            current_df = build_current_view(df, self.primary_keys, self.order_column)
            current_name = artifact_names.get("current", "current")
            outputs[current_name] = self.writer.write_dataset(current_name, current_df)
        else:
            raise ValueError(f"Unsupported load pattern {pattern}")

        return outputs


def write_silver_outputs(
    df: pd.DataFrame,
    output_dir: Path,
    pattern: LoadPattern,
    primary_keys: List[str],
    order_column: str | None,
    write_parquet: bool,
    write_csv: bool,
    parquet_compression: str,
    artifact_names: Dict[str, str],
    partition_columns: List[str],
    error_cfg: Dict[str, Any],
) -> Dict[str, List[Path]]:
    writer = DatasetWriter(
        base_dir=output_dir,
        primary_keys=primary_keys,
        partition_columns=partition_columns,
        error_cfg=error_cfg,
        write_parquet=write_parquet,
        write_csv=write_csv,
        parquet_compression=parquet_compression,
    )
    planner = SilverOutputPlanner(writer, primary_keys, order_column)
    return planner.render(df, pattern, artifact_names)


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
class PromotionContext:
    cfg: Optional[Dict[str, Any]]
    run_date: dt.date
    bronze_path: Path
    silver_partition: Path
    load_pattern: LoadPattern
    require_checksum: bool
    primary_keys: List[str]
    order_column: Optional[str]
    write_parquet: bool
    write_csv: bool
    parquet_compression: str
    artifact_names: Dict[str, str]
    partition_columns: List[str]
    schema_cfg: Dict[str, Any]
    normalization_cfg: Dict[str, Any]
    error_cfg: Dict[str, Any]
    domain: Any
    entity: Any
    version: int
    load_partition_name: str
    include_pattern_folder: bool


class SilverPromotionService:
    """High-level orchestrator for Silver CLI operations."""

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
        self.parser = parser
        self.args = args
        self.cfg_list = load_configs(args.config) if args.config else None
        self._silver_identity: Optional[Tuple[Any, Any, int, str, bool]] = None

    def execute(self) -> int:
        if self.args.validate_only:
            self._handle_validate_only()
            return 0

        cfg = self._select_config()
        run_date = self._resolve_run_date()
        bronze_path = self._resolve_bronze_path(cfg, run_date)
        if not bronze_path.exists() or not bronze_path.is_dir():
            raise FileNotFoundError(f"Bronze path '{bronze_path}' does not exist or is not a directory")

        load_pattern = self._resolve_load_pattern(cfg, bronze_path)
        silver_partition = self._resolve_silver_partition(cfg, bronze_path, load_pattern, run_date)
        logger.info("Bronze partition: %s", bronze_path)
        logger.info("Silver partition: %s", silver_partition)
        logger.info("Load pattern: %s", load_pattern.value)
        context = self._build_context(cfg, bronze_path, silver_partition, load_pattern, run_date)
        self._maybe_verify_checksum(context)

        if self.args.dry_run:
            logger.info("Dry run complete; no files written")
            return 0

        df = load_bronze_records(bronze_path)
        normalized_df = apply_schema_settings(df, context.schema_cfg)
        normalized_df = normalize_dataframe(normalized_df, context.normalization_cfg)
        logger.info("Loaded %s records from Bronze path %s", len(normalized_df), bronze_path)

        outputs = write_silver_outputs(
            normalized_df,
            silver_partition,
            context.load_pattern,
            context.primary_keys,
            context.order_column,
            context.write_parquet,
            context.write_csv,
            context.parquet_compression,
            context.artifact_names,
            context.partition_columns,
            context.error_cfg,
        )

        self._write_metadata(normalized_df, outputs, context)

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
        schema_cfg = silver_cfg.get("schema", {"rename_map": {}, "column_order": None})
        normalization_cfg = silver_cfg.get("normalization", {"trim_strings": False, "empty_strings_as_null": False})
        error_cfg = silver_cfg.get("error_handling", {"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0})

        cli_primary = parse_primary_keys(self.args.primary_key) if self.args.primary_key is not None else None
        primary_keys = cli_primary if cli_primary is not None else silver_cfg.get("primary_keys", [])

        cli_order_column = self.args.order_column if self.args.order_column is not None else None
        order_column = cli_order_column if cli_order_column is not None else silver_cfg.get("order_column")

        rename_map = schema_cfg.get("rename_map") or {}
        primary_keys = [rename_map.get(pk, pk) for pk in primary_keys]
        if order_column:
            order_column = rename_map.get(order_column, order_column)

        partition_columns = silver_cfg.get("partitioning", {}).get("columns", [])
        partition_columns = [rename_map.get(col, col) for col in partition_columns]

        write_parquet = (
            self.args.write_parquet if self.args.write_parquet is not None else silver_cfg.get("write_parquet", True)
        )
        write_csv = self.args.write_csv if self.args.write_csv is not None else silver_cfg.get("write_csv", False)
        if not write_parquet and not write_csv:
            self.parser.error("At least one output format (Parquet or CSV) must be enabled")

        parquet_compression = (
            self.args.parquet_compression
            if self.args.parquet_compression
            else silver_cfg.get("parquet_compression", "snappy")
        )

        artifact_names = {
            "full_snapshot": self.args.full_output_name or silver_cfg.get("full_output_name", "full_snapshot"),
            "cdc": self.args.cdc_output_name or silver_cfg.get("cdc_output_name", "cdc_changes"),
            "current": self.args.current_output_name or silver_cfg.get("current_output_name", "current"),
            "history": self.args.history_output_name or silver_cfg.get("history_output_name", "history"),
        }

        require_checksum = (
            self.args.require_checksum
            if self.args.require_checksum is not None
            else silver_cfg.get("require_checksum", False)
        )

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
            require_checksum=require_checksum,
            primary_keys=primary_keys,
            order_column=order_column,
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
            artifact_names=artifact_names,
            partition_columns=partition_columns,
            schema_cfg=schema_cfg,
            normalization_cfg=normalization_cfg,
            error_cfg=error_cfg,
            domain=domain,
            entity=entity,
            version=version,
            load_partition_name=load_partition_name,
            include_pattern_folder=include_pattern_folder,
        )

    def _maybe_verify_checksum(self, context: PromotionContext) -> None:
        if not context.require_checksum:
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
        df: pd.DataFrame,
        outputs: Dict[str, List[Path]],
        context: PromotionContext,
    ) -> None:
        metadata_path = write_batch_metadata(
            context.silver_partition,
            record_count=len(df),
            chunk_count=len(outputs),
            extra_metadata={
                "load_pattern": context.load_pattern.value,
                "bronze_path": str(context.bronze_path),
                "primary_keys": context.primary_keys,
                "order_column": context.order_column,
                "domain": context.domain,
                "entity": context.entity,
                "version": context.version,
                "load_partition_name": context.load_partition_name,
                "include_pattern_folder": context.include_pattern_folder,
                "partition_columns": context.partition_columns,
                "write_parquet": context.write_parquet,
                "write_csv": context.write_csv,
                "parquet_compression": context.parquet_compression,
                "normalization": context.normalization_cfg,
                "schema": context.schema_cfg,
                "error_handling": context.error_cfg,
                "artifacts": {label: [p.name for p in paths] for label, paths in outputs.items()},
                "require_checksum": context.require_checksum,
            },
        )
        logger.debug("Wrote Silver metadata to %s", metadata_path)


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
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO, format_type=args.log_format)

    service = SilverPromotionService(parser, args)
    return service.execute()


if __name__ == "__main__":
    raise SystemExit(main())
