"""Streaming helpers for Silver promotions without building a giant DataFrame."""

from __future__ import annotations

import logging
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
from core.patterns import LoadPattern
from core.run_options import RunOptions

logger = logging.getLogger(__name__)


def _sanitize_partition_value(value: Any) -> str:
    return str(value).replace("/", "_").replace(" ", "_")


def apply_schema_settings(df: pd.DataFrame, schema_cfg: Dict[str, Any]) -> pd.DataFrame:
    rename_map = schema_cfg.get("rename_map") or {}
    column_order = schema_cfg.get("column_order")

    result = df.copy()
    if rename_map:
        missing = [col for col in rename_map if col not in result.columns]
        if missing:
            logger.debug("schema.rename_map references missing columns: %s", missing)
        result = result.rename(columns=rename_map)

    if column_order:
        ordered = [col for col in column_order if col in result.columns]
        remaining = [col for col in result.columns if col not in ordered]
        result = result[ordered + remaining]

    return result


def normalize_dataframe(df: pd.DataFrame, normalization_cfg: Dict[str, Any]) -> pd.DataFrame:
    trim_strings = normalization_cfg.get("trim_strings", False)
    empty_as_null = normalization_cfg.get("empty_strings_as_null", False)

    result = df.copy()
    if not (trim_strings or empty_as_null):
        return result

    object_cols = result.select_dtypes(include="object").columns
    for col in object_cols:
        if trim_strings:
            result[col] = result[col].apply(lambda val: val.strip() if isinstance(val, str) else val)
        if empty_as_null:
            result[col] = result[col].apply(lambda val: None if isinstance(val, str) and val == "" else val)
    return result


def handle_error_rows(
    df: pd.DataFrame,
    primary_keys: List[str],
    error_cfg: Dict[str, Any],
    dataset_name: str,
    output_dir: Path,
) -> pd.DataFrame:
    if not error_cfg.get("enabled") or not primary_keys:
        return df

    missing_cols = [col for col in primary_keys if col not in df.columns]
    if missing_cols:
        logger.warning("Primary key columns %s not found; skipping error handling for %s", missing_cols, dataset_name)
        return df

    invalid_mask = df[primary_keys].isnull().any(axis=1)
    invalid_count = int(invalid_mask.sum())
    if invalid_count == 0:
        return df

    error_dir = output_dir / "_errors"
    error_dir.mkdir(parents=True, exist_ok=True)
    error_path = error_dir / f"{dataset_name}.csv"
    df.loc[invalid_mask].to_csv(error_path, index=False)
    logger.warning("Wrote %s invalid rows to %s", invalid_count, error_path)

    max_records = error_cfg.get("max_bad_records", 0)
    max_percent = error_cfg.get("max_bad_percent", 0.0)
    total_rows = len(df)
    percent = (invalid_count / total_rows) * 100 if total_rows else 0

    if (max_records == 0 and invalid_count > 0) or (max_records and invalid_count > max_records and percent > max_percent):
        raise ValueError(
            f"Error threshold exceeded for {dataset_name}: {invalid_count} invalid rows ({percent:.2f}%)"
        )

    return df.loc[~invalid_mask].copy()


def partition_dataframe(df: pd.DataFrame, partition_columns: List[str]) -> List[Tuple[List[str], pd.DataFrame]]:
    if not partition_columns:
        return [([], df)]

    partitions = [([], df)]
    for column in partition_columns:
        if column not in df.columns:
            logger.warning("Partition column '%s' not found; skipping", column)
            continue
        new_partitions: List[Tuple[List[str], pd.DataFrame]] = []
        for path_parts, subset in partitions:
            for value, group in subset.groupby(column):
                safe_value = _sanitize_partition_value(value)
                new_partitions.append((path_parts + [f"{column}={safe_value}"], group))
        partitions = new_partitions

    return partitions


def _iter_bronze_frames(bronze_path: Path) -> Iterable[pd.DataFrame]:
    csv_files = sorted(bronze_path.glob("*.csv"))
    parquet_files = sorted(bronze_path.glob("*.parquet"))
    for csv_path in csv_files:
        yield pd.read_csv(csv_path)
    for parquet_path in parquet_files:
        yield pd.read_parquet(parquet_path)


def stream_silver_promotion(
    bronze_path: Path,
    output_dir: Path,
    pattern: LoadPattern,
    run_opts: RunOptions,
    schema_cfg: Dict[str, Any],
    normalization_cfg: Dict[str, Any],
    error_cfg: Dict[str, Any],
) -> Tuple[Dict[str, List[Path]], int, int, List[Dict[str, str]]]:
    writer = StreamingSilverWriter(output_dir, run_opts, error_cfg)
    state = CurrentHistoryState(run_opts.primary_keys, run_opts.order_column)
    outputs: DefaultDict[str, List[Path]] = DefaultDict(list)
    schema_snapshot: List[Dict[str, str]] = []
    record_count = 0
    chunk_count = 0

    artifact_names = run_opts.artifact_names
    full_name = artifact_names.get("full_snapshot", "full_snapshot")
    cdc_name = artifact_names.get("cdc", "cdc_changes")
    history_name = artifact_names.get("history", "history")
    current_name = artifact_names.get("current", "current")

    for chunk_index, chunk in enumerate(_iter_bronze_frames(bronze_path), start=1):
        normalized = apply_schema_settings(chunk, schema_cfg)
        normalized = normalize_dataframe(normalized, normalization_cfg)
        if normalized.empty:
            continue

        record_count += len(normalized)
        chunk_count += 1
        if not schema_snapshot:
            schema_snapshot = [{"name": col, "dtype": str(dtype)} for col, dtype in normalized.dtypes.items()]

        if pattern == LoadPattern.FULL:
            target_name = full_name
        elif pattern == LoadPattern.CDC:
            target_name = cdc_name
        else:
            target_name = history_name

        chunk_tag = f"{chunk_index:04d}"
        outputs[target_name].extend(writer.write_chunk(target_name, normalized, chunk_tag))

        if pattern == LoadPattern.CURRENT_HISTORY and run_opts.primary_keys:
            state.update(normalized)

    if pattern == LoadPattern.CURRENT_HISTORY and not state.snapshot().empty:
        chunk_count += 1
        current_df = state.snapshot()
        outputs[current_name].extend(writer.write_chunk(current_name, current_df, f"current-{chunk_count:04d}"))
        record_count += len(current_df)

    return dict(outputs), chunk_count, record_count, schema_snapshot


def _write_dataset_chunk(
    df: pd.DataFrame,
    base_name: str,
    output_dir: Path,
    write_parquet: bool,
    write_csv: bool,
    parquet_compression: str,
    chunk_tag: str,
) -> List[Path]:
    files: List[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"-{chunk_tag}"
    if write_parquet:
        parquet_path = output_dir / f"{base_name}{suffix}.parquet"
        df.to_parquet(parquet_path, index=False, compression=parquet_compression)
        files.append(parquet_path)
    if write_csv:
        csv_path = output_dir / f"{base_name}{suffix}.csv"
        df.to_csv(csv_path, index=False)
        files.append(csv_path)
    return files


class CurrentHistoryState:
    def __init__(self, primary_keys: List[str], order_column: str | None):
        self.primary_keys = primary_keys
        self.order_column = order_column
        self.state: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

    def update(self, chunk: pd.DataFrame) -> None:
        for _, row in chunk.iterrows():
            key = tuple(row.get(pk) for pk in self.primary_keys)
            if any(pd.isna(val) for val in key):
                continue
            current = self.state.get(key)
            if not self.order_column:
                self.state[key] = row.to_dict()
                continue
            row_value = row.get(self.order_column)
            if current:
                existing_value = current.get(self.order_column)
                if existing_value is None or row_value >= existing_value:
                    self.state[key] = row.to_dict()
            else:
                self.state[key] = row.to_dict()

    def snapshot(self) -> pd.DataFrame:
        if not self.state:
            return pd.DataFrame()
        return pd.DataFrame(list(self.state.values()))


class StreamingSilverWriter:
    def __init__(self, output_dir: Path, run_opts: RunOptions, error_cfg: Dict[str, Any]):
        self.output_dir = output_dir
        self.run_opts = run_opts
        self.error_cfg = error_cfg

    def write_chunk(self, dataset_name: str, df: pd.DataFrame, chunk_tag: str) -> List[Path]:
        partitions = partition_dataframe(df, self.run_opts.partition_columns) or [([], df)]
        written: List[Path] = []
        for path_parts, partition_df in partitions:
            target_dir = self.output_dir
            for part in path_parts:
                target_dir = target_dir / part
            cleaned_df = handle_error_rows(partition_df, self.run_opts.primary_keys, self.error_cfg, dataset_name, target_dir)
            written.extend(
                _write_dataset_chunk(
                    cleaned_df,
                    dataset_name,
                    target_dir,
                    self.run_opts.write_parquet,
                    self.run_opts.write_csv,
                    self.run_opts.parquet_compression,
                    chunk_tag,
                )
            )
        return written
