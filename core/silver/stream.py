"""Streaming helpers for Silver promotions without building a giant DataFrame.

Refactored to reuse `DatasetWriter` from artifacts for consistent partition/error
handling and compression settings. This avoids divergence between streaming and
batch promotion paths.
"""

from __future__ import annotations

import logging
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple, DefaultDict

import pandas as pd
from core.run_options import RunOptions
from core.silver.models import SilverModel
from core.silver.artifacts import (
    apply_schema_settings,
    normalize_dataframe,
    partition_dataframe,
    handle_error_rows,
    DatasetWriter,
)

logger = logging.getLogger(__name__)


def _sanitize_partition_value(value: Any) -> str:
    return str(value).replace("/", "_").replace(" ", "_")




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
    run_opts: RunOptions,
    schema_cfg: Dict[str, Any],
    normalization_cfg: Dict[str, Any],
    error_cfg: Dict[str, Any],
    silver_model: SilverModel,
) -> Tuple[Dict[str, List[Path]], int, int, List[Dict[str, str]]]:
    dataset_writer = DatasetWriter(
        base_dir=output_dir,
        primary_keys=run_opts.primary_keys,
        partition_columns=run_opts.partition_columns,
        error_cfg=error_cfg,
        write_parquet=run_opts.write_parquet,
        write_csv=run_opts.write_csv,
        parquet_compression=run_opts.parquet_compression,
    )
    state = CurrentHistoryState(run_opts.primary_keys, run_opts.order_column)
    outputs: DefaultDict[str, List[Path]] = defaultdict(list)
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
        if not schema_snapshot:
            schema_snapshot = [{"name": col, "dtype": str(dtype)} for col, dtype in normalized.dtypes.items()]

        chunk_tag = f"{chunk_index:04d}"

        if silver_model == SilverModel.PERIODIC_SNAPSHOT:
            chunk_count += 1
            outputs[full_name].extend(dataset_writer.write_dataset_chunk(full_name, normalized, chunk_tag))
        elif silver_model == SilverModel.INCREMENTAL_MERGE:
            chunk_count += 1
            outputs[cdc_name].extend(dataset_writer.write_dataset_chunk(cdc_name, normalized, chunk_tag))
        elif silver_model == SilverModel.SCD_TYPE_2:
            chunk_count += 1
            outputs[history_name].extend(dataset_writer.write_dataset_chunk(history_name, normalized, chunk_tag))
            state.update(normalized)
        elif silver_model in {SilverModel.SCD_TYPE_1, SilverModel.FULL_MERGE_DEDUPE}:
            state.update(normalized)
        else:
            raise ValueError(f"Unsupported silver model '{silver_model.value}'")

    if silver_model == SilverModel.SCD_TYPE_1:
        current_df = state.snapshot()
        if not current_df.empty:
            chunk_count += 1
            outputs[current_name].extend(
                dataset_writer.write_dataset_chunk(current_name, current_df, f"current-{chunk_count:04d}")
            )
            record_count += len(current_df)
    elif silver_model == SilverModel.FULL_MERGE_DEDUPE:
        full_df = state.snapshot()
        if not full_df.empty:
            chunk_count += 1
            outputs[full_name].extend(
                dataset_writer.write_dataset_chunk(full_name, full_df, f"full-{chunk_count:04d}")
            )
            record_count += len(full_df)
    elif silver_model == SilverModel.SCD_TYPE_2:
        current_df = state.snapshot()
        if not current_df.empty:
            chunk_count += 1
            outputs[current_name].extend(
                dataset_writer.write_dataset_chunk(current_name, current_df, f"current-{chunk_count:04d}")
            )
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


class StreamingSilverWriter:  # retained for backward import compatibility
    def __init__(self, *args: Any, **kwargs: Any):  # pragma: no cover - deprecated path
        logger.warning(
            "StreamingSilverWriter is deprecated; streaming now reuses DatasetWriter directly"
        )
