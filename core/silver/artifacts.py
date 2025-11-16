from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

from core.patterns import LoadPattern
from core.silver.models import SilverModel

logger = logging.getLogger(__name__)


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


class SilverModelPlanner:
    """Generate Silver artifacts for a given asset model."""

    def __init__(
        self,
        writer: DatasetWriter,
        primary_keys: List[str],
        order_column: str | None,
        artifact_names: Dict[str, str],
        silver_model: SilverModel,
    ) -> None:
        self.writer = writer
        self.primary_keys = primary_keys
        self.order_column = order_column
        self.artifact_names = {
            "full_snapshot": artifact_names.get("full_snapshot", "full_snapshot"),
            "cdc": artifact_names.get("cdc", "cdc_changes"),
            "history": artifact_names.get("history", "history"),
            "current": artifact_names.get("current", "current"),
        }
        self.silver_model = silver_model

    def render(self, df: pd.DataFrame) -> Dict[str, List[Path]]:
        artifact_frames = self.prepare_artifacts(df)
        outputs: Dict[str, List[Path]] = {}

        for label, dataset_df in artifact_frames.items():
            if dataset_df.empty:
                continue
            target_name = self.artifact_names.get(label, label)
            outputs[target_name] = self.writer.write_dataset(target_name, dataset_df)

        return outputs

    def prepare_artifacts(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        return self._build_artifacts(df)

    def _build_artifacts(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if self.silver_model == SilverModel.PERIODIC_SNAPSHOT:
            return {"full_snapshot": df}
        if self.silver_model == SilverModel.INCREMENTAL_MERGE:
            return {"cdc": df}
        if self.silver_model == SilverModel.FULL_MERGE_DEDUPE:
            deduped = self._dedupe_frame(df)
            return {"full_snapshot": deduped}
        if self.silver_model == SilverModel.SCD_TYPE_1:
            deduped = self._dedupe_frame(df)
            return {"current": deduped}
        if self.silver_model == SilverModel.SCD_TYPE_2:
            deduped = self._dedupe_frame(df)
            history = self._build_history_frame(df, deduped)
            return {"history": history, "current": deduped}
        raise ValueError(f"Unsupported silver model '{self.silver_model.value}'")

    def _dedupe_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.primary_keys:
            raise ValueError("Primary keys are required for deduplicated Silver models")
        if not self.order_column:
            raise ValueError("order_column is required for deduplicated Silver models")
        if self.order_column not in df.columns:
            raise ValueError(f"order_column '{self.order_column}' not found in Bronze data")
        return build_current_view(df, self.primary_keys, self.order_column)

    def _build_history_frame(self, df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
        history = df.copy()
        if not self.primary_keys or not self.order_column:
            return history

        current_index = {
            (tuple(row[pk] for pk in self.primary_keys), row[self.order_column])
            for _, row in current_df.iterrows()
        }

        def mark_current(row: pd.Series) -> int:
            key = tuple(row.get(pk) for pk in self.primary_keys)
            key_tuple = (key, row.get(self.order_column))
            return 1 if key_tuple in current_index else 0

        history["is_current"] = history.apply(mark_current, axis=1)
        return history


def write_silver_outputs(
    df: pd.DataFrame,
    primary_keys: List[str],
    order_column: str | None,
    write_parquet: bool,
    write_csv: bool,
    parquet_compression: str,
    artifact_names: Dict[str, str],
    partition_columns: List[str],
    error_cfg: Dict[str, Any],
    silver_model: SilverModel,
    output_dir: Path,
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
    planner = SilverModelPlanner(writer, primary_keys, order_column, artifact_names, silver_model)
    return planner.render(df)
