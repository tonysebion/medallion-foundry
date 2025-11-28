"""SilverProcessor implements the intent-driven pattern engine for Silver."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.config.dataset import (
    DatasetConfig,
    DeleteMode,
    EntityKind,
    HistoryMode,
    InputMode,
    SchemaMode,
)
from core.silver.artifacts import DatasetWriter

logger = logging.getLogger(__name__)


@dataclass
class SilverRunMetrics:
    rows_read: int = 0
    rows_written: int = 0
    changed_keys: int = 0
    derived_events: int = 0
    bronze_partitions: int = 1


@dataclass
class SilverProcessorResult:
    outputs: Dict[str, List[Path]] = field(default_factory=dict)
    schema_snapshot: List[Dict[str, str]] = field(default_factory=list)
    metrics: SilverRunMetrics = field(default_factory=SilverRunMetrics)


class SilverProcessor:
    """Pattern-dispatching Silver engine that reads Bronze and writes curated targets."""

    def __init__(
        self,
        dataset: DatasetConfig,
        bronze_path: Path,
        silver_partition: Path,
        run_date: date,
        *,
        write_parquet: bool = True,
        write_csv: bool = False,
        parquet_compression: str = "snappy",
        chunk_tag: str | None = None,
    ) -> None:
        self.dataset = dataset
        self.bronze_path = bronze_path
        self.silver_partition = silver_partition
        self.run_date = run_date
        self.write_parquet = write_parquet
        self.write_csv = write_csv
        self.parquet_compression = parquet_compression
        self.chunk_tag = chunk_tag
        self.load_batch_id = f"{dataset.dataset_id}-{run_date.isoformat()}"

    def run(self) -> SilverProcessorResult:
        metrics = SilverRunMetrics()
        if not self.dataset.silver.enabled:
            logger.info(
                "Silver disabled for %s; skipping promotion", self.dataset.dataset_id
            )
            return SilverProcessorResult(metrics=metrics)

        df = self._load_bronze_dataframe()
        metrics.rows_read = len(df)
        if df.empty:
            logger.warning("Bronze partition %s has no data", self.bronze_path)
            return SilverProcessorResult(metrics=metrics)

        prepared = self._prepare_dataframe(df)
        metrics.changed_keys = (
            prepared[self.dataset.silver.natural_keys].drop_duplicates().shape[0]
        )
        frames = self._dispatch_patterns(prepared, metrics)
        if not frames:
            logger.warning(
                "No Silver datasets produced for %s", self.dataset.dataset_id
            )
            return SilverProcessorResult(metrics=metrics)

        writer = DatasetWriter(
            base_dir=self.silver_partition,
            primary_keys=self.dataset.silver.natural_keys,
            partition_columns=self._resolve_partition_columns(frames),
            error_cfg={"enabled": False, "max_bad_records": 0, "max_bad_percent": 0.0},
            write_parquet=self.write_parquet,
            write_csv=self.write_csv,
            parquet_compression=self.parquet_compression,
        )

        outputs: Dict[str, List[Path]] = {}
        schema_snapshot: List[Dict[str, str]] = []

        for name, frame in frames.items():
            if frame.empty:
                continue
            enriched = self._append_metadata(frame.copy())
            if not schema_snapshot:
                schema_snapshot = [
                    {"name": col, "dtype": str(dtype)}
                    for col, dtype in enriched.dtypes.items()
                ]
            if self.chunk_tag:
                written = writer.write_dataset_chunk(name, enriched, self.chunk_tag)
            else:
                written = writer.write_dataset(name, enriched)
            outputs[name] = written
            metrics.rows_written += len(enriched)

        return SilverProcessorResult(
            outputs=outputs, schema_snapshot=schema_snapshot, metrics=metrics
        )

    # ------------------------------------------------------------------ helpers

    def _load_bronze_dataframe(self) -> pd.DataFrame:
        csv_files = sorted(self.bronze_path.glob("*.csv"))
        parquet_files = sorted(self.bronze_path.glob("*.parquet"))
        frames: List[pd.DataFrame] = []
        for csv_path in csv_files:
            logger.debug("Reading Bronze CSV %s", csv_path.name)
            frames.append(pd.read_csv(csv_path))
        for parquet_path in parquet_files:
            logger.debug("Reading Bronze Parquet %s", parquet_path.name)
            frames.append(pd.read_parquet(parquet_path))
        if not frames:
            raise FileNotFoundError(f"No chunk files found in {self.bronze_path}")
        return pd.concat(frames, ignore_index=True)

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        expected = set(self.dataset.silver.natural_keys)
        if (
            self.dataset.silver.entity_kind.is_event_like
            and self.dataset.silver.event_ts_column
        ):
            expected.add(self.dataset.silver.event_ts_column)
        if (
            self.dataset.silver.entity_kind.is_state_like
            and self.dataset.silver.change_ts_column
        ):
            expected.add(self.dataset.silver.change_ts_column)
        expected.update(self.dataset.silver.attributes)

        missing = [col for col in expected if col not in df.columns]
        if missing:
            raise ValueError(
                f"Bronze data missing required columns for {self.dataset.dataset_id}: {missing}"
            )

        allowed = set(expected)
        allowed.update({"is_deleted", "deleted_flag"})
        allowed.update(col for col in df.columns if col.startswith("_"))
        allowed.update(COMMON_METADATA_COLUMNS)
        extras = [col for col in df.columns if col not in allowed]
        if extras and self.dataset.silver.schema_mode == SchemaMode.STRICT:
            raise ValueError(
                f"Bronze data contains unexpected columns: {extras}. "
                "Update silver.attributes or switch to schema_mode=allow_new_columns."
            )

        selected_cols = [col for col in df.columns if col in allowed]
        working = df[selected_cols].copy()

        if self.dataset.silver.event_ts_column:
            working[self.dataset.silver.event_ts_column] = pd.to_datetime(
                working[self.dataset.silver.event_ts_column], errors="coerce"
            )
        if self.dataset.silver.change_ts_column:
            working[self.dataset.silver.change_ts_column] = pd.to_datetime(
                working[self.dataset.silver.change_ts_column], errors="coerce"
            )

        drop_subset = list(self.dataset.silver.natural_keys)
        if (
            self.dataset.silver.entity_kind.is_event_like
            and self.dataset.silver.event_ts_column
        ):
            drop_subset.append(self.dataset.silver.event_ts_column)
        elif self.dataset.silver.change_ts_column:
            drop_subset.append(self.dataset.silver.change_ts_column)
        working = working.sort_values(drop_subset).drop_duplicates(
            subset=drop_subset, keep="last"
        )
        return working.reset_index(drop=True)

    def _dispatch_patterns(
        self, df: pd.DataFrame, metrics: SilverRunMetrics
    ) -> Dict[str, pd.DataFrame]:
        kind = self.dataset.silver.entity_kind
        if kind == EntityKind.EVENT:
            return {"events": self._process_events(df)}
        if kind == EntityKind.STATE:
            return self._process_state(df)
        if kind == EntityKind.DERIVED_STATE:
            return self._process_state(df, derived=True)
        if kind == EntityKind.DERIVED_EVENT:
            derived = self._process_derived_events(df)
            metrics.derived_events = len(derived)
            return {"derived_events": derived}
        raise ValueError(f"Unsupported entity_kind '{kind.value}'")

    def _process_events(self, df: pd.DataFrame) -> pd.DataFrame:
        ts_col = self.dataset.silver.event_ts_column
        if not ts_col:
            raise ValueError("event_ts_column is required for event datasets")
        sorted_df = df.sort_values(ts_col)
        if self.dataset.silver.input_mode == InputMode.REPLACE_DAILY:
            date_col = f"{ts_col}_date"
            sorted_df[date_col] = sorted_df[ts_col].dt.date.astype(str)
            subset = self.dataset.silver.natural_keys + [date_col]
            sorted_df = sorted_df.drop_duplicates(subset=subset, keep="last")
        return sorted_df.reset_index(drop=True)

    def _process_state(
        self, df: pd.DataFrame, derived: bool = False
    ) -> Dict[str, pd.DataFrame]:
        history_mode = self.dataset.silver.history_mode or HistoryMode.SCD2
        ts_col = (
            self.dataset.silver.change_ts_column or self.dataset.silver.event_ts_column
        )
        if not ts_col:
            raise ValueError("change_ts_column is required for state datasets")
        ordered = df.sort_values(self.dataset.silver.natural_keys + [ts_col]).copy()
        if history_mode == HistoryMode.SCD1 or history_mode == HistoryMode.LATEST_ONLY:
            current = ordered.drop_duplicates(
                subset=self.dataset.silver.natural_keys, keep="last"
            )
            return {"state_current": current.reset_index(drop=True)}

        history = ordered.copy()
        history["effective_from"] = history[ts_col]
        history["effective_to"] = history.groupby(self.dataset.silver.natural_keys)[
            "effective_from"
        ].shift(-1)
        history["is_current"] = history["effective_to"].isna().astype(int)
        current = history[history["is_current"] == 1].copy()
        return {
            "state_history": history.reset_index(drop=True),
            "state_current": current.reset_index(drop=True),
        }

    def _process_derived_events(self, df: pd.DataFrame) -> pd.DataFrame:
        ts_col = (
            self.dataset.silver.change_ts_column or self.dataset.silver.event_ts_column
        )
        if not ts_col:
            raise ValueError(
                "change_ts_column (or event_ts_column) required for derived_event datasets"
            )
        attrs = self.dataset.silver.attributes or []
        rows: List[Dict[str, Any]] = []
        grouped = df.sort_values(self.dataset.silver.natural_keys + [ts_col]).groupby(
            self.dataset.silver.natural_keys
        )
        for _, group in grouped:
            prev = None
            for _, row in group.iterrows():
                change_type = "upsert"
                changed_cols = list(attrs)
                if prev is not None:
                    changed_cols = [
                        col for col in attrs if row.get(col) != prev.get(col)
                    ]
                    if (
                        not changed_cols
                        and self.dataset.silver.delete_mode == DeleteMode.IGNORE
                    ):
                        prev = row
                        continue
                    change_type = "update" if changed_cols else "noop"
                if (
                    self.dataset.silver.delete_mode == DeleteMode.TOMBSTONE_EVENT
                    and row.get("is_deleted")
                ):
                    change_type = "delete"
                event = {key: row[key] for key in self.dataset.silver.natural_keys}
                for attr in attrs:
                    event[attr] = row.get(attr)
                event_ts_name = self.dataset.silver.event_ts_column or "event_ts"
                event[event_ts_name] = row[ts_col]
                event["change_type"] = change_type
                event["changed_columns"] = ",".join(changed_cols)
                rows.append(event)
                prev = row
        if not rows:
            return pd.DataFrame(columns=self.dataset.silver.natural_keys + attrs)
        return pd.DataFrame(rows)

    def _append_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df["load_batch_id"] = self.load_batch_id
        df["record_source"] = self.dataset.system
        df["pipeline_run_at"] = datetime.utcnow()
        df["environment"] = self.dataset.environment or "unknown"
        df["domain"] = self.dataset.domain or self.dataset.system
        df["entity"] = self.dataset.entity
        df["bronze_owner"] = self.dataset.bronze.owner_team or "platform-team"
        df["silver_owner"] = self.dataset.silver.semantic_owner or "semantic-team"
        return df

    def _resolve_partition_columns(self, frames: Dict[str, pd.DataFrame]) -> List[str]:
        partition_by = self.dataset.silver.partition_by
        if partition_by:
            for frame in frames.values():
                for column in partition_by:
                    if column in frame.columns:
                        continue
                    if column.endswith("_dt"):
                        source = (
                            self.dataset.silver.event_ts_column
                            if self.dataset.silver.event_ts_column in frame.columns
                            else self.dataset.silver.change_ts_column
                        )
                        if source and source in frame.columns:
                            frame[column] = pd.to_datetime(
                                frame[source], errors="coerce"
                            ).dt.date.astype(str)
                        else:
                            raise ValueError(
                                f"Unable to derive partition column '{column}'"
                            )
                    else:
                        raise ValueError(
                            f"Partition column '{column}' missing from Silver output"
                        )
            return partition_by

        # Defaults when not provided.
        if self.dataset.silver.entity_kind.is_event_like:
            column = (self.dataset.silver.event_ts_column or "event_ts") + "_dt"
            for frame in frames.values():
                source = (
                    self.dataset.silver.event_ts_column
                    or self.dataset.silver.change_ts_column
                )
                if source and source in frame.columns:
                    frame[column] = pd.to_datetime(
                        frame[source], errors="coerce"
                    ).dt.date.astype(str)
            return [column]

        column = "effective_from_dt"
        for frame in frames.values():
            if "effective_from" in frame.columns:
                frame[column] = pd.to_datetime(
                    frame["effective_from"], errors="coerce"
                ).dt.date.astype(str)
            elif (
                self.dataset.silver.change_ts_column
                and self.dataset.silver.change_ts_column in frame.columns
            ):
                frame[column] = pd.to_datetime(
                    frame[self.dataset.silver.change_ts_column], errors="coerce"
                ).dt.date.astype(str)
        return [column]


def build_intent_silver_partition(
    dataset: DatasetConfig, run_date: date, base_dir: Optional[Path] = None
) -> Path:
    """Derive the Silver partition path that pairs with Bronze for intent configs."""
    if base_dir:
        base_path = base_dir
    elif dataset.silver.output_dir:
        base_path = Path(dataset.silver.output_dir)
    else:
        base_path = dataset.silver_base_path
    domain = dataset.domain or dataset.system
    partition = (
        base_path
        / f"domain={domain}"
        / f"entity={dataset.entity}"
        / f"v{dataset.silver.version}"
    )
    if dataset.silver.include_pattern_folder:
        partition = partition / f"pattern={dataset.silver.entity_kind.value}"
    partition = (
        partition / f"{dataset.silver.load_partition_name}={run_date.isoformat()}"
    )
    return partition


COMMON_METADATA_COLUMNS = {"run_date"}
