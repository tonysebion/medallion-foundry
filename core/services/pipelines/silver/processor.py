"""SilverProcessor implements the intent-driven pattern engine for Silver."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, cast, TYPE_CHECKING

import pandas as pd

from core.infrastructure.config.dataset import (
    DatasetConfig,
    DeleteMode,
    EntityKind,
    HistoryMode,
    InputMode,
    SchemaMode,
)
from core.services.pipelines.silver.io import DatasetWriter
from core.runtime.file_io import DataFrameLoader
from core.infrastructure.storage.uri import StorageURI
from core.infrastructure.storage.filesystem import create_filesystem
from core.io.storage.checksum import (
    ChecksumVerificationResult,
    verify_checksum_manifest_with_result,
)
from core.io.storage.quarantine import quarantine_corrupted_files, QuarantineConfig

if TYPE_CHECKING:
    from core.runtime.config import EnvironmentConfig

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
        env_config: Optional["EnvironmentConfig"] = None,
        write_parquet: bool = True,
        write_csv: bool = False,
        parquet_compression: str = "snappy",
        chunk_tag: str | None = None,
        verify_checksum: bool | None = None,
        skip_verification_if_fresh: bool = True,
        freshness_threshold_seconds: int = 300,
        quarantine_on_failure: bool = True,
    ) -> None:
        self.dataset = dataset
        self.bronze_path = bronze_path
        self.silver_partition = silver_partition
        self.run_date = run_date
        self.env_config = env_config
        self.write_parquet = write_parquet
        self.write_csv = write_csv
        self.parquet_compression = parquet_compression
        self.chunk_tag = chunk_tag
        self.load_batch_id = f"{dataset.dataset_id}-{run_date.isoformat()}"

        # Checksum verification settings
        self._verify_checksum = verify_checksum
        self._skip_verification_if_fresh = skip_verification_if_fresh
        self._freshness_threshold = freshness_threshold_seconds
        self._quarantine_on_failure = quarantine_on_failure

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

    def _should_verify_checksum(self) -> bool:
        """Determine if checksum verification should run.

        Returns True if:
        - verify_checksum is explicitly True, OR
        - verify_checksum is None and dataset.silver.require_checksum is True

        Returns False if:
        - verify_checksum is explicitly False
        - Fast path: manifest is fresh (less than freshness_threshold seconds old)
        """
        # Explicit override takes precedence
        if self._verify_checksum is not None:
            verify = self._verify_checksum
        else:
            # Fall back to dataset config
            verify = getattr(self.dataset.silver, "require_checksum", False)

        if not verify:
            return False

        # Fast path: skip verification if manifest is very fresh
        if self._skip_verification_if_fresh:
            manifest_path = self.bronze_path / "_checksums.json"
            if manifest_path.exists():
                try:
                    mtime = manifest_path.stat().st_mtime
                    age_seconds = time.time() - mtime
                    if age_seconds < self._freshness_threshold:
                        logger.debug(
                            "Skipping checksum verification - manifest age %.1fs < threshold %ds",
                            age_seconds,
                            self._freshness_threshold,
                        )
                        return False
                except OSError:
                    pass  # Proceed with verification if we can't check freshness

        return True

    def _verify_bronze_checksums(self) -> ChecksumVerificationResult:
        """Verify Bronze chunk checksums and quarantine corrupted files.

        Returns:
            ChecksumVerificationResult with verification details

        Raises:
            ValueError: If verification fails and quarantine was performed
        """
        result = verify_checksum_manifest_with_result(self.bronze_path)

        if result.valid:
            logger.info(
                "Bronze checksum verification passed: %d files verified in %.1fms",
                len(result.verified_files),
                result.verification_time_ms,
            )
            return result

        # Verification failed - handle corrupted files
        corrupted = result.mismatched_files + result.missing_files
        logger.error(
            "Bronze checksum verification failed for %s: %d corrupted/missing files",
            self.bronze_path,
            len(corrupted),
        )

        if self._quarantine_on_failure and result.mismatched_files:
            quarantine_result = quarantine_corrupted_files(
                self.bronze_path,
                result.mismatched_files,
                reason="checksum_verification_failed",
            )
            logger.warning(
                "Quarantined %d corrupted files to %s",
                quarantine_result.count,
                quarantine_result.quarantine_path,
            )

        raise ValueError(
            f"Bronze checksum verification failed for {self.bronze_path}: "
            f"{len(result.mismatched_files)} mismatched, {len(result.missing_files)} missing. "
            f"Corrupted files have been quarantined."
            if self._quarantine_on_failure
            else f"Bronze checksum verification failed for {self.bronze_path}: "
            f"{len(result.mismatched_files)} mismatched, {len(result.missing_files)} missing."
        )

    def _load_bronze_dataframe(self) -> pd.DataFrame:
        """Load Bronze data from local or S3 storage with optional checksum verification."""
        # Verify checksums if configured
        if self._should_verify_checksum():
            self._verify_bronze_checksums()

        # Determine storage backend
        input_storage = self.dataset.silver.input_storage

        if input_storage == "local":
            return self._load_bronze_from_local()
        elif input_storage == "s3":
            return self._load_bronze_from_s3()
        else:
            raise ValueError(f"Unsupported Silver input storage: {input_storage}")

    def _load_bronze_from_local(self) -> pd.DataFrame:
        """Load Bronze data from local filesystem."""
        return DataFrameLoader.from_directory(self.bronze_path, recursive=False)

    def _load_bronze_from_s3(self) -> pd.DataFrame:
        """Load Bronze data from S3 using streaming."""
        if not self.env_config or not self.env_config.s3:
            raise ValueError(
                "S3 storage requires environment config with S3 settings. "
                "Please provide an environment config when initializing SilverProcessor."
            )

        # Build S3 URI for Bronze path
        bucket_ref = self.dataset.bronze.output_bucket or "bronze_data"
        bucket = self.env_config.s3.get_bucket(bucket_ref)

        # Convert bronze_path to S3 key
        bronze_key = str(self.bronze_path).replace("\\", "/")
        if bronze_key.startswith("./"):
            bronze_key = bronze_key[2:]

        # Prepend prefix if configured
        if self.dataset.bronze.output_prefix:
            bronze_key = f"{self.dataset.bronze.output_prefix.rstrip('/')}/{bronze_key}"

        s3_uri = StorageURI(
            backend="s3",
            bucket=bucket,
            key=bronze_key,
            original=f"s3://{bucket}/{bronze_key}",
        )

        # Create filesystem and load using shared utility
        fs = create_filesystem(s3_uri, self.env_config)
        fsspec_path = s3_uri.to_fsspec_path(self.env_config)

        return DataFrameLoader.from_s3(fsspec_path, fs)

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
            prev: Optional[pd.Series] = None
            for _, row_raw in group.iterrows():
                row = cast(pd.Series, row_raw)
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
        df["pipeline_run_at"] = datetime.combine(self.run_date, datetime.min.time())
        df["environment"] = self.dataset.environment or "unknown"
        df["domain"] = self.dataset.domain or self.dataset.system
        df["entity"] = self.dataset.entity
        df["bronze_owner"] = self.dataset.bronze.owner_team or "platform-team"
        df["silver_owner"] = self.dataset.silver.semantic_owner or "semantic-team"
        return df

    def _resolve_partition_columns(self, frames: Dict[str, pd.DataFrame]) -> List[str]:
        # V1: Unified temporal configuration - use record_time_partition if available
        if self.dataset.silver.record_time_partition:
            partition_key = self.dataset.silver.record_time_partition
            source_column = self.dataset.silver.record_time_column

            if not source_column:
                raise ValueError(
                    "record_time_partition specified but record_time_column is missing"
                )

            for frame in frames.values():
                if source_column not in frame.columns:
                    raise ValueError(
                        f"record_time_column '{source_column}' not found in Silver output"
                    )
                # Create partition column from record time (event_ts or change_ts)
                frame[partition_key] = pd.to_datetime(
                    frame[source_column], errors="coerce"
                ).dt.date.astype(str)

            return [partition_key]

        # Legacy behavior: use partition_by if provided
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

    # Get path structure keys from config
    silver_keys = dataset.path_structure.silver if dataset.path_structure else {}
    domain_key = silver_keys.get("domain_key", "domain")
    entity_key = silver_keys.get("entity_key", "entity")
    version_key = silver_keys.get("version_key", "v")
    pattern_key = silver_keys.get("pattern_key", "pattern")
    load_date_key = silver_keys.get("load_date_key", "load_date")

    partition = (
        base_path
        / f"{domain_key}={domain}"
        / f"{entity_key}={dataset.entity}"
        / f"{version_key}{dataset.silver.version}"
    )
    if dataset.silver.include_pattern_folder:
        # Use pattern_folder from bronze.options if available, otherwise fall back to entity_kind enum value
        pattern_value = dataset.bronze.options.get("pattern_folder") or dataset.silver.entity_kind.value
        partition = partition / f"{pattern_key}={pattern_value}"
    partition = (
        partition / f"{load_date_key}={run_date.isoformat()}"
    )
    return partition


COMMON_METADATA_COLUMNS = {"run_date"}
