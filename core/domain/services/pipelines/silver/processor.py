"""SilverProcessor implements the intent-driven pattern engine for Silver."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

import pandas as pd

from core.infrastructure.config import (
    DatasetConfig,
    EntityKind,
)
from core.infrastructure.io.storage import (
    ChecksumVerificationResult,
    QuarantineResult,
    StorageURI,
    create_filesystem,
)
from core.infrastructure.runtime.file_io import DataFrameLoader

from core.domain.services.pipelines.silver.io import DatasetWriter
from core.domain.services.pipelines.silver.verification import (
    ChecksumVerifier,
    VerificationConfig,
)
from core.domain.services.pipelines.silver.preparation import DataFramePreparer
from core.domain.services.pipelines.silver.handlers import (
    BasePatternHandler,
    create_handler,
)
from core.domain.services.pipelines.silver.partition_resolver import PartitionColumnResolver

if TYPE_CHECKING:
    from core.infrastructure.config import EnvironmentConfig

logger = logging.getLogger(__name__)


@dataclass
class SilverRunMetrics:
    """Metrics collected during Silver processing."""

    rows_read: int = 0
    rows_written: int = 0
    changed_keys: int = 0
    derived_events: int = 0
    bronze_partitions: int = 1


@dataclass
class SilverProcessorResult:
    """Result of Silver processing run."""

    outputs: Dict[str, List[Path]] = field(default_factory=dict)
    schema_snapshot: List[Dict[str, str]] = field(default_factory=list)
    metrics: SilverRunMetrics = field(default_factory=SilverRunMetrics)
    checksum_result: Optional[ChecksumVerificationResult] = None
    quarantine_result: Optional[QuarantineResult] = None


class SilverProcessor:
    """Pattern-dispatching Silver engine that reads Bronze and writes curated targets.

    This processor uses composition to delegate to specialized components:
    - ChecksumVerifier: Verifies Bronze data integrity
    - DataFramePreparer: Validates and prepares DataFrames
    - Pattern handlers: Process data according to entity type (Event, State, DerivedEvent)
    """

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

        # Initialize components
        self._verifier = ChecksumVerifier(
            bronze_path=bronze_path,
            dataset=dataset,
            config=VerificationConfig(
                verify_checksum=verify_checksum,
                skip_verification_if_fresh=skip_verification_if_fresh,
                freshness_threshold_seconds=freshness_threshold_seconds,
                quarantine_on_failure=quarantine_on_failure,
            ),
        )
        self._preparer = DataFramePreparer(dataset)
        self._handlers = self._create_handlers()

    def _create_handlers(self) -> Dict[EntityKind, BasePatternHandler]:
        """Create pattern handlers for each entity kind using the registry.

        Uses the handler registry pattern (matching extractors and storage backends)
        to dynamically create handlers based on registered decorators.
        """
        handlers: Dict[EntityKind, BasePatternHandler] = {}
        for kind in EntityKind:
            try:
                handlers[kind] = create_handler(kind, self.dataset)
            except ValueError:
                # Handler not registered for this kind (this is expected for some kinds)
                logger.debug("No handler registered for entity kind: %s", kind.value)
        return handlers

    def run(self) -> SilverProcessorResult:
        """Execute the Silver processing pipeline."""
        metrics = SilverRunMetrics()

        if not self.dataset.silver.enabled:
            logger.info(
                "Silver disabled for %s; skipping promotion", self.dataset.dataset_id
            )
            return SilverProcessorResult(metrics=metrics)

        # Step 1: Verify checksums if required
        checksum_result, quarantine_result = self._run_verification()

        # Step 2: Load Bronze data
        df = self._load_bronze_dataframe()
        metrics.rows_read = len(df)
        if df.empty:
            logger.warning("Bronze partition %s has no data", self.bronze_path)
            return SilverProcessorResult(metrics=metrics)

        # Step 3: Prepare DataFrame
        prepared = self._preparer.prepare(df)
        metrics.changed_keys = (
            prepared[self.dataset.silver.natural_keys].drop_duplicates().shape[0]
        )

        # Step 4: Dispatch to pattern handler
        frames = self._dispatch_patterns(prepared, metrics)
        if not frames:
            logger.warning(
                "No Silver datasets produced for %s", self.dataset.dataset_id
            )
            return SilverProcessorResult(metrics=metrics)

        # Step 5: Write output
        outputs, schema_snapshot = self._write_outputs(frames, metrics)

        return SilverProcessorResult(
            outputs=outputs,
            schema_snapshot=schema_snapshot,
            metrics=metrics,
            checksum_result=checksum_result,
            quarantine_result=quarantine_result,
        )

    def _run_verification(
        self,
    ) -> tuple[Optional[ChecksumVerificationResult], Optional[QuarantineResult]]:
        """Run checksum verification if required."""
        if not self._verifier.should_verify():
            return None, None

        checksum_result, quarantine_result = self._verifier.verify()
        if not checksum_result.valid:
            reason = (
                " Corrupted files have been quarantined."
                if self._verifier.config.quarantine_on_failure
                else ""
            )
            raise ValueError(
                f"Bronze checksum verification failed for {self.bronze_path}: "
                f"{len(checksum_result.mismatched_files)} mismatched, "
                f"{len(checksum_result.missing_files)} missing.{reason}"
            )

        return checksum_result, quarantine_result

    def _load_bronze_dataframe(self) -> pd.DataFrame:
        """Load Bronze data from local or S3 storage."""
        input_storage = self.dataset.silver.input_storage

        if input_storage == "local":
            return DataFrameLoader.from_directory(self.bronze_path, recursive=False)
        elif input_storage == "s3":
            return self._load_bronze_from_s3()
        else:
            raise ValueError(f"Unsupported Silver input storage: {input_storage}")

    def _load_bronze_from_s3(self) -> pd.DataFrame:
        """Load Bronze data from S3 using streaming."""
        if not self.env_config or not self.env_config.s3:
            raise ValueError(
                "S3 storage requires environment config with S3 settings. "
                "Please provide an environment config when initializing SilverProcessor."
            )

        bucket_ref = self.dataset.bronze.output_bucket or "bronze_data"
        bucket = self.env_config.s3.get_bucket(bucket_ref)

        bronze_key = str(self.bronze_path).replace("\\", "/")
        if bronze_key.startswith("./"):
            bronze_key = bronze_key[2:]

        if self.dataset.bronze.output_prefix:
            bronze_key = f"{self.dataset.bronze.output_prefix.rstrip('/')}/{bronze_key}"

        s3_uri = StorageURI(
            backend="s3",
            bucket=bucket,
            key=bronze_key,
            original=f"s3://{bucket}/{bronze_key}",
        )

        fs = create_filesystem(s3_uri, self.env_config)
        fsspec_path = s3_uri.to_fsspec_path(self.env_config)

        return DataFrameLoader.from_s3(fsspec_path, fs)

    def _dispatch_patterns(
        self, df: pd.DataFrame, metrics: SilverRunMetrics
    ) -> Dict[str, pd.DataFrame]:
        """Dispatch to appropriate pattern handler."""
        kind = self.dataset.silver.entity_kind

        if kind not in self._handlers:
            raise ValueError(f"Unsupported entity_kind '{kind.value}'")

        handler = self._handlers[kind]
        result = handler.process(df)

        # Track derived events count
        if kind == EntityKind.DERIVED_EVENT and "derived_events" in result:
            metrics.derived_events = len(result["derived_events"])

        return result

    def _write_outputs(
        self, frames: Dict[str, pd.DataFrame], metrics: SilverRunMetrics
    ) -> tuple[Dict[str, List[Path]], List[Dict[str, str]]]:
        """Write output DataFrames to Silver partition."""
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

        return outputs, schema_snapshot

    def _append_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Append standard metadata columns to DataFrame."""
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
        """Resolve partition columns for output.

        Delegates to PartitionColumnResolver for cleaner separation of concerns.
        """
        resolver = PartitionColumnResolver(self.dataset)
        return resolver.resolve(frames)


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
        pattern_value = (
            dataset.bronze.options.get("pattern_folder")
            or dataset.silver.entity_kind.value
        )
        partition = partition / f"{pattern_key}={pattern_value}"
    partition = partition / f"{load_date_key}={run_date.isoformat()}"
    return partition
