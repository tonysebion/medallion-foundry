"""Silver layer entity abstraction.

The Silver layer curates Bronze data by applying:
- Deduplication by natural keys
- Type enforcement
- History management (SCD1/SCD2)

Silver layer rules:
- NO business logic (that's Gold layer)
- Only curation operations: dedupe, type, historize
- Schema can be explicit (attributes list) or implicit (all columns)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ibis  # type: ignore[import-untyped]

from pipelines.lib.artifact_writer import write_artifacts
from pipelines.lib.storage_config import (
    InputMode,
    _configure_duckdb_s3,
    _extract_storage_options,
)
from pipelines.lib.env import utc_now_iso
from pipelines.lib.curate import apply_cdc, build_history, dedupe_latest
from pipelines.lib.io import infer_column_types, maybe_dry_run
from pipelines.lib._path_utils import (
    is_object_storage_path,
    is_s3_path,
    parse_s3_uri,
    resolve_target_path,
    storage_path_exists,
)
from pipelines.lib.observability import get_structlog_logger

# Use structlog for structured logging with pipeline context
logger = get_structlog_logger(__name__)

__all__ = [
    "DeleteMode",
    "EntityKind",
    "HistoryMode",
    "InputMode",
    "ModelSpec",
    "MODEL_SPECS",
    "SilverEntity",
    "SilverModel",
    "SILVER_MODEL_PRESETS",
]


class EntityKind(Enum):
    """What kind of entity is this?"""

    STATE = "state"  # Slowly changing dimension (customer, product, account)
    EVENT = "event"  # Immutable event log (orders, clicks, payments)


class HistoryMode(Enum):
    """How to handle historical changes."""

    CURRENT_ONLY = "current_only"  # SCD Type 1 - only keep latest
    FULL_HISTORY = "full_history"  # SCD Type 2 - keep all versions


class SilverModel(Enum):
    """Pre-built Silver transformation patterns.

    These are convenience presets that configure entity_kind + history_mode +
    input_mode together for common use cases.

    Use a model when you want a standard pattern; use explicit settings
    when you need custom behavior.
    """

    PERIODIC_SNAPSHOT = "periodic_snapshot"  # Simple dimension refresh
    FULL_MERGE_DEDUPE = "full_merge_dedupe"  # Dedupe accumulated changes
    INCREMENTAL_MERGE = "incremental_merge"  # CDC with merge
    SCD_TYPE_2 = "scd_type_2"  # Full history tracking
    EVENT_LOG = "event_log"  # Immutable event stream

    # Unified CDC model (recommended) - configure with keep_history and handle_deletes
    CDC = "cdc"  # Unified CDC model with explicit options

    # Legacy CDC presets (deprecated - use model: cdc with options instead)
    CDC_CURRENT = "cdc_current"  # CDC → SCD1, ignore deletes
    CDC_CURRENT_TOMBSTONE = "cdc_current_tombstone"  # CDC → SCD1, soft deletes
    CDC_CURRENT_HARD_DELETE = "cdc_current_hard_delete"  # CDC → SCD1, remove deletes
    CDC_HISTORY = "cdc_history"  # CDC → SCD2, ignore deletes
    CDC_HISTORY_TOMBSTONE = "cdc_history_tombstone"  # CDC → SCD2, soft deletes
    CDC_HISTORY_HARD_DELETE = "cdc_history_hard_delete"  # CDC → SCD2, remove deletes


class DeleteMode(Enum):
    """How to handle delete operations in CDC data.

    - IGNORE: Filter out delete records, only process Inserts/Updates
    - TOMBSTONE: Keep deleted records with _deleted=true flag
    - HARD_DELETE: Remove records from Silver when Delete operation received
    """

    IGNORE = "ignore"  # Filter out delete records
    TOMBSTONE = "tombstone"  # Keep with _deleted=true flag
    HARD_DELETE = "hard_delete"  # Remove from Silver


@dataclass(frozen=True)
class ModelSpec:
    """Specification for a Silver model preset.

    This is the single source of truth for model configuration AND validation rules.
    Each model defines both its settings and its compatibility constraints.
    """

    # Configuration settings (applied to SilverEntity)
    entity_kind: str
    history_mode: str
    input_mode: str
    delete_mode: Optional[str] = None

    # Validation constraints (used by config_loader)
    requires_keys: bool = (
        True  # Does this model require unique_columns and last_updated_column?
    )
    requires_cdc_bronze: bool = False  # Must Bronze use load_pattern: cdc?
    valid_bronze_patterns: Optional[tuple[str, ...]] = (
        None  # Allowed Bronze patterns (None = all)
    )
    warns_on_bronze_patterns: Optional[tuple[str, ...]] = (
        None  # Works but warns (inefficient)
    )

    def to_dict(self) -> dict[str, str]:
        """Return configuration dict for backward compatibility."""
        result = {
            "entity_kind": self.entity_kind,
            "history_mode": self.history_mode,
            "input_mode": self.input_mode,
        }
        if self.delete_mode:
            result["delete_mode"] = self.delete_mode
        return result


# Model specifications - single source of truth for config AND validation
MODEL_SPECS: dict[str, ModelSpec] = {
    # Standard models
    "periodic_snapshot": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="replace_daily",
        requires_keys=False,  # No deduplication needed
        valid_bronze_patterns=("full_snapshot",),  # Only valid with full_snapshot
    ),
    "full_merge_dedupe": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="append_log",
        requires_keys=True,
        valid_bronze_patterns=("incremental", "full_snapshot", "cdc"),
        warns_on_bronze_patterns=(
            "full_snapshot",
            "cdc",
        ),  # cdc works but loses delete info
    ),
    "incremental_merge": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="append_log",
        requires_keys=True,
        valid_bronze_patterns=("incremental", "full_snapshot", "cdc"),
        warns_on_bronze_patterns=("full_snapshot", "cdc"),
    ),
    "scd_type_2": ModelSpec(
        entity_kind="state",
        history_mode="full_history",
        input_mode="append_log",
        requires_keys=True,
        valid_bronze_patterns=("incremental", "full_snapshot", "cdc"),
        warns_on_bronze_patterns=("full_snapshot", "cdc"),
    ),
    "event_log": ModelSpec(
        entity_kind="event",
        history_mode="current_only",
        input_mode="append_log",
        requires_keys=True,
        valid_bronze_patterns=("incremental", "full_snapshot", "cdc"),
        warns_on_bronze_patterns=("full_snapshot", "cdc"),
    ),
    # Unified CDC model (recommended) - options are set dynamically based on
    # keep_history and handle_deletes config values in config_loader
    "cdc": ModelSpec(
        entity_kind="state",
        history_mode="current_only",  # Default, overridden by keep_history
        input_mode="append_log",
        delete_mode="ignore",  # Default, overridden by handle_deletes
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    # Legacy CDC models (deprecated - use model: cdc with options instead)
    "cdc_current": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="append_log",
        delete_mode="ignore",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    "cdc_current_tombstone": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="append_log",
        delete_mode="tombstone",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    "cdc_current_hard_delete": ModelSpec(
        entity_kind="state",
        history_mode="current_only",
        input_mode="append_log",
        delete_mode="hard_delete",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    "cdc_history": ModelSpec(
        entity_kind="state",
        history_mode="full_history",
        input_mode="append_log",
        delete_mode="ignore",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    "cdc_history_tombstone": ModelSpec(
        entity_kind="state",
        history_mode="full_history",
        input_mode="append_log",
        delete_mode="tombstone",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
    "cdc_history_hard_delete": ModelSpec(
        entity_kind="state",
        history_mode="full_history",
        input_mode="append_log",
        delete_mode="hard_delete",
        requires_keys=True,
        requires_cdc_bronze=True,
        valid_bronze_patterns=("cdc",),
    ),
}


# Backward compatibility: dict-based presets derived from MODEL_SPECS
SILVER_MODEL_PRESETS: dict[str, dict[str, str]] = {
    name: spec.to_dict() for name, spec in MODEL_SPECS.items()
}


# Default target path template - can be overridden
# Uses domain=/subject= to distinguish from Bronze's system=/entity=
DEFAULT_SILVER_TARGET = "./silver/domain={domain}/subject={subject}/dt={run_date}/"


@dataclass
class SilverEntity:
    """Declarative Silver layer entity definition.

    Intentionally limited to curation operations only.
    No filtering, no joins, no derived columns - that's Gold layer.

    Example (minimal - used with Pipeline):
        # When used with Pipeline, source_path is auto-wired from Bronze
        entity = SilverEntity(
            unique_columns=["order_id"],
            last_updated_column="updated_at",
        )

    Example (standalone with explicit paths):
        entity = SilverEntity(
            source_path="s3://bronze/system=claims_dw/entity=claims_header/dt={run_date}/*.parquet",
            target_path="s3://silver/claims/header/",
            unique_columns=["ClaimID"],
            last_updated_column="LastUpdated",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

    Example (SCD Type 2 with full history):
        entity = SilverEntity(
            unique_columns=["customer_id"],
            last_updated_column="updated_at",
            history_mode=HistoryMode.FULL_HISTORY,  # Keep all versions
        )
    """

    # Identity (required for most models, optional for periodic_snapshot)
    unique_columns: Optional[List[str]] = (
        None  # Column(s) that uniquely identify each row
    )

    # Temporal (required for most models, optional for periodic_snapshot)
    last_updated_column: Optional[str] = (
        None  # Column showing when each row was last modified
    )

    # Business domain and subject (optional - auto-wired from Bronze or inferred from paths)
    domain: str = ""  # Business domain name (e.g., "sales", "finance", "hr")
    subject: str = ""  # Subject area name (e.g., "orders", "customers")

    # Source and target paths (optional when used with Pipeline)
    source_path: str = ""  # Path to Bronze data (auto-wired from Pipeline)
    target_path: str = ""  # Where to write Silver output (auto-generated if empty)

    # Schema (optional - defaults to all columns)
    attributes: Optional[List[str]] = None  # Columns to include (None = all)
    exclude_columns: Optional[List[str]] = None  # Columns to always exclude
    column_mapping: Optional[Dict[str, str]] = (
        None  # Rename columns: {old_name: new_name}
    )

    # Behavior
    entity_kind: EntityKind = EntityKind.STATE
    history_mode: HistoryMode = HistoryMode.CURRENT_ONLY
    input_mode: Optional[InputMode] = (
        None  # How to interpret Bronze partitions (auto-wired from Bronze)
    )
    delete_mode: DeleteMode = DeleteMode.IGNORE  # How to handle CDC deletes

    # CDC options (auto-wired from Bronze when load_pattern=cdc)
    cdc_options: Optional[Dict[str, str]] = None

    # Partitioning
    partition_by: Optional[List[str]] = None

    # Output options
    output_formats: List[str] = field(default_factory=lambda: ["parquet"])
    parquet_compression: str = "snappy"

    # Validation options
    validate_source: str = "skip"  # "skip", "warn", or "strict"

    # Storage options (for S3-compatible storage like Nutanix Objects)
    # Maps to: signature_version, addressing_style, endpoint_url, key, secret, region
    storage_options: Optional[Dict[str, Any]] = None

    # Internal flag to track if this entity is used standalone or with Pipeline
    _standalone: bool = field(default=True, repr=False)

    # Bronze layer references (set by Pipeline for source_path substitution)
    # These allow {system} and {entity} placeholders in source_path
    _bronze_system: Optional[str] = field(default=None, repr=False)
    _bronze_entity: Optional[str] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate configuration on instantiation.

        Note: source_path and target_path validation is relaxed because
        they can be auto-wired when used with Pipeline class.
        """
        # Don't validate paths here - they may be set by Pipeline later
        # Only validate the required semantic fields
        errors = self._validate(check_paths=False)
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"SilverEntity configuration errors:\n{error_msg}\n\n"
                "Fix the configuration and try again."
            )

    def _validate(self, check_paths: bool = True) -> List[str]:
        """Validate configuration and return list of errors.

        Args:
            check_paths: If True, validate source_path and target_path.
                        Set to False when used with Pipeline (paths auto-wired).
        """
        errors = []

        if check_paths:
            if not self.source_path:
                errors.append(
                    "source_path is required (or use with Pipeline to auto-wire from Bronze)"
                )

            if not self.target_path:
                errors.append(
                    "target_path is required (or use with Pipeline for auto-generation)"
                )

        # For periodic_snapshot model (replace_daily + current_only), keys are optional
        # since there's no deduplication or history tracking - just simple replacement
        is_periodic_snapshot = (
            self.input_mode == InputMode.REPLACE_DAILY
            and self.history_mode == HistoryMode.CURRENT_ONLY
        )

        if not self.unique_columns and not is_periodic_snapshot:
            errors.append(
                "unique_columns is required for deduplication. "
                "Add 'unique_columns: [column1, column2]' to identify unique rows. "
                "Run 'python -m pipelines inspect-source --file your_data.csv' for suggestions."
            )

        if not self.last_updated_column and not is_periodic_snapshot:
            errors.append(
                "last_updated_column is required for ordering changes. "
                "Set 'last_updated_column: updated_at' to a column that shows when rows were modified. "
                "Run 'python -m pipelines inspect-source --file your_data.csv' for suggestions."
            )

        if self.attributes and self.exclude_columns:
            errors.append(
                "Cannot specify both attributes and exclude_columns. "
                "Use one or the other."
            )

        # Warnings (logged but don't fail)
        if self.entity_kind == EntityKind.EVENT:
            if self.history_mode == HistoryMode.FULL_HISTORY:
                logger.warning(
                    "silver_event_full_history_warning",
                    message="EVENT entities are immutable - FULL_HISTORY may not be needed",
                )

        return errors

    def validate(
        self,
        run_date: Optional[str] = None,
        *,
        check_source: bool = True,
    ) -> List[str]:
        """Validate configuration and optionally check source data exists.

        Performs pre-flight checks to ensure the pipeline can run successfully.

        Args:
            run_date: Optional run date for path resolution
            check_source: If True, verify source data exists

        Returns:
            List of validation issues (empty if valid)

        Example:
            >>> issues = entity.validate("2025-01-15")
            >>> if issues:
            ...     for issue in issues:
            ...         print(issue)
            ... else:
            ...     print("Configuration is valid")
        """
        from pipelines.lib.config_loader import validate_silver_entity

        issues: List[str] = []

        # Run configuration validation
        config_issues = validate_silver_entity(self)
        issues.extend(str(issue) for issue in config_issues)

        # Check source data exists if requested
        if check_source and run_date:
            source_issues = self._check_source(run_date)
            issues.extend(source_issues)

        return issues

    def _get_source_format_vars(self, run_date: str) -> Dict[str, str]:
        """Get format variables for source_path substitution.

        Supports placeholders: {run_date}, {system}, {entity}, {domain}, {subject}
        """
        vars = {"run_date": run_date, "domain": self.domain, "subject": self.subject}
        # Add Bronze system/entity if available (for source_path pointing to Bronze)
        if self._bronze_system:
            vars["system"] = self._bronze_system
        if self._bronze_entity:
            vars["entity"] = self._bronze_entity
        return vars

    def _resolve_source_path(self, run_date: str) -> str:
        """Resolve source_path with placeholder substitution."""
        return self.source_path.format(**self._get_source_format_vars(run_date))

    def _discover_input_mode_from_bronze_metadata(
        self, source_path: str
    ) -> Optional[InputMode]:
        """Discover input_mode from Bronze _metadata.json.

        When Silver's input_mode is not explicitly set, this method reads the
        Bronze partition's metadata to determine the appropriate input_mode
        based on the Bronze load_pattern.

        Mapping:
        - full_snapshot → REPLACE_DAILY
        - incremental / incremental_append → APPEND_LOG
        - cdc → APPEND_LOG

        Args:
            source_path: The resolved source path (may contain globs)

        Returns:
            InputMode if discovered from metadata, None otherwise
        """
        # Use the new boundary-aware discovery
        input_mode, boundary = self._discover_partition_boundaries(source_path)
        self._partition_boundary = boundary  # Store for filtering later
        return input_mode

    def _discover_partition_boundaries(
        self, source_path: str
    ) -> Tuple[Optional[InputMode], Optional[str]]:
        """Discover input_mode and partition boundaries from Bronze metadata.

        Scans partitions backwards to find full_snapshot boundaries.
        When a full_snapshot partition is found, partitions before it are
        not needed because the snapshot contains complete state.

        Args:
            source_path: The resolved source path (may contain globs)

        Returns:
            Tuple of (InputMode, boundary_date).
            - InputMode: REPLACE_DAILY or APPEND_LOG based on latest partition
            - boundary_date: Earliest partition date to read (None = read all)
              When a full_snapshot is found, this is that partition's date.
        """
        # Extract base directory from source_path pattern
        if "*" in source_path:
            base_dir = source_path.split("*")[0].rstrip("/")
            if "dt=" in base_dir:
                parent_dir = "/".join(base_dir.rstrip("/").split("/")[:-1])
                if parent_dir:
                    base_dir = parent_dir
        else:
            base_dir = str(Path(source_path).parent)

        try:
            partitions = self._list_partitions(base_dir)
            if not partitions:
                return (None, None)

            # Sort partitions chronologically (lexicographic works for ISO dates)
            sorted_partitions = sorted(partitions)

            # Read metadata from LATEST partition to determine input_mode
            latest_partition = sorted_partitions[-1]
            latest_metadata = self._read_partition_metadata(base_dir, latest_partition)

            if not latest_metadata:
                return (None, None)

            latest_pattern = (
                latest_metadata.get("load_pattern")
                or latest_metadata.get("extra", {}).get("load_pattern")
                or "full_snapshot"
            ).lower()

            # Map to input_mode
            pattern_to_input = {
                "full_snapshot": InputMode.REPLACE_DAILY,
                "incremental_append": InputMode.APPEND_LOG,
                "incremental": InputMode.APPEND_LOG,
                "cdc": InputMode.APPEND_LOG,
            }

            input_mode = pattern_to_input.get(latest_pattern, InputMode.REPLACE_DAILY)

            # If latest is full_snapshot, only read that partition
            if input_mode == InputMode.REPLACE_DAILY:
                logger.info(
                    "silver_input_mode_from_metadata",
                    load_pattern=latest_pattern,
                    input_mode=input_mode.value,
                    boundary=latest_partition,
                    reason="latest partition is full_snapshot",
                )
                return (input_mode, latest_partition)

            # If latest is incremental/cdc, scan backwards for full_snapshot boundary
            boundary_date = None

            for partition in reversed(sorted_partitions):
                metadata = self._read_partition_metadata(base_dir, partition)
                if not metadata:
                    continue

                pattern = (
                    metadata.get("load_pattern")
                    or metadata.get("extra", {}).get("load_pattern")
                    or "incremental"
                ).lower()

                if pattern == "full_snapshot":
                    # Found boundary - don't need to read partitions before this
                    boundary_date = partition
                    logger.info(
                        "silver_partition_boundary_found",
                        boundary=partition,
                        reason="full_snapshot partition found",
                    )
                    break

            logger.info(
                "silver_input_mode_from_metadata",
                load_pattern=latest_pattern,
                input_mode=input_mode.value,
                boundary=boundary_date,
                total_partitions=len(sorted_partitions),
            )

            return (input_mode, boundary_date)

        except Exception as e:
            logger.debug("silver_metadata_discovery_failed", error=str(e))
            return (None, None)

    def _list_partitions(self, base_dir: str) -> List[str]:
        """List all dt=YYYY-MM-DD partitions in the base directory.

        Returns list of partition date strings (e.g., ['2025-01-06', '2025-01-07']).
        """
        import re

        partitions = []
        dt_pattern = re.compile(r"dt=(\d{4}-?\d{2}-?\d{2})")

        try:
            if is_s3_path(base_dir):
                from pipelines.lib.storage import S3Storage

                bucket, prefix = parse_s3_uri(base_dir)
                storage_opts = _extract_storage_options(self.storage_options)
                storage = S3Storage(f"s3://{bucket}/", **storage_opts)

                items = storage.list_files(prefix, recursive=True)
                seen = set()
                for item in items:
                    match = dt_pattern.search(item.path)
                    if match:
                        dt_raw = match.group(1)
                        # Normalize to YYYY-MM-DD format
                        dt_norm = (
                            f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
                            if len(dt_raw) == 8 and "-" not in dt_raw
                            else dt_raw
                        )
                        if dt_norm not in seen:
                            seen.add(dt_norm)
                            partitions.append(dt_norm)
            else:
                # Local path
                base = Path(base_dir)
                if base.exists():
                    for entry in base.iterdir():
                        if entry.is_dir():
                            match = dt_pattern.search(entry.name)
                            if match:
                                dt_raw = match.group(1)
                                dt_norm = (
                                    f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
                                    if len(dt_raw) == 8 and "-" not in dt_raw
                                    else dt_raw
                                )
                                partitions.append(dt_norm)
        except Exception as e:
            logger.debug("silver_list_partitions_failed", error=str(e))

        return partitions

    def _read_partition_metadata(
        self, base_dir: str, partition_date: str
    ) -> Optional[Dict[str, Any]]:
        """Read _metadata.json from a specific partition.

        Args:
            base_dir: Base directory containing dt= partitions
            partition_date: Partition date string (YYYY-MM-DD)

        Returns:
            Parsed metadata dict, or None if not found
        """
        import json

        try:
            if is_s3_path(base_dir):
                from pipelines.lib.storage import S3Storage

                bucket, prefix = parse_s3_uri(base_dir)
                storage_opts = _extract_storage_options(self.storage_options)
                storage = S3Storage(f"s3://{bucket}/", **storage_opts)

                metadata_key = (
                    f"{prefix.rstrip('/')}/dt={partition_date}/_metadata.json"
                )
                content = storage.read_bytes(metadata_key)
                result: Dict[str, Any] = json.loads(content.decode("utf-8"))
                return result
            else:
                metadata_path = (
                    Path(base_dir) / f"dt={partition_date}" / "_metadata.json"
                )
                if metadata_path.exists():
                    with open(metadata_path, encoding="utf-8") as f:
                        result = json.load(f)
                        return result
        except Exception as e:
            logger.debug(
                "silver_read_partition_metadata_failed",
                partition=partition_date,
                error=str(e),
            )

        return None

    def _check_source(self, run_date: str) -> List[str]:
        """Check that source data exists.

        Returns list of issues found.
        """
        issues: List[str] = []

        source_path = self._resolve_source_path(run_date)

        if not storage_path_exists(source_path):
            issues.append(
                f"Source data not found: {source_path}\n"
                "  Run the Bronze layer first to generate source data."
            )

        return issues

    def run(
        self,
        run_date: str,
        *,
        target_override: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute the Silver curation pipeline.

        Args:
            run_date: The date for this curation run (YYYY-MM-DD format)
            target_override: Override the target path (useful for local dev)
            dry_run: Validate configuration without writing

        Returns:
            Dictionary with curation results including row_count, target path

        Raises:
            ValueError: If source_path or target_path is not configured
        """
        from pipelines.lib.trace import step, get_tracer, PipelineStep

        tracer = get_tracer()

        # Determine subject name for tracing
        subject_label = (
            f"{self.domain}.{self.subject}"
            if self.domain and self.subject
            else self.subject or "silver"
        )

        with step(PipelineStep.SILVER_START, subject_label):
            # Validate paths at runtime (they may have been set by Pipeline)
            if not self.source_path:
                raise ValueError(
                    "source_path is not configured. Either:\n"
                    "  1. Set source_path explicitly, or\n"
                    "  2. Use Pipeline(bronze=..., silver=...) to auto-wire from Bronze"
                )

            source = self._resolve_source_path(run_date)
            target = self._resolve_target(target_override, run_date)

            # Discover input_mode from Bronze metadata if not explicitly set
            if self.input_mode is None:
                discovered_mode = self._discover_input_mode_from_bronze_metadata(source)
                if discovered_mode:
                    # Temporarily set input_mode for this run
                    self._effective_input_mode = discovered_mode
                else:
                    # Default to REPLACE_DAILY if discovery fails
                    self._effective_input_mode = InputMode.REPLACE_DAILY
            else:
                self._effective_input_mode = self.input_mode

            dry_run_result = maybe_dry_run(
                dry_run=dry_run,
                logger=logger,
                message="[DRY RUN] Would curate %s to %s",
                message_args=(source, target),
                target=target,
                extra={
                    "source": source,
                    "entity_kind": self.entity_kind.value,
                    "history_mode": self.history_mode.value,
                },
            )
            if dry_run_result:
                return dry_run_result

            # Validate Bronze source checksums (if configured)
            validation_result = None
            if self.validate_source != "skip":
                with step(PipelineStep.SILVER_VALIDATE_SOURCE):
                    validation_result = self._validate_source(source)
                    if validation_result:
                        tracer.detail(
                            f"Validated {len(validation_result.get('verified_files', []))} files"
                        )

            # Read from Bronze
            with step(PipelineStep.SILVER_READ_BRONZE):
                con = ibis.duckdb.connect()

                # Configure S3 if source or target is cloud storage
                if is_object_storage_path(source) or is_object_storage_path(target):
                    _configure_duckdb_s3(
                        con, self.storage_options
                    )  # Pass storage_options for MinIO/custom endpoints

                t = self._read_source(con, source)
                row_count = t.count().execute()
                tracer.detail(f"Read {row_count:,} records from Bronze")

            if row_count == 0:
                logger.warning("silver_no_source_rows", source=source)
                return {
                    "row_count": 0,
                    "source": source,
                    "target": target,
                }

            # Select columns (if specified)
            with step(PipelineStep.SILVER_SELECT_COLUMNS):
                t = self._select_columns(t)
                tracer.detail(f"Selected {len(t.columns)} columns")

            # Apply curation based on entity kind and history mode
            with step(PipelineStep.SILVER_DEDUPLICATE):
                t = self._curate(t)
                curated_count = t.count().execute()
                tracer.detail(f"Curated to {curated_count:,} records")

            # Add Silver metadata
            with step(PipelineStep.SILVER_ADD_METADATA):
                t = self._add_metadata(t, run_date)

            # Write output
            with step(PipelineStep.SILVER_WRITE_OUTPUT):
                result = self._write(t, target, run_date, source)
                tracer.detail(
                    f"Wrote {result.get('row_count', 0):,} records to {target}"
                )

            # Include validation result if performed
            if validation_result:
                result["source_validation"] = validation_result

            return result

    def _resolve_target(self, target_override: Optional[str], run_date: str) -> str:
        """Resolve the target path."""
        return resolve_target_path(
            template=self.target_path,
            target_override=target_override,
            env_var="SILVER_TARGET_ROOT",
            format_vars={
                "run_date": run_date,
                "domain": self.domain,
                "subject": self.subject,
            },
        )

    def _validate_source(self, source: str) -> Optional[Dict[str, Any]]:
        """Validate Bronze source data before processing.

        Args:
            source: Source path (may contain glob patterns)

        Returns:
            Validation result dict or None if validation is skipped
        """
        if self.validate_source == "skip":
            return None

        from pipelines.lib.checksum import validate_bronze_checksums

        # For glob patterns, extract the directory path
        if "*" in source or "?" in source:
            # Get the base directory before any wildcards
            parts = source.split("*")[0].split("?")[0]
            # If path ends with separator, use it directly; otherwise get parent
            source_dir = (
                Path(parts.rstrip("/\\"))
                if parts.endswith(("/", "\\"))
                else Path(parts).parent
                if parts
                else Path(".")
            )
        else:
            source_dir = Path(source)
            if source_dir.is_file():
                source_dir = source_dir.parent

        # Only validate if it's a local path
        if is_object_storage_path(str(source_dir)):
            logger.debug("silver_skip_checksum_validation", reason="cloud_source")
            return None

        result = validate_bronze_checksums(
            source_dir,
            validation_mode=self.validate_source,
        )

        return {
            "valid": result.valid,
            "verified_files": result.verified_files,
            "missing_files": result.missing_files,
            "mismatched_files": result.mismatched_files,
            "verification_time_ms": result.verification_time_ms,
        }

    def _expand_glob(self, source: str) -> List[str]:
        """Expand glob patterns for S3 or local paths.

        Returns list of matching file paths, or [source] if no glob pattern.
        Returns empty list if pattern matches no files.
        """
        if "*" not in source and "?" not in source:
            return [source]

        if is_s3_path(source):
            from pipelines.lib.storage import S3Storage

            bucket, _ = parse_s3_uri(source)
            storage_opts = _extract_storage_options(self.storage_options)
            storage = S3Storage(f"s3://{bucket}/", **storage_opts)
            matches = storage.glob(source)
            return [f"s3://{bucket}/{m}" for m in matches] if matches else []

        import glob as glob_module

        return glob_module.glob(source) or []

    def _filter_latest_partition_files(self, files: List[str]) -> List[str]:
        """Keep only files from the latest dt= partition."""
        import re

        dt_regex = re.compile(r"dt=(\d{4}-?\d{2}-?\d{2})")
        latest = None

        for path in files:
            match = dt_regex.search(path)
            if not match:
                continue
            dt_raw = match.group(1)
            dt_norm = (
                f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
                if len(dt_raw) == 8 and "-" not in dt_raw
                else dt_raw
            )
            if latest is None or dt_norm > latest:
                latest = dt_norm

        if not latest:
            return files

        filtered = []
        for path in files:
            match = dt_regex.search(path)
            if not match:
                continue
            dt_raw = match.group(1)
            dt_norm = (
                f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
                if len(dt_raw) == 8 and "-" not in dt_raw
                else dt_raw
            )
            if dt_norm == latest:
                filtered.append(path)

        return filtered or files

    def _filter_partitions_by_boundary(
        self, files: List[str], boundary_date: str
    ) -> List[str]:
        """Filter files to only include partitions >= boundary_date.

        When a full_snapshot partition is found during boundary discovery,
        this method filters out all partitions before that date.

        Args:
            files: List of file paths with dt= partition segments
            boundary_date: Earliest partition date to include (YYYY-MM-DD)

        Returns:
            Filtered list of files from partitions >= boundary_date
        """
        import re

        dt_regex = re.compile(r"dt=(\d{4}-?\d{2}-?\d{2})")
        filtered = []

        for path in files:
            match = dt_regex.search(path)
            if not match:
                # Include files without dt= partition (shouldn't happen, but be safe)
                filtered.append(path)
                continue

            dt_raw = match.group(1)
            # Normalize to YYYY-MM-DD format
            dt_norm = (
                f"{dt_raw[:4]}-{dt_raw[4:6]}-{dt_raw[6:]}"
                if len(dt_raw) == 8 and "-" not in dt_raw
                else dt_raw
            )

            # Include if partition date >= boundary date (lexicographic comparison)
            if dt_norm >= boundary_date:
                filtered.append(path)

        if len(filtered) < len(files):
            excluded_count = len(files) - len(filtered)
            logger.info(
                "silver_partitions_filtered_by_boundary",
                boundary=boundary_date,
                included=len(filtered),
                excluded=excluded_count,
            )

        return filtered or files

    def _read_source(self, con: ibis.BaseBackend, source: str) -> ibis.Table:
        """Read from Bronze source.

        Handles glob patterns (e.g., *.parquet) by expanding them first.
        Works with both local and S3/cloud paths.

        When a partition boundary was discovered (full_snapshot found), only
        files from partitions >= the boundary date are included.
        """
        # Use effective_input_mode (may be discovered from metadata) or fall back to self.input_mode
        effective_mode = getattr(self, "_effective_input_mode", None) or self.input_mode

        # For APPEND_LOG mode, expand to read all partitions
        if effective_mode == InputMode.APPEND_LOG:
            source = self._expand_to_all_partitions(source)
            logger.debug("silver_append_log_mode", expanded_source=source)

        # For S3 paths, add glob pattern to avoid DuckDB's URL encoding issues
        if is_s3_path(source) and "*" not in source and "?" not in source:
            if source.endswith("/") or not source.split("/")[-1].count("."):
                source = source.rstrip("/") + "/*.parquet"
                logger.debug("silver_added_glob_pattern", source=source)

        # Expand glob and read files
        files = self._expand_glob(source)
        if not files:
            logger.warning("silver_no_files_found", pattern=source)
            import pandas as pd

            return con.create_table("empty", pd.DataFrame())

        if effective_mode == InputMode.REPLACE_DAILY and (
            "*" in source or "?" in source
        ):
            files = self._filter_latest_partition_files(files)

        # Apply partition boundary filtering if a boundary was discovered
        # This excludes partitions before a full_snapshot boundary
        partition_boundary = getattr(self, "_partition_boundary", None)
        if partition_boundary and effective_mode == InputMode.APPEND_LOG:
            files = self._filter_partitions_by_boundary(files, partition_boundary)

        # Read as CSV or parquet based on extension
        if len(files) == 1 and files[0].endswith(".csv"):
            return con.read_csv(files[0])
        return con.read_parquet(files if len(files) > 1 else files[0])

    def _expand_to_all_partitions(self, source: str) -> str:
        """Expand a single-partition source path to read all partitions.

        Converts paths like:
          s3://bucket/bronze/system=retail/entity=orders/dt=2025-01-15/*.parquet
        To:
          s3://bucket/bronze/system=retail/entity=orders/dt=*/*.parquet

        This allows reading ALL Bronze partitions when input_mode=APPEND_LOG.
        """
        import re

        # Replace dt=YYYY-MM-DD with dt=* to match all date partitions
        # Handles common date formats: YYYY-MM-DD, YYYYMMDD, etc.
        expanded = re.sub(r"dt=\d{4}-?\d{2}-?\d{2}", "dt=*", source)

        if expanded == source:
            # No date partition found - warn but continue
            logger.warning(
                "silver_no_date_partition",
                source=source,
                message="Could not find dt= partition to expand for append_log mode",
            )

        return expanded

    def _select_columns(self, t: ibis.Table) -> ibis.Table:
        """Apply column selection and renaming logic.

        Priority:
        1. If attributes specified, use only those (plus keys and timestamp)
        2. If exclude_columns specified, exclude those
        3. Otherwise, keep all columns
        4. If column_mapping specified, rename columns

        Column order is preserved: unique_columns first, then last_updated_column,
        then attributes in their specified order.
        """
        if self.attributes is not None:
            # Explicit allow list - only these columns
            # Use dict.fromkeys to dedupe while preserving order:
            # unique_columns first, then last_updated_column, then attributes
            keys = self.unique_columns or []
            ts = [self.last_updated_column] if self.last_updated_column else []
            ordered_cols = list(dict.fromkeys(keys + ts + self.attributes))
            # Filter to columns that actually exist, preserving order
            existing = [c for c in ordered_cols if c in t.columns]
            missing = set(ordered_cols) - set(existing)
            if missing:
                logger.warning("silver_missing_columns", columns=list(missing))
            t = t.select(*existing)

        elif self.exclude_columns:
            # Exclude list - all except these
            cols = [c for c in t.columns if c not in self.exclude_columns]
            t = t.select(*cols)

        # Apply column renaming if specified
        if self.column_mapping:
            # Build rename mapping for columns that exist
            # Note: ibis.rename expects {new_name: old_name}, but our config uses
            # {old_name: new_name} which is more intuitive for users
            rename_map = {
                new_name: old_name
                for old_name, new_name in self.column_mapping.items()
                if old_name in t.columns
            }
            if rename_map:
                t = t.rename(rename_map)
                logger.debug(
                    "silver_columns_renamed",
                    renamed={v: k for k, v in rename_map.items()},  # Log as old->new
                )
            # Warn about columns that don't exist
            missing = set(self.column_mapping.keys()) - set(t.columns)
            if missing:
                logger.warning(
                    "silver_column_mapping_missing",
                    message="Column mapping references columns that don't exist",
                    columns=list(missing),
                )

        return t

    def _curate(self, t: ibis.Table) -> ibis.Table:
        """Apply curation based on entity kind and history mode.

        If cdc_options is set, applies CDC processing first (handling I/U/D
        operation codes), then applies the standard curation logic.
        """
        # Apply CDC processing if configured
        if self.cdc_options:
            if not self.unique_columns:
                raise ValueError("unique_columns required when using CDC")
            if not self.last_updated_column:
                raise ValueError("last_updated_column required when using CDC")
            t = apply_cdc(
                t,
                self.unique_columns,
                self.last_updated_column,
                self.delete_mode.value,
                self.cdc_options,
            )
            logger.debug(
                "silver_cdc_applied",
                delete_mode=self.delete_mode.value,
                operation_column=self.cdc_options.get("operation_column"),
            )

        if self.entity_kind == EntityKind.STATE:
            return self._curate_state(t)
        else:
            return self._curate_event(t)

    def _curate_state(self, t: ibis.Table) -> ibis.Table:
        """Curate a STATE entity (slowly changing dimension).

        For periodic_snapshot model (no unique_columns/last_updated_column),
        returns data as-is without deduplication.
        """
        # If no unique_columns or last_updated_column, skip deduplication (periodic_snapshot)
        if not self.unique_columns or not self.last_updated_column:
            return t

        if self.history_mode == HistoryMode.CURRENT_ONLY:
            # SCD1: Keep only the latest version per unique columns
            return dedupe_latest(t, self.unique_columns, self.last_updated_column)
        else:
            # SCD2: Build full history with effective dates
            return build_history(t, self.unique_columns, self.last_updated_column)

    def _curate_event(self, t: ibis.Table) -> ibis.Table:
        """Curate an EVENT entity (immutable log).

        Events are immutable - just dedupe exact duplicates.
        """
        return t.distinct()

    def _add_metadata(self, t: ibis.Table, run_date: str) -> ibis.Table:
        """Add Silver metadata columns."""
        now = utc_now_iso()
        return t.mutate(
            _silver_curated_at=ibis.literal(now),
            _silver_run_date=ibis.literal(run_date),
        )

    def _infer_subject_name(self, target: str) -> str:
        """Infer subject name from target path.

        Extracts subject name from Hive-style partition paths like:
        - 'silver/domain=sales/subject=orders/' -> 'orders'
        - 's3://bucket/silver/orders/' -> 'orders'
        - './silver/orders/' -> 'orders'

        Falls back to 'data' if subject cannot be determined.
        """
        import re

        # Try to extract from subject= partition (new Silver path format)
        match = re.search(r"subject=([^/\\]+)", target)
        if match:
            return match.group(1)

        # Fall back to last non-empty path segment
        # Normalize path separators for cross-platform compatibility
        normalized = target.replace("\\", "/").rstrip("/")
        target_parts = normalized.split("/")
        for part in reversed(target_parts):
            if part and "=" not in part and part not in ("silver", "bronze"):
                return part

        return "data"

    def _write(
        self,
        t: ibis.Table,
        target: str,
        run_date: str,
        source: str,
    ) -> Dict[str, Any]:
        """Write to Silver target with metadata and checksums."""
        # Determine subject name for filename
        subject_name = (
            self.subject if self.subject else self._infer_subject_name(target)
        )

        # Infer column types for metadata (include SQL types for PolyBase DDL)
        columns = infer_column_types(t, include_sql_types=True)

        # Silver-specific metadata
        silver_extra = {
            "entity_kind": self.entity_kind.value,
            "history_mode": self.history_mode.value,
            "delete_mode": self.delete_mode.value,
            "unique_columns": self.unique_columns,
            "last_updated_column": self.last_updated_column,
            "source_path": source,
            "domain": self.domain,
            "subject": self.subject,
        }

        # Silver-specific checksum metadata
        checksum_extra = {
            "entity_kind": self.entity_kind.value,
            "history_mode": self.history_mode.value,
            "unique_columns": self.unique_columns,
        }

        # Write using unified artifact writer
        write_result = write_artifacts(
            table=t,
            target=target,
            entity_name=subject_name,
            columns=columns,
            run_date=run_date,
            extra_metadata=silver_extra,
            storage_options=self.storage_options,
            write_metadata=True,
            write_checksums=True,
            checksum_extra=checksum_extra,
            partition_by=self.partition_by,
            compression=self.parquet_compression or "snappy",
        )

        if write_result.row_count == 0:
            logger.warning("silver_no_rows_after_curation", target=target)
            return {
                "row_count": 0,
                "target": target,
            }

        # Write PolyBase DDL script for cloud storage
        polybase_file = None
        if is_object_storage_path(target):
            polybase_file = self._write_polybase_ddl(target, columns)

        # Also write CSV if requested (for local filesystem)
        if "csv" in self.output_formats and "parquet" in self.output_formats:
            if not is_object_storage_path(target):
                output_dir = Path(target)
                csv_file = output_dir / f"{subject_name}.csv"
                t.execute().to_csv(str(csv_file), index=False)

        logger.info(
            "silver_curation_complete", row_count=write_result.row_count, target=target
        )

        result: Dict[str, Any] = {
            "row_count": write_result.row_count,
            "target": target,
            "columns": list(t.columns),
            "entity_kind": self.entity_kind.value,
            "history_mode": self.history_mode.value,
            "files": write_result.data_files,
        }

        if write_result.metadata_file:
            result["metadata_file"] = write_result.metadata_file
        if write_result.checksums_file:
            result["checksums_file"] = write_result.checksums_file
        if polybase_file:
            result["polybase_file"] = polybase_file

        return result

    def _write_polybase_ddl(
        self, target: str, columns: List[Dict[str, Any]]
    ) -> Optional[str]:
        """Write PolyBase DDL script to cloud storage."""
        from pipelines.lib.polybase import write_polybase_artifacts

        return write_polybase_artifacts(
            target,
            columns,
            domain=self.domain,
            subject=self.subject,
            entity_kind=self.entity_kind.value,
            history_mode=self.history_mode.value,
            delete_mode=self.delete_mode.value,
            unique_columns=self.unique_columns,
            last_updated_column=self.last_updated_column,
            storage_options=_extract_storage_options(self.storage_options),
        )
