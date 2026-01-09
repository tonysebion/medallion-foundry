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
from typing import Any, Dict, List, Optional, Union

import ibis

from pipelines.lib.storage_config import InputMode, _configure_duckdb_s3, _extract_storage_options
from pipelines.lib.env import utc_now_iso
from pipelines.lib.curate import apply_cdc, build_history, dedupe_latest
from pipelines.lib.io import maybe_dry_run
from pipelines.lib._path_utils import is_object_storage_path, resolve_target_path, storage_path_exists
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
    # CDC presets - simplify CDC configuration
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
    requires_keys: bool = True  # Does this model require natural_keys and change_timestamp?
    requires_cdc_bronze: bool = False  # Must Bronze use load_pattern: cdc?
    valid_bronze_patterns: Optional[tuple[str, ...]] = None  # Allowed Bronze patterns (None = all)
    warns_on_bronze_patterns: Optional[tuple[str, ...]] = None  # Works but warns (inefficient)

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
        warns_on_bronze_patterns=("full_snapshot", "cdc"),  # cdc works but loses delete info
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
    # CDC models - require Bronze CDC pattern
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
            natural_keys=["order_id"],
            change_timestamp="updated_at",
        )

    Example (standalone with explicit paths):
        entity = SilverEntity(
            source_path="s3://bronze/system=claims_dw/entity=claims_header/dt={run_date}/*.parquet",
            target_path="s3://silver/claims/header/",
            natural_keys=["ClaimID"],
            change_timestamp="LastUpdated",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

    Example (SCD Type 2 with full history):
        entity = SilverEntity(
            natural_keys=["customer_id"],
            change_timestamp="updated_at",
            history_mode=HistoryMode.FULL_HISTORY,  # Keep all versions
        )
    """

    # Identity (required for most models, optional for periodic_snapshot)
    natural_keys: Optional[List[str]] = None  # What makes a record unique

    # Temporal (required for most models, optional for periodic_snapshot)
    change_timestamp: Optional[str] = None  # When the source record changed

    # Business domain and subject (optional - auto-wired from Bronze or inferred from paths)
    domain: str = ""  # Business domain name (e.g., "sales", "finance", "hr")
    subject: str = ""  # Subject area name (e.g., "orders", "customers")

    # Source and target paths (optional when used with Pipeline)
    source_path: str = ""  # Path to Bronze data (auto-wired from Pipeline)
    target_path: str = ""  # Where to write Silver output (auto-generated if empty)

    # Schema (optional - defaults to all columns)
    attributes: Optional[List[str]] = None  # Columns to include (None = all)
    exclude_columns: Optional[List[str]] = None  # Columns to always exclude
    column_mapping: Optional[Dict[str, str]] = None  # Rename columns: {old_name: new_name}

    # Behavior
    entity_kind: EntityKind = EntityKind.STATE
    history_mode: HistoryMode = HistoryMode.CURRENT_ONLY
    input_mode: Optional[InputMode] = None  # How to interpret Bronze partitions (auto-wired from Bronze)
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

        if not self.natural_keys and not is_periodic_snapshot:
            errors.append(
                "natural_keys is required (what makes a record unique?)"
            )

        if not self.change_timestamp and not is_periodic_snapshot:
            errors.append(
                "change_timestamp is required (when was the record updated?)"
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
        subject_label = f"{self.domain}.{self.subject}" if self.domain and self.subject else self.subject or "silver"

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
                        tracer.detail(f"Validated {len(validation_result.get('verified_files', []))} files")

            # Read from Bronze
            with step(PipelineStep.SILVER_READ_BRONZE):
                con = ibis.duckdb.connect()

                # Configure S3 if source or target is cloud storage
                if is_object_storage_path(source) or is_object_storage_path(target):
                    _configure_duckdb_s3(con, self.storage_options)  # Pass storage_options for MinIO/custom endpoints

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
                tracer.detail(f"Wrote {result.get('row_count', 0):,} records to {target}")

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
            source_dir = Path(parts.rstrip("/\\")) if parts.endswith(("/", "\\")) else Path(parts).parent if parts else Path(".")
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

    def _read_source(self, con: ibis.BaseBackend, source: str) -> ibis.Table:
        """Read from Bronze source.

        Handles glob patterns (e.g., *.parquet) by expanding them first.
        Works with both local and S3/cloud paths.

        When input_mode=APPEND_LOG, reads ALL Bronze partitions and unions them.
        When input_mode=REPLACE_DAILY (or None), reads just the specified partition.
        """
        # For APPEND_LOG mode, expand to read all partitions
        if self.input_mode == InputMode.APPEND_LOG:
            source = self._expand_to_all_partitions(source)
            logger.debug("silver_append_log_mode", expanded_source=source)

        # For S3 paths without glob patterns, add /*.parquet to avoid DuckDB's URL encoding issues
        # DuckDB's httpfs URL-encodes '=' in paths (to %3D), which causes 404 errors with some
        # S3-compatible storage. Using S3Storage.glob() with boto3 avoids this issue.
        if source.startswith("s3://") and "*" not in source and "?" not in source:
            # Add glob pattern if source looks like a directory (ends with / or no extension)
            if source.endswith("/") or not source.split("/")[-1].count("."):
                source = source.rstrip("/") + "/*.parquet"
                logger.debug("silver_added_glob_pattern", source=source)

        # Expand glob patterns if present
        source_to_read: Union[str, List[str]] = source
        if "*" in source or "?" in source:
            if source.startswith("s3://"):
                # Use S3Storage for S3 glob operations
                from pipelines.lib.storage import S3Storage

                # Extract bucket from source path
                s3_path = source[5:]  # Remove s3://
                bucket = s3_path.split("/")[0]

                # Build storage options from self.storage_options and environment
                storage_opts = _extract_storage_options(self.storage_options)

                storage = S3Storage(f"s3://{bucket}/", **storage_opts)
                matches = storage.glob(source)

                if not matches:
                    logger.warning("silver_no_files_found", pattern=source)
                    # Return empty DataFrame as Ibis table
                    import pandas as pd
                    return con.create_table("empty", pd.DataFrame())

                # glob returns keys without bucket prefix, add full s3:// path back
                source_to_read = [f"s3://{bucket}/{m}" for m in matches]
            else:
                # Use standard glob for local paths
                import glob as glob_module
                local_files = glob_module.glob(source)
                if not local_files:
                    logger.warning("silver_no_files_found", pattern=source)
                    import pandas as pd
                    return con.create_table("empty", pd.DataFrame())
                source_to_read = local_files

        if isinstance(source_to_read, str) and source_to_read.endswith(".csv"):
            return con.read_csv(source_to_read)
        else:
            # Default to parquet
            return con.read_parquet(source_to_read)

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
        expanded = re.sub(
            r"dt=\d{4}-?\d{2}-?\d{2}",
            "dt=*",
            source
        )

        if expanded == source:
            # No date partition found - warn but continue
            logger.warning(
                "silver_no_date_partition",
                source=source,
                message="Could not find dt= partition to expand for append_log mode"
            )

        return expanded

    def _select_columns(self, t: ibis.Table) -> ibis.Table:
        """Apply column selection and renaming logic.

        Priority:
        1. If attributes specified, use only those (plus keys and timestamp)
        2. If exclude_columns specified, exclude those
        3. Otherwise, keep all columns
        4. If column_mapping specified, rename columns

        Column order is preserved: natural_keys first, then change_timestamp,
        then attributes in their specified order.
        """
        if self.attributes is not None:
            # Explicit allow list - only these columns
            # Use dict.fromkeys to dedupe while preserving order:
            # natural_keys first, then change_timestamp, then attributes
            keys = self.natural_keys or []
            ts = [self.change_timestamp] if self.change_timestamp else []
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
            t = apply_cdc(
                t,
                self.natural_keys,
                self.change_timestamp,
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

        For periodic_snapshot model (no natural_keys/change_timestamp),
        returns data as-is without deduplication.
        """
        # If no natural_keys or change_timestamp, skip deduplication (periodic_snapshot)
        if not self.natural_keys or not self.change_timestamp:
            return t

        if self.history_mode == HistoryMode.CURRENT_ONLY:
            # SCD1: Keep only the latest version per natural key
            return dedupe_latest(t, self.natural_keys, self.change_timestamp)
        else:
            # SCD2: Build full history with effective dates
            return build_history(t, self.natural_keys, self.change_timestamp)

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
        from pipelines.lib.checksum import (
            compute_bytes_sha256,
            write_checksum_manifest_s3,
        )
        from pipelines.lib.io import OutputMetadata, infer_column_types, write_silver_with_artifacts
        from pipelines.lib.storage import get_storage

        # Execute count before writing (Ibis is lazy)
        row_count = t.count().execute()

        if row_count == 0:
            logger.warning("silver_no_rows_after_curation", target=target)
            return {
                "row_count": 0,
                "target": target,
            }

        # For cloud storage, write data with metadata and checksums
        if is_object_storage_path(target):
            # Extract storage options from self.storage_options (which may contain
            # s3_signature_version, s3_addressing_style from YAML config)
            storage_opts = _extract_storage_options(self.storage_options)
            storage = get_storage(target, **storage_opts)
            storage.makedirs("")

            write_opts = {}
            if self.partition_by:
                write_opts["partition_by"] = self.partition_by

            # Determine subject name for filename
            subject_name = self.subject if self.subject else self._infer_subject_name(target)

            # Write parquet file with subject name
            parquet_filename = f"{subject_name}.parquet"
            written_files = []
            if "parquet" in self.output_formats:
                output_file = target.rstrip("/") + f"/{parquet_filename}"
                if self.partition_by:
                    # Partitioned writes - need DuckDB with S3 configured
                    con = ibis.duckdb.connect()
                    _configure_duckdb_s3(con, self.storage_options)
                    arrow_table = t.to_pyarrow()
                    duck_table = con.create_table("_temp_silver", arrow_table)
                    duck_table.to_parquet(output_file, **write_opts)
                else:
                    # Non-partitioned - write via boto3 for reliable S3 endpoint handling
                    import io
                    import pyarrow.parquet as pq

                    arrow_table = t.to_pyarrow()
                    buffer = io.BytesIO()
                    pq.write_table(
                        arrow_table,
                        buffer,
                        compression=self.parquet_compression or "snappy",
                    )
                    parquet_bytes = buffer.getvalue()

                    result = storage.write_bytes(parquet_filename, parquet_bytes)
                    if not result.success:
                        raise RuntimeError(f"Failed to write parquet to S3: {result.error}")
                written_files.append(output_file)

            # Infer column types for metadata
            columns = infer_column_types(t, include_sql_types=False)
            now = utc_now_iso()

            # Write metadata to cloud storage
            metadata = OutputMetadata(
                row_count=int(row_count),
                columns=columns,
                written_at=now,
                run_date=run_date,
                data_files=[parquet_filename],
                extra={
                    "entity_kind": self.entity_kind.value,
                    "history_mode": self.history_mode.value,
                    "delete_mode": self.delete_mode.value,
                    "natural_keys": self.natural_keys,
                    "change_timestamp": self.change_timestamp,
                    "source_path": source,
                },
            )
            storage.write_text("_metadata.json", metadata.to_json())
            logger.debug("silver_metadata_written", target=target)

            # Write checksums to cloud storage
            try:
                # Read parquet file back to compute checksum
                parquet_data = storage.read_bytes(parquet_filename)
                file_checksum_data = [{
                    "path": parquet_filename,
                    "size_bytes": len(parquet_data),
                    "sha256": compute_bytes_sha256(parquet_data),
                }]
                write_checksum_manifest_s3(
                    storage,
                    file_checksum_data,
                    entity_kind=self.entity_kind.value,
                    history_mode=self.history_mode.value,
                    row_count=int(row_count),
                    extra_metadata={
                        "natural_keys": self.natural_keys,
                    },
                )
                logger.debug("silver_checksums_written", target=target)
            except Exception as e:
                logger.warning("silver_checksum_write_failed", error=str(e))

            # Write PolyBase DDL script to cloud storage
            try:
                from pipelines.lib.polybase import PolyBaseConfig, write_polybase_ddl_s3

                # Extract entity name from target path (last non-empty segment)
                target_parts = target.rstrip("/").split("/")
                entity_name = target_parts[-1] if target_parts else "entity"

                # Extract bucket/container from S3 URI for data source location
                # s3://bucket/prefix/... -> s3://bucket/silver/
                if target.startswith("s3://"):
                    bucket = target.split("/")[2]
                    data_source_location = f"s3://{bucket}/silver/"
                    data_source_name = f"silver_{bucket}"
                else:
                    # ADLS: abfs://container@account.dfs.core.windows.net/...
                    data_source_location = target.rsplit("/silver/", 1)[0] + "/silver/"
                    data_source_name = "silver_adls"

                # Build metadata dict for PolyBase generation
                polybase_metadata = {
                    "columns": columns,
                    "entity_kind": self.entity_kind.value,
                    "history_mode": self.history_mode.value,
                    "delete_mode": self.delete_mode.value,
                    "natural_keys": self.natural_keys,
                    "change_timestamp": self.change_timestamp,
                }

                # Get S3 endpoint from environment if available
                import os
                s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "https://s3.amazonaws.com")
                s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "<your_access_key>")

                polybase_config = PolyBaseConfig(
                    data_source_name=data_source_name,
                    data_source_location=data_source_location,
                    credential_name="s3_credential",  # Placeholder - user must configure
                    s3_endpoint=s3_endpoint,
                    s3_access_key=s3_access_key,
                )

                write_polybase_ddl_s3(
                    storage,
                    polybase_metadata,
                    polybase_config,
                    entity_name=entity_name,
                )
                logger.debug("silver_polybase_ddl_written", target=target)
            except Exception as e:
                logger.warning("silver_polybase_write_failed", error=str(e))

            logger.info("silver_curation_complete", row_count=row_count, target=target)

            return {
                "row_count": int(row_count),
                "target": target,
                "columns": list(t.columns),
                "entity_kind": self.entity_kind.value,
                "history_mode": self.history_mode.value,
                "files": written_files,
                "metadata_file": "_metadata.json",
                "checksums_file": "_checksums.json",
                "polybase_file": "_polybase.sql",
            }

        # Determine subject name for filename
        subject_name = self.subject if self.subject else self._infer_subject_name(target)

        # For local filesystem, use enhanced write with artifacts
        silver_metadata = write_silver_with_artifacts(
            t,
            target,
            entity_kind=self.entity_kind.value,
            history_mode=self.history_mode.value,
            natural_keys=self.natural_keys,
            change_timestamp=self.change_timestamp,
            format="parquet" if "parquet" in self.output_formats else "csv",
            compression=self.parquet_compression,
            partition_by=self.partition_by,
            run_date=run_date,
            source_path=source,
            write_checksums=True,
            subject_name=subject_name,
        )

        # Also write CSV if requested
        if "csv" in self.output_formats and "parquet" in self.output_formats:
            output_dir = Path(target)
            csv_file = output_dir / f"{subject_name}.csv"
            t.execute().to_csv(str(csv_file), index=False)

        return {
            "row_count": silver_metadata.row_count,
            "target": target,
            "columns": [c["name"] for c in silver_metadata.columns],
            "entity_kind": self.entity_kind.value,
            "history_mode": self.history_mode.value,
            "files": silver_metadata.data_files,
            "metadata_file": "_metadata.json",
            "checksums_file": "_checksums.json",
        }
