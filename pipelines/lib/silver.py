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

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import ibis

from pipelines.lib.curate import build_history, dedupe_latest
from pipelines.lib._path_utils import resolve_target_path, storage_path_exists
from pipelines.lib.storage import get_storage

logger = logging.getLogger(__name__)

__all__ = ["EntityKind", "HistoryMode", "SilverEntity"]


class EntityKind(Enum):
    """What kind of entity is this?"""

    STATE = "state"  # Slowly changing dimension (customer, product, account)
    EVENT = "event"  # Immutable event log (orders, clicks, payments)


class HistoryMode(Enum):
    """How to handle historical changes."""

    CURRENT_ONLY = "current_only"  # SCD Type 1 - only keep latest
    FULL_HISTORY = "full_history"  # SCD Type 2 - keep all versions


@dataclass
class SilverEntity:
    """Declarative Silver layer entity definition.

    Intentionally limited to curation operations only.
    No filtering, no joins, no derived columns - that's Gold layer.

    Example:
        entity = SilverEntity(
            source_path="s3://bronze/system=claims_dw/entity=claims_header/dt={run_date}/*.parquet",
            target_path="s3://silver/claims/header/",
            natural_keys=["ClaimID"],
            change_timestamp="LastUpdated",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )
        result = entity.run("2025-01-15")
    """

    # Source and target paths
    source_path: str  # Path to Bronze data (supports {run_date} template)
    target_path: str  # Where to write Silver output

    # Identity
    natural_keys: List[str]  # What makes a record unique

    # Temporal
    change_timestamp: str  # When the source record changed

    # Schema (optional - defaults to all columns)
    attributes: Optional[List[str]] = None  # Columns to include (None = all)
    exclude_columns: Optional[List[str]] = None  # Columns to always exclude

    # Behavior
    entity_kind: EntityKind = EntityKind.STATE
    history_mode: HistoryMode = HistoryMode.CURRENT_ONLY

    # Partitioning
    partition_by: Optional[List[str]] = None

    # Output options
    output_formats: List[str] = field(default_factory=lambda: ["parquet"])
    parquet_compression: str = "snappy"

    # Validation options
    validate_source: str = "skip"  # "skip", "warn", or "strict"

    def __post_init__(self) -> None:
        """Validate configuration on instantiation."""
        errors = self._validate()
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"SilverEntity configuration errors:\n{error_msg}\n\n"
                "Fix the configuration and try again."
            )

    def _validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []

        if not self.source_path:
            errors.append("source_path is required")

        if not self.target_path:
            errors.append("target_path is required")

        if not self.natural_keys:
            errors.append(
                "natural_keys is required (what makes a record unique?)"
            )

        if not self.change_timestamp:
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
                    "EVENT entities are immutable - FULL_HISTORY may not be needed. "
                    "Consider using CURRENT_ONLY."
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
        from pipelines.lib.validate import validate_silver_entity

        issues: List[str] = []

        # Run configuration validation
        config_issues = validate_silver_entity(self)
        issues.extend(str(issue) for issue in config_issues)

        # Check source data exists if requested
        if check_source and run_date:
            source_issues = self._check_source(run_date)
            issues.extend(source_issues)

        return issues

    def _check_source(self, run_date: str) -> List[str]:
        """Check that source data exists.

        Returns list of issues found.
        """
        issues: List[str] = []

        source_path = self.source_path.format(run_date=run_date)

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
        """
        source = self.source_path.format(run_date=run_date)
        target = self._resolve_target(target_override, run_date)

        if dry_run:
            logger.info("[DRY RUN] Would curate %s to %s", source, target)
            return {
                "dry_run": True,
                "source": source,
                "target": target,
                "entity_kind": self.entity_kind.value,
                "history_mode": self.history_mode.value,
            }

        # Validate Bronze source checksums (if configured)
        validation_result = self._validate_source(source)

        # Read from Bronze
        con = ibis.duckdb.connect()
        t = self._read_source(con, source)

        if t.count().execute() == 0:
            logger.warning("No rows found in source: %s", source)
            return {
                "row_count": 0,
                "source": source,
                "target": target,
            }

        # Select columns (if specified)
        t = self._select_columns(t)

        # Apply curation based on entity kind and history mode
        t = self._curate(t)

        # Add Silver metadata
        t = self._add_metadata(t, run_date)

        # Write output
        result = self._write(t, target, run_date, source)

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
            format_vars={"run_date": run_date},
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
            source_dir = Path(parts).parent if parts else Path(".")
        else:
            source_dir = Path(source)
            if source_dir.is_file():
                source_dir = source_dir.parent

        # Only validate if it's a local path
        if str(source_dir).startswith("s3://"):
            logger.debug("Skipping checksum validation for S3 source")
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
        """
        import glob as glob_module

        # Expand glob patterns if present
        if "*" in source or "?" in source:
            files = glob_module.glob(source)
            if not files:
                logger.warning("No files found matching pattern: %s", source)
                # Return empty table
                return con.memtable([])
            source = files  # Pass list of files to read_parquet

        if isinstance(source, str) and source.endswith(".csv"):
            return con.read_csv(source)
        else:
            # Default to parquet
            return con.read_parquet(source)

    def _select_columns(self, t: ibis.Table) -> ibis.Table:
        """Apply column selection logic.

        Priority:
        1. If attributes specified, use only those (plus keys and timestamp)
        2. If exclude_columns specified, exclude those
        3. Otherwise, keep all columns
        """
        if self.attributes is not None:
            # Explicit allow list - only these columns
            cols = list(
                set(self.natural_keys + [self.change_timestamp] + self.attributes)
            )
            # Filter to columns that actually exist
            existing = [c for c in cols if c in t.columns]
            missing = set(cols) - set(existing)
            if missing:
                logger.warning("Columns not found in source: %s", missing)
            return t.select(*existing)

        elif self.exclude_columns:
            # Exclude list - all except these
            cols = [c for c in t.columns if c not in self.exclude_columns]
            return t.select(*cols)

        else:
            # Default: all columns from Bronze
            return t

    def _curate(self, t: ibis.Table) -> ibis.Table:
        """Apply curation based on entity kind and history mode."""
        if self.entity_kind == EntityKind.STATE:
            return self._curate_state(t)
        else:
            return self._curate_event(t)

    def _curate_state(self, t: ibis.Table) -> ibis.Table:
        """Curate a STATE entity (slowly changing dimension)."""
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
        now = datetime.now(timezone.utc).isoformat()
        return t.mutate(
            _silver_curated_at=ibis.literal(now),
            _silver_run_date=ibis.literal(run_date),
        )

    def _write(
        self,
        t: ibis.Table,
        target: str,
        run_date: str,
        source: str,
    ) -> Dict[str, Any]:
        """Write to Silver target with metadata and checksums."""
        from pipelines.lib.io import write_silver_with_artifacts

        # Execute count before writing (Ibis is lazy)
        row_count = t.count().execute()

        if row_count == 0:
            logger.warning("No rows to write after curation")
            return {
                "row_count": 0,
                "target": target,
            }

        # For S3, use standard Ibis write (checksums not applicable)
        if target.startswith("s3://"):
            write_opts = {}
            if self.partition_by:
                write_opts["partition_by"] = self.partition_by

            written_files = []
            if "parquet" in self.output_formats:
                t.to_parquet(target, **write_opts)
                written_files.append(target)

            logger.info("Wrote %d rows to %s", row_count, target)

            return {
                "row_count": row_count,
                "target": target,
                "columns": list(t.columns),
                "entity_kind": self.entity_kind.value,
                "history_mode": self.history_mode.value,
                "files": written_files,
            }

        # For local filesystem, use enhanced write with artifacts
        metadata = write_silver_with_artifacts(
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
        )

        # Also write CSV if requested
        if "csv" in self.output_formats and "parquet" in self.output_formats:
            output_dir = Path(target)
            csv_file = output_dir / "data.csv"
            t.execute().to_csv(str(csv_file), index=False)

        return {
            "row_count": metadata.row_count,
            "target": target,
            "columns": [c["name"] for c in metadata.columns],
            "entity_kind": self.entity_kind.value,
            "history_mode": self.history_mode.value,
            "files": metadata.data_files,
            "metadata_file": "_metadata.json",
            "checksums_file": "_checksums.json",
        }
