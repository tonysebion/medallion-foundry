"""Bronze layer source abstraction.

The Bronze layer lands raw data exactly as received from sources,
adding only technical metadata for auditability.

Bronze layer rules:
- Land data exactly as received (no transforms)
- Add only technical metadata (_extracted_at, _source_file, etc.)
- Partition by load date for auditability
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import ibis
import pandas as pd

from pipelines.lib.connections import get_connection
from pipelines.lib.env import expand_env_vars, expand_options
from pipelines.lib.watermark import get_watermark, save_watermark

logger = logging.getLogger(__name__)

__all__ = ["BronzeSource", "LoadPattern", "SourceType"]


class SourceType(Enum):
    """Where the data comes from."""

    FILE_CSV = "file_csv"
    FILE_PARQUET = "file_parquet"
    FILE_SPACE_DELIMITED = "file_space_delimited"
    FILE_FIXED_WIDTH = "file_fixed_width"
    DATABASE_MSSQL = "database_mssql"
    DATABASE_POSTGRES = "database_postgres"
    API_REST = "api_rest"


class LoadPattern(Enum):
    """How to load the data."""

    FULL_SNAPSHOT = "full_snapshot"  # Replace everything each run
    INCREMENTAL_APPEND = "incremental"  # Append new records only
    CDC = "cdc"  # Change data capture deltas


@dataclass
class BronzeSource:
    """Declarative Bronze layer source definition.

    Bronze layer rules:
    - Land data exactly as received (no transforms)
    - Add only technical metadata (_extracted_at, _source_file, etc.)
    - Partition by load date for auditability

    Example:
        source = BronzeSource(
            system="claims_dw",
            entity="claims_header",
            source_type=SourceType.DATABASE_MSSQL,
            source_path="",
            target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
            options={
                "connection_name": "claims_db",
                "host": "claims-server.database.com",
                "database": "ClaimsDB",
            },
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column="LastUpdated",
        )
        result = source.run("2025-01-15")
    """

    # Identity
    system: str  # Source system name
    entity: str  # Table/endpoint name

    # Source location
    source_type: SourceType
    source_path: str  # File path, connection string, or API URL

    # Target
    target_path: str  # Where to land in Bronze

    # Behavior
    load_pattern: LoadPattern = LoadPattern.FULL_SNAPSHOT
    watermark_column: Optional[str] = None  # For incremental loads

    # Source-specific options
    options: Dict[str, Any] = field(default_factory=dict)

    # Partitioning (Bronze always partitions by load date)
    partition_by: List[str] = field(default_factory=lambda: ["_load_date"])

    def __post_init__(self) -> None:
        """Validate configuration on instantiation."""
        errors = self._validate()
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"BronzeSource configuration errors for {self.system}.{self.entity}:\n"
                f"{error_msg}\n\n"
                "Fix the configuration and try again."
            )

    def _validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors: List[str] = []

        if not self.system:
            errors.append("system is required (source system name)")

        if not self.entity:
            errors.append("entity is required (table/endpoint name)")

        if not self.target_path:
            errors.append("target_path is required")

        # Database-specific validation
        db_types = (SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES)
        if self.source_type in db_types:
            if "host" not in self.options:
                errors.append(
                    "Database sources require 'host' in options. "
                    'Add options={"host": "your-server.database.com", ...}'
                )
            if "database" not in self.options:
                errors.append(
                    "Database sources require 'database' in options. "
                    'Add "database": "YourDatabase" to options.'
                )

        # File-specific validation
        if self.source_type in (
            SourceType.FILE_CSV,
            SourceType.FILE_PARQUET,
            SourceType.FILE_SPACE_DELIMITED,
        ):
            if not self.source_path:
                errors.append(
                    "File sources require source_path. "
                    "Add source_path='/path/to/files/{run_date}/*.csv'"
                )

        # Fixed-width specific validation
        if self.source_type == SourceType.FILE_FIXED_WIDTH:
            if "columns" not in self.options:
                errors.append(
                    "Fixed-width files require 'columns' list in options."
                )
            if "widths" not in self.options:
                errors.append(
                    "Fixed-width files require 'widths' list in options."
                )

        # Incremental load validation
        if self.load_pattern == LoadPattern.INCREMENTAL_APPEND:
            if not self.watermark_column:
                errors.append(
                    "Incremental loads require watermark_column. "
                    "Add watermark_column='LastUpdated'"
                )

        # Warnings (logged but don't fail)
        if self.load_pattern == LoadPattern.FULL_SNAPSHOT and self.watermark_column:
            logger.warning(
                "watermark_column is set but load_pattern is FULL_SNAPSHOT. "
                "The watermark will be ignored. Use INCREMENTAL_APPEND or remove it."
            )

        return errors

    def run(
        self,
        run_date: str,
        *,
        target_override: Optional[str] = None,
        skip_if_exists: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute the Bronze extraction pipeline.

        Args:
            run_date: The date for this extraction run (YYYY-MM-DD format)
            target_override: Override the target path (useful for local dev)
            skip_if_exists: Skip extraction if data already exists
            dry_run: Validate configuration without extracting

        Returns:
            Dictionary with extraction results including row_count, target path
        """
        target = self._resolve_target(run_date, target_override)

        if skip_if_exists and self._already_ran(target):
            logger.info(
                "Skipping %s.%s - data already exists at %s",
                self.system,
                self.entity,
                target,
            )
            return {"skipped": True, "reason": "already_exists", "target": target}

        if dry_run:
            logger.info(
                "[DRY RUN] Would extract %s.%s to %s",
                self.system,
                self.entity,
                target,
            )
            return {
                "dry_run": True,
                "target": target,
                "source_type": self.source_type.value,
            }

        # Get watermark for incremental loads
        last_watermark = None
        if (
            self.load_pattern == LoadPattern.INCREMENTAL_APPEND
            and self.watermark_column
        ):
            last_watermark = get_watermark(self.system, self.entity)
            if last_watermark:
                logger.info(
                    "Incremental load for %s.%s from watermark: %s",
                    self.system,
                    self.entity,
                    last_watermark,
                )

        # Read from source
        con = ibis.duckdb.connect()
        t = self._read_source(con, run_date, last_watermark)

        # Add Bronze technical metadata (the ONLY transforms allowed)
        t = self._add_metadata(t, run_date)

        # Write to target
        result = self._write(t, target, run_date)

        # Save new watermark for incremental
        if self.watermark_column and result.get("row_count", 0) > 0:
            new_watermark = self._get_max_watermark(t)
            if new_watermark:
                save_watermark(self.system, self.entity, str(new_watermark))
                result["new_watermark"] = str(new_watermark)

        return result

    def _resolve_target(self, run_date: str, target_override: Optional[str]) -> str:
        """Resolve the target path with template substitution."""
        base = (
            target_override or os.environ.get("BRONZE_TARGET_ROOT") or self.target_path
        )
        return base.format(
            system=self.system,
            entity=self.entity,
            run_date=run_date,
        )

    def _already_ran(self, target: str) -> bool:
        """Check if data already exists for this run."""
        if target.startswith("s3://"):
            # For S3, check via fsspec
            try:
                import fsspec

                fs = fsspec.filesystem("s3")
                return fs.exists(target) and len(fs.ls(target)) > 0
            except Exception:
                return False
        else:
            # Local filesystem
            path = Path(target)
            return path.exists() and any(path.iterdir())

    def _read_source(
        self,
        con: ibis.BaseBackend,
        run_date: str,
        last_watermark: Optional[str],
    ) -> ibis.Table:
        """Read from source based on source type."""
        source_path = self.source_path.format(run_date=run_date)

        if self.source_type == SourceType.FILE_CSV:
            return con.read_csv(source_path, **self.options.get("csv_options", {}))

        elif self.source_type == SourceType.FILE_PARQUET:
            return con.read_parquet(source_path)

        elif self.source_type == SourceType.FILE_SPACE_DELIMITED:
            csv_opts = {"delim": " ", **self.options.get("csv_options", {})}
            return con.read_csv(source_path, **csv_opts)

        elif self.source_type == SourceType.FILE_FIXED_WIDTH:
            return self._read_fixed_width(con, run_date)

        elif self.source_type in (
            SourceType.DATABASE_MSSQL,
            SourceType.DATABASE_POSTGRES,
        ):
            return self._read_database(con, run_date, last_watermark)

        elif self.source_type == SourceType.API_REST:
            records = self._fetch_api(run_date, last_watermark)
            return ibis.memtable(records)

        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

    def _get_expanded_options(self) -> Dict[str, Any]:
        """Get options with environment variables expanded."""
        return expand_options(self.options)

    def _read_database(
        self,
        con: ibis.BaseBackend,
        run_date: str,
        last_watermark: Optional[str],
    ) -> ibis.Table:
        """Read from database source using connection pooling."""
        opts = self._get_expanded_options()
        connection_name = opts.get(
            "connection_name", f"{self.system}_{self.entity}"
        )
        db_con = get_connection(connection_name, self.source_type, opts)

        query = self.options.get("query")
        if query:
            # If we have a watermark, inject it into the query
            if last_watermark and self.watermark_column:
                if "?" in query:
                    # Parameterized query
                    query = query.replace("?", f"'{last_watermark}'")
                elif "WHERE" in query.upper():
                    query = f"{query} AND {self.watermark_column} > '{last_watermark}'"
                else:
                    query = (
                        f"{query} WHERE {self.watermark_column} > '{last_watermark}'"
                    )
            return db_con.sql(query)
        else:
            # Default: SELECT * FROM entity
            table = db_con.table(self.entity)
            if last_watermark and self.watermark_column:
                table = table.filter(table[self.watermark_column] > last_watermark)
            return table

    def _read_fixed_width(self, con: ibis.BaseBackend, run_date: str) -> ibis.Table:
        """Read fixed-width file with column positions.

        Requires options:
            - columns: list of column names
            - widths: list of column widths (integers)
        """
        source_path = self.source_path.format(run_date=run_date)
        columns = self.options.get("columns", [])
        widths = self.options.get("widths", [])

        if not columns or not widths:
            raise ValueError(
                "Fixed-width files require 'columns' and 'widths' in options"
            )

        # Use pandas to read fixed-width, then convert to Ibis
        df = pd.read_fwf(source_path, widths=widths, names=columns)
        return ibis.memtable(df)

    def _fetch_api(
        self,
        run_date: str,
        last_watermark: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Fetch from REST API.

        For now, delegates to a simple requests-based fetch.
        Complex pagination/auth can be extended here.
        """
        import requests

        opts = self._get_expanded_options()
        url = expand_env_vars(self.source_path.format(run_date=run_date))
        headers = opts.get("headers", {})
        params = opts.get("params", {})

        if last_watermark and self.watermark_column:
            params[self.watermark_column] = last_watermark

        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()

        # Handle common API response patterns
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Try common keys for data arrays
            for key in ("data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # If no array found, wrap the dict
            return [data]
        else:
            raise ValueError(f"Unexpected API response type: {type(data)}")

    def _add_metadata(self, t: ibis.Table, run_date: str) -> ibis.Table:
        """Add Bronze technical metadata columns.

        These are the ONLY additions allowed in Bronze.
        """
        now = datetime.now(timezone.utc).isoformat()
        return t.mutate(
            _load_date=ibis.literal(run_date),
            _extracted_at=ibis.literal(now),
            _source_system=ibis.literal(self.system),
            _source_entity=ibis.literal(self.entity),
        )

    def _write(self, t: ibis.Table, target: str, run_date: str) -> Dict[str, Any]:
        """Write to Bronze target."""
        # Execute count before writing (Ibis is lazy)
        row_count = t.count().execute()

        if row_count == 0:
            logger.warning("No rows to write for %s.%s", self.system, self.entity)
            return {
                "row_count": 0,
                "target": target,
                "source_type": self.source_type.value,
            }

        # Determine output format based on target
        if target.startswith("s3://"):
            t.to_parquet(target, partition_by=self.partition_by)
        else:
            # Local filesystem
            output_dir = Path(target)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{self.entity}.parquet"
            t.to_parquet(str(output_file))

        logger.info(
            "Wrote %d rows for %s.%s to %s",
            row_count,
            self.system,
            self.entity,
            target,
        )

        return {
            "row_count": row_count,
            "target": target,
            "source_type": self.source_type.value,
            "columns": list(t.columns),
        }

    def _get_max_watermark(self, t: ibis.Table) -> Optional[str]:
        """Get the maximum value of the watermark column."""
        if not self.watermark_column or self.watermark_column not in t.columns:
            return None

        try:
            max_val = t.select(t[self.watermark_column].max()).execute()
            # Handle different return types from Ibis
            if hasattr(max_val, "iloc"):
                return str(max_val.iloc[0, 0]) if not max_val.empty else None
            return str(max_val) if max_val is not None else None
        except Exception as e:
            logger.warning("Failed to get max watermark: %s", e)
            return None
