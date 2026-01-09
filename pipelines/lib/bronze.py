"""Bronze layer source abstraction.

The Bronze layer lands raw data exactly as received from sources,
adding only technical metadata for auditability.

Bronze layer rules:
- Land data exactly as received (no transforms)
- Add only technical metadata (_extracted_at, _source_file, etc.)
- Partition by load date for auditability
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import ibis
import pandas as pd

from pipelines.lib.connections import get_connection
from pipelines.lib.env import expand_env_vars, expand_options, utc_now_iso
from pipelines.lib.io import OutputMetadata, infer_column_types, maybe_dry_run, maybe_skip_if_exists
from pipelines.lib.storage import get_storage
from pipelines.lib.state import (
    WatermarkSource,
    delete_watermark,
    get_watermark_with_source,
    save_full_refresh,
    save_watermark,
    should_force_full_refresh,
)
from pipelines.lib._path_utils import (
    is_object_storage_path,
    path_has_data,
    resolve_target_path,
    storage_path_exists,
)
from pipelines.lib.observability import get_structlog_logger
from pipelines.lib.storage_config import (
    InputMode,
    _configure_duckdb_s3,
    _extract_storage_options,
)

# Use structlog for structured logging with pipeline context
logger = get_structlog_logger(__name__)

# Re-export InputMode and WatermarkSource for backward compatibility (external code may import from bronze)
__all__ = ["BronzeOutputMetadata", "BronzeSource", "InputMode", "LoadPattern", "SourceType", "WatermarkSource"]


# Backwards compatibility: BronzeOutputMetadata is now OutputMetadata
BronzeOutputMetadata = OutputMetadata


class SourceType(Enum):
    """Where the data comes from."""

    # File sources
    FILE_CSV = "file_csv"
    FILE_PARQUET = "file_parquet"
    FILE_SPACE_DELIMITED = "file_space_delimited"
    FILE_FIXED_WIDTH = "file_fixed_width"
    FILE_JSON = "file_json"
    FILE_JSONL = "file_jsonl"
    FILE_EXCEL = "file_excel"

    # Database sources
    DATABASE_MSSQL = "database_mssql"
    DATABASE_POSTGRES = "database_postgres"
    DATABASE_MYSQL = "database_mysql"
    DATABASE_DB2 = "database_db2"

    # API sources
    API_REST = "api_rest"


class LoadPattern(Enum):
    """How to load the data."""

    FULL_SNAPSHOT = "full_snapshot"  # Replace everything each run
    INCREMENTAL_APPEND = "incremental"  # Append new records only
    CDC = "cdc"  # Change data capture deltas


# Default target path template - can be overridden
DEFAULT_BRONZE_TARGET = "./bronze/system={system}/entity={entity}/dt={run_date}/"


@dataclass
class BronzeSource:
    """Declarative Bronze layer source definition.

    Bronze layer rules:
    - Land data exactly as received (no transforms)
    - Add only technical metadata (_extracted_at, _source_file, etc.)
    - Partition by load date for auditability

    Example (simple):
        # Minimal config - paths auto-generated
        source = BronzeSource(
            system="retail",
            entity="orders",
            source_type=SourceType.FILE_CSV,
            source_path="./data/orders_{run_date}.csv",
        )

    Example (database with top-level params):
        source = BronzeSource(
            system="claims_dw",
            entity="claims_header",
            source_type=SourceType.DATABASE_MSSQL,
            # Top-level database params (simpler than nested options)
            host="${DB_HOST}",
            database="ClaimsDB",
            query="SELECT * FROM dbo.ClaimsHeader",
            # Incremental load
            load_pattern=LoadPattern.INCREMENTAL_APPEND,
            watermark_column="LastUpdated",
        )

    Example (explicit target path):
        source = BronzeSource(
            system="claims_dw",
            entity="claims_header",
            source_type=SourceType.DATABASE_MSSQL,
            # Explicit path overrides the default
            target_path="s3://my-bucket/bronze/custom/path/dt={run_date}/",
            host="${DB_HOST}",
            database="ClaimsDB",
        )
    """

    # Identity (required)
    system: str  # Source system name (e.g., "salesforce", "erp", "legacy")
    entity: str  # Table/endpoint name (e.g., "customers", "orders")

    # Source location (required for file sources)
    source_type: SourceType
    source_path: str = ""  # File path, connection string, or API URL

    # Target (optional - defaults to ./bronze/system={system}/entity={entity}/dt={run_date}/)
    target_path: str = ""  # Where to land in Bronze - auto-generated if empty

    # Behavior
    load_pattern: LoadPattern = LoadPattern.FULL_SNAPSHOT
    input_mode: Optional[InputMode] = None  # How Silver interprets partitions (required in YAML)
    watermark_column: Optional[str] = None  # For incremental loads
    watermark_source: WatermarkSource = WatermarkSource.DESTINATION  # Where to read watermarks

    # Database connection params (convenience - merged into options)
    # These are simpler than nesting in options dict
    connection: Optional[str] = None  # Connection name for reuse
    host: Optional[str] = None  # Database host or env var like ${DB_HOST}
    database: Optional[str] = None  # Database name
    query: Optional[str] = None  # Custom SQL query

    # Source-specific options (for advanced cases)
    options: Dict[str, Any] = field(default_factory=dict)

    # Partitioning (Bronze always partitions by load date)
    partition_by: List[str] = field(default_factory=lambda: ["_load_date"])

    # Large data handling
    chunk_size: Optional[int] = None  # Rows per chunk (None = load all at once)

    # Periodic full refresh (scheduler-agnostic)
    # When set, automatically forces a full refresh every N days
    full_refresh_days: Optional[int] = None  # None = never auto-refresh

    # Artifact generation options
    write_checksums: bool = True  # Write _checksums.json for data integrity
    write_metadata: bool = True  # Write _metadata.json for lineage/PolyBase

    def __post_init__(self) -> None:
        """Initialize defaults and validate configuration."""
        # Apply smart default for target_path if not specified
        if not self.target_path:
            self.target_path = DEFAULT_BRONZE_TARGET

        # Merge top-level database params into options
        # Top-level params take precedence over options dict
        self._merge_top_level_params()

        # Validate configuration
        from pipelines.lib.validators import validate_and_raise
        try:
            validate_and_raise(source=self)
        except ValueError as exc:
            raise ValueError(
                f"BronzeSource configuration errors for {self.system}.{self.entity}:\n"
                f"{exc}"
            ) from exc

    def _merge_top_level_params(self) -> None:
        """Merge top-level convenience params into options dict.

        Top-level params (connection, host, database, query) are merged into
        the options dict for backwards compatibility with existing code.
        Top-level params take precedence over options dict values.
        """
        # Connection name
        if self.connection is not None:
            self.options["connection_name"] = self.connection

        # Database host
        if self.host is not None:
            self.options["host"] = self.host

        # Database name
        if self.database is not None:
            self.options["database"] = self.database

        # Custom query - can be top-level or in options
        if self.query is not None:
            self.options["query"] = self.query

    def validate(
        self,
        run_date: Optional[str] = None,
        *,
        check_connectivity: bool = True,
    ) -> List[str]:
        """Validate configuration and optionally check connectivity.

        Performs pre-flight checks to ensure the pipeline can run successfully.

        Args:
            run_date: Optional run date for path resolution
            check_connectivity: If True, test source connectivity

        Returns:
            List of validation issues (empty if valid)

        Example:
            >>> issues = source.validate()
            >>> if issues:
            ...     for issue in issues:
            ...         print(issue)
            ... else:
            ...     print("Configuration is valid")
        """
        from pipelines.lib.config_loader import validate_bronze_source

        issues: List[str] = []

        # Run configuration validation
        config_issues = validate_bronze_source(self)
        issues.extend(str(issue) for issue in config_issues)

        # Check connectivity if requested
        if check_connectivity:
            connectivity_issues = self._check_connectivity(run_date)
            issues.extend(connectivity_issues)

        return issues

    def _check_connectivity(self, run_date: Optional[str] = None) -> List[str]:
        """Check source connectivity.

        Returns list of issues found.
        """
        issues: List[str] = []

        if self.source_type in (
            SourceType.DATABASE_MSSQL,
            SourceType.DATABASE_POSTGRES,
            SourceType.DATABASE_MYSQL,
            SourceType.DATABASE_DB2,
        ):
            try:
                opts = self._get_expanded_options()
                connection_name = opts.get(
                    "connection_name", f"{self.system}_{self.entity}"
                )
                con = get_connection(connection_name, self.source_type, opts)
                con.list_tables()
            except Exception as e:
                issues.append(f"Database connection failed: {e}")

        elif self.source_type in (
            SourceType.FILE_CSV,
            SourceType.FILE_PARQUET,
            SourceType.FILE_SPACE_DELIMITED,
            SourceType.FILE_FIXED_WIDTH,
            SourceType.FILE_JSON,
            SourceType.FILE_JSONL,
            SourceType.FILE_EXCEL,
        ):
            source_path = self.source_path
            if run_date:
                source_path = source_path.format(run_date=run_date)

            if "{run_date}" not in self.source_path or run_date:
                if not storage_path_exists(source_path):
                    issues.append(f"Source path not found: {source_path}")

        return issues

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
        from pipelines.lib.trace import step, get_tracer, PipelineStep

        tracer = get_tracer()

        with step(PipelineStep.BRONZE_START, f"{self.system}.{self.entity}"):
            target = self._resolve_target(run_date, target_override)

            skip_result = maybe_skip_if_exists(
                skip_if_exists=skip_if_exists,
                exists_fn=lambda: self._already_ran(target),
                target=target,
                logger=logger,
                context=f"{self.system}.{self.entity}",
            )
            if skip_result:
                return skip_result

            dry_run_result = maybe_dry_run(
                dry_run=dry_run,
                logger=logger,
                message="[DRY RUN] Would extract %s.%s to %s",
                message_args=(self.system, self.entity, target),
                target=target,
                extra={"source_type": self.source_type.value},
            )
            if dry_run_result:
                return dry_run_result

            # Check if periodic full refresh is due
            is_full_refresh = False
            if should_force_full_refresh(self.system, self.entity, self.full_refresh_days):
                logger.info(
                    "bronze_full_refresh_triggered",
                    system=self.system,
                    entity=self.entity,
                    reason="periodic_refresh",
                )
                delete_watermark(self.system, self.entity)
                is_full_refresh = True

            # Get watermark for incremental loads
            last_watermark = None
            if (
                self.load_pattern == LoadPattern.INCREMENTAL_APPEND
                and self.watermark_column
            ):
                last_watermark = get_watermark_with_source(
                    system=self.system,
                    entity=self.entity,
                    source=self.watermark_source,
                    target_path=target,
                    watermark_column=self.watermark_column,
                    storage_options=_extract_storage_options(self.options),
                )
                if last_watermark:
                    logger.info(
                        "bronze_incremental_load",
                        system=self.system,
                        entity=self.entity,
                        watermark=last_watermark,
                        watermark_source=self.watermark_source.value,
                    )

            # Connect to DuckDB
            with step(PipelineStep.BRONZE_CONNECT_SOURCE, self.source_type.value):
                con = ibis.duckdb.connect()
                # Configure S3 if target is object storage
                if is_object_storage_path(target):
                    _configure_duckdb_s3(con, self.options)

            # Read from source
            with step(PipelineStep.BRONZE_READ_SOURCE):
                t = self._read_source(con, run_date, last_watermark)
                row_count = t.count().execute()
                tracer.detail(f"Read {row_count:,} records from source")

            # Add Bronze technical metadata (the ONLY transforms allowed)
            with step(PipelineStep.BRONZE_ADD_METADATA):
                t = self._add_metadata(t, run_date)

            # Write to target
            with step(PipelineStep.BRONZE_WRITE_OUTPUT):
                result = self._write(t, target, run_date, last_watermark)
                tracer.detail(f"Wrote {result.get('row_count', 0):,} records to {target}")

            # Save new watermark for incremental
            if self.watermark_column and result.get("row_count", 0) > 0:
                with step(PipelineStep.BRONZE_SAVE_WATERMARK):
                    new_watermark = self._get_max_watermark(t)
                    if new_watermark:
                        save_watermark(self.system, self.entity, str(new_watermark))
                        result["new_watermark"] = str(new_watermark)
                        tracer.detail(f"Saved watermark: {new_watermark}")

            # Record full refresh if it was triggered
            if is_full_refresh and result.get("row_count", 0) > 0:
                save_full_refresh(self.system, self.entity)
                result["full_refresh"] = True

            return result

    def _resolve_target(self, run_date: str, target_override: Optional[str]) -> str:
        """Resolve the target path with template substitution."""
        return resolve_target_path(
            template=self.target_path,
            target_override=target_override,
            env_var="BRONZE_TARGET_ROOT",
            format_vars={
                "system": self.system,
                "entity": self.entity,
                "run_date": run_date,
            },
        )

    def _already_ran(self, target: str) -> bool:
        """Check if data already exists for this run."""
        return path_has_data(target)

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
            csv_opts = dict(self.options.get("csv_options", {}))
            fixed_width_keys = (
                "widths",
                "field_widths",
                "column_widths",
                "colspecs",
                "column_specs",
                "column_specifications",
            )
            if any(key in csv_opts for key in fixed_width_keys) or any(
                key in self.options for key in fixed_width_keys
            ):
                return self._read_fixed_width(source_path)
            return self._read_character_delimited(source_path)

        elif self.source_type == SourceType.FILE_FIXED_WIDTH:
            return self._read_fixed_width(source_path)

        elif self.source_type == SourceType.FILE_JSON:
            return self._read_json(con, source_path)

        elif self.source_type == SourceType.FILE_JSONL:
            return self._read_jsonl(con, source_path)

        elif self.source_type == SourceType.FILE_EXCEL:
            return self._read_excel(source_path)

        elif self.source_type in (
            SourceType.DATABASE_MSSQL,
            SourceType.DATABASE_POSTGRES,
            SourceType.DATABASE_MYSQL,
            SourceType.DATABASE_DB2,
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
            # Execute the query first, then apply watermark filter via Ibis
            # This avoids SQL injection vulnerabilities from string interpolation
            table = db_con.sql(query)
            if last_watermark and self.watermark_column:
                # Apply watermark filter using Ibis (safe parameterized approach)
                table = table.filter(table[self.watermark_column] > last_watermark)
            return table
        else:
            # Default: SELECT * FROM entity
            table = db_con.table(self.entity)
            if last_watermark and self.watermark_column:
                table = table.filter(table[self.watermark_column] > last_watermark)
            return table

    def _read_fixed_width(self, source_path: str) -> ibis.Table:
        """Read fixed-width files using explicit column spans.

        Supports two modes:
        1. Single record type: All lines have the same format (standard mode)
        2. Multi-record type: Parent-child patterns (ABABBB) where different
           line types have different formats
        """
        # Check for multi-record-type configuration
        record_type_position = self.options.get("record_type_position")
        record_types = self.options.get("record_types")

        if record_type_position and record_types:
            return self._read_parent_child_fixed_width(
                source_path, record_type_position, record_types
            )

        # Original single-record-type logic
        csv_opts: Dict[str, Any] = dict(self.options.get("csv_options", {}))

        def _pick(*keys: str) -> Optional[Any]:
            """Return the first match from csv_options or top-level options."""
            for key in keys:
                if key in csv_opts:
                    return csv_opts.pop(key)
                if key in self.options:
                    return self.options[key]
            return None

        columns = _pick("columns", "column_names", "col_names") or []
        widths = _pick("widths", "field_widths", "column_widths")
        colspecs = _pick("colspecs", "column_specs", "column_specifications")

        if widths is None and colspecs is None:
            raise ValueError(
                "Fixed-width files require 'widths', 'field_widths', or 'colspecs' in options"
            )

        pandas_opts: Dict[str, Any] = {}
        pandas_opts.update(csv_opts)

        if widths is not None:
            pandas_opts["widths"] = widths
        if colspecs is not None:
            pandas_opts["colspecs"] = colspecs
        if columns:
            pandas_opts["names"] = columns

        df = pd.read_fwf(source_path, **pandas_opts)
        return ibis.memtable(df)

    def _read_parent_child_fixed_width(
        self,
        source_path: str,
        type_position: List[int],
        record_types: List[Dict[str, Any]],
    ) -> ibis.Table:
        """Parse fixed-width file with parent-child record relationships.

        Supports ABABBB, ABBAB patterns where:
        - Parent (A) lines define a master record
        - Child (B) lines belong to the most recent parent
        - Output: flattened rows with parent columns repeated on each child

        Args:
            source_path: Path to the fixed-width file
            type_position: [start, end] character positions for type indicator
            record_types: List of record type definitions with type, role, columns, widths

        Returns:
            ibis.Table with parsed records
        """
        start_pos, end_pos = type_position
        output_mode = self.options.get("output_mode", "flatten")

        # Build lookup: type_code -> config
        type_configs = {rt["type"]: rt for rt in record_types}

        # Identify parent and child configs
        parent_config = next(
            (rt for rt in record_types if rt.get("role") == "parent"), None
        )
        child_config = next(
            (rt for rt in record_types if rt.get("role") == "child"), None
        )

        if not parent_config or not child_config:
            raise ValueError(
                "Parent-child pattern requires one 'parent' and one 'child' record type"
            )

        parent_columns = parent_config.get("columns", [])
        parent_widths = parent_config.get("widths", [])
        child_columns = child_config.get("columns", [])
        child_widths = child_config.get("widths", [])

        # Determine output columns based on mode
        if output_mode == "flatten":
            all_columns = parent_columns + child_columns
        elif output_mode == "parent_only":
            all_columns = parent_columns
        elif output_mode == "child_only":
            all_columns = child_columns
        else:
            all_columns = parent_columns + child_columns

        rows: List[List[str]] = []
        current_parent: Optional[List[str]] = None

        with open(source_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.rstrip("\n\r")
                if not line:
                    continue

                type_code = line[start_pos:end_pos]
                config = type_configs.get(type_code)

                if config is None or config.get("role") == "skip":
                    continue

                data_portion = line[end_pos:]

                if config.get("role") == "parent":
                    # Parse and store parent values
                    current_parent = self._parse_fixed_width_line(
                        data_portion, parent_widths
                    )

                    if output_mode == "parent_only":
                        rows.append(current_parent)

                elif config.get("role") == "child":
                    if current_parent is None:
                        raise ValueError(
                            f"Child record at line {line_num} has no parent"
                        )

                    child_values = self._parse_fixed_width_line(
                        data_portion, child_widths
                    )

                    if output_mode == "flatten":
                        # Combine parent + child into single row
                        row = current_parent + child_values
                        rows.append(row)
                    elif output_mode == "child_only":
                        rows.append(child_values)

        df = pd.DataFrame(rows, columns=all_columns)
        return ibis.memtable(df)

    def _parse_fixed_width_line(
        self, line: str, widths: List[int]
    ) -> List[str]:
        """Parse a single fixed-width line into column values.

        Args:
            line: The line content (already stripped of type indicator)
            widths: List of column widths

        Returns:
            List of stripped string values
        """
        values: List[str] = []
        pos = 0
        for width in widths:
            values.append(line[pos : pos + width].strip())
            pos += width
        return values

    def _read_character_delimited(self, source_path: str) -> ibis.Table:
        """Read files where columns are separated by characters (spaces, pipes, tabs)."""
        csv_opts = dict(self.options.get("csv_options", {}))

        column_names = csv_opts.pop("columns", None) or csv_opts.pop(
            "column_names", None
        ) or csv_opts.pop("col_names", None)
        # Allow overriding delimiter/sep via options
        delimiter = csv_opts.pop("delimiter", None)
        sep = csv_opts.pop("sep", None)
        treat_whitespace = csv_opts.pop("delim_whitespace", True)

        pandas_opts: Dict[str, Any] = {"engine": "python"}
        pandas_opts.update(csv_opts)

        if column_names:
            pandas_opts["names"] = column_names

        if delimiter is not None:
            pandas_opts["sep"] = delimiter
        elif sep is not None:
            pandas_opts["sep"] = sep
        elif treat_whitespace:
            pandas_opts["sep"] = r"\s+"
        elif "sep" not in pandas_opts:
            pandas_opts["sep"] = " "

        df = pd.read_csv(source_path, **pandas_opts)
        return ibis.memtable(df)

    def _read_json(self, _con: ibis.BaseBackend, source_path: str) -> ibis.Table:
        """Read JSON file.

        Options:
            data_path: Dot-notation path to extract data (e.g., "response.data.items")
            flatten: If True, flatten nested structures (default: False)
        """
        import json

        with open(source_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Navigate to nested data if data_path specified
        data_path = self.options.get("data_path")
        if data_path:
            for key in data_path.split("."):
                if isinstance(data, dict) and key in data:
                    data = data[key]
                else:
                    raise ValueError(f"Path '{data_path}' not found in JSON structure")

        # Ensure we have a list of records
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError(f"Expected list or dict, got {type(data)}")

        # Flatten nested structures if requested
        if self.options.get("flatten", False):
            data = [pd.json_normalize(record).to_dict(orient="records")[0] for record in data]

        return ibis.memtable(data)

    def _read_jsonl(self, _con: ibis.BaseBackend, source_path: str) -> ibis.Table:
        """Read JSON Lines (newline-delimited JSON) file.

        Each line is a separate JSON object.

        Options:
            flatten: If True, flatten nested structures (default: False)
        """
        import json

        records = []
        with open(source_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    records.append(json.loads(line))

        # Flatten nested structures if requested
        if self.options.get("flatten", False):
            records = [pd.json_normalize(record).to_dict(orient="records")[0] for record in records]

        return ibis.memtable(records)

    def _read_excel(self, source_path: str) -> ibis.Table:
        """Read Excel file (.xlsx, .xls).

        Options:
            sheet: Sheet name or index (default: 0, first sheet)
            header_row: Row number to use as header (default: 0)
            skip_rows: Number of rows to skip at top (default: 0)
        """
        sheet = self.options.get("sheet", 0)
        header_row = self.options.get("header_row", 0)
        skip_rows = self.options.get("skip_rows", 0)

        df = pd.read_excel(
            source_path,
            sheet_name=sheet,
            header=header_row,
            skiprows=skip_rows if skip_rows else None,
        )
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
            return list(data)
        elif isinstance(data, dict):
            # Try common keys for data arrays
            for key in ("data", "results", "items", "records"):
                if key in data and isinstance(data[key], list):
                    return list(data[key])
            # If no array found, wrap the dict
            return [dict(data)]
        else:
            raise ValueError(f"Unexpected API response type: {type(data)}")

    def _add_metadata(self, t: ibis.Table, run_date: str) -> ibis.Table:
        """Add Bronze technical metadata columns.

        These are the ONLY additions allowed in Bronze.
        """
        now = utc_now_iso()
        return t.mutate(
            _load_date=ibis.literal(run_date),
            _extracted_at=ibis.literal(now),
            _source_system=ibis.literal(self.system),
            _source_entity=ibis.literal(self.entity),
        )

    def _write(
        self,
        t: ibis.Table,
        target: str,
        run_date: str,
        last_watermark: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Write to Bronze target with optional checksums and metadata."""
        from pipelines.lib.checksum import (
            compute_bytes_sha256,
            write_checksum_manifest,
            write_checksum_manifest_s3,
        )
        from pipelines.lib.storage import parse_uri

        # Execute count before writing (Ibis is lazy)
        # Ibis count() returns different types depending on backend
        count_result = t.count().execute()
        row_count: int = int(count_result.iloc[0] if hasattr(count_result, "iloc") else count_result)

        if row_count == 0:
            logger.warning("bronze_no_rows", system=self.system, entity=self.entity)
            return {
                "row_count": 0,
                "target": target,
                "source_type": self.source_type.value,
            }

        # Infer column types for metadata using shared function
        columns = infer_column_types(t, include_sql_types=False)
        now = utc_now_iso()

        # Determine output format based on target using storage backend
        data_files: List[str] = []
        scheme, _ = parse_uri(target)

        # Bronze-specific metadata fields
        bronze_extra = {
            "system": self.system,
            "entity": self.entity,
            "source_type": self.source_type.value,
            "load_pattern": self.load_pattern.value,
            "input_mode": self.input_mode.value if self.input_mode else None,
            "watermark_column": self.watermark_column,
            "last_watermark": last_watermark,
        }

        if scheme in ("s3", "abfs"):
            # Cloud storage - use storage backend for metadata writes
            storage_opts = _extract_storage_options(self.options)
            storage = get_storage(target, **storage_opts)
            storage.makedirs("")  # Ensure target exists
            # Only pass partition_by if not empty (DuckDB doesn't handle empty list well)
            parquet_filename = f"{self.entity}.parquet"
            if self.partition_by:
                t.to_parquet(target, partition_by=self.partition_by)
                data_files.append(target)
            else:
                # No partitioning - write a single file with explicit filename
                output_file = target.rstrip("/") + f"/{parquet_filename}"
                t.to_parquet(output_file)
                data_files.append(output_file)

            # Write metadata to cloud storage
            if self.write_metadata:
                metadata = OutputMetadata(
                    row_count=row_count,
                    columns=columns,
                    written_at=now,
                    run_date=run_date,
                    data_files=[parquet_filename],
                    extra=bronze_extra,
                )
                storage.write_text("_metadata.json", metadata.to_json())
                logger.debug("bronze_metadata_written", target=target)

            # Write checksums to cloud storage
            if self.write_checksums:
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
                        entity_kind="bronze",
                        row_count=row_count,
                        extra_metadata={
                            "system": self.system,
                            "entity": self.entity,
                            "load_pattern": self.load_pattern.value,
                        },
                    )
                    logger.debug("bronze_checksums_written", target=target)
                except Exception as e:
                    logger.warning("bronze_checksum_write_failed", error=str(e))
        else:
            # Local filesystem - write with artifacts
            storage = get_storage(target)
            storage.makedirs("")
            local_output_file = Path(target) / f"{self.entity}.parquet"
            t.to_parquet(str(local_output_file))
            data_files.append(str(local_output_file))

            # Write metadata
            if self.write_metadata:
                metadata = OutputMetadata(
                    row_count=row_count,
                    columns=columns,
                    written_at=now,
                    run_date=run_date,
                    data_files=[Path(f).name for f in data_files],
                    extra=bronze_extra,
                )
                storage.write_text("_metadata.json", metadata.to_json())
                logger.debug("bronze_metadata_written", target=target)

            # Write checksums
            if self.write_checksums:
                write_checksum_manifest(
                    Path(target),
                    [Path(f) for f in data_files],
                    entity_kind="bronze",
                    row_count=row_count,
                    extra_metadata={
                        "system": self.system,
                        "entity": self.entity,
                        "load_pattern": self.load_pattern.value,
                    },
                )

        logger.info(
            "bronze_extraction_complete",
            row_count=row_count,
            system=self.system,
            entity=self.entity,
            target=target,
        )

        result: Dict[str, Any] = {
            "row_count": row_count,
            "target": target,
            "source_type": self.source_type.value,
            "columns": [c["name"] for c in columns],
            "files": [Path(f).name for f in data_files] if scheme == "local" else data_files,
        }

        if self.write_metadata:
            result["metadata_file"] = "_metadata.json"
        if self.write_checksums:
            result["checksums_file"] = "_checksums.json"

        return result

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
            logger.warning("bronze_watermark_failed", error=str(e))
            return None
