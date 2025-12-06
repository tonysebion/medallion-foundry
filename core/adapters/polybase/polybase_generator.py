"""Generate Polybase external table configurations from DatasetConfig.

Enhanced per spec Section 8 to support:
- Joined Silver assets (multi_source_join pattern)
- Lookup table external tables
- CTE views for common query patterns
"""

from core.infrastructure.config.dataset import (
    DatasetConfig,
    PathStructure,
    PolybaseExternalDataSource,
    PolybaseExternalFileFormat,
    PolybaseExternalTable,
    PolybaseSetup,
)
from typing import Any, Dict, List, Optional


def generate_polybase_setup(
    dataset: DatasetConfig,
    external_data_source_location: Optional[str] = None,
    external_data_source_name: Optional[str] = None,
    schema_name: str = "dbo",
) -> PolybaseSetup:
    """
    Generate a Polybase setup configuration from a DatasetConfig.

    This function derives the Polybase setup automatically from the dataset configuration,
    avoiding repetition across all pattern YAML files.

    Args:
        dataset: The DatasetConfig instance
        external_data_source_location: Override location (defaults to derived from silver path)
        external_data_source_name: Override data source name (defaults to pattern-based name)
        schema_name: SQL schema name (defaults to "dbo")

    Returns:
        PolybaseSetup: Complete Polybase configuration
    """
    if not dataset.silver.enabled:
        return PolybaseSetup(enabled=False)

    # Derive data source location using domain-aware Silver path structure
    if not external_data_source_location:
        silver_base = dataset.silver_base_path
        # Extract path structure keys or use defaults
        path_struct = dataset.path_structure or PathStructure()
        silver_keys = path_struct.silver if path_struct.silver else {}
        
        domain = dataset.domain or "default"
        entity = dataset.entity
        version = dataset.silver.version or 1
        
        domain_key = silver_keys.get("domain_key", "domain")
        entity_key = silver_keys.get("entity_key", "entity")
        version_key = silver_keys.get("version_key", "v")
        
        # Build domain-aware path: domain=X/entity=Y/vN/
        external_data_source_location = (
            f"/{silver_base.as_posix()}/"
            f"{domain_key}={domain}/{entity_key}={entity}/{version_key}{version}/"
        )

    # Derive data source name from pattern or entity, including version
    if not external_data_source_name:
        pattern_folder = dataset.bronze.options.get("pattern_folder", dataset.entity)
        version = dataset.silver.version or 1
        if dataset.silver.entity_kind.is_state_like:
            external_data_source_name = f"silver_{pattern_folder}_v{version}_state_source"
        else:
            external_data_source_name = f"silver_{pattern_folder}_v{version}_events_source"

    # Create external data source
    external_data_source = PolybaseExternalDataSource(
        name=external_data_source_name,
        data_source_type="HADOOP",
        location=external_data_source_location,
        credential_name=None,
    )

    # Create external file format (always Parquet for silver layer)
    external_file_format = PolybaseExternalFileFormat(
        name="parquet_format",
        format_type="PARQUET",
        compression="SNAPPY",
    )

    # Create external table(s)
    pattern_folder = dataset.bronze.options.get("pattern_folder", dataset.entity)
    # Use pattern_key from path structure if available
    path_struct = dataset.path_structure or PathStructure()
    silver_keys = path_struct.silver if path_struct.silver else {}
    pattern_key = silver_keys.get("pattern_key", "pattern")
    
    # Artifact location uses pattern_key format: pattern=pattern_folder
    artifact_location = f"{pattern_key}={pattern_folder}"
    table_name = _derive_external_table_name(dataset, pattern_folder)
    partition_columns = (
        dataset.silver.record_time_partition or dataset.silver.partition_by or []
    )
    partition_columns = (
        [partition_columns] if isinstance(partition_columns, str) else partition_columns
    )

    # Generate sample queries based on entity kind
    sample_queries = _generate_sample_queries(dataset, table_name, schema_name)

    external_table = PolybaseExternalTable(
        artifact_name=artifact_location,
        schema_name=schema_name,
        table_name=table_name,
        partition_columns=partition_columns,
        reject_type="VALUE",
        reject_value=0,
        sample_queries=sample_queries,
    )

    return PolybaseSetup(
        enabled=True,
        external_data_source=external_data_source,
        external_file_format=external_file_format,
        external_tables=[external_table],
        trino_enabled=False,  # Future: Trino migration
        iceberg_enabled=False,  # Future: Iceberg migration
    )


def _derive_external_table_name(dataset: DatasetConfig, artifact_name: str) -> str:
    """Derive the external table name from the dataset configuration.
    
    Includes version in the table name to support side-by-side querying of different versions.
    Example: orders_v1_events_external, orders_v2_state_external
    """
    entity = dataset.entity
    version = dataset.silver.version or 1
    if dataset.silver.entity_kind.is_state_like:
        return f"{entity}_v{version}_state_external"
    else:
        return f"{entity}_v{version}_events_external"


def _generate_sample_queries(
    dataset: DatasetConfig,
    table_name: str,
    schema_name: str,
) -> list[str]:
    """Generate sample point-in-time queries based on entity kind."""
    fq_table = f"{schema_name}.{table_name}"
    queries = []

    if dataset.silver.entity_kind.is_state_like:
        # State entity - temporal queries
        partition_col = dataset.silver.record_time_partition or "effective_from_date"
        queries.append(
            f"SELECT * FROM {fq_table} "
            f"WHERE {partition_col} = '2025-11-13' AND is_current = true "
            f"ORDER BY {', '.join(dataset.silver.natural_keys)} LIMIT 100"
        )
        queries.append(
            f"SELECT {', '.join(dataset.silver.natural_keys)}, "
            f"effective_from_dt, effective_to_dt FROM {fq_table} "
            f"WHERE {', '.join(dataset.silver.natural_keys[0:1])} = 'KEY_VALUE' "
            f"ORDER BY effective_from_dt DESC"
        )
        queries.append(
            f"SELECT {', '.join(dataset.silver.natural_keys)} FROM {fq_table} "
            f"WHERE {partition_col} <= '2025-11-13' "
            f"AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-13') "
            f"ORDER BY {', '.join(dataset.silver.natural_keys)}"
        )
    else:
        # Event entity - time-range and aggregate queries
        partition_col = dataset.silver.record_time_partition or "event_date"
        ts_col = dataset.silver.event_ts_column or "updated_at"
        load_date_col = "load_date"  # Standard load_date column for version-aware queries
        
        # Query 1: Specific date data for current version
        queries.append(
            f"SELECT * FROM {fq_table} "
            f"WHERE {partition_col} = '2025-11-13' "
            f"ORDER BY {', '.join(dataset.silver.natural_keys)}, {ts_col} LIMIT 100"
        )
        
        # Query 2: Time-range aggregation with load_date filtering
        queries.append(
            f"SELECT {', '.join(dataset.silver.natural_keys)}, "
            f"COUNT(*) as event_count FROM {fq_table} "
            f"WHERE {partition_col} >= '2025-11-01' "
            f"AND {partition_col} <= '2025-11-30' "
            f"GROUP BY {', '.join(dataset.silver.natural_keys)}"
        )
        
        # Query 3: Load date aware query
        queries.append(
            f"SELECT {', '.join(dataset.silver.natural_keys)}, {ts_col}, {load_date_col} FROM {fq_table} "
            f"WHERE {load_date_col} = '2025-11-13' "
            f"ORDER BY {', '.join(dataset.silver.natural_keys)}, {ts_col}"
        )

    return queries


def generate_temporal_functions_sql(
    dataset: DatasetConfig,
    schema_name: str = "dbo",
) -> str:
    """
    Generate parameterized SQL functions for point-in-time queries.

    These functions allow querying temporal data by passing a date parameter,
    making it easy for users to retrieve data "as of" a specific date.

    Args:
        dataset: The DatasetConfig instance
        schema_name: SQL schema name for the functions

    Returns:
        SQL DDL for inline table-valued functions (ITVFs)
    """
    if not dataset.silver.enabled:
        return ""

    polybase_setup = generate_polybase_setup(dataset, schema_name=schema_name)
    if not polybase_setup.external_tables or not polybase_setup.enabled:
        return ""

    ext_table = polybase_setup.external_tables[0]
    table_name = f"[{ext_table.schema_name}].[{ext_table.table_name}]"

    functions = []

    if dataset.silver.entity_kind.is_state_like:
        # State entity: function for point-in-time state snapshot
        partition_col = dataset.silver.record_time_partition or "effective_from_date"
        pk_cols = ", ".join([f"[{col}]" for col in dataset.silver.natural_keys])

        func_sql = (
            f"CREATE FUNCTION [{schema_name}].[get_{ext_table.table_name}_as_of]\n"
            f"(\n"
            f"    @target_date DATE\n"
            f")\n"
            f"RETURNS TABLE\n"
            f"AS\n"
            f"RETURN\n"
            f"    SELECT {pk_cols}, *\n"
            f"    FROM {table_name}\n"
            f"    WHERE {partition_col} <= @target_date\n"
            f"    AND (effective_to_dt IS NULL OR effective_to_dt > @target_date)\n"
            f"    AND is_current = 1;\n"
            f"\n"
            f"-- Usage: SELECT * FROM [{schema_name}].[get_{ext_table.table_name}_as_of]('2025-11-13')\n"
        )
        functions.append(func_sql)

        # History function for all changes to a specific entity
        pk_col = dataset.silver.natural_keys[0] if dataset.silver.natural_keys else "id"
        history_sql = (
            f"CREATE FUNCTION [{schema_name}].[get_{ext_table.table_name}_history]\n"
            f"(\n"
            f"    @{pk_col} VARCHAR(255)\n"
            f")\n"
            f"RETURNS TABLE\n"
            f"AS\n"
            f"RETURN\n"
            f"    SELECT *\n"
            f"    FROM {table_name}\n"
            f"    WHERE [{pk_col}] = @{pk_col}\n"
            f"    ORDER BY effective_from_dt DESC;\n"
            f"\n"
            f"-- Usage: SELECT * FROM [{schema_name}].[get_{ext_table.table_name}_history]('ORD123')\n"
        )
        functions.append(history_sql)

    else:
        # Event entity: function for time-range events
        partition_col = dataset.silver.record_time_partition or "event_date"

        range_sql = (
            f"CREATE FUNCTION [{schema_name}].[get_{ext_table.table_name}_for_date_range]\n"
            f"(\n"
            f"    @start_date DATE,\n"
            f"    @end_date DATE\n"
            f")\n"
            f"RETURNS TABLE\n"
            f"AS\n"
            f"RETURN\n"
            f"    SELECT *\n"
            f"    FROM {table_name}\n"
            f"    WHERE {partition_col} >= @start_date\n"
            f"    AND {partition_col} <= @end_date\n"
            f"    ORDER BY {partition_col} DESC;\n"
            f"\n"
            f"-- Usage: SELECT * FROM [{schema_name}].[get_{ext_table.table_name}_for_date_range]("
            f"'2025-11-01', '2025-11-13')\n"
        )
        functions.append(range_sql)

        # Point-in-time function for events on a specific date
        point_sql = (
            f"CREATE FUNCTION [{schema_name}].[get_{ext_table.table_name}_for_date]\n"
            f"(\n"
            f"    @target_date DATE\n"
            f")\n"
            f"RETURNS TABLE\n"
            f"AS\n"
            f"RETURN\n"
            f"    SELECT *\n"
            f"    FROM {table_name}\n"
            f"    WHERE {partition_col} = @target_date\n"
            f"    ORDER BY {', '.join([f'[{col}]' for col in dataset.silver.natural_keys])};\n"
            f"\n"
            f"-- Usage: SELECT * FROM [{schema_name}].[get_{ext_table.table_name}_for_date]('2025-11-13')\n"
        )
        functions.append(point_sql)

    return "\n".join(functions)


def generate_joined_table_ddl(
    join_config: Dict[str, Any],
    table_name: str,
    location: str,
    data_source_name: str,
    file_format_name: str = "parquet_format",
    schema_name: str = "dbo",
) -> str:
    """Generate External Table DDL for a joined Silver asset.

    This supports the multi_source_join pattern where multiple Bronze sources
    are joined into a single Silver output.

    Args:
        join_config: Join configuration with sources and result_columns
        table_name: Name for the external table
        location: Storage location for the joined data
        data_source_name: Name of the external data source
        file_format_name: Name of the file format
        schema_name: SQL schema name

    Returns:
        DDL string for creating the external table
    """
    columns = join_config.get("result_columns", [])
    if not columns:
        # Derive columns from sources if not specified
        for source in join_config.get("sources", []):
            for col in source.get("columns", []):
                columns.append({"name": col, "type": "NVARCHAR(4000)"})

    column_defs = []
    for col in columns:
        col_name = col.get("name", "unnamed")
        col_type = _map_type_to_sql(col.get("type", "string"), col)
        column_defs.append(f"    [{col_name}] {col_type}")

    columns_sql = ",\n".join(column_defs)

    ddl = f"""-- External Table for Joined Silver Asset
-- Sources: {', '.join(s.get('name', 'unknown') for s in join_config.get('sources', []))}

IF OBJECT_ID('[{schema_name}].[{table_name}]', 'U') IS NOT NULL
    DROP EXTERNAL TABLE [{schema_name}].[{table_name}];

CREATE EXTERNAL TABLE [{schema_name}].[{table_name}]
(
{columns_sql}
)
WITH
(
    LOCATION = '{location}',
    DATA_SOURCE = [{data_source_name}],
    FILE_FORMAT = [{file_format_name}],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
);
"""
    return ddl


def generate_lookup_table_ddl(
    lookup_config: Dict[str, Any],
    table_name: str,
    location: str,
    data_source_name: str,
    file_format_name: str = "parquet_format",
    schema_name: str = "dbo",
) -> str:
    """Generate External Table DDL for a lookup/reference table.

    Lookup tables are typically smaller reference data used for enrichment
    in the single_source_with_lookups pattern.

    Args:
        lookup_config: Lookup configuration with columns
        table_name: Name for the external table (e.g., ext_status_lookup)
        location: Storage location for the lookup data
        data_source_name: Name of the external data source
        file_format_name: Name of the file format
        schema_name: SQL schema name

    Returns:
        DDL string for creating the external table
    """
    columns = lookup_config.get("columns", [])
    if not columns:
        # Default columns for lookup tables
        columns = [
            {"name": "code", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "description", "type": "string"},
        ]

    column_defs = []
    for col in columns:
        col_name = col.get("name", "unnamed")
        col_type = _map_type_to_sql(col.get("type", "string"), col)
        nullable = " NULL" if col.get("nullable", True) else " NOT NULL"
        column_defs.append(f"    [{col_name}] {col_type}{nullable}")

    columns_sql = ",\n".join(column_defs)

    lookup_key = lookup_config.get("lookup_key", columns[0].get("name", "code"))

    ddl = f"""-- External Table for Lookup/Reference Data
-- Lookup Key: {lookup_key}

IF OBJECT_ID('[{schema_name}].[{table_name}]', 'U') IS NOT NULL
    DROP EXTERNAL TABLE [{schema_name}].[{table_name}];

CREATE EXTERNAL TABLE [{schema_name}].[{table_name}]
(
{columns_sql}
)
WITH
(
    LOCATION = '{location}',
    DATA_SOURCE = [{data_source_name}],
    FILE_FORMAT = [{file_format_name}],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
);

-- Suggested index for lookup performance (create after loading to regular table):
-- CREATE NONCLUSTERED INDEX IX_{table_name}_{lookup_key} ON [{schema_name}].[{table_name}] ([{lookup_key}]);
"""
    return ddl


def generate_cte_view_ddl(
    view_name: str,
    base_table: str,
    cte_definitions: List[Dict[str, str]],
    final_select: str,
    schema_name: str = "dbo",
    description: str = "",
) -> str:
    """Generate a CTE-based view for complex queries.

    Common patterns:
    - Aggregations with lookups
    - Multi-table joins
    - Temporal queries with history

    Args:
        view_name: Name for the view
        base_table: Primary table being queried
        cte_definitions: List of CTEs with 'name' and 'query' keys
        final_select: The final SELECT statement
        schema_name: SQL schema name
        description: View description for documentation

    Returns:
        DDL string for creating the view
    """
    cte_parts = []
    for cte in cte_definitions:
        cte_name = cte.get("name", "cte")
        cte_query = cte.get("query", "SELECT 1")
        cte_parts.append(f"{cte_name} AS (\n{cte_query}\n)")

    ctes_sql = ",\n".join(cte_parts)

    ddl = f"""-- {description or f'CTE View for {base_table}'}
CREATE OR ALTER VIEW [{schema_name}].[{view_name}]
AS
WITH {ctes_sql}
{final_select};
"""
    return ddl


def generate_current_state_view_ddl(
    dataset: DatasetConfig,
    external_table_name: str,
    schema_name: str = "dbo",
) -> str:
    """Generate a view for current state (latest version) of entities.

    For state/SCD entities, this view returns only the current version
    of each entity, filtering out historical records.

    Args:
        dataset: DatasetConfig with entity configuration
        external_table_name: Name of the external table
        schema_name: SQL schema name

    Returns:
        DDL for the current state view
    """
    if not dataset.silver.entity_kind.is_state_like:
        return ""

    pk_cols = ", ".join([f"[{col}]" for col in dataset.silver.natural_keys])
    view_name = f"vw_{dataset.entity}_current"

    ddl = f"""-- Current State View for {dataset.entity}
-- Returns only the latest version of each entity

CREATE OR ALTER VIEW [{schema_name}].[{view_name}]
AS
SELECT *
FROM [{schema_name}].[{external_table_name}]
WHERE is_current = 1;

-- Alternative using ROW_NUMBER for explicit latest selection:
/*
CREATE OR ALTER VIEW [{schema_name}].[{view_name}_v2]
AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY {pk_cols}
               ORDER BY effective_from DESC
           ) as rn
    FROM [{schema_name}].[{external_table_name}]
)
SELECT *
FROM ranked
WHERE rn = 1;
*/
"""
    return ddl


def generate_history_summary_view_ddl(
    dataset: DatasetConfig,
    external_table_name: str,
    schema_name: str = "dbo",
) -> str:
    """Generate a view summarizing entity history.

    Provides counts and date ranges for historical tracking.

    Args:
        dataset: DatasetConfig with entity configuration
        external_table_name: Name of the external table
        schema_name: SQL schema name

    Returns:
        DDL for the history summary view
    """
    if not dataset.silver.entity_kind.is_state_like:
        return ""

    pk_cols = ", ".join([f"[{col}]" for col in dataset.silver.natural_keys])
    view_name = f"vw_{dataset.entity}_history_summary"

    ddl = f"""-- History Summary View for {dataset.entity}
-- Shows version count and date range for each entity

CREATE OR ALTER VIEW [{schema_name}].[{view_name}]
AS
SELECT
    {pk_cols},
    COUNT(*) as version_count,
    MIN(effective_from) as first_version_date,
    MAX(effective_from) as latest_version_date,
    DATEDIFF(day, MIN(effective_from), MAX(effective_from)) as history_span_days
FROM [{schema_name}].[{external_table_name}]
GROUP BY {pk_cols};
"""
    return ddl


def generate_event_aggregation_view_ddl(
    dataset: DatasetConfig,
    external_table_name: str,
    aggregation_period: str = "day",
    schema_name: str = "dbo",
) -> str:
    """Generate a view for event aggregations.

    Provides daily/hourly counts and metrics for event entities.

    Args:
        dataset: DatasetConfig with entity configuration
        external_table_name: Name of the external table
        aggregation_period: 'day' or 'hour'
        schema_name: SQL schema name

    Returns:
        DDL for the event aggregation view
    """
    if not dataset.silver.entity_kind.is_event_like:
        return ""

    ts_col = dataset.silver.event_ts_column or "event_ts"
    partition_col = dataset.silver.record_time_partition or f"{ts_col}_dt"
    view_name = f"vw_{dataset.entity}_daily_summary"

    if aggregation_period == "hour":
        view_name = f"vw_{dataset.entity}_hourly_summary"
        date_expr = f"DATEADD(hour, DATEDIFF(hour, 0, [{ts_col}]), 0)"
    else:
        date_expr = f"CAST([{ts_col}] AS DATE)"

    ddl = f"""-- Event Aggregation View for {dataset.entity}
-- Provides {aggregation_period}ly counts and metrics

CREATE OR ALTER VIEW [{schema_name}].[{view_name}]
AS
SELECT
    {date_expr} as period,
    COUNT(*) as event_count,
    COUNT(DISTINCT [{dataset.silver.natural_keys[0]}]) as unique_entities
FROM [{schema_name}].[{external_table_name}]
GROUP BY {date_expr};
"""
    return ddl


def _map_type_to_sql(data_type: str, col_spec: Optional[Dict[str, Any]] = None) -> str:
    """Map data type string to SQL Server type.

    Args:
        data_type: Type name (string, integer, decimal, etc.)
        col_spec: Column specification with precision/scale

    Returns:
        SQL Server type string
    """
    col_spec = col_spec or {}
    type_map = {
        "string": "NVARCHAR(4000)",
        "varchar": "NVARCHAR(4000)",
        "text": "NVARCHAR(MAX)",
        "integer": "INT",
        "int": "INT",
        "bigint": "BIGINT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "FLOAT",
        "decimal": "DECIMAL",
        "numeric": "DECIMAL",
        "boolean": "BIT",
        "bool": "BIT",
        "date": "DATE",
        "timestamp": "DATETIME2",
        "datetime": "DATETIME2",
        "binary": "VARBINARY(MAX)",
    }

    sql_type = type_map.get(data_type.lower(), "NVARCHAR(4000)")

    # Handle decimal precision/scale
    if sql_type == "DECIMAL":
        precision = col_spec.get("precision", 18)
        scale = col_spec.get("scale", 2)
        sql_type = f"DECIMAL({precision},{scale})"

    return sql_type
