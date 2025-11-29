"""Generate Polybase external table configurations from DatasetConfig."""

from core.config.dataset import (
    DatasetConfig,
    PolybaseExternalDataSource,
    PolybaseExternalFileFormat,
    PolybaseExternalTable,
    PolybaseSetup,
)
from typing import Optional


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

    # Derive data source location (where silver samples are stored)
    if not external_data_source_location:
        silver_base = dataset.silver_base_path
        external_data_source_location = f"/{silver_base.as_posix()}/{dataset.bronze_relative_prefix()}/"

    # Derive data source name from pattern or entity
    if not external_data_source_name:
        pattern_folder = dataset.bronze.options.get("pattern_folder", dataset.entity)
        if dataset.silver.entity_kind.is_state_like:
            external_data_source_name = f"silver_{pattern_folder}_state_source"
        else:
            external_data_source_name = f"silver_{pattern_folder}_events_source"

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
    # Simplify artifact name: just use pattern_folder for location
    artifact_location = pattern_folder
    table_name = _derive_external_table_name(dataset, pattern_folder)
    partition_columns = dataset.silver.record_time_partition or dataset.silver.partition_by or []
    partition_columns = [partition_columns] if isinstance(partition_columns, str) else partition_columns

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
    """Derive the external table name from the dataset configuration."""
    entity = dataset.entity
    if dataset.silver.entity_kind.is_state_like:
        return f"{entity}_state_external"
    else:
        return f"{entity}_events_external"


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
        queries.append(
            f"SELECT * FROM {fq_table} "
            f"WHERE {partition_col} = '2025-11-13' "
            f"ORDER BY {', '.join(dataset.silver.natural_keys)}, {ts_col} LIMIT 100"
        )
        queries.append(
            f"SELECT {', '.join(dataset.silver.natural_keys)}, "
            f"COUNT(*) as event_count FROM {fq_table} "
            f"WHERE {partition_col} >= '2025-11-01' "
            f"GROUP BY {', '.join(dataset.silver.natural_keys)}"
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
            f"-- Usage: SELECT * FROM [{schema_name}].[get_{ext_table.table_name}_for_date_range]('2025-11-01', '2025-11-13')\n"
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
