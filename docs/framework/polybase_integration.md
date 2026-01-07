# Polybase External Tables for SQL Server

This document describes how to use Polybase external tables to query the Silver layer artifacts from SQL Server Management Studio (SSMS), enabling point-in-time temporal analysis across all data patterns.

## Overview

Polybase allows SQL Server to query data stored in external sources (local file system, Azure Blob Storage, Hadoop, etc.) as if it were a native SQL Server table. The Bronze-Foundry framework automatically generates Polybase external table configurations for each dataset pattern.

### Key Benefits

- **Point-in-time Queries**: Query data "as of" a specific date using partition-aware filtering
- **No Data Movement**: Data stays in the Silver layer; SQL Server queries it in place
- **Temporal Semantics**: Distinct handling for event vs. state patterns (SCD Type 2, SCD Type 1, etc.)
- **Pattern-Driven Configuration**: DDL is generated automatically from pattern configurations
- **Deterministic Partitioning**: Record-time partitioning ensures reproducible temporal queries

## Architecture

### Temporal Partitioning Strategy (V1)

The Silver layer uses **record-time partitioning** (not load-time):

- **Event Patterns**: Partitioned by `event_date` (derived from `event_ts_column`)
- **State Patterns**: Partitioned by `effective_from_date` (derived from `change_ts_column`)
- **Load Batch Tracking**: Stored in data columns + metadata, not in directory structure

This means queries can efficiently scan only the relevant date partitions using partition pruning.

### Pattern-Specific Temporal Semantics

#### Event Entities (pattern_full, pattern_cdc)
- Records represent point-in-time events
- Partitioned by event date for efficient time-range queries
- Support append-only and replace-daily input modes

#### State Entities (pattern_current_history, pattern_hybrid_cdc_cumulative, pattern_hybrid_incremental_cumulative)
- Records represent state changes (SCD Type 2 or SCD Type 1)
- Include `effective_from_dt` and `effective_to_dt` columns for temporal tracking
- Partitioned by `effective_from_date` for point-in-time state queries
- Support "as-of" queries to retrieve state at a specific point in time

#### Derived Event Entities (pattern_hybrid_cdc_point)
- Derived events from CDC source
- Partitioned by event date (change time)
- Support aggregation across change events

#### Derived State Entities (pattern_hybrid_cdc_cumulative, pattern_hybrid_incremental_point)
- Derived states from CDC or incremental sources
- Partitioned by `effective_from_date` for state history queries
- Support point-in-time state analysis

## Generating Polybase DDL

### Using the DDL Generator Script

The `scripts/generate_polybase_ddl.py` script automatically generates SQL DDL from dataset YAML configurations:

```bash
# Generate DDL for a single pattern
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml

# Generate DDL for all patterns
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_*.yaml

# Write DDL to files (one per dataset)
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_*.yaml -o ./ddl_output
```

### What Gets Generated

The script generates SQL for three main objects:

1. **External Data Source**: Defines the location of the Silver layer artifacts
2. **External File Format**: Specifies Parquet format with Snappy compression
3. **External Table**: Maps parquet files to SQL Server table schema

### Customization

The generation is automatic based on the dataset configuration:

- **Data Source Location**: Derived from `silver_base_path` + `bronze_relative_prefix()`
- **External Table Name**: Derived from entity type (e.g., `orders_events_external`, `orders_state_external`)
- **Partition Columns**: Derived from `record_time_partition` configuration
- **Sample Queries**: Generated based on entity kind (event vs. state)

## Running the DDL in SQL Server

### Prerequisites

1. SQL Server 2016+ with Polybase feature installed
2. Access to the Silver layer parquet files (local path or cloud storage)
3. Appropriate credentials if using cloud storage

### Execution Steps

1. **Generate DDL**:
   ```bash
   python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml
   ```

2. **Copy the generated SQL**:
   - Each pattern generates DDL for CREATE EXTERNAL DATA SOURCE, FILE FORMAT, and TABLE

3. **Execute in SSMS**:
   ```sql
   -- Paste the generated DDL and execute
   -- Creates external data source [silver_pattern1_full_events_events_source]
   -- Creates external file format [parquet_format]
   -- Creates external table [dbo].[orders_events_external]
   ```

4. **Query the External Table**:
   ```sql
   -- Now you can query like any other table
   SELECT * FROM [dbo].[orders_events_external]
   WHERE event_date = '2025-11-13'
   ORDER BY order_id
   LIMIT 100;
   ```

## Point-in-Time Query Examples

### Event Pattern Queries

#### Time-Range Event Queries
```sql
-- Query all events on a specific date
SELECT *
FROM [dbo].[orders_events_external]
WHERE event_date = '2025-11-13'
ORDER BY order_id, updated_at
LIMIT 100;

-- Query events over a date range
SELECT order_id, status, COUNT(*) as event_count
FROM [dbo].[orders_events_external]
WHERE event_date >= '2025-11-01'
GROUP BY order_id, status
ORDER BY event_count DESC;
```

#### Time-Window Aggregation
```sql
-- Daily event volume by order status
SELECT
    event_date,
    status,
    COUNT(*) as event_count,
    COUNT(DISTINCT order_id) as unique_orders
FROM [dbo].[orders_events_external]
WHERE event_date >= '2025-11-01'
GROUP BY event_date, status
ORDER BY event_date DESC, event_count DESC;
```

### State Pattern Queries

#### Point-in-Time State Snapshot
```sql
-- Get current state (as of today)
SELECT order_id, status, customer_id
FROM [dbo].[orders_state_external]
WHERE effective_from_date <= '2025-11-13'
AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-13')
ORDER BY order_id;

-- This query returns the effective state at a specific point in time
-- by selecting records where:
-- - effective_from_date <= target_date (change happened on or before target)
-- - effective_to_dt IS NULL OR effective_to_dt > target_date (still current or future)
```

#### Historical State Changes
```sql
-- Get all state changes for a specific order
SELECT
    order_id,
    status,
    customer_id,
    effective_from_dt,
    effective_to_dt,
    DATEDIFF(day, effective_from_dt, COALESCE(effective_to_dt, GETDATE())) as days_in_state
FROM [dbo].[orders_state_external]
WHERE order_id = 'ORD123'
ORDER BY effective_from_dt DESC;

-- This shows the complete audit trail of state changes
```

#### State Duration Analysis
```sql
-- How long did orders stay in each status?
SELECT
    status,
    COUNT(*) as change_count,
    AVG(DATEDIFF(day, effective_from_dt, COALESCE(effective_to_dt, GETDATE()))) as avg_days,
    MIN(DATEDIFF(day, effective_from_dt, COALESCE(effective_to_dt, GETDATE()))) as min_days,
    MAX(DATEDIFF(day, effective_from_dt, COALESCE(effective_to_dt, GETDATE()))) as max_days
FROM [dbo].[orders_state_external]
WHERE effective_from_date >= '2025-11-01'
GROUP BY status
ORDER BY avg_days DESC;
```

#### SCD Type 2 Temporal Joins
```sql
-- Compare state at two different points in time
WITH state_2025_11_01 AS (
    SELECT order_id, status, customer_id
    FROM [dbo].[orders_state_external]
    WHERE effective_from_date <= '2025-11-01'
    AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-01')
),
state_2025_11_13 AS (
    SELECT order_id, status, customer_id
    FROM [dbo].[orders_state_external]
    WHERE effective_from_date <= '2025-11-13'
    AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-13')
)
SELECT
    s1.order_id,
    s1.status as status_2025_11_01,
    s2.status as status_2025_11_13,
    CASE WHEN s1.status <> s2.status THEN 'CHANGED' ELSE 'UNCHANGED' END as status_change
FROM state_2025_11_01 s1
FULL OUTER JOIN state_2025_11_13 s2 ON s1.order_id = s2.order_id
WHERE s1.status <> s2.status OR s1.status IS NULL OR s2.status IS NULL
ORDER BY s1.order_id;
```

## Partition Pruning and Query Performance

### How Partition Pruning Works

Polybase uses partition column filters to avoid scanning unnecessary data:

```sql
-- GOOD: Partition column in WHERE clause
-- Polybase will only scan partitions matching the filter
SELECT * FROM [dbo].[orders_events_external]
WHERE event_date = '2025-11-13';

-- BETTER: Date range (if query pushdown supported)
-- Scans multiple specific partitions
SELECT * FROM [dbo].[orders_events_external]
WHERE event_date >= '2025-11-01' AND event_date <= '2025-11-13';

-- SUBOPTIMAL: Non-partition column filter only
-- May require scanning all partitions
SELECT * FROM [dbo].[orders_events_external]
WHERE order_id = 'ORD123';
```

### Best Practices

1. **Always filter by partition column** when possible to maximize performance
2. **Use explicit date literals** rather than functions for predictable partition pruning
3. **Combine date filters with business logic** for balanced query performance
4. **Understand your partition scheme** (event_date vs. effective_from_date)

## Configuration Reference

### Generating PolyBase DDL

Use the `pipelines.lib.polybase` module to generate DDL:

```python
from pipelines.lib.polybase import generate_from_metadata, PolyBaseConfig
from pathlib import Path

config = PolyBaseConfig(
    data_source_name="silver_source",
    data_source_location="wasbs://silver@account.blob.core.windows.net/",
)

# Generate DDL from Silver metadata
ddl = generate_from_metadata(Path("./silver/orders/_metadata.json"), config)
print(ddl)  # Outputs CREATE EXTERNAL TABLE and views
```

## Troubleshooting

### Issue: "Cannot open external data source"

**Cause**: The location path doesn't exist or credentials are missing.

**Solution**:
1. Verify the Silver layer path exists
2. For cloud storage, ensure credentials are configured
3. Update the LOCATION parameter in the CREATE EXTERNAL DATA SOURCE statement

### Issue: "Columns mismatch between external file and table definition"

**Cause**: The parquet file schema doesn't match the external table schema.

**Solution**:
1. Verify the attributes in your pattern YAML match the actual parquet files
2. Regenerate the DDL with the correct dataset configuration
3. Add missing columns as VARCHAR(255) with NULL defaults

### Issue: "Column 'event_date' not found in parquet file"

**Cause**: The partition column derivation is incorrect.

**Solution**:
1. Verify `record_time_partition` is set correctly in your pattern YAML
2. Verify `record_time_column` points to the actual timestamp column in the data
3. Check that the Silver processor correctly derives the partition column

## See Also

- [SQL Server Polybase Documentation](https://docs.microsoft.com/en-us/sql/relational-databases/polybase/polybase-guide)
