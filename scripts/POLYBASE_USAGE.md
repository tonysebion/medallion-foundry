# Polybase DDL Generation - Quick Start Guide

## Overview

The `generate_polybase_ddl.py` script automatically generates SQL Server Polybase external table definitions from Bronze-Foundry dataset YAML configurations.

## Installation

No additional dependencies required beyond the Bronze-Foundry environment:

```bash
# Already included in requirements:
pip install pyyaml
```

## Basic Usage

### Generate DDL for a Single Pattern

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml
```

Output: SQL DDL printed to stdout ready to copy/paste into SSMS

### Generate DDL for All Patterns

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_*.yaml
```

Output: DDL for all 7 patterns, separated by `================` markers

### Save DDL to Files

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_*.yaml -o ./ddl_output
```

Output: Each pattern generates a SQL file named `{system}.{entity}_polybase.sql`

## What Gets Generated

For each pattern, the script generates:

### 1. External Data Source
```sql
CREATE EXTERNAL DATA SOURCE [silver_pattern1_full_events_events_source]
WITH (
    TYPE = HADOOP,
    LOCATION = '/sampledata/silver_samples/env=dev/system=retail_demo/entity=orders/'
);
```

### 2. External File Format
```sql
CREATE EXTERNAL FILE FORMAT [parquet_format]
WITH (
  FORMAT_TYPE = PARQUET,
  COMPRESSION = 'SNAPPY'
);
```

### 3. External Table
```sql
CREATE EXTERNAL TABLE [dbo].[orders_events_external] (
    [status] VARCHAR(255),
    [order_total] VARCHAR(255),
    [event_date] VARCHAR(255)
)
WITH (
    LOCATION = 'pattern1_full_events/',
    DATA_SOURCE = [silver_pattern1_full_events_events_source],
    FILE_FORMAT = [parquet_format],
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0
);
```

### 4. Sample Queries (as SQL comments)
```sql
-- Sample Queries for orders_events_external
-- Point-in-time queries to analyze temporal data:
-- Query 1:
-- SELECT * FROM dbo.orders_events_external WHERE event_date = '2025-11-13' ORDER BY order_id, updated_at LIMIT 100
-- Query 2:
-- SELECT order_id, COUNT(*) as event_count FROM dbo.orders_events_external WHERE event_date >= '2025-11-01' GROUP BY order_id
```

## Customization

The DDL is generated automatically based on the dataset configuration. To customize:

### 1. Custom Schema Name

Modify the Python script to call `generate_polybase_setup()` with a custom schema:

```python
from core.polybase import generate_polybase_setup

polybase = generate_polybase_setup(
    dataset,
    schema_name="silver"  # Use "silver" instead of "dbo"
)
```

### 2. Custom Data Source Location

Override the default location:

```python
polybase = generate_polybase_setup(
    dataset,
    external_data_source_location="wasbs://container@account.blob.core.windows.net/silver/"
)
```

### 3. Custom Data Source Name

Override the auto-generated name:

```python
polybase = generate_polybase_setup(
    dataset,
    external_data_source_name="MyCustomDataSource"
)
```

## Workflow

### Step 1: Generate DDL

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml > pattern_full.sql
```

### Step 2: Review Generated SQL

Open `pattern_full.sql` and verify:
- Data source location is correct
- External table schema matches your parquet files
- Partition columns are as expected

### Step 3: Execute in SSMS

1. Open SQL Server Management Studio
2. Connect to your SQL Server instance
3. Create a new query window
4. Copy/paste the generated SQL
5. Execute (F5)

### Step 4: Test Query

```sql
-- Test the external table
SELECT TOP 100 * FROM [dbo].[orders_events_external]
WHERE event_date = '2025-11-13'
ORDER BY order_id;
```

## Pattern-Specific Behavior

### Event Patterns (full, cdc, hybrid_cdc_point)
- External table name: `{entity}_events_external`
- Partition column: `event_date` (derived from event_ts_column)
- Sample queries: Time-range and aggregation examples

### State Patterns (current_history, hybrid_cdc_cumulative, hybrid_incremental_*)
- External table name: `{entity}_state_external`
- Partition column: `effective_from_date` (derived from change_ts_column)
- Sample queries: Point-in-time state and temporal join examples

## Command Line Reference

```bash
usage: generate_polybase_ddl.py [-h] [-o OUTPUT_DIR] yaml_files [yaml_files ...]

Generate SQL DDL for Polybase external tables from dataset YAML configs

positional arguments:
  yaml_files            Path(s) to dataset YAML configuration file(s)

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory for generated SQL files (default: stdout)
```

## Examples

### Generate for all patterns and save to ddl/ directory

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_*.yaml -o ./ddl
```

### Generate for a custom dataset configuration

```bash
python scripts/generate_polybase_ddl.py path/to/my_dataset.yaml
```

### Generate and pipe to a file

```bash
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml > my_external_table.sql
```

### Generate and compare event vs state patterns

```bash
# Event pattern
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_full.yaml

# State pattern
python scripts/generate_polybase_ddl.py docs/examples/configs/patterns/pattern_current_history.yaml
```

## Troubleshooting

### Error: "Dataset configuration must be a dictionary"

**Cause**: The YAML file is not valid.

**Solution**:
1. Validate the YAML: `python -c "import yaml; yaml.safe_load(open('file.yaml'))"`
2. Ensure file uses proper YAML indentation (2 spaces)

### Error: "silver section must be a dictionary"

**Cause**: Missing or invalid silver configuration.

**Solution**:
1. Verify pattern YAML has a `silver:` section
2. Check that `silver:` has proper indentation

### Error: "silver.entity_kind is required"

**Cause**: Missing entity_kind in silver configuration.

**Solution**: Add `entity_kind: event` or `entity_kind: state` to your pattern YAML

## See Also

- [Polybase Integration Guide](../docs/framework/polybase_integration.md)
- [Pattern Configuration Reference](../docs/examples/configs/patterns/README.md)
- [Silver Patterns Framework](../docs/framework/silver_patterns.md)
