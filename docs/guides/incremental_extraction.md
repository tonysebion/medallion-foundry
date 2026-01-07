# Incremental Extraction Guide

This guide explains how to configure and use incremental extraction patterns in Bronze Foundry. Incremental extraction allows you to extract only new or changed data, significantly reducing extraction time and resource usage for large datasets.

## Overview

Incremental extraction uses **watermarks** to track progress and **checkpoints** to resume from failures. Bronze Foundry supports multiple incremental patterns:

| Pattern | Description | Use Case |
|---------|-------------|----------|
| `incremental_append` | Append new records only | Event logs, immutable facts |
| `incremental_merge` | Upsert by primary key | Mutable entities, order updates |
| `cdc` | Change Data Capture (alias for `incremental_append`) | Database change streams |

## Watermark Concepts

A **watermark** tracks the highest processed value of a column (typically a timestamp or sequence number). On subsequent runs, only records with values greater than the watermark are extracted.

### Watermark Types

| Type | Description | Example Column | Example Value |
|------|-------------|----------------|---------------|
| `timestamp` | ISO timestamp | `updated_at` | `2025-01-15T10:30:00Z` |
| `date` | Date only | `event_date` | `2025-01-15` |
| `integer` | Sequence number | `record_id` | `1000000` |
| `string` | Lexicographic | `partition_key` | `2025-01-15` |

### Watermark Storage

Watermarks are stored in JSON files, either locally or in cloud storage:

```
.state/
├── salesforce_accounts_watermark.json
├── postgres_orders_watermark.json
└── s3_events_watermark.json
```

**Watermark file structure:**
```json
{
  "source_key": "salesforce.accounts",
  "watermark_column": "SystemModstamp",
  "watermark_value": "2025-01-15T10:30:00Z",
  "watermark_type": "timestamp",
  "last_run_id": "run_2025-01-15_001",
  "last_run_date": "2025-01-15",
  "record_count": 1500,
  "created_at": "2025-01-10T08:00:00Z",
  "updated_at": "2025-01-15T10:30:00Z"
}
```

---

## Configuration

### Basic Incremental Configuration

```yaml
source:
  system: salesforce
  table: accounts
  type: api
  run:
    load_pattern: incremental_append
    watermark_column: SystemModstamp
    watermark_type: timestamp
```

### Watermark Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `watermark_column` | string | Required | Column to use for tracking progress |
| `watermark_type` | string | `timestamp` | Type: `timestamp`, `date`, `integer`, `string` |
| `watermark_initial` | string | None | Initial watermark value for first run |
| `watermark_lookback` | string | None | Lookback period for safety margin (e.g., `1h`, `1d`) |

### Reference Mode Configuration

Reference mode controls how Bronze handles incremental vs full extractions:

```yaml
source:
  run:
    load_pattern: incremental_append
    reference_mode:
      enabled: true
      role: delta          # 'reference', 'delta', or 'auto'
      cadence_days: 7      # Full refresh every N days (for 'auto')
      delta_patterns:      # Patterns that qualify as delta
        - incremental_*
        - cdc
```

**Reference Mode Roles:**

| Role | Behavior |
|------|----------|
| `reference` | Always produce full snapshots (ignore watermark) |
| `delta` | Always produce incremental deltas |
| `auto` | Alternate between full and delta based on `cadence_days` |

---

## Watermark Column Selection

### Best Practices

Choose watermark columns that:

1. **Never decrease** - Values should only increase over time
2. **Are indexed** - Improves query performance at source
3. **Have high cardinality** - Avoids many records with same value
4. **Are always populated** - No NULL values

### Common Patterns

**Timestamp columns (recommended):**
```yaml
watermark_column: updated_at      # Most common
watermark_column: modified_date   # Alternative naming
watermark_column: SystemModstamp  # Salesforce
watermark_column: _sdc_extracted_at  # Stitch/Fivetran
```

**Sequence columns:**
```yaml
watermark_column: id              # Auto-increment primary key
watermark_column: sequence_num    # Explicit sequence
watermark_column: version         # Record version number
```

**Date partitions:**
```yaml
watermark_column: event_date      # Date-partitioned tables
watermark_column: partition_date  # Partition column
```

---

## Source-Specific Examples

### API Sources

APIs typically support filtering via query parameters:

```yaml
source:
  system: stripe
  table: charges
  type: api
  api:
    endpoint: https://api.stripe.com/v1/charges
    method: GET
    pagination:
      type: cursor
      cursor_param: starting_after
    # Watermark passed as query parameter
    watermark_param: created[gte]
  run:
    load_pattern: incremental_append
    watermark_column: created
    watermark_type: integer  # Unix timestamp
```

**Salesforce example:**
```yaml
source:
  system: salesforce
  table: Account
  type: api
  api:
    endpoint: /services/data/v57.0/query
    method: GET
    # SOQL query with watermark filter
    query_template: |
      SELECT Id, Name, Industry, AnnualRevenue, SystemModstamp
      FROM Account
      WHERE SystemModstamp > {watermark}
      ORDER BY SystemModstamp ASC
  run:
    load_pattern: incremental_append
    watermark_column: SystemModstamp
    watermark_type: timestamp
```

### Database Sources

Databases use watermark in WHERE clause:

```yaml
source:
  system: postgres
  table: orders
  type: db
  db:
    connection_string: ${DB_CONNECTION_STRING}
    # Query with watermark filter
    query: |
      SELECT *
      FROM orders
      WHERE updated_at > :watermark
      ORDER BY updated_at ASC
    # Or use table scan with filter
    table: orders
    filter_column: updated_at
  run:
    load_pattern: incremental_merge
    watermark_column: updated_at
    watermark_type: timestamp
    primary_keys:
      - order_id
```

**MySQL CDC example:**
```yaml
source:
  system: mysql
  table: customers
  type: db
  db:
    connection_string: ${MYSQL_CONNECTION_STRING}
    query: |
      SELECT *
      FROM customers
      WHERE updated_at > :watermark
        OR (updated_at = :watermark AND id > :watermark_id)
      ORDER BY updated_at, id
  run:
    load_pattern: incremental_merge
    watermark_column: updated_at
    watermark_type: timestamp
    # Secondary watermark for tie-breaking
    secondary_watermark:
      column: id
      type: integer
```

### File Sources

File sources use modification time or filename patterns:

```yaml
source:
  system: s3_events
  table: clickstream
  type: file
  file:
    path: s3://data-lake/events/
    format: parquet
    # Use file modification time as watermark
    watermark_source: file_mtime
    # Or use filename pattern
    filename_pattern: "events_{date}.parquet"
  run:
    load_pattern: incremental_append
    watermark_column: event_timestamp
    watermark_type: timestamp
```

**Date-partitioned files:**
```yaml
source:
  system: data_lake
  table: transactions
  type: file
  file:
    path: s3://bucket/transactions/
    format: parquet
    # Partition discovery
    partition_columns:
      - year
      - month
      - day
    # Only process new partitions
    partition_watermark: true
  run:
    load_pattern: incremental_append
    watermark_column: transaction_date
    watermark_type: date
```

---

## Checkpoint and Recovery

### Checkpoint Behavior

Checkpoints track extraction progress for recovery:

1. **Before extraction**: Load existing watermark
2. **During extraction**: Track records processed
3. **After success**: Update watermark to max value
4. **On failure**: Watermark unchanged (safe retry)

### Recovery Scenarios

**Scenario 1: Extraction fails mid-batch**
```
Run 1: Extract records 1-1000, watermark=1000 ✓
Run 2: Extract records 1001-2000, FAILS at 1500
        Watermark stays at 1000 (unchanged)
Run 3: Retry extracts 1001-2000, watermark=2000 ✓
```

**Scenario 2: Duplicate handling**
```yaml
source:
  run:
    load_pattern: incremental_append
    # Small lookback for safety
    watermark_lookback: 1h
    # Deduplication in Silver layer
    dedup_on_silver: true
```

### Manual Checkpoint Management

**Reset watermark to re-extract:**
```bash
# Delete watermark file to start fresh
rm .state/salesforce_accounts_watermark.json
```

**Backfill historical data:**
```yaml
source:
  run:
    load_pattern: incremental_append
    # Override watermark for backfill
    backfill:
      enabled: true
      start_date: 2024-01-01
      end_date: 2024-12-31
      force_full: true  # Ignore existing watermark
```

---

## Checkpoint Storage Backends

### Local Storage (Default)

```yaml
platform:
  bronze:
    storage_backend: local
    local_path: ./output
    # Watermarks stored in ./output/_watermarks/
```

### S3 Storage

```yaml
platform:
  bronze:
    storage_backend: s3
    s3_bucket: my-data-bucket
    s3_prefix: bronze
    # Watermarks stored in s3://my-data-bucket/bronze/_watermarks/
```

### Azure Blob Storage

```yaml
platform:
  bronze:
    storage_backend: azure
    azure_container: bronze-data
    azure_prefix: extracts
    # Watermarks stored in azure://bronze-data/extracts/_watermarks/
```

---

## Handling Edge Cases

### Late-Arriving Data

Data that arrives after its timestamp:

```yaml
source:
  run:
    load_pattern: incremental_append
    watermark_column: event_time
    # Look back 1 hour to catch late data
    watermark_lookback: 1h
    late_data:
      mode: allow        # allow, reject, quarantine
      threshold_hours: 24
      quarantine_path: ./quarantine/
```

### Gaps in Data

Handle gaps in sequence columns:

```yaml
source:
  run:
    load_pattern: incremental_append
    watermark_column: sequence_id
    watermark_type: integer
    # Alert if gap > threshold
    gap_detection:
      enabled: true
      max_gap: 1000
      on_gap: warn  # warn, error, ignore
```

### Schema Changes

Handle evolving source schemas:

```yaml
source:
  run:
    load_pattern: incremental_merge
    schema_evolution:
      mode: allow_new_columns  # strict, allow_new_columns, ignore_unknown
      protected_columns:
        - id
        - created_at
```

---

## Monitoring and Troubleshooting

### Key Metrics

Monitor these for incremental extractions:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `watermark_lag` | Time since last watermark | > 24 hours |
| `records_per_run` | Records extracted per run | Sudden drops |
| `extraction_duration` | Time per extraction | > 2x baseline |
| `checkpoint_age` | Age of checkpoint file | > 48 hours |

### Common Issues

**Issue: Watermark not advancing**
```
Cause: No new records, or watermark column has NULLs
Fix: Check source data, ensure watermark column is populated
```

**Issue: Duplicate records**
```
Cause: Watermark lookback too large, or source has duplicates
Fix: Reduce lookback, add deduplication in Silver layer
```

**Issue: Missing records**
```
Cause: Watermark advanced too fast (batch failure mid-write)
Fix: Use transactions, add lookback margin
```

### Debug Commands

```bash
# View current watermark
cat .state/salesforce_accounts_watermark.json | python -m json.tool

# List all watermarks
ls -la .state/*_watermark.json

# Dry run to see what would be extracted
python bronze_extract.py --config config.yaml --dry-run

# Validate configuration
python bronze_extract.py --config config.yaml --validate-only
```

---

## Complete Example

Here's a complete configuration for incremental extraction from a PostgreSQL database:

```yaml
# config/incremental_orders.yaml
config_version: 1
environment: prod
domain: ecommerce

platform:
  bronze:
    storage_backend: s3
    s3_bucket: my-data-lake
    s3_prefix: bronze

source:
  system: postgres_prod
  table: orders
  type: db
  db:
    connection_string: ${POSTGRES_CONNECTION_STRING}
    query: |
      SELECT
        order_id,
        customer_id,
        order_total,
        status,
        created_at,
        updated_at
      FROM orders
      WHERE updated_at > :watermark
      ORDER BY updated_at ASC
      LIMIT 100000
  run:
    load_pattern: incremental_merge
    watermark_column: updated_at
    watermark_type: timestamp
    watermark_lookback: 5m
    primary_keys:
      - order_id
    reference_mode:
      enabled: true
      role: auto
      cadence_days: 7
    max_rows_per_file: 100000
    parallel_workers: 4

quality_rules:
  - id: order_id_not_null
    expression: "order_id IS NOT NULL"
    level: error
  - id: valid_order_total
    expression: "order_total >= 0"
    level: warn
```

**Run the extraction:**
```bash
# Initial run (full extraction)
python bronze_extract.py --config config/incremental_orders.yaml --date 2025-01-15

# Subsequent runs (incremental from watermark)
python bronze_extract.py --config config/incremental_orders.yaml --date 2025-01-16
```

---

## Related Documentation

- [Load Patterns Guide](./load_patterns.md) - Pattern behaviors and selection
- [Error Handling Guide](./error_handling.md) - Retry and resilience
- [API Rate Limiting](./api_rate_limiting.md) - API throttling configuration
- [Configuration Reference](../reference/config_reference.md) - Full config options
