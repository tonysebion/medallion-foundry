# Silver Models Explained

This guide explains the different Silver asset models available in medallion-foundry and when to use each one.

## Overview

Silver models determine how Bronze data is transformed into curated datasets. Each model serves different analytical needs and handles data differently.

## Available Models

### 1. periodic_snapshot
**Use Case**: Simple periodic refreshes, complete dataset snapshots

**Behavior**:
- Takes the entire Bronze partition as-is
- No deduplication or transformation
- Preserves all source data exactly

**When to Use**:
- Source provides clean, complete snapshots
- No need for historical tracking
- Simple reporting on latest state

**Example Output**:
```
silver/domain=retail/entity=orders/v1/load_date=2025-11-27/
  orders_snapshot.parquet
```

### 2. full_merge_dedupe
**Use Case**: Deduplicated current view from full snapshots

**Behavior**:
- Deduplicates by natural keys
- Keeps most recent record per key
- Orders by specified timestamp column

**When to Use**:
- Full snapshots with potential duplicates
- Need current state without history
- SCD Type 1 style updates

**Configuration**:
```yaml
silver:
  natural_keys: ["order_id"]
  order_column: "updated_at"
  model: full_merge_dedupe
```

### 3. incremental_merge
**Use Case**: Incremental changes from CDC streams

**Behavior**:
- Processes change events (insert/update/delete)
- Builds timeline of changes
- Preserves all historical changes

**When to Use**:
- CDC/event streams
- Need to track all changes over time
- Point-in-time analysis

**Configuration**:
```yaml
silver:
  natural_keys: ["order_id"]
  event_ts_column: "changed_at"
  change_type_column: "change_type"  # optional
  model: incremental_merge
```

### 4. scd_type_1
**Use Case**: Current state only, overwrite old values

**Behavior**:
- Keeps only latest record per natural key
- Overwrites previous versions
- No historical tracking

**When to Use**:
- Current state is all that's needed
- Historical changes not required
- Simple dimensional data

**Configuration**:
```yaml
silver:
  natural_keys: ["customer_id"]
  order_column: "updated_at"
  model: scd_type_1
```

### 5. scd_type_2
**Use Case**: Full history with current flags (Slowly Changing Dimension Type 2)

**Behavior**:
- Maintains complete historical record
- Adds `is_current` flag to indicate active records
- Adds effective date ranges

**When to Use**:
- Need both current and historical views
- Auditing and compliance requirements
- Slowly changing dimensions

**Configuration**:
```yaml
silver:
  natural_keys: ["customer_id"]
  change_ts_column: "updated_at"
  model: scd_type_2
```

**Output Structure**:
```
silver/domain=retail/entity=customers/v1/load_date=2025-11-27/
  customers_current.parquet    # is_current = 1
  customers_history.parquet    # All records with effective dates
```

## Model Selection Guide

| Data Pattern | Recommended Model | Reasoning |
|-------------|------------------|-----------|
| Full snapshots | `periodic_snapshot` or `full_merge_dedupe` | Simple periodic loads |
| CDC events | `incremental_merge` | Track all changes |
| Current + history | `scd_type_2` | Full historical tracking |
| Dimension tables | `scd_type_1` or `scd_type_2` | Depends on history needs |

## Configuration Options

### Common Settings

```yaml
silver:
  model: scd_type_2  # Choose your model

  # Key identification
  natural_keys: ["id"]  # Unique business keys

  # Temporal columns
  event_ts_column: "event_time"      # When event occurred
  change_ts_column: "updated_at"     # When record changed
  record_time_column: "created_at"   # Source record timestamp

  # Ordering
  order_column: "updated_at"         # Sort order for deduplication

  # Output control
  write_parquet: true
  write_csv: false
  parquet_compression: "snappy"
```

### Advanced Options

```yaml
silver:
  # Schema transformations
  schema:
    rename_map:
      old_name: new_name
    column_order: ["col1", "col2"]

  # Data quality
  normalization:
    trim_strings: true
    empty_strings_as_null: true

  # Error handling
  error_handling:
    enabled: true
    max_bad_records: 100
    max_bad_percent: 5.0

  # Partitioning
  partitioning:
    columns: ["status", "region"]
```

## Model Profiles

For convenience, you can use named profiles that set common configurations:

```yaml
silver:
  model_profile: analytics  # Sets model to scd_type_2 with common settings
```

Available profiles:
- `analytics`: Full history with current views
- `operational`: Current state only
- `merge_ready`: Deduplicated for merging
- `cdc_delta`: Incremental changes
- `snapshot`: Periodic snapshots

## Examples by Use Case

### E-commerce Orders
```yaml
silver:
  model: incremental_merge
  natural_keys: ["order_id"]
  event_ts_column: "order_date"
  change_type_column: "operation"
```

### Customer Master Data
```yaml
silver:
  model: scd_type_2
  natural_keys: ["customer_id"]
  change_ts_column: "updated_at"
```

### Product Catalog
```yaml
silver:
  model: full_merge_dedupe
  natural_keys: ["product_id"]
  order_column: "last_modified"
```

## Performance Considerations

- **incremental_merge**: Best for high-volume CDC streams
- **scd_type_2**: Higher storage cost due to history retention
- **full_merge_dedupe**: Good balance for moderate change volumes
- **periodic_snapshot**: Lowest processing overhead

## Monitoring and Validation

After running Silver promotion, validate:

1. **Record counts**: Compare Bronze vs Silver
2. **Key uniqueness**: Natural keys should be unique in current views
3. **Temporal consistency**: Dates should be valid and in order
4. **Data quality**: Check for nulls in required fields

Use the generated `_metadata.json` and `_checksums.json` files to verify processing.