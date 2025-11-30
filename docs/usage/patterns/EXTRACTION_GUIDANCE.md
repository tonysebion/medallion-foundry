# Bronze + Silver Pattern Guidance

This guide shows how all 7 Bronze extraction patterns work together with Silver asset models. Each Bronze pattern has compatible Silver models that optimize for different analytical needs.

## Pattern + Model Combinations

### 1. Full Snapshot Pattern

**Bronze Pattern**: `full` - Complete dataset refresh
**When to use**: Daily/weekly exports, small-to-medium datasets, clean snapshots

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `periodic_snapshot` | **Default choice** - Simple pass-through | `model: periodic_snapshot` |
| `full_merge_dedupe` | Remove duplicates, keep latest | `model: full_merge_dedupe`<br>`natural_keys: ["id"]`<br>`order_column: "updated_at"` |
| `scd_type_1` | Current state only | `model: scd_type_1`<br>`natural_keys: ["id"]`<br>`order_column: "updated_at"` |

**Example**: Product catalog updated daily
```yaml
source:
  run:
    load_pattern: full
silver:
  model: periodic_snapshot  # Simple refresh
```

### 2. CDC Pattern

**Bronze Pattern**: `cdc` - Change events only (inserts/updates/deletes)
**When to use**: High-volume transaction data, audit trails, real-time updates

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `incremental_merge` | **Default choice** - Apply change events | `model: incremental_merge`<br>`natural_keys: ["order_id"]`<br>`event_ts_column: "changed_at"` |
| `full_merge_dedupe` | Build current state from changes | `model: full_merge_dedupe`<br>`natural_keys: ["order_id"]`<br>`order_column: "changed_at"` |

**Example**: Order status updates
```yaml
source:
  run:
    load_pattern: cdc
silver:
  model: incremental_merge
  natural_keys: ["order_id"]
  event_ts_column: "order_date"
  change_type_column: "operation"  # INSERT, UPDATE, DELETE
```

### 3. Current + History Pattern

**Bronze Pattern**: `current_history` - SCD Type 2 style with active/inactive records
**When to use**: Slowly changing dimensions, master data with history requirements

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `scd_type_2` | **Default choice** - Full history + current view | `model: scd_type_2`<br>`natural_keys: ["customer_id"]`<br>`change_ts_column: "updated_at"` |
| `incremental_merge` | Raw change timeline | `model: incremental_merge`<br>`natural_keys: ["customer_id"]`<br>`event_ts_column: "updated_at"` |

**Example**: Customer address changes
```yaml
source:
  run:
    load_pattern: current_history
silver:
  model: scd_type_2
  natural_keys: ["customer_id"]
  change_ts_column: "updated_at"
```

### 4. CDC Hybrid Point-in-Time Pattern

**Bronze Pattern**: `cdc` with reference mode (point-in-time)
**When to use**: Reference data + delta changes, point-in-time views

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `incremental_merge` | **Default choice** - Reference + deltas | `model: incremental_merge`<br>`natural_keys: ["customer_id"]`<br>`event_ts_column: "changed_at"` |

**Example**: Customer master + recent updates
```yaml
source:
  run:
    load_pattern: cdc
    reference_mode:
      role: reference
      cadence_days: 7
silver:
  model: incremental_merge
  natural_keys: ["customer_id"]
  event_ts_column: "changed_at"
```

### 5. CDC Hybrid Cumulative Pattern

**Bronze Pattern**: `cdc` with cumulative deltas
**When to use**: Layered cumulative change files, complete change history

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `incremental_merge` | **Default choice** - Cumulative changes | `model: incremental_merge`<br>`natural_keys: ["order_id"]`<br>`event_ts_column: "changed_at"` |

**Example**: Complete audit trail with accumulation
```yaml
source:
  run:
    load_pattern: cdc
    delta_mode: cumulative
silver:
  model: incremental_merge
  natural_keys: ["order_id"]
  event_ts_column: "changed_at"
```

### 6. Incremental Point-in-Time Pattern

**Bronze Pattern**: `cdc` incremental with latest value wins
**When to use**: Incremental snapshots, status updates, counters

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `full_merge_dedupe` | **Default choice** - Latest values | `model: full_merge_dedupe`<br>`natural_keys: ["user_id"]`<br>`order_column: "updated_at"` |

**Example**: User status updates (latest wins)
```yaml
source:
  run:
    load_pattern: cdc
    delta_mode: point_in_time
silver:
  model: full_merge_dedupe
  natural_keys: ["user_id"]
  order_column: "updated_at"
```

### 7. Incremental SCD1 Cumulative Pattern

**Bronze Pattern**: `cdc` incremental with current state overwrites
**When to use**: Settings, configurations, current state only

| Silver Model | Use Case | Configuration |
|-------------|----------|---------------|
| `scd_type_1` | **Default choice** - Current overwrites | `model: scd_type_1`<br>`natural_keys: ["user_id"]`<br>`order_column: "updated_at"` |

**Example**: User profile settings (latest overwrites)
```yaml
source:
  run:
    load_pattern: cdc
    delta_mode: cumulative
silver:
  model: scd_type_1
  natural_keys: ["user_id"]
  order_column: "updated_at"
```

## Decision Framework

### Choose Bronze Pattern First

| Data Characteristics | Bronze Pattern | Key Indicators |
|---------------------|----------------|----------------|
| **Complete refreshes** | `full` | - Daily exports<br>- Small datasets<br>- No change tracking |
| **Change events** | `cdc` | - Transaction logs<br>- High volume<br>- Insert/update/delete events |
| **Master data with history** | `current_history` | - Customer profiles<br>- Product catalogs<br>- Address changes |
| **Reference + deltas** | `cdc` (hybrid point) | - Master data + changes<br>- Point-in-time views<br>- Reference cadence |
| **Cumulative changes** | `cdc` (hybrid cumulative) | - Audit trails<br>- Complete history<br>- Change accumulation |
| **Latest values** | `cdc` (incremental point) | - Status updates<br>- Counters<br>- Latest wins |
| **Current overwrites** | `cdc` (incremental cumulative) | - Settings<br>- Configurations<br>- SCD Type 1 |

### Then Choose Silver Model

| Analytical Need | Silver Model | Best With Bronze Pattern |
|----------------|--------------|-------------------------|
| **Simple refresh** | `periodic_snapshot` | `full` |
| **Current state only** | `scd_type_1`, `full_merge_dedupe` | `full`, `cdc` (incremental) |
| **Change events** | `incremental_merge` | `cdc` variants |
| **Full history** | `scd_type_2` | `current_history`, `cdc` |

## Configuration Examples

### E-commerce Platform

```yaml
# Orders (transaction data)
source:
  system: ecomm
  table: orders
  run:
    load_pattern: cdc
silver:
  model: incremental_merge
  natural_keys: ["order_id"]
  event_ts_column: "order_date"

# Customers (master data)
source:
  system: ecomm
  table: customers
  run:
    load_pattern: current_history
silver:
  model: scd_type_2
  natural_keys: ["customer_id"]
  change_ts_column: "updated_at"

# Products (reference data)
source:
  system: ecomm
  table: products
  run:
    load_pattern: full
silver:
  model: full_merge_dedupe
  natural_keys: ["product_id"]
  order_column: "last_modified"

# User sessions (events)
source:
  system: ecomm
  table: sessions
  run:
    load_pattern: cdc
    delta_mode: point_in_time
silver:
  model: full_merge_dedupe
  natural_keys: ["session_id"]
  order_column: "last_activity"

# User profiles (current state)
source:
  system: ecomm
  table: profiles
  run:
    load_pattern: cdc
    delta_mode: cumulative
silver:
  model: scd_type_1
  natural_keys: ["user_id"]
  order_column: "updated_at"
```

### Healthcare System

```yaml
# Patient visits (events)
source:
  system: health
  table: visits
  run:
    load_pattern: cdc
silver:
  model: incremental_merge
  natural_keys: ["visit_id"]
  event_ts_column: "visit_date"

# Patient records (master data)
source:
  system: health
  table: patients
  run:
    load_pattern: current_history
silver:
  model: scd_type_2
  natural_keys: ["patient_id"]
  change_ts_column: "updated_at"

# Provider directory (reference)
source:
  system: health
  table: providers
  run:
    load_pattern: full
silver:
  model: periodic_snapshot

# Lab results (cumulative)
source:
  system: health
  table: lab_results
  run:
    load_pattern: cdc
    delta_mode: cumulative
silver:
  model: incremental_merge
  natural_keys: ["result_id"]
  event_ts_column: "result_date"

# Appointments (current status)
source:
  system: health
  table: appointments
  run:
    load_pattern: cdc
    reference_mode:
      role: reference
      cadence_days: 7
silver:
  model: incremental_merge
  natural_keys: ["appointment_id"]
  event_ts_column: "scheduled_date"
```

## Performance Considerations

### Bronze Pattern Performance

- **`full`**: Simple but scales linearly with data size
- **`cdc`**: Efficient for high-change scenarios, smaller storage footprint
- **`current_history`**: Moderate overhead for history tracking

### Silver Model Performance

- **`periodic_snapshot`**: Lowest processing cost, highest storage for large datasets
- **`incremental_merge`**: Optimal for CDC streams, preserves change semantics
- **`scd_type_2`**: Higher storage cost due to history retention
- **`full_merge_dedupe`**: Good balance for moderate change volumes

## Migration Between Patterns

### From Full to CDC

```yaml
# Before (full snapshots)
source:
  run:
    load_pattern: full
silver:
  model: periodic_snapshot

# After (CDC events)
source:
  run:
    load_pattern: cdc
silver:
  model: incremental_merge
  natural_keys: ["id"]
  event_ts_column: "changed_at"
```

### From CDC to Current + History

```yaml
# Before (change events)
source:
  run:
    load_pattern: cdc
silver:
  model: incremental_merge

# After (SCD Type 2)
source:
  run:
    load_pattern: current_history
silver:
  model: scd_type_2
  natural_keys: ["id"]
  change_ts_column: "updated_at"
```

## Validation and Monitoring

After implementing a pattern + model combination:

1. **Check data completeness**: Compare Bronze vs Silver record counts
2. **Validate key constraints**: Natural keys should be unique in current views
3. **Verify temporal ordering**: Timestamps should be valid and sequential
4. **Monitor performance**: Track processing time and storage growth
5. **Test edge cases**: Handle null keys, duplicate events, schema changes

Use the generated `_metadata.json` files to verify processing integrity and track lineage from Bronze to Silver.
