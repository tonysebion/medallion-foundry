# Model Selection Guide

This guide helps you choose the right Bronze and Silver patterns for your data pipeline. Understanding these patterns is essential for building pipelines that produce correct results over time.

## Key Concept: Data Accumulates Over Time

When you run a pipeline daily, Bronze creates a new partition each day:

```
Day 1: bronze/dt=2025-01-15/  →  Silver reads and produces output
Day 2: bronze/dt=2025-01-16/  →  Silver reads and produces output
Day 3: bronze/dt=2025-01-17/  →  Silver reads and produces output
...
```

**Critical Question:** When Silver runs on Day 3, which Bronze partitions does it read?

The answer depends on your configuration, and getting it wrong can cause **data loss** or **duplicate data**.

---

## Bronze Load Patterns

Bronze `load_pattern` controls **what goes INTO each daily partition**.

| Pattern | Each Partition Contains | Use When |
|---------|------------------------|----------|
| **full_snapshot** | ALL current records from source | Small-medium tables; source doesn't track changes |
| **incremental** | Only NEW/CHANGED records since last run | Source has `updated_at` column; large tables |
| **cdc** | Insert/Update/Delete operations with op codes | Source provides CDC stream (Debezium, SQL Server CT) |

### full_snapshot - Complete Table Each Run

```
Source table has 1000 customers

Day 1: bronze/dt=01-15/ → 1000 rows (all customers)
Day 2: bronze/dt=01-16/ → 1003 rows (all customers, 3 new)
Day 3: bronze/dt=01-17/ → 1003 rows (all customers, some updated)

Each partition is a complete snapshot - data overlaps between partitions!
```

**When to use:**
- Small dimension tables (< 100K rows)
- Source system doesn't track modification timestamps
- You want the simplest possible configuration

**Configuration:**
```yaml
bronze:
  load_pattern: full_snapshot
  # No watermark needed - extracts everything each run
```

### incremental - Only Changed Records

```
Source table has 1000 customers with updated_at column

Day 1: bronze/dt=01-15/ → 1000 rows (initial load, watermark=NULL)
Day 2: bronze/dt=01-16/ → 8 rows (only records changed since Day 1)
Day 3: bronze/dt=01-17/ → 5 rows (only records changed since Day 2)

Partitions are additive - no overlap, only deltas!
```

**When to use:**
- Large tables (> 100K rows)
- Source has a reliable `updated_at` or `modified_date` column
- You want efficient incremental loads

**Configuration:**
```yaml
bronze:
  load_pattern: incremental
  incremental_column: updated_at  # Required! Column to track changes
```

### cdc - Change Data Capture Stream

```
CDC stream from Debezium or SQL Server Change Tracking

Day 1: bronze/dt=01-15/ → 1000 inserts (op='I')
Day 2: bronze/dt=01-16/ → 50 changes (40 updates op='U', 8 inserts op='I', 2 deletes op='D')
Day 3: bronze/dt=01-17/ → 12 changes (3 updates, 9 inserts)

Partitions contain change OPERATIONS, not full records!
```

**When to use:**
- Source provides CDC stream (Debezium, Kafka Connect, SQL Server CT)
- You need to track deletes
- You want true change-by-change audit trail

**Configuration:**
```yaml
bronze:
  load_pattern: cdc
  cdc_operation_column: op  # Column containing I/U/D codes
```

---

## Silver Models

Silver `model` controls **how Silver reads Bronze partitions** and **what output it produces**.

| Model | Reads Partitions | Output | Use For |
|-------|-----------------|--------|---------|
| **periodic_snapshot** | Today only | Today's data | Simple dimension refresh |
| **full_merge_dedupe** | All partitions | Current state (SCD1) | Accumulated changes → latest version |
| **scd_type_2** | All partitions | Full history | Audit trail with effective dates |
| **event_log** | All partitions | All events | Immutable event streams |
| **cdc_current** | All partitions | Current state | CDC → current state |
| **cdc_history** | All partitions | Full history | CDC → full audit trail |

### periodic_snapshot - Simple Table Refresh

Silver reads **only today's Bronze partition** and outputs it directly.

```
Day 3: Silver reads bronze/dt=01-17/ only
       Outputs: Whatever was in today's Bronze partition
```

**Use when:** Bronze is `full_snapshot` and you just want the latest snapshot.

**Configuration:**
```yaml
silver:
  model: periodic_snapshot
  # unique_columns and last_updated_column are OPTIONAL for this model
```

### full_merge_dedupe - Accumulated Current State (SCD Type 1)

Silver reads **ALL Bronze partitions**, unions them, and deduplicates to keep the latest version of each record.

```
Day 3: Silver reads bronze/dt=01-15/ UNION dt=01-16/ UNION dt=01-17/
       Total rows: 1000 + 8 + 5 = 1013 rows
       After dedupe by unique_columns: ~1008 unique records (latest version of each)
```

**Use when:** Bronze is `incremental` and you want current state.

**Configuration:**
```yaml
silver:
  model: full_merge_dedupe
  unique_columns: [customer_id]        # Required - what makes a record unique
  last_updated_column: updated_at     # Required - which version is newest
```

### scd_type_2 - Full History with Effective Dates

Silver reads **ALL Bronze partitions** and builds a history table with effective dates.

```
Day 3: Silver reads all partitions
       Output includes ALL versions of each record:

       | customer_id | name  | effective_from | effective_to | is_current |
       |-------------|-------|----------------|--------------|------------|
       | 1           | John  | 2025-01-15     | 2025-01-16   | false      |
       | 1           | Johnny| 2025-01-16     | NULL         | true       |
```

**Use when:** You need audit history or point-in-time queries.

**Configuration:**
```yaml
silver:
  model: scd_type_2
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

### event_log - Immutable Event Stream

Silver reads **ALL Bronze partitions** and deduplicates exact duplicates only.

```
Day 3: Silver reads all event partitions
       Keeps all unique events (by event_id)
       Does NOT merge/update - events are immutable
```

**Use when:** Loading immutable events (orders, clicks, logs).

**Configuration:**
```yaml
silver:
  model: event_log
  unique_columns: [event_id]
  last_updated_column: event_time
```

### CDC Models - For CDC Bronze Streams

| Model | Deletes Handling | History |
|-------|-----------------|---------|
| `cdc_current` | Ignored | Current only |
| `cdc_current_tombstone` | Soft delete (`_deleted=true`) | Current only |
| `cdc_current_hard_delete` | Removed from output | Current only |
| `cdc_history` | Ignored | Full history |
| `cdc_history_tombstone` | Soft delete in history | Full history |
| `cdc_history_hard_delete` | Removed from output | Full history |

**Configuration:**
```yaml
bronze:
  load_pattern: cdc
  cdc_operation_column: op

silver:
  model: cdc_current_tombstone  # or any cdc_* model
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

---

## Compatibility Matrix

Not all Bronze + Silver combinations work correctly:

| Bronze Pattern | periodic_snapshot | full_merge_dedupe | scd_type_2 | event_log | cdc_* |
|---------------|:-----------------:|:-----------------:|:----------:|:---------:|:-----:|
| **full_snapshot** | ✅ Perfect | ⚠️ Warning | ⚠️ Warning | ⚠️ Warning | ❌ Error |
| **incremental** | ❌ Error | ✅ Perfect | ✅ Perfect | ✅ Perfect | ❌ Error |
| **cdc** | ❌ Error | ⚠️ Warning | ⚠️ Warning | ⚠️ Warning | ✅ Perfect |

**Legend:**
- ✅ Perfect - Recommended combination
- ⚠️ Warning - Works but may not be what you want
- ❌ Error - Invalid combination, blocked by validation

### Why full_snapshot + full_merge_dedupe is Inefficient (Warning)

```
Day 1: Bronze full_snapshot → 1000 customers
Day 2: Bronze full_snapshot → 1003 customers (overlapping!)

Silver with full_merge_dedupe reads ALL partitions:
  - Unions: 1000 + 1003 = 2003 rows
  - Deduplicates to 1003 rows (correct result!)
  - BUT: Has to dedupe overlapping data every single run
  - AND: Bronze storage grows unbounded with duplicate data

This WORKS but is INEFFICIENT.

BETTER: Use periodic_snapshot (just use today's snapshot)
    OR: Change Bronze to incremental (no overlap)
```

### Why incremental + periodic_snapshot Causes Data Loss (Error)

This combination is **blocked by validation** because it causes data loss.

```
Day 1: Bronze incremental → 1000 customers (initial load)
Day 2: Bronze incremental → 8 customers (only changes)
Day 3: Bronze incremental → 5 customers (only changes)

Silver with periodic_snapshot reads ONLY today's partition:
  Day 3: Reads 5 rows → Outputs 5 rows

PROBLEM: You lost 995+ customers! They were loaded on Day 1
         but Silver only sees today's 5 changes.

SOLUTION: Use full_merge_dedupe (reads ALL partitions)
```

---

## Decision Flowchart

```
START: What kind of source data do you have?
│
├─► "Database table - I extract all rows each run"
│   │
│   └─► Is the table small? (< 100K rows)
│       ├─► YES → full_snapshot + periodic_snapshot
│       └─► NO → Does it have an updated_at column?
│           ├─► YES → incremental + full_merge_dedupe
│           └─► NO → full_snapshot + periodic_snapshot (accept the cost)
│
├─► "Database table - I only get changed records"
│   │
│   └─► Do you need to track history?
│       ├─► NO → incremental + full_merge_dedupe
│       └─► YES → incremental + scd_type_2
│
├─► "Event stream (orders, clicks, logs)"
│   │
│   └─► Are events immutable (never updated)?
│       ├─► YES → incremental + event_log
│       └─► NO → Treat as state: incremental + full_merge_dedupe
│
└─► "CDC stream with Insert/Update/Delete markers"
    │
    └─► What should happen to deleted records?
        ├─► Ignore them → cdc + cdc_current
        ├─► Mark as deleted → cdc + cdc_current_tombstone
        ├─► Remove from Silver → cdc + cdc_current_hard_delete
        │
        └─► Need full history? Use cdc_history variants instead
```

---

## Common Use Cases

### 1. Small Dimension Table (Customers, Products)

Full extract, simple refresh each day.

```yaml
name: daily_customers
bronze:
  system: crm
  entity: customers
  source_type: database_mssql
  load_pattern: full_snapshot
  query: SELECT * FROM Customers

silver:
  model: periodic_snapshot
```

### 2. Large Table with Timestamps

Incremental extract, accumulate to current state.

```yaml
name: incremental_orders
bronze:
  system: sales
  entity: orders
  source_type: database_mssql
  load_pattern: incremental
  incremental_column: updated_at
  query: SELECT * FROM Orders WHERE updated_at > '{watermark}'

silver:
  model: full_merge_dedupe
  unique_columns: [order_id]
  last_updated_column: updated_at
```

### 3. Audit Trail with Full History

Track all changes with effective dates.

```yaml
name: customer_history
bronze:
  system: crm
  entity: customers
  source_type: database_mssql
  load_pattern: incremental
  incremental_column: modified_date

silver:
  model: scd_type_2
  unique_columns: [customer_id]
  last_updated_column: modified_date
```

### 4. Immutable Event Log

Accumulate events, dedupe exact duplicates.

```yaml
name: order_events
bronze:
  system: orders
  entity: events
  source_type: api_rest
  load_pattern: incremental
  incremental_column: event_time

silver:
  model: event_log
  unique_columns: [event_id]
  last_updated_column: event_time
```

### 5. CDC Stream with Soft Deletes

Process CDC with delete tracking.

```yaml
name: cdc_customers
bronze:
  system: crm
  entity: customers
  source_type: file_csv  # Or database with CDC
  load_pattern: cdc
  cdc_operation_column: op

silver:
  model: cdc_current_tombstone
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

---

## Querying Silver Output

### Current State (full_merge_dedupe, cdc_current)

```sql
-- Direct query - all records are current
SELECT * FROM silver_customers
```

### Point-in-Time (scd_type_2, cdc_history)

```sql
-- State as of a specific date
SELECT * FROM silver_customers_history
WHERE effective_from_date <= '2025-01-15'
  AND (effective_to_date IS NULL OR effective_to_date > '2025-01-15')
```

### Current Records Only (scd_type_2)

```sql
-- Only current versions
SELECT * FROM silver_customers_history
WHERE is_current = true
```

---

## Troubleshooting

### "My Silver output is missing data"

**Symptom:** Silver only shows a few rows, expected thousands.

**Cause:** Using `periodic_snapshot` with `incremental` Bronze.

**Fix:** Change Silver to `full_merge_dedupe` to read all Bronze partitions.

### "My Silver output has duplicates"

**Symptom:** Same record appears multiple times with different timestamps.

**Cause:** Either:
1. Missing `unique_columns` - Silver can't identify duplicates
2. Wrong `last_updated_column` - Silver can't determine which is newest

**Fix:** Ensure `unique_columns` uniquely identifies records and `last_updated_column` is the modification date.

### "Validation error: incompatible patterns"

**Symptom:** Error like "full_snapshot is incompatible with full_merge_dedupe"

**Cause:** Bronze and Silver patterns don't make sense together.

**Fix:** See compatibility matrix above and choose a valid combination.

### "Pipeline runs are slow"

**Symptom:** Silver takes longer each day.

**Cause:** With `append_log` input mode, Silver reads ALL Bronze partitions. As partitions accumulate, this grows.

**Fix:**
1. For `full_snapshot` → Consider switching to `incremental` to reduce Bronze size
2. For `incremental` → This is expected; consider partitioning strategies or data retention policies
