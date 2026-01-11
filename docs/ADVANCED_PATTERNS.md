# Advanced Patterns

This document covers advanced Silver layer features for complex curation scenarios.

## Silver Models (Presets)

Silver Models are pre-built transformation patterns that configure `entity_kind`, `history_mode`, and `input_mode` together. Use these when you want a standard pattern without manually configuring each setting.

### Available Models

| Model | Description | Use Case |
|-------|-------------|----------|
| `periodic_snapshot` | Simple dimension refresh, no deduplication | Daily dimension snapshots |
| `full_merge_dedupe` | Deduplicated current view (SCD Type 1) | Accumulated CDC data |
| `incremental_merge` | CDC stream processing with change tracking | Real-time data feeds |
| `scd_type_2` | Full history with effective dates | Audit-required dimensions |
| `event_log` | Immutable event stream | Transaction logs, clicks |

### Model Configuration Mappings

```
periodic_snapshot:   entity_kind=state, history_mode=current_only, input_mode=replace_daily
full_merge_dedupe:   entity_kind=state, history_mode=current_only, input_mode=append_log
incremental_merge:   entity_kind=state, history_mode=current_only, input_mode=append_log
scd_type_2:          entity_kind=state, history_mode=full_history, input_mode=append_log
event_log:           entity_kind=event, history_mode=current_only, input_mode=append_log
```

### YAML Usage

```yaml
silver:
  model: scd_type_2
  unique_columns: [customer_id]
  last_updated_column: updated_at
  attributes:
    - name
    - email
    - status
```

### Python Usage

```python
from pipelines.lib.silver import SilverEntity, SilverModel

# Using from_model() class method
entity = SilverEntity.from_model(
    model=SilverModel.SCD_TYPE_2,
    unique_columns=["customer_id"],
    last_updated_column="updated_at",
)
```

---

## Delete Mode (CDC Handling)

When processing CDC (Change Data Capture) data with insert/update/delete operations, `delete_mode` controls how delete records are handled.

### Available Modes

| Mode | Behavior | Result |
|------|----------|--------|
| `ignore` | Filter out delete records | Only I/U records appear in Silver |
| `tombstone` | Keep deleted records with `_deleted=true` flag | Soft deletes preserved for auditing |
| `hard_delete` | Remove records from Silver | Records are physically removed |

### YAML Usage

```yaml
silver:
  unique_columns: [customer_id]
  last_updated_column: updated_at
  delete_mode: tombstone
  cdc_options:
    operation_column: op
    insert_code: I
    update_code: U
    delete_code: D
```

### Python Usage

```python
from pipelines.lib.silver import SilverEntity, DeleteMode

entity = SilverEntity(
    unique_columns=["customer_id"],
    last_updated_column="updated_at",
    delete_mode=DeleteMode.TOMBSTONE,
    cdc_options={
        "operation_column": "op",
        "insert_code": "I",
        "update_code": "U",
        "delete_code": "D",
    },
)
```

---

## Input Mode

`input_mode` controls how Silver interprets Bronze partitions when processing multiple dates.

### Available Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `replace_daily` | Each Bronze partition is a complete snapshot | Full table extracts, dimension loads |
| `append_log` | Bronze partitions are additive (union all) | CDC streams, event logs, incremental appends |

### Auto-Wiring Behavior

When using a `Pipeline` with both Bronze and Silver:
- Bronze with `full_snapshot` load pattern → Silver defaults to `replace_daily`
- Bronze with `incremental` or `cdc` load pattern → Silver defaults to `append_log`

### YAML Usage

```yaml
silver:
  unique_columns: [order_id]
  last_updated_column: created_at
  input_mode: append_log
```

---

## CDC Processing

The `apply_cdc()` function handles Change Data Capture streams with insert/update/delete markers.

### How It Works

1. Deduplicates by natural keys (keeping latest record per key)
2. Inspects operation code column (I/U/D)
3. Applies delete handling based on `delete_mode`

### Direct Usage (Advanced)

```python
from pipelines.lib.curate import apply_cdc

result = apply_cdc(
    table,
    keys=["customer_id"],
    order_by="updated_at",
    delete_mode="tombstone",
    cdc_options={
        "operation_column": "op",
        "insert_code": "I",
        "update_code": "U",
        "delete_code": "D",
    },
)
```

---

## Curate Helper Functions

The `pipelines.lib.curate` module provides low-level functions for building custom curation logic.

### dedupe_latest(table, keys, order_by)

Keep only the latest record per natural key (SCD Type 1).

```python
from pipelines.lib.curate import dedupe_latest

# Keep latest customer record by updated_at
deduped = dedupe_latest(table, ["customer_id"], "updated_at")
```

### dedupe_earliest(table, keys, order_by)

Keep only the earliest record per natural key.

```python
from pipelines.lib.curate import dedupe_earliest

# Keep first occurrence of each event
first_events = dedupe_earliest(table, ["event_id"], "event_time")
```

### dedupe_exact(table)

Remove exact duplicate rows (all columns identical).

```python
from pipelines.lib.curate import dedupe_exact

distinct_rows = dedupe_exact(table)
```

### build_history(table, keys, ts_col)

Build SCD Type 2 history with effective date columns.

Adds columns:
- `effective_from`: When this version became active
- `effective_to`: When this version was superseded (NULL if current)
- `is_current`: 1 if current version, 0 otherwise

```python
from pipelines.lib.curate import build_history

# Add SCD2 columns to product history
with_history = build_history(table, ["product_id"], "updated_at")
```

### rank_by_keys(table, keys, order_by, descending=True, rank_column="_rank")

Add a rank column partitioned by keys.

```python
from pipelines.lib.curate import rank_by_keys

# Rank orders by amount within each customer
ranked = rank_by_keys(table, ["customer_id"], "order_amount")
top_orders = ranked.filter(ranked._rank <= 3)  # Top 3 per customer
```

### coalesce_columns(table, column, *fallbacks)

Create a coalesced column from multiple source columns (useful for schema evolution).

```python
from pipelines.lib.curate import coalesce_columns

# Handle renamed column: try new_name first, fall back to old_name
result = coalesce_columns(table, "email", "email_address", "contact_email")
```

### filter_incremental(table, watermark_col, last_watermark)

Filter to records after a watermark timestamp.

```python
from pipelines.lib.curate import filter_incremental

# Get only new records since last run
new_records = filter_incremental(table, "created_at", "2025-01-01T00:00:00")
```

### union_dedupe(tables, keys, order_by)

Union multiple tables and deduplicate by keys.

```python
from pipelines.lib.curate import union_dedupe

# Combine multiple Bronze partitions and dedupe
combined = union_dedupe([table1, table2, table3], ["order_id"], "updated_at")
```
