# Load Patterns Guide

This guide documents the load patterns supported by Bronze Foundry and their expected behaviors. Understanding these patterns is essential for selecting the right extraction strategy and verifying that data flows correctly.

## Overview

Bronze Foundry supports four load patterns, each designed for different data extraction scenarios:

| Pattern | Use Case | Watermark | Merge Required | Data Volume |
|---------|----------|-----------|----------------|-------------|
| **SNAPSHOT** | Full replacement | No | No | Small-Medium |
| **INCREMENTAL_APPEND** | Append-only CDC | Yes | No | Large |
| **INCREMENTAL_MERGE** | Upsert by key | Yes | Yes | Large |
| **CURRENT_HISTORY** | SCD Type 2 | Yes | Yes | Medium-Large |

## SNAPSHOT

### Behavior
Complete replacement each run. Every extraction produces a full dataset that replaces any previous data.

### When to Use
- Small reference tables (< 1M rows)
- Dimension tables with infrequent changes
- Configuration data
- Data sources without reliable change tracking

### Data Flow
```
┌─────────────┐      ┌─────────────┐
│   Source    │ ───► │   Bronze    │
│ (Full Data) │      │ (Replace)   │
└─────────────┘      └─────────────┘

Run 1: Source has 1000 rows → Bronze gets 1000 rows
Run 2: Source has 1050 rows → Bronze gets 1050 rows (replaces Run 1)
```

### Configuration
```yaml
source:
  run:
    load_pattern: snapshot
```

### Verification
- Output row count matches input row count exactly
- No watermark tracking
- Each run produces a complete, independent dataset
- Previous data is fully replaced

### Test Assertions
```yaml
pattern: snapshot
row_assertions:
  row_count: 1000  # Exact match
pattern_assertions:
  replaces_previous: true
```

---

## INCREMENTAL_APPEND

### Behavior
Append new records only (insert-only CDC). Each extraction captures only NEW records since the last run, based on a watermark column.

### When to Use
- Event logs and audit trails
- Immutable fact tables
- High-volume append-only data streams
- Time-series data (metrics, sensor readings)

### Data Flow
```
┌─────────────┐      ┌─────────────┐
│   Source    │ ───► │   Bronze    │
│ (New Only)  │      │ (Append)    │
└─────────────┘      └─────────────┘

Run 1 (T0): Source rows 1-1000 → Bronze gets 1000 rows (watermark: row 1000)
Run 2 (T1): Source rows 1001-1100 → Bronze appends 100 rows (watermark: row 1100)
Run 3 (T2): Source rows 1101-1200 → Bronze appends 100 rows (watermark: row 1200)
```

### Configuration
```yaml
source:
  run:
    load_pattern: incremental_append
    watermark_column: created_at  # or event_id, sequence_num
```

### Watermark Tracking
The watermark tracks the "high water mark" of processed data:
- **Timestamp watermark**: `WHERE created_at > '2024-01-15T10:30:00'`
- **Integer watermark**: `WHERE event_id > 100000`
- **String watermark**: `WHERE partition_key > '2024-01-15'`

### Verification
- Each batch contains only NEW records (no overlap with previous batches)
- Watermark advances after each successful run
- No duplicates across batches
- Record IDs are sequential beyond previous maximum

### Test Assertions
```yaml
pattern: incremental_append
row_assertions:
  row_count: 100  # New rows only
pattern_assertions:
  appends_to_existing: true
  new_rows_count: 100
```

---

## INCREMENTAL_MERGE

### Behavior
Upsert by primary key. Each extraction captures both UPDATES to existing records AND NEW inserts, based on a watermark column.

### When to Use
- Mutable entities (customers, orders, products)
- Data with frequent updates to existing records
- Sources that track `updated_at` timestamps
- When you need current state of entities

### Data Flow
```
┌─────────────┐      ┌─────────────┐
│   Source    │ ───► │   Bronze    │
│ (Changes)   │      │ (Upsert)    │
└─────────────┘      └─────────────┘

Run 1 (T0): 1000 orders → Bronze gets 1000 rows
Run 2 (T1): 200 updated + 100 new → Bronze upserts 300 rows
  - 200 existing orders get new status/amounts
  - 100 new orders added
```

### Configuration
```yaml
source:
  run:
    load_pattern: incremental_merge
    watermark_column: updated_at
    primary_keys: [order_id]  # Keys for merge
```

### Merge Behavior
1. Records with matching `primary_keys` are UPDATED
2. Records without matching keys are INSERTED
3. Watermark advances to max `updated_at` in batch

### Verification
- Batch contains mix of updates and inserts
- Updates: records exist in previous batch (by primary key)
- Inserts: records don't exist in previous batch
- Updated records have different values (status, amounts, timestamps)

### Test Assertions
```yaml
pattern: incremental_merge
row_assertions:
  row_count: 300  # Updates + inserts
pattern_assertions:
  updated_rows_count: 200
  inserted_rows_count: 100
```

---

## CURRENT_HISTORY (SCD Type 2)

### Behavior
Maintains both current and historical views. Each entity can have multiple versions over time, enabling point-in-time queries.

### When to Use
- Slowly changing dimensions (SCD Type 2)
- Audit requirements (track all changes)
- Historical analysis (what was the state at time X?)
- Regulatory compliance (data lineage)

### Data Flow
```
┌─────────────┐      ┌──────────────────────────────┐
│   Source    │ ───► │          Bronze              │
│ (Changes)   │      │  ┌─────────┬───────────┐    │
└─────────────┘      │  │ Current │  History  │    │
                     │  └─────────┴───────────┘    │
                     └──────────────────────────────┘

Entity A:
  v1 (2024-01-01): status=active
  v2 (2024-01-15): status=inactive  ← current
  v3 (2024-02-01): status=archived  ← new current

History table: all 3 versions
Current table: only v3
```

### Configuration
```yaml
source:
  run:
    load_pattern: current_history
    primary_keys: [entity_id]
    order_column: effective_from  # Determines version ordering

silver:
  model_profile: scd_type_2
```

### SCD Type 2 Columns
Each record includes:
- `entity_id`: Natural key
- `version`: Version number (1, 2, 3...)
- `is_current`: Boolean flag for current version
- `effective_from`: When this version became active
- `effective_to`: When this version was superseded (NULL if current)

### Verification
- Each entity can have multiple versions
- Only one version per entity has `is_current = true`
- Versions are sequential (1, 2, 3...)
- `effective_from` dates don't overlap

### Test Assertions
```yaml
pattern: current_history
row_assertions:
  columns_present: [entity_id, version, is_current, effective_from, effective_to]
pattern_assertions:
  current_rows: 500     # Entities with is_current=true
  history_rows: 1500    # Total rows including history
```

---

## Pattern Selection Guide

### Decision Tree

```
                    ┌─────────────────────────────┐
                    │ Does source track changes?  │
                    └─────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │ No                            │ Yes
              ▼                               ▼
        ┌───────────┐           ┌─────────────────────────────┐
        │ SNAPSHOT  │           │ Are records ever updated?   │
        └───────────┘           └─────────────────────────────┘
                                              │
                              ┌───────────────┴───────────────┐
                              │ No                            │ Yes
                              ▼                               ▼
                    ┌─────────────────┐     ┌─────────────────────────────┐
                    │ INCREMENTAL_    │     │ Need historical versions?   │
                    │ APPEND          │     └─────────────────────────────┘
                    └─────────────────┘                   │
                                              ┌───────────┴───────────────┐
                                              │ No                        │ Yes
                                              ▼                           ▼
                                    ┌─────────────────┐     ┌─────────────────┐
                                    │ INCREMENTAL_    │     │ CURRENT_        │
                                    │ MERGE           │     │ HISTORY         │
                                    └─────────────────┘     └─────────────────┘
```

### Pattern Comparison

| Scenario | Recommended Pattern | Rationale |
|----------|---------------------|-----------|
| Daily product catalog refresh | SNAPSHOT | Small, complete replacement |
| Real-time event stream | INCREMENTAL_APPEND | High volume, immutable events |
| Order status updates | INCREMENTAL_MERGE | Orders change status over time |
| Customer dimension | CURRENT_HISTORY | Need to track customer changes |
| Log file ingestion | INCREMENTAL_APPEND | Append-only by nature |
| Inventory levels | INCREMENTAL_MERGE | Frequently updated quantities |
| Employee records | CURRENT_HISTORY | HR audit requirements |

---

## Testing Patterns

### Pattern Verification Framework

Bronze Foundry includes a pattern verification framework for testing:

```python
from tests.integration.pattern_data import (
    PatternTestDataGenerator,
    AssertionValidator,
    create_snapshot_assertions,
)

# Generate test data
generator = PatternTestDataGenerator(seed=42, base_rows=1000)
scenario = generator.generate_snapshot_scenario()

# Validate against assertions
assertions = create_snapshot_assertions(
    row_count=1000,
    columns=list(scenario.t0.columns),
)
validator = AssertionValidator(assertions)
report = validator.validate_all(scenario.t0)

assert report.passed
```

### Running Pattern Tests

```bash
# Run all pattern verification tests
pytest tests/integration/test_pattern_verification.py -v

# Run specific pattern
pytest tests/integration/test_pattern_verification.py -k "snapshot"
pytest tests/integration/test_pattern_verification.py -k "incremental_merge"

# Generate test data manually
python scripts/generate_pattern_test_data.py --pattern snapshot --rows 1000
python scripts/generate_pattern_test_data.py --all --generate-assertions
```

### Test Data Generation

The CLI script generates multi-batch test data:

```bash
# SNAPSHOT: single batch
python scripts/generate_pattern_test_data.py \
    --pattern snapshot \
    --rows 1000 \
    --output ./test_data

# INCREMENTAL_MERGE: multiple batches with updates
python scripts/generate_pattern_test_data.py \
    --pattern incremental_merge \
    --rows 1000 \
    --batches 4 \
    --update-rate 0.2 \
    --insert-rate 0.1 \
    --output ./test_data

# CURRENT_HISTORY: entity versions
python scripts/generate_pattern_test_data.py \
    --pattern current_history \
    --entities 500 \
    --changes-per-entity 3 \
    --output ./test_data
```

---

## Best Practices

### SNAPSHOT
- Use for tables under 1M rows
- Schedule during low-traffic periods
- Monitor for unexpected row count changes

### INCREMENTAL_APPEND
- Ensure watermark column is indexed at source
- Monitor for gaps in sequence numbers
- Set up alerting for late-arriving data

### INCREMENTAL_MERGE
- Choose appropriate primary keys (natural keys preferred)
- Index watermark column at source
- Consider partitioning for large tables

### CURRENT_HISTORY
- Define clear `effective_from` semantics
- Implement soft deletes with `is_deleted` flag
- Consider retention policies for old versions

---

## Related Documentation

- [Bronze Extraction Guide](./bronze_extraction.md)
- [Incremental Extraction](./incremental_extraction.md)
- [Testing Strategy](./testing_strategy.md)
- [Configuration Reference](../reference/config_reference.md)
