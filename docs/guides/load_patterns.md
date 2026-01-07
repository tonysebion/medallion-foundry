# Load Patterns Guide

This guide documents the load patterns supported by Bronze Foundry.

## Overview

Bronze Foundry supports these load patterns:

| Pattern | Use Case | Watermark | Data Volume |
|---------|----------|-----------|-------------|
| **FULL_SNAPSHOT** | Full replacement | No | Small-Medium |
| **INCREMENTAL_APPEND** | Append new records | Yes | Large |

## FULL_SNAPSHOT

### Behavior
Complete replacement each run. Every extraction produces a full dataset.

### When to Use
- Small reference tables (< 1M rows)
- Dimension tables with infrequent changes
- Configuration data
- Data sources without reliable change tracking

### Configuration
```yaml
bronze:
  load_pattern: full_snapshot
```

Or in Python:
```python
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType

bronze = BronzeSource(
    system="retail",
    entity="products",
    source_type=SourceType.FILE_CSV,
    load_pattern=LoadPattern.FULL_SNAPSHOT,
    source_path="./data/products.csv",
)
```

## INCREMENTAL_APPEND

### Behavior
Append new records only. Each extraction captures only NEW records since the last run, based on a watermark column.

### When to Use
- Event logs and audit trails
- Immutable fact tables
- High-volume append-only data streams
- Time-series data

### Configuration
```yaml
bronze:
  load_pattern: incremental
  watermark_column: created_at
```

Or in Python:
```python
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType

bronze = BronzeSource(
    system="events",
    entity="audit_log",
    source_type=SourceType.DATABASE_MSSQL,
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="created_at",
    query="SELECT * FROM audit_log",
    host="${DB_HOST}",
    database="EventDB",
)
```

### Watermark Tracking
The watermark tracks the "high water mark" of processed data:
- **Timestamp watermark**: `WHERE created_at > '2024-01-15T10:30:00'`
- **Integer watermark**: `WHERE event_id > 100000`

### Full Refresh Option
Use `full_refresh_days` to periodically do a full reload:
```yaml
bronze:
  load_pattern: incremental
  watermark_column: updated_at
  full_refresh_days: [1]  # Full refresh on 1st of each month
```

## Pattern Selection Guide

```
        Does source track changes?
                  |
      +-----------+-----------+
      | No                    | Yes
      v                       v
  FULL_SNAPSHOT       INCREMENTAL_APPEND
  (small tables)      (append-only events)
```

## Silver Layer Processing

After Bronze extraction, the Silver layer applies curation based on entity kind and history mode.

### Entity Kinds
- **STATE** - Slowly changing dimension (dedupe to latest per key)
- **EVENT** - Immutable event log (dedupe exact duplicates only)

### History Modes
- **CURRENT_ONLY** - Keep only latest version (SCD Type 1)
- **FULL_HISTORY** - Keep all versions with effective dates (SCD Type 2)

Example Silver configuration:
```yaml
silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  entity_kind: state
  history_mode: current_only
```

## Related Documentation

- [Incremental Extraction](./incremental_extraction.md)
- [Testing Strategy](./testing_strategy.md)
