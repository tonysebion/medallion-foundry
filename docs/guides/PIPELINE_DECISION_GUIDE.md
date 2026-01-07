# Pipeline Configuration Decision Guide

This guide helps you choose the right configuration options when setting up a new pipeline.
Focus on the **hard decisions** that affect how your data is processed.

## 1. Load Pattern: How does data arrive from your source?

```
How does data arrive from your source system?
│
├── Complete replacement each time (full dump)
│   └── Use LoadPattern.FULL_SNAPSHOT
│       Example: Daily export of all active products
│
├── Only new/changed records since last run
│   └── Use LoadPattern.INCREMENTAL_APPEND
│       Requires: watermark_column (e.g., "LastModified", "UpdatedAt")
│       Example: Transaction log, audit trail
│
└── Source provides explicit change markers (I/U/D flags)
    └── Use LoadPattern.CDC (not yet fully implemented)
        Note: CDC enum exists but processing logic is limited
```

### When to Use Each Pattern

| Pattern | Best For | Data Size | Complexity |
|---------|----------|-----------|------------|
| FULL_SNAPSHOT | Reference data, small dimensions | < 100K rows | Low |
| INCREMENTAL_APPEND | Event logs, large tables with timestamp | Any size | Medium |

## 2. History Mode: How should Silver track changes over time?

```
Do you need to see historical values, or just current state?
│
├── Only need current state (latest values)
│   └── Use HistoryMode.CURRENT_ONLY (SCD Type 1)
│       Silver overwrites previous values
│       Example: Customer current address
│
└── Need to track what values were at any point in time
    └── Use HistoryMode.FULL_HISTORY (SCD Type 2)
        Silver maintains effective_from/effective_to dates
        Example: Price history, employee job history
```

### SCD1 vs SCD2 Trade-offs

| Feature | SCD Type 1 (CURRENT_ONLY) | SCD Type 2 (FULL_HISTORY) |
|---------|---------------------------|---------------------------|
| Storage | Lower | Higher (keeps all versions) |
| Query complexity | Simple | Requires date filtering |
| Audit trail | No historical values | Complete history |
| Use case | Current state views | Time-travel queries |

## 3. Entity Kind: What does each row represent?

```
What does one row in your source data represent?
│
├── A thing that changes over time (customer, product, account)
│   └── Use EntityKind.STATE
│       Has natural keys that identify the entity
│       Example: Customer record with CustomerID
│
└── Something that happened (transaction, event, log entry)
    └── Use EntityKind.EVENT
        Immutable once created, append-only
        Example: Order placed, payment received
```

### Key Differences

| Aspect | STATE | EVENT |
|--------|-------|-------|
| Updates | Same key can have new values | Each occurrence is unique |
| Deduplication | By natural key | Typically none |
| History | Tracks value changes | Tracks occurrences |
| Example columns | CustomerID, ProductID | TransactionID, EventTimestamp |

## 4. Incremental vs Full: When to use each?

```
How big is your source data? How often does it change?
│
├── Small dataset (<100K rows) OR changes are unpredictable
│   └── Use FULL_SNAPSHOT
│       Simpler, no watermark tracking needed
│       OK to reload everything each run
│
├── Large dataset with reliable timestamp column
│   └── Use INCREMENTAL_APPEND
│       Set watermark_column to your timestamp field
│       Consider full_refresh_days for periodic full reloads
│
└── Very large dataset (millions of rows)
    └── Use INCREMENTAL_APPEND + chunk_size
        Set chunk_size=100000 to avoid memory issues
        Example: chunk_size=100000, watermark_column="ModifiedDate"
```

## 5. Periodic Full Refresh: When to reset and reload?

```
Should the pipeline occasionally do a full reload?
│
├── No - incremental is always sufficient
│   └── Don't set full_refresh_days (default: None)
│
├── Yes - weekly full refresh for data integrity
│   └── Set full_refresh_days=7
│       Automatically clears watermark every 7 days
│
└── Yes - monthly full refresh
    └── Set full_refresh_days=30
```

### Why Use Periodic Full Refreshes?

- **Data drift**: Source fixes may not have update timestamps
- **Delete detection**: Incremental can't detect deleted records
- **Schema evolution**: Ensure all records have new columns
- **Data quality**: Periodic validation of complete dataset

## Common Patterns Quick Reference

| Scenario | LoadPattern | EntityKind | HistoryMode | full_refresh_days | chunk_size |
|----------|-------------|------------|-------------|-------------------|------------|
| Daily product catalog | FULL_SNAPSHOT | STATE | CURRENT_ONLY | None | None |
| Transaction log | INCREMENTAL_APPEND | EVENT | N/A | None | None |
| Customer master (track changes) | INCREMENTAL_APPEND | STATE | FULL_HISTORY | 7 | None |
| Large fact table (millions) | INCREMENTAL_APPEND | EVENT | N/A | 30 | 100000 |
| Reference data (small) | FULL_SNAPSHOT | STATE | CURRENT_ONLY | None | None |

## Example Configurations

### Small Reference Table (Products)

```python
bronze = BronzeSource(
    system="erp",
    entity="products",
    source_type=SourceType.DATABASE_MSSQL,
    load_pattern=LoadPattern.FULL_SNAPSHOT,  # Reload everything daily
    host="${DB_HOST}",
    database="ERP",
)

silver = SilverEntity(
    natural_keys=["ProductID"],
    change_timestamp="LastUpdated",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,  # Just current values
)
```

### Large Transaction Table with History

```python
bronze = BronzeSource(
    system="sales",
    entity="orders",
    source_type=SourceType.DATABASE_MYSQL,
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="OrderDate",
    full_refresh_days=30,  # Monthly full refresh
    chunk_size=100000,     # Process in 100K chunks
    host="${MYSQL_HOST}",
    database="sales_db",
)

silver = SilverEntity(
    natural_keys=["OrderID"],
    change_timestamp="OrderDate",
    entity_kind=EntityKind.EVENT,  # Orders are events
)
```

### Customer Master with Full History

```python
bronze = BronzeSource(
    system="crm",
    entity="customers",
    source_type=SourceType.DATABASE_POSTGRES,
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="ModifiedAt",
    full_refresh_days=7,  # Weekly full refresh
    host="${PG_HOST}",
    database="crm_db",
)

silver = SilverEntity(
    natural_keys=["CustomerID"],
    change_timestamp="ModifiedAt",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.FULL_HISTORY,  # Track all changes
)
```

## Source Type Reference

| SourceType | Use For | Required Options |
|------------|---------|------------------|
| FILE_CSV | CSV files | source_path |
| FILE_JSON | JSON files | source_path, optional: data_path |
| FILE_JSONL | JSON Lines files | source_path |
| FILE_EXCEL | Excel files (.xlsx) | source_path, optional: sheet |
| FILE_PARQUET | Parquet files | source_path |
| FILE_FIXED_WIDTH | Mainframe reports | source_path, widths or colspecs |
| DATABASE_MSSQL | SQL Server | host, database |
| DATABASE_POSTGRES | PostgreSQL | host, database |
| DATABASE_MYSQL | MySQL/MariaDB | host, database |
| DATABASE_DB2 | IBM DB2 | host, database, requires ODBC driver |
| API_REST | REST APIs | source_path (URL), optional: headers |

## Getting Help

- Run `python -m pipelines --help` for CLI options
- Run `python -m pipelines.create` for interactive wizard
- See `pipelines/templates/` for copy-paste examples
