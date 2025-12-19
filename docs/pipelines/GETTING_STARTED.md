# Getting Started with Pipelines

This guide walks you through creating and running your first data pipeline using the
bronze-foundry pipeline framework.

## What is a Pipeline?

A pipeline moves data through the medallion architecture:

```
Source → Bronze (raw) → Silver (curated)
```

- **Bronze**: Lands raw data exactly as received, with technical metadata
- **Silver**: Curates data (deduplication, typing, history tracking)

## Quick Start (5 minutes)

### 1. Create Your First Pipeline

Copy a template and customize it:

```bash
# Copy the simple template
cp pipelines/templates/simple_pipeline.py pipelines/myteam/orders.py
```

Edit `pipelines/myteam/orders.py`:

```python
from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# Bronze: Extract from source
bronze = BronzeSource(
    system="retail",           # Your source system name
    entity="orders",           # Table/entity name
    source_type=SourceType.DATABASE_MSSQL,
    source_path="",
    options={
        "host": "retail-db.company.com",
        "database": "RetailDB",
    },
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# Silver: Curate the data
silver = SilverEntity(
    source_path="s3://bronze/system=retail/entity=orders/dt={run_date}/*.parquet",
    target_path="s3://silver/retail/orders/",
    natural_keys=["order_id"],
    change_timestamp="last_updated",
    entity_kind=EntityKind.EVENT,
    history_mode=HistoryMode.CURRENT_ONLY,
)

def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)

def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)

def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
```

### 2. Test Your Pipeline

Validate without running:

```bash
python -m pipelines myteam.orders --date 2025-01-15 --dry-run
```

### 3. Run the Pipeline

```bash
# Full pipeline (Bronze + Silver)
python -m pipelines myteam.orders --date 2025-01-15

# Bronze only
python -m pipelines myteam.orders:bronze --date 2025-01-15

# Silver only
python -m pipelines myteam.orders:silver --date 2025-01-15
```

## Using the Interactive Creator

For guided pipeline creation:

```bash
python -m pipelines.create
```

This wizard prompts for:
- Source system and entity name
- Data source type (database, file, API)
- Connection details
- Key columns and history mode
- Output path

## Source Types

### Database (SQL Server)

```python
bronze = BronzeSource(
    system="claims",
    entity="claims_header",
    source_type=SourceType.DATABASE_MSSQL,
    source_path="",
    options={
        "host": "${DB_HOST}",           # From environment variable
        "database": "ClaimsDB",
        "query": """
            SELECT ClaimID, MemberID, Amount, LastUpdated
            FROM dbo.ClaimsHeader
            WHERE LastUpdated > ?
        """,
    },
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="LastUpdated",
)
```

### CSV File

```python
bronze = BronzeSource(
    system="reports",
    entity="daily_sales",
    source_type=SourceType.FILE_CSV,
    source_path="/data/exports/sales_{run_date}.csv",
    options={
        "csv_options": {
            "header": True,
            "delimiter": ",",
        }
    },
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)
```

### Space-Delimited File

```python
bronze = BronzeSource(
    system="legacy",
    entity="transactions",
    source_type=SourceType.FILE_SPACE_DELIMITED,
    source_path="/mnt/mainframe/txn_{run_date}.txt",
    options={
        "csv_options": {
            "columns": ["txn_id", "account", "amount", "date"],
            "header": False,
        }
    },
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)
```

### REST API

```python
bronze = BronzeSource(
    system="external",
    entity="weather",
    source_type=SourceType.API_REST,
    source_path="https://api.weather.com/data?date={run_date}",
    options={
        "headers": {"Authorization": "Bearer ${API_TOKEN}"},
    },
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)
```

## Entity Kinds (Silver)

### STATE (Dimensions)

For slowly changing entities like customers, products, or accounts:

```python
silver = SilverEntity(
    source_path="...",
    target_path="s3://silver/customers/",
    natural_keys=["customer_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.FULL_HISTORY,  # SCD Type 2
)
```

### EVENT (Facts)

For immutable events like orders, clicks, or payments:

```python
silver = SilverEntity(
    source_path="...",
    target_path="s3://silver/orders/",
    natural_keys=["order_id"],
    change_timestamp="order_date",
    entity_kind=EntityKind.EVENT,
    history_mode=HistoryMode.CURRENT_ONLY,
)
```

## Load Patterns (Bronze)

### Full Snapshot

Replaces all data each run. Use for:
- Small tables
- Daily exports
- Reference data

```python
load_pattern=LoadPattern.FULL_SNAPSHOT
```

### Incremental Append

Appends only new records. Use for:
- Large tables
- Transaction logs
- Real-time feeds

```python
load_pattern=LoadPattern.INCREMENTAL_APPEND,
watermark_column="LastUpdated",
```

## History Modes (Silver)

### Current Only (SCD Type 1)

Keeps only the latest version of each record:

```python
history_mode=HistoryMode.CURRENT_ONLY
```

### Full History (SCD Type 2)

Keeps all versions with effective dates:

```python
history_mode=HistoryMode.FULL_HISTORY
```

Adds columns: `effective_from`, `effective_to`, `is_current`

## Adding Data Quality Rules

```python
from pipelines.lib.quality import not_null, valid_timestamp, check_quality

# Define rules
rules = [
    *not_null("order_id", "customer_id"),
    valid_timestamp("order_date"),
]

# Check quality in your pipeline
def run_silver(run_date: str, **kwargs):
    import ibis
    con = ibis.duckdb.connect()
    t = con.read_parquet(silver.source_path.format(run_date=run_date))

    result = check_quality(t, rules)
    if not result.passed:
        logger.warning(f"Quality check failed: {result}")

    return silver.run(run_date, **kwargs)
```

## Handling Flaky Sources

Use the retry decorator for unreliable sources:

```python
from pipelines.lib.resilience import with_retry

@with_retry(max_attempts=3, backoff_seconds=5.0, exponential=True)
def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)
```

## Local Development

### Override Target Paths

```bash
# Via command line
python -m pipelines myteam.orders --date 2025-01-15 --target ./local/

# Via environment variable
export BRONZE_TARGET_ROOT=./local_bronze/
python -m pipelines myteam.orders --date 2025-01-15
```

### Reset Incremental State

Delete the watermark to restart from the beginning:

```bash
rm .state/retail_orders_watermark.json
```

Or programmatically:

```python
from pipelines.lib.watermark import delete_watermark
delete_watermark("retail", "orders")
```

## Directory Structure

Organize pipelines by team or domain:

```
pipelines/
├── lib/               # Framework code (don't modify)
├── templates/         # Copy-paste templates
├── examples/          # Reference implementations
└── myteam/            # Your pipelines
    ├── orders.py
    └── customers.py
```

## Next Steps

- Browse `pipelines/templates/` for more examples
- See `pipelines/QUICKREF.md` for command reference
- Check `pipelines/examples/` for advanced patterns

## Common Patterns

### Multiple Tables from Same Database

Use connection pooling:

```python
bronze_orders = BronzeSource(
    system="retail",
    entity="orders",
    options={"connection_name": "retail_db", "host": "...", "database": "..."},
    ...
)

bronze_items = BronzeSource(
    system="retail",
    entity="order_items",
    options={"connection_name": "retail_db", "host": "...", "database": "..."},
    ...
)
# Both use the same connection
```

### Partitioned Output

```python
silver = SilverEntity(
    ...
    partition_by=["order_year", "order_month"],
)
```

### Custom Column Selection

Include only specific columns:

```python
silver = SilverEntity(
    ...
    attributes=["customer_id", "total_amount", "status"],
)
```

Or exclude columns:

```python
silver = SilverEntity(
    ...
    exclude_columns=["internal_id", "debug_flag"],
)
```
