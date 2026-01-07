# Getting Started with Pipelines

This guide walks you through creating and running your first data pipeline.

## Installation

```bash
# Clone and setup
git clone https://github.com/tonysebion/medallion-foundry.git
cd medallion-foundry

# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Install
pip install -e .
```

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
cp pipelines/templates/simple_pipeline.py pipelines/myteam/orders.py
```

Edit `pipelines/myteam/orders.py`:

```python
from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# Bronze: Extract from source
bronze = BronzeSource(
    system="retail",
    entity="orders",
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

CLI flags: `--dry-run` (validate), `--check` (test connectivity), `--explain` (show plan), `--target ./local/` (override paths), `-v` (verbose), `--json-log` (JSON output).

## Interactive Creator

For guided pipeline creation:

```bash
python -m pipelines.create
```

## CLI Commands

```bash
python -m pipelines --list                    # List all pipelines
python -m pipelines test-connection db --host ... --database ...
python -m pipelines inspect-source --file ./data.csv
python -m pipelines new myteam.orders --source-type database_mssql
```

## Source Types

### Database (SQL Server)

```python
bronze = BronzeSource(
    system="claims",
    entity="claims_header",
    source_type=SourceType.DATABASE_MSSQL,
    source_path="",
    options={
        "host": "${DB_HOST}",
        "database": "ClaimsDB",
        "query": "SELECT * FROM dbo.ClaimsHeader WHERE LastUpdated > ?",
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
    options={"headers": {"Authorization": "Bearer ${API_TOKEN}"}},
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)
```

## Entity Kinds (Silver)

### STATE (Dimensions)

For slowly changing entities (customers, products):

```python
silver = SilverEntity(
    natural_keys=["customer_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.FULL_HISTORY,  # SCD Type 2
)
```

### EVENT (Facts)

For immutable events (orders, clicks):

```python
silver = SilverEntity(
    natural_keys=["order_id"],
    change_timestamp="order_date",
    entity_kind=EntityKind.EVENT,
    history_mode=HistoryMode.CURRENT_ONLY,
)
```

## Load Patterns (Bronze)

| Pattern | Use Case |
|---------|----------|
| `FULL_SNAPSHOT` | Small tables, daily exports, reference data |
| `INCREMENTAL_APPEND` | Large tables, transaction logs (requires `watermark_column`) |

## History Modes (Silver)

| Mode | Behavior |
|------|----------|
| `CURRENT_ONLY` | SCD Type 1 - keeps only latest version |
| `FULL_HISTORY` | SCD Type 2 - keeps all versions with effective dates |

## Data Quality

```python
from pipelines.lib.quality import not_null, valid_timestamp, check_quality

rules = [
    *not_null("order_id", "customer_id"),
    valid_timestamp("order_date"),
]

result = check_quality(table, rules)
```

## Local Development

```bash
# Override target paths
python -m pipelines myteam.orders --date 2025-01-15 --target ./local/

# Or via environment
export BRONZE_TARGET_ROOT=./local_bronze/

# Reset incremental state
rm .state/retail_orders_watermark.json
```

## Sample Data

Generate test fixtures:

```bash
python scripts/generate_bronze_samples.py --all
python scripts/generate_silver_samples.py --all
```

## Directory Structure

```
pipelines/
├── lib/               # Framework code (don't modify)
├── templates/         # Copy-paste templates
├── examples/          # Reference implementations
└── myteam/            # Your pipelines
```

## Next Steps

- Browse `pipelines/templates/` for ready-to-edit scaffolds
- Check `pipelines/examples/` for advanced patterns
- Run `python -m pipelines --list` to verify pipelines are discoverable
