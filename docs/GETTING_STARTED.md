# Getting Started with Pipelines

This guide walks you through creating and running your first data pipeline.

## Installation

```bash
# Clone and setup
git clone https://github.com/tonysebion/bronze-foundry.git
cd bronze-foundry

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

Pipelines are defined in **YAML** (recommended) or Python. YAML is simpler and provides editor autocomplete.

### 1. Choose a Template

| Your Situation | Template to Copy |
|---------------|------------------|
| CSV file, daily export | `csv_snapshot.yaml` |
| Database table, full load | `mssql_dimension.yaml` |
| Database table, incremental | `incremental_load.yaml` |
| REST API | `api_rest.yaml` |
| Fixed-width file | `fixed_width.yaml` |
| Event/fact table (immutable) | `event_log.yaml` |
| Not sure / General purpose | `pipeline_template.yaml` |

### 2. Copy and Edit

```bash
cp pipelines/templates/csv_snapshot.yaml my_pipeline.yaml
```

Edit the file - the schema provides autocomplete in VSCode:
- Set `system` and `entity` to identify your data source
- Set `source_path` to your data location
- Set `target_path` for output storage (local, S3, or ADLS)
- Set `natural_keys` and `change_timestamp` for Silver deduplication

Example `my_pipeline.yaml`:
```yaml
# yaml-language-server: $schema=pipelines/schema/pipeline.schema.json
name: retail_orders
description: Load retail orders from CSV

bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders_{run_date}.csv

  # Output location - uses Hive-style partitioning
  # Local: ./bronze/system=retail/entity=orders/dt={run_date}/
  # S3:    s3://my-bucket/bronze/system=retail/entity=orders/dt={run_date}/
  # ADLS:  abfss://container@account.dfs.core.windows.net/bronze/...
  target_path: ./bronze/system=retail/entity=orders/dt={run_date}/

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  target_path: ./silver/system=retail/entity=orders/
```

**Output Structure:**
```
bronze/system=retail/entity=orders/dt=2025-01-15/
  ├── orders.parquet      # Raw data
  ├── _metadata.json      # Lineage and watermarks
  └── _checksums.json     # Data integrity

silver/system=retail/entity=orders/
  ├── data.parquet        # Curated data
  └── _metadata.json      # Schema info
```

### 3. Run

```bash
# Validate configuration
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --dry-run

# Run the pipeline
python -m pipelines ./my_pipeline.yaml --date 2025-01-15

# Run Bronze or Silver only
python -m pipelines ./my_pipeline.yaml:bronze --date 2025-01-15
python -m pipelines ./my_pipeline.yaml:silver --date 2025-01-15
```

### Storage Options

**Local filesystem** (default):
```yaml
target_path: ./bronze/system=retail/entity=orders/dt={run_date}/
```

**AWS S3**:
```yaml
target_path: s3://my-bucket/bronze/system=retail/entity=orders/dt={run_date}/
# Set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
```

**S3-Compatible (MinIO, Nutanix Objects)**:
```yaml
target_path: s3://my-bucket/bronze/system=retail/entity=orders/dt={run_date}/
s3_endpoint_url: ${S3_ENDPOINT_URL}
s3_signature_version: s3v4
s3_addressing_style: path
# Set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT_URL
```

**Azure Data Lake Storage (ADLS)**:
```yaml
target_path: abfss://container@account.dfs.core.windows.net/bronze/system=retail/entity=orders/
# Set: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY (or use service principal)
```

CLI flags: `--dry-run` (validate), `--check` (test connectivity), `--explain` (show plan), `--target ./local/` (override paths), `-v` (verbose), `--json-log` (JSON output).

**Tip:** The `# yaml-language-server: $schema=...` line enables autocomplete for all options in VSCode.

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

## Advanced: Python Pipelines

Use Python when you need complex logic, retry decorators, or runtime-computed configuration.

### Database (SQL Server)

```python
from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern

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
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

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
