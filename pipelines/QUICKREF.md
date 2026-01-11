# Pipeline Quick Reference

**New to this?** See the [Quick Glossary](../docs/FIRST_PIPELINE.md#quick-glossary) for term definitions.

## Running Pipelines

```bash
# List available pipelines
python -m pipelines --list

# Full pipeline (Bronze -> Silver)
python -m pipelines claims.header --date 2025-01-15

# Bronze only
python -m pipelines claims.header:bronze --date 2025-01-15

# Silver only
python -m pipelines claims.header:silver --date 2025-01-15

# Validate configuration and connectivity (pre-flight check)
python -m pipelines claims.header --date 2025-01-15 --check

# Show what pipeline would do without executing
python -m pipelines claims.header --date 2025-01-15 --explain

# Dry run (validate without executing)
python -m pipelines claims.header --date 2025-01-15 --dry-run

# Local development (override target paths)
python -m pipelines claims.header --date 2025-01-15 --target ./local_output/

# Verbose logging
python -m pipelines claims.header --date 2025-01-15 -v

# Test database connection
python -m pipelines test-connection claims_db --host myserver.com --database ClaimsDB
```

## Source Types (Bronze)

| Type | Description | Required Options |
|------|-------------|------------------|
| `SourceType.DATABASE_MSSQL` | SQL Server database | `host`, `database` |
| `SourceType.DATABASE_POSTGRES` | PostgreSQL database | `host`, `database` |
| `SourceType.FILE_CSV` | CSV file | `source_path` |
| `SourceType.FILE_PARQUET` | Parquet file | `source_path` |
| `SourceType.FILE_SPACE_DELIMITED` | Space-separated file | `source_path` |
| `SourceType.FILE_FIXED_WIDTH` | Fixed-width file | `source_path`, `columns`, `widths` |
| `SourceType.API_REST` | REST API | `source_path` (URL) |

## Load Patterns (Bronze)

| Pattern | Description | When to Use |
|---------|-------------|-------------|
| `LoadPattern.FULL_SNAPSHOT` | Replace all each run | Daily full exports, small tables |
| `LoadPattern.INCREMENTAL_APPEND` | Only new records | Large tables, real-time feeds |
| `LoadPattern.CDC` | Change data capture | Database replication |

## Common Patterns (Copy-Paste YAML)

These are the most common Bronze + Silver combinations. Copy the pattern that matches your use case.

### Pattern 1: Daily File Full Replacement (No Dedup)

**Use when:** Small file delivered daily with complete data (< 100K rows), source doesn't track changes.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: daily_customers
description: Full customer list refreshed daily

bronze:
  system: crm
  entity: customers
  source_type: file_csv
  source_path: ./data/customers_{run_date}.csv
  load_pattern: full_snapshot  # Extracts everything each run

silver:
  domain: crm
  subject: customers
  model: periodic_snapshot     # Uses only today's Bronze partition
  # No unique_columns or last_updated_column = NO deduplication
```

### Pattern 1b: Daily File Full Replacement (With Dedup)

**Use when:** Same as above, but source file may contain duplicate rows you want to remove.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: daily_customers_deduped
description: Full customer list refreshed daily, duplicates removed

bronze:
  system: crm
  entity: customers
  source_type: file_csv
  source_path: ./data/customers_{run_date}.csv
  load_pattern: full_snapshot

silver:
  domain: crm
  subject: customers
  model: periodic_snapshot
  unique_columns: [customer_id]       # Providing this enables deduplication
  last_updated_column: updated_at     # Picks the row with the latest timestamp
```

> **periodic_snapshot Dedup Behavior:**
>
> The deduplication behavior depends on both `entity_kind` AND whether you provide `unique_columns`/`last_updated_column`:
>
> | Configuration | Dedup Behavior |
> |--------------|----------------|
> | `entity_kind: state` (default) **without** keys | **No dedup** — all rows pass through unchanged |
> | `entity_kind: event` **without** keys | **Full row distinct** — removes exact duplicate rows |
> | Either entity_kind **with** `unique_columns` + `last_updated_column` | **Key-based dedup** — keeps latest version per unique key |
>
> **Example: Event entity with full-row distinct (no keys needed)**
> ```yaml
> silver:
>   model: periodic_snapshot
>   entity_kind: event        # Triggers .distinct() deduplication
>   # No unique_columns or last_updated_column needed
> ```

### Pattern 2: Large Table with Timestamps (SCD Type 1)

**Use when:** Large table (> 100K rows) with `updated_at` column, you only need current state.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: incremental_orders
description: Orders with incremental load, keep latest version

bronze:
  system: sales
  entity: orders
  source_type: database_mssql
  host: ${DB_HOST}
  database: SalesDB
  load_pattern: incremental         # Only rows where updated_at > last watermark
  incremental_column: updated_at    # Column to track changes

silver:
  domain: sales
  subject: orders
  model: full_merge_dedupe          # Reads ALL Bronze partitions, dedupes by key
  unique_columns: [order_id]          # What makes a record unique
  last_updated_column: updated_at     # Which version is newest
```

### Pattern 3: Audit Trail with Full History (SCD Type 2)

**Use when:** You need to track all changes over time with effective dates.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: customer_history
description: Customer changes with full history (SCD Type 2)

bronze:
  system: crm
  entity: customers
  source_type: database_mssql
  host: ${DB_HOST}
  database: CRM_DB
  load_pattern: incremental
  incremental_column: modified_date

silver:
  domain: crm
  subject: customers
  model: scd_type_2                  # Keeps ALL versions with effective dates
  unique_columns: [customer_id]
  last_updated_column: modified_date
  # Output adds: effective_from, effective_to, is_current
```

### Pattern 4: Immutable Event Log

**Use when:** Events are never updated (orders, clicks, logs). Dedupe exact duplicates only.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: order_events
description: Immutable order events

bronze:
  system: orders
  entity: events
  source_type: api_rest
  base_url: https://api.example.com
  endpoint: /v1/events
  load_pattern: incremental
  incremental_column: event_time

silver:
  domain: orders
  subject: events
  model: event_log                   # Keeps all events, dedupes exact duplicates
  unique_columns: [event_id]
  last_updated_column: event_time
```

### Pattern 5: CDC with Soft Deletes

**Use when:** Source provides CDC stream with Insert/Update/Delete operation codes.

```yaml
# yaml-language-server: $schema=./schema/pipeline.schema.json
name: cdc_customers
description: CDC stream with soft delete tracking

bronze:
  system: crm
  entity: customers
  source_type: file_csv              # Or database with CDC enabled
  source_path: ./data/cdc_customers_{run_date}.csv
  load_pattern: cdc
  cdc_options:
    operation_column: op             # Column containing I/U/D codes
    insert_code: I
    update_code: U
    delete_code: D

silver:
  domain: crm
  subject: customers
  model: cdc                         # Unified CDC model (recommended)
  keep_history: false                # false = current only (SCD1), true = full history (SCD2)
  handle_deletes: flag               # flag = adds _deleted column, remove = hard delete, ignore = skip deletes
  unique_columns: [customer_id]
  last_updated_column: updated_at
  # Output adds: _deleted column (true for deleted records)
```

---

## Pattern Transitions

Silver intelligently handles pattern transitions with **partition boundary detection**.

### How It Works

Silver determines how to read Bronze partitions based on the **LATEST partition's** metadata:
- Latest is `full_snapshot` → Silver uses `replace_daily` (reads ONLY latest partition)
- Latest is `incremental` or `cdc` → Silver uses `append_log` with **boundary detection**

### Partition Boundary Detection

When Silver uses `append_log` mode, it scans backwards through partition metadata to find `full_snapshot` boundaries:

```
Partitions:  dt=01-06 (incr) → dt=01-07 (incr) → dt=01-08 (full) → dt=01-09 (incr) → dt=01-10 (incr)
                                                      ↑                                      ↑
                                               BOUNDARY FOUND                    Silver starts here
                                               (full_snapshot)                   Scans backward...
                                                                                 Stops at 01-08

Result: Silver reads ONLY dt=01-08, dt=01-09, dt=01-10 (not 01-06, 01-07)
```

**Why this works:** A `full_snapshot` contains complete state at that point in time. Partitions before it are obsolete - their data is already incorporated into the snapshot.

| Scenario | Latest Pattern | Silver Reads | Result |
|----------|---------------|--------------|--------|
| Days 1-5 incremental, Day 6 full_snapshot (STOP) | full_snapshot | Day 6 only | Correct - snapshot has everything |
| Days 1-5 incremental, Day 6 full, Day 7+ incremental | incremental | Days 6-7+ only | Correct - boundary at day 6 |
| Days 1-5 full_snapshot, Day 6 incremental | incremental | Days 1-6 (all) | Deduplication handles overlap |

### Safe Practices

1. **Consistent patterns are simplest:** Choose a load pattern and stick with it
2. **Weekly full refreshes work well:** Insert `full_snapshot` partitions weekly; Silver automatically uses them as boundaries
3. **Full_snapshot = complete data:** Only use `load_pattern: full_snapshot` when the partition truly contains complete data

### Recovery: Switching from Incremental to Full Snapshot

If you need to completely reset (e.g., source system changed):

```bash
# 1. Delete all existing Bronze partitions
rm -rf ./bronze/system=myteam/entity=orders/

# 2. Delete watermark
python -c "from pipelines.lib.state import delete_watermark; delete_watermark('myteam', 'orders')"

# 3. Update YAML to new load_pattern
# 4. Run fresh
python -m pipelines ./my_pipeline.yaml --date 2025-01-15
```

See [docs/CONCEPTS.md](../docs/CONCEPTS.md#pattern-transitions) for detailed explanation.

---

## Entity Types (Silver)

| Type | Description | Examples |
|------|-------------|----------|
| `EntityKind.STATE` | Slowly changing dimension | Customer, Product, Account |
| `EntityKind.EVENT` | Immutable fact/transaction | Orders, Clicks, Payments |

## History Modes (Silver)

| Mode | Description | SQL Equivalent |
|------|-------------|----------------|
| `HistoryMode.CURRENT_ONLY` | Keep only latest version | SCD Type 1 |
| `HistoryMode.FULL_HISTORY` | Keep all versions with effective dates | SCD Type 2 |

## Common Tasks

### Add a New Pipeline

1. Copy a template from `pipelines/examples/`
2. Rename to `my_pipeline.yaml`
3. Fill in the configuration sections
4. Test with `--dry-run` first

### Change a Database Query

Edit the `query` field in your Bronze configuration:

```yaml
bronze:
  system: sales
  entity: orders
  source_type: database_mssql
  host: ${DB_HOST}
  database: SalesDB
  query: |
    SELECT col1, col2, col3
    FROM dbo.YourTable
    WHERE LastUpdated > ?
```

### Add Columns to Silver

By default, all columns are included. To specify a subset:

```yaml
silver:
  domain: sales
  subject: orders
  model: full_merge_dedupe
  unique_columns: [order_id]
  last_updated_column: updated_at
  attributes:                    # Only include these columns in Silver output
    - order_id
    - customer_id
    - order_total
    - status
    - updated_at
```

### Enable Retry for Flaky Sources

Retry is configured via environment variables or CLI options:

```bash
# Environment variables
export PIPELINE_MAX_RETRIES=3
export PIPELINE_RETRY_BACKOFF=5.0

# Or via CLI
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --retries 3
```

For API sources, use rate limiting to prevent overwhelming the server:

```yaml
bronze:
  system: external_api
  entity: products
  source_type: api_rest
  base_url: https://api.example.com
  endpoint: /v1/products
  requests_per_second: 10.0    # Built-in rate limiting
```

### Test Locally

```bash
# Override target to local directory
python -m pipelines claims.header --date 2025-01-15 --target ./local/

# Or set environment variable
export BRONZE_TARGET_ROOT=./local_bronze/
export SILVER_TARGET_ROOT=./local_silver/
python -m pipelines claims.header --date 2025-01-15
```

### Reset Incremental Load

Delete the watermark to start from scratch:

```bash
# Delete watermark via CLI
python -m pipelines reset-watermark --system claims_dw --entity claims_header

# Or delete the file manually
rm .state/claims_dw_claims_header_watermark.json
```

## Storage Backends

Pipelines support multiple storage backends for reading and writing data:

### Supported URI Schemes

| Scheme | Storage Type | Example |
|--------|-------------|---------|
| Local path | Local filesystem | `./bronze/`, `/data/silver/` |
| `s3://` | AWS S3 | `s3://my-bucket/bronze/` |
| `abfss://` | Azure Data Lake Gen2 | `abfss://container@account.dfs.core.windows.net/bronze/` |
| `wasbs://` | Azure Blob Storage | `wasbs://container@account.blob.core.windows.net/bronze/` |
| `az://` | Azure (short form) | `az://container/bronze/` |

### Cloud Storage Examples

```yaml
# AWS S3
name: s3_orders
bronze:
  system: sales
  entity: orders
  source_type: file_parquet
  source_path: s3://source-bucket/orders/{run_date}/*.parquet
  target_path: s3://bronze-bucket/system={system}/entity={entity}/dt={run_date}/

silver:
  domain: sales
  subject: orders
  source_path: s3://bronze-bucket/system=sales/entity=orders/dt={run_date}/*.parquet
  target_path: s3://silver-bucket/domain={domain}/subject={subject}/dt={run_date}/
  unique_columns: [order_id]
  last_updated_column: updated_at
```

```yaml
# Azure Data Lake Storage Gen2
name: adls_claims
bronze:
  system: claims
  entity: header
  source_type: database_mssql
  host: ${DB_HOST}
  database: ClaimsDB
  target_path: abfss://bronze@myaccount.dfs.core.windows.net/system={system}/entity={entity}/dt={run_date}/

silver:
  domain: claims
  subject: header
  source_path: abfss://bronze@myaccount.dfs.core.windows.net/system=claims/entity=header/dt={run_date}/*.parquet
  target_path: abfss://silver@myaccount.dfs.core.windows.net/domain={domain}/subject={subject}/dt={run_date}/
  unique_columns: [claim_id]
  last_updated_column: modified_date
```

### Storage Backend API (Advanced Python Use)

> **Note:** This section is for advanced programmatic use cases. Most users don't need this - YAML pipelines handle storage automatically.

For advanced use cases, you can use the storage backend directly:

```python
from pipelines.lib.storage import get_storage, parse_uri

# Auto-detect storage type from URI
storage = get_storage("s3://my-bucket/data/")

# Check if path exists
if storage.exists("file.parquet"):
    data = storage.read_bytes("file.parquet")

# Write files
storage.write_text("manifest.json", '{"version": 1}')
storage.write_bytes("data.parquet", parquet_bytes)

# List files
files = storage.list_files(pattern="*.parquet")
for f in files:
    print(f"{f.path}: {f.size} bytes")

# Parse URI to get scheme
scheme, path = parse_uri("s3://bucket/prefix/")
# scheme = "s3", path = "bucket/prefix/"
```

### Cloud Credentials

**AWS S3:**
- Uses default AWS credential chain (environment, config file, IAM role)
- Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for explicit credentials
- For custom endpoints (MinIO, LocalStack): pass `endpoint_url` option

**Azure ADLS:**
- Set `AZURE_STORAGE_ACCOUNT_KEY` for account key auth
- Or use service principal: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
- For Azure AD auth, ensure `azure-identity` is installed

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BRONZE_TARGET_ROOT` | Override Bronze target path |
| `SILVER_TARGET_ROOT` | Override Silver target path |
| `PIPELINE_STATE_DIR` | Directory for watermark files (default: `.state`) |
| `${VAR_NAME}` in options | Resolved from environment |
| `AWS_ACCESS_KEY_ID` | AWS access key for S3 |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key for S3 |
| `AZURE_STORAGE_ACCOUNT_KEY` | Azure storage account key |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | Azure AD client ID |
| `AZURE_CLIENT_SECRET` | Azure AD client secret |

## Troubleshooting

### "Module not found" Error

Make sure the pipeline file exists at:
- `pipelines/{system}_{entity}.py` (underscores)
- `pipelines/{system}/{entity}.py` (nested directories)

### "Configuration validation failed"

Run with `--dry-run` to see detailed validation errors:
```bash
python -m pipelines claims.header --date 2025-01-15 --dry-run
```

### Database Connection Issues

1. Check environment variables are set: `echo $CLAIMS_DB_HOST`
2. Test connection: `python -m pipelines test-connection claims_db --host myserver.com --database ClaimsDB`
3. Enable retries for transient failures: `python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --retries 3`

### Incremental Load Not Working

1. Check watermark column exists in source
2. Verify watermark file: `cat .state/system_entity_watermark.json`
3. Delete watermark to reset: `rm .state/system_entity_watermark.json`

## Validation Commands

Use CLI flags to validate your pipeline configuration before running:

```bash
# Pre-flight check (validates config and connectivity)
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --check

# Dry run (validates without executing)
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --dry-run

# Show execution plan
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --explain
```

## Error Messages

Common error types you may encounter:

| Error Type | Cause | Solution |
|------------|-------|----------|
| `ConfigurationError` | Invalid YAML syntax or missing required fields | Check YAML with `--dry-run` |
| `ConnectionError` | Database/API unreachable | Verify credentials and network |
| `SourceNotFoundError` | Source file or table doesn't exist | Check `source_path` or table name |
| `ChecksumError` | Data integrity failure | Re-run Bronze extraction |
| `ValidationError` | Schema validation failed | Check field types and required fields |
