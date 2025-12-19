# Pipeline Quick Reference

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

1. Copy a template from `pipelines/templates/`
2. Rename to `pipelines/{system}/{entity}.py`
3. Fill in the `# <-- CHANGE THIS` sections
4. Test with `--dry-run` first

### Change a Database Query

Edit the `"query"` option in your BronzeSource:

```python
options={
    "query": """
        SELECT col1, col2, col3
        FROM dbo.YourTable
        WHERE LastUpdated > ?
    """,
}
```

### Add Columns to Silver

By default, all columns are included. To specify a subset:

```python
silver = SilverEntity(
    ...
    attributes=["Column1", "Column2", "Column3"],
)
```

### Enable Retry for Flaky Sources

```python
from pipelines.lib.resilience import with_retry

@with_retry(max_attempts=3, backoff_seconds=5.0)
def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)
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

```python
from pipelines.lib.watermark import delete_watermark
delete_watermark("claims_dw", "claims_header")
```

Or delete the file manually:
```bash
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

```python
# AWS S3
bronze = BronzeSource(
    system="sales",
    entity="orders",
    source_type=SourceType.FILE_PARQUET,
    source_path="s3://source-bucket/orders/{run_date}/*.parquet",
    target_path="s3://bronze-bucket/system={system}/entity={entity}/dt={run_date}/",
)

# Azure Data Lake Storage Gen2
bronze = BronzeSource(
    system="claims",
    entity="header",
    source_type=SourceType.DATABASE_MSSQL,
    target_path="abfss://bronze@myaccount.dfs.core.windows.net/system={system}/entity={entity}/dt={run_date}/",
    options={
        "connection_name": "claims_db",
        "host": "${DB_HOST}",
        "database": "ClaimsDB",
    },
)
```

### Storage Backend API

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
3. Add `@with_retry` for transient failures

### Incremental Load Not Working

1. Check watermark column exists in source
2. Verify watermark file: `cat .state/system_entity_watermark.json`
3. Delete watermark to reset: `rm .state/system_entity_watermark.json`

## Programmatic Validation

You can validate pipeline configuration before running:

```python
# Validate BronzeSource configuration
issues = bronze.validate(run_date="2025-01-15", check_connectivity=True)
if issues:
    for issue in issues:
        print(issue)

# Validate SilverEntity configuration
issues = silver.validate(run_date="2025-01-15", check_source=True)
if issues:
    for issue in issues:
        print(issue)
```

## Structured Exceptions

The pipelines library provides specific exception types for better error handling:

```python
from pipelines.lib.errors import (
    PipelineError,           # Base exception
    BronzeExtractionError,   # Source extraction failures
    SilverCurationError,     # Curation/dedup failures
    ConnectionError,         # Database/API connection issues
    ConfigurationError,      # Invalid configuration
    ChecksumError,           # Data integrity failures
    SourceNotFoundError,     # Missing source files/tables
    ValidationError,         # Configuration validation issues
)

try:
    result = bronze.run("2025-01-15")
except BronzeExtractionError as e:
    print(f"Extraction failed: {e}")
    print(f"System: {e.system}, Entity: {e.entity}")
    print(f"Details: {e.details}")
```
