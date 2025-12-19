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

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BRONZE_TARGET_ROOT` | Override Bronze target path |
| `SILVER_TARGET_ROOT` | Override Silver target path |
| `PIPELINE_STATE_DIR` | Directory for watermark files (default: `.state`) |
| `${VAR_NAME}` in options | Resolved from environment |

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
