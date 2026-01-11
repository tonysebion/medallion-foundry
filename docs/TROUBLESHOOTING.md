# Troubleshooting Guide

This guide helps you diagnose and fix common issues when running pipelines.

## Quick Diagnostics

### Pre-Flight Check

Before running a pipeline, validate your configuration:

```bash
# Validate YAML without running
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --dry-run

# Pre-flight check (validates config + tests connectivity)
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --check

# Show execution plan
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --explain
```

### Debug Mode

Enable verbose logging to see exactly what's happening:

```bash
# Set debug log level
export BRONZE_LOG_LEVEL=DEBUG

# Run with step-by-step tracing
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --debug
```

### Check Output Artifacts

After a run, inspect the generated files:

```bash
# View metadata
cat bronze/system=myteam/entity=orders/dt=2025-01-15/_metadata.json | python -m json.tool

# Verify checksums
python -c "from pipelines.lib.checksum import verify_checksum_manifest; print(verify_checksum_manifest('bronze/system=myteam/entity=orders/dt=2025-01-15/'))"
```

---

## Common Configuration Errors

### "source_type must be one of..."

**Error:**
```
ValueError: source_type must be one of: file_csv, file_parquet, file_json, ...
```

**Cause:** Invalid or misspelled source_type value.

**Fix:** Use one of the valid source types:
- File sources: `file_csv`, `file_parquet`, `file_json`, `file_jsonl`, `file_excel`, `file_fixed_width`, `file_space_delimited`
- Database sources: `database_mssql`, `database_postgres`, `database_mysql`, `database_db2`
- API sources: `api_rest`

See `pipelines/examples/` for examples of each source type.

---

### "Database sources require 'host' in options"

**Error:**
```
ValueError: Database sources require 'host' in options
```

**Cause:** Missing required database connection parameters.

**Fix:** Add connection details under `bronze.options`:

```yaml
bronze:
  source_type: database_mssql
  options:
    host: your-server.database.windows.net
    database: MyDatabase
    username: ${DB_USER}
    password: ${DB_PASSWORD}
```

---

### "INCREMENTAL_APPEND requires watermark_column"

**Error:**
```
ValueError: INCREMENTAL_APPEND requires watermark_column to track progress
```

**Cause:** Using incremental load pattern without specifying which column tracks changes.

**Fix:** Add `watermark_column` to your Bronze config:

```yaml
bronze:
  load_pattern: incremental
  watermark_column: updated_at  # Column that increases over time (timestamp or ID)
```

The watermark column should be a timestamp or monotonically increasing ID that indicates new/changed records.

---

### "natural_keys required for state entities"

**Error:**
```
ValueError: natural_keys required for state entities (dimensions). Add natural_keys: [column1, column2] to identify unique records.
```

**Cause:** State entities (dimensions) need a primary key for deduplication.

**Fix:** Add `natural_keys` to your Silver config:

```yaml
silver:
  entity_kind: state  # or using model: full_merge_dedupe, scd_type_2, etc.
  natural_keys: [customer_id]  # Columns that uniquely identify each record
  change_timestamp: updated_at
```

---

### "Column 'X' not found in table"

**Error:**
```
ValueError: Column 'updated_at' not found in table. Available columns: ['id', 'name', 'modified_date']
```

**Cause:** The column referenced in config doesn't exist in the source data.

**Fix:**
1. Check the actual column names in your source data
2. Update your config to use the correct column name:

```yaml
silver:
  change_timestamp: modified_date  # Use the actual column name from source
```

---

## Storage Errors

### S3 Connection Failures

**Error:**
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Cause:** AWS credentials not configured.

**Fix:** Set up credentials via one of these methods:

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1

# Option 2: AWS CLI
aws configure

# Option 3: For MinIO/S3-compatible storage
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

---

### S3 Access Denied

**Error:**
```
botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation
```

**Cause:** Credentials are valid but lack required permissions.

**Fix:**
1. Verify your identity: `aws sts get-caller-identity`
2. Test bucket access: `aws s3 ls s3://your-bucket/`
3. Ensure IAM policy includes: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`

---

### Path Not Found

**Error:**
```
FileNotFoundError: Source path does not exist: ./data/orders_2025-01-15.csv
```

**Cause:** Source file doesn't exist at the specified path.

**Fix:**
1. Check the file exists: `ls -la ./data/orders_2025-01-15.csv`
2. Verify path placeholders: `{run_date}` is replaced with your `--date` value
3. Check working directory: paths are relative to where you run the command

---

## Database Errors

### Connection Timeout

**Error:**
```
pyodbc.OperationalError: ('HYT00', '[HYT00] Login timeout expired (0)')
```

**Cause:** Unable to reach the database server.

**Fix:**
1. Verify network connectivity: `ping your-server.database.windows.net`
2. Check firewall rules allow your IP
3. Verify port is correct (SQL Server: 1433, PostgreSQL: 5432, MySQL: 3306)

```bash
# Test MSSQL connection
python -m pipelines test-connection db \
  --host your-server.database.windows.net \
  --database MyDatabase \
  --driver mssql
```

---

### Authentication Failed

**Error:**
```
pyodbc.InterfaceError: ('28000', "[28000] Login failed for user 'myuser'")
```

**Cause:** Invalid username or password.

**Fix:**
1. Verify credentials are correct
2. Check environment variable expansion: `echo $DB_PASSWORD`
3. Ensure user has access to the specified database

---

## API Errors

### Rate Limiting (429)

**Error:**
```
HTTPStatusError: 429 Too Many Requests
```

**Cause:** Exceeded API rate limits.

**Fix:** Configure rate limiting in your pipeline:

```yaml
bronze:
  source_type: api_rest
  requests_per_second: 5.0  # Reduce request rate
```

Or via environment:
```bash
export BRONZE_API_RPS=5
```

---

### Authentication Failed (401/403)

**Error:**
```
HTTPStatusError: 401 Unauthorized
```

**Cause:** Invalid or expired API credentials.

**Fix:**
1. Verify your token: `echo $API_TOKEN`
2. Check token expiration
3. Regenerate credentials if needed

```yaml
bronze:
  source_type: api_rest
  auth:
    auth_type: bearer
    token: ${API_TOKEN}  # Make sure this env var is set
```

---

### Pagination Issues

**Error:**
```
ValueError: Cursor path 'meta.next_cursor' not found in response
```

**Cause:** API response structure doesn't match pagination config.

**Fix:** Verify the response structure and update `cursor_path`:

```yaml
bronze:
  pagination:
    strategy: cursor
    cursor_param: cursor
    cursor_path: pagination.next  # Match actual API response structure
```

Test with curl first:
```bash
curl -H "Authorization: Bearer $API_TOKEN" "https://api.example.com/v1/items" | python -m json.tool
```

---

## Watermark & State Issues

### "Watermark is ahead of data"

**Cause:** The stored watermark value is newer than available source data.

**Diagnosis:**
```bash
# Check current watermark
python -c "from pipelines.lib.state import get_watermark; print(get_watermark('myteam', 'orders'))"
```

**Fix:** Reset the watermark:
```bash
# Delete watermark to start fresh
python -c "from pipelines.lib.state import delete_watermark; delete_watermark('myteam', 'orders')"
```

---

### Reprocessing a Day

To reprocess data for a specific date:

```bash
# Simply run again - pipelines are idempotent
python -m pipelines ./my_pipeline.yaml --date 2025-01-15

# For incremental, you may need to reset watermark first
python -c "from pipelines.lib.state import delete_watermark; delete_watermark('myteam', 'orders')"
python -m pipelines ./my_pipeline.yaml --date 2025-01-15
```

---

## Debugging Techniques

### Reading _metadata.json

Each Bronze/Silver output includes metadata with useful debug info:

```json
{
  "source_system": "retail",
  "source_entity": "orders",
  "extracted_at": "2025-01-15T10:30:00Z",
  "row_count": 1500,
  "watermark": {
    "column": "updated_at",
    "value": "2025-01-15T10:00:00Z"
  }
}
```

### Verifying Checksums

Detect data corruption:

```python
from pipelines.lib.checksum import verify_checksum_manifest
from pathlib import Path

result = verify_checksum_manifest(Path("bronze/system=retail/entity=orders/dt=2025-01-15/"))
if not result.valid:
    print(f"Corrupted files: {result.mismatched_files}")
```

### Inspecting Watermarks

```python
from pipelines.lib.state import list_watermarks, get_watermark

# List all watermarks
for wm in list_watermarks():
    print(f"{wm.system}/{wm.entity}: {wm.value}")

# Get specific watermark
wm = get_watermark("retail", "orders")
print(f"Last processed: {wm}")
```

---

## FAQ

### How do I test my config without writing data?

Use `--dry-run`:
```bash
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --dry-run
```

### How do I see what the pipeline will do before running?

Use `--explain`:
```bash
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 --explain
```

### How do I run only Bronze or only Silver?

Append `:bronze` or `:silver` to the config path:
```bash
python -m pipelines ./my_pipeline.yaml:bronze --date 2025-01-15
python -m pipelines ./my_pipeline.yaml:silver --date 2025-01-15
```

### Where are logs stored?

Logs go to stderr by default. Redirect to file:
```bash
python -m pipelines ./my_pipeline.yaml --date 2025-01-15 2>&1 | tee pipeline.log
```

### How do I clear all watermarks and start fresh?

```python
from pipelines.lib.state import clear_all_watermarks
clear_all_watermarks()
```

Or delete the state directory:
```bash
rm -rf .state/
```

---

## Getting Help

If you're still stuck:

1. Enable debug logging and capture full output
2. Check the error message for specific file/line references
3. Review the [examples](../pipelines/examples/) for working configurations
4. Check [Model Selection Guide](MODEL_SELECTION.md) for pattern guidance
