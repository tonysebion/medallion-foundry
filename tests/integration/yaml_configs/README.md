# Test YAML Pipeline Configurations

This directory contains YAML pipeline configurations and templates used by integration tests.

## Directory Structure

```
yaml_configs/
├── README.md              # This file
├── template_loader.py     # Utility for loading templates with substitutions
├── patterns/              # Static YAML pattern templates
│   ├── snapshot_state_scd1.yaml     # Full snapshot → State → SCD1
│   ├── snapshot_state_scd2.yaml     # Full snapshot → State → SCD2
│   ├── incremental_state_scd1.yaml  # Incremental → State → SCD1
│   ├── incremental_state_scd2.yaml  # Incremental → State → SCD2
│   ├── snapshot_event.yaml          # Full snapshot → Event
│   ├── incremental_event.yaml       # Incremental → Event
│   └── skipped_days.yaml            # Non-consecutive run dates
├── s3_orders_full.yaml    # (Legacy) Example SCD1 orders config
└── s3_customers_scd2.yaml # (Legacy) Example SCD2 customers config
```

## Pattern Templates

The `patterns/` directory contains static YAML templates for all Bronze→Silver combinations:

### State Entities (Dimensions)

| Pattern | Bronze Load | Silver Entity | Silver History | Use Case |
|---------|-------------|---------------|----------------|----------|
| `snapshot_state_scd1` | full_snapshot | state | current_only | Customer master, reference tables |
| `snapshot_state_scd2` | full_snapshot | state | full_history | Product catalog with audit trail |
| `incremental_state_scd1` | incremental | state | current_only | Large dimension updates |
| `incremental_state_scd2` | incremental | state | full_history | Track all historical changes |

### Event Entities (Facts)

| Pattern | Bronze Load | Silver Entity | Silver History | Use Case |
|---------|-------------|---------------|----------------|----------|
| `snapshot_event` | full_snapshot | event | current_only | Daily order snapshots |
| `incremental_event` | incremental | event | current_only | Streaming events, logs |

### Special Scenarios

| Pattern | Description |
|---------|-------------|
| `skipped_days` | Pipeline runs with gaps in dates (weekends, holidays) |

## Template Placeholders

Templates use `{placeholder}` syntax for runtime substitution:

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{bucket}` | MinIO/S3 bucket name | `mdf` |
| `{prefix}` | Test-specific prefix for isolation | `test_pattern_abc123` |
| `{source_path}` | Path to source CSV file | `/tmp/orders.csv` |
| `{run_date}` | Pipeline run date | `2025-01-15` |
| `{system}` | Source system name | `retail`, `crm` |
| `{entity}` | Entity name | `orders`, `customers` |
| `{unique_columns}` | Column(s) that uniquely identify rows | `order_id` |
| `{last_updated_column}` | Column showing when row was last modified | `updated_at` |
| `{incremental_column}` | Column to track for incremental loads | `updated_at` |

## Using the Template Loader

```python
from tests.integration.yaml_configs.template_loader import (
    get_pattern_template,
    load_yaml_with_substitutions,
)

# Get pattern template path
template_path = get_pattern_template("snapshot_state_scd1")

# Define substitutions
substitutions = {
    "bucket": "mdf",
    "prefix": "test_123",
    "source_path": "/tmp/orders.csv",
    "run_date": "2025-01-15",
    "system": "retail",
    "entity": "orders",
    "unique_columns": "order_id",
    "last_updated_column": "updated_at",
}

# Load and substitute
yaml_content = load_yaml_with_substitutions(template_path, substitutions)

# Write to temp file and load pipeline
yaml_file = tmp_path / "test.yaml"
yaml_file.write_text(yaml_content)
pipeline = load_pipeline(yaml_file)
```

## Sample Data

Tests use sample data from `pipelines/examples/sample_data/`:

- `orders_2025-01-15.csv` - 5 order records
- `customers_2025-01-15.csv` - 6 customer records with SCD2 history

To regenerate sample data:

```bash
python scripts/generate_yaml_test_data.py --date 2025-01-15
```

## Storage Configuration

Tests use S3-compatible object storage. The templates support any S3-compatible backend:

| Backend | Example Use Case |
|---------|------------------|
| MinIO | Local development/testing |
| AWS S3 | Production cloud storage |
| Azure ADLS (via S3 API) | Azure data lake |
| GCS (via S3 API) | Google Cloud storage |

### Environment Variables

Configure your storage backend with these environment variables:

```bash
# S3-compatible endpoint (required for non-AWS)
export AWS_ENDPOINT_URL=http://localhost:9000

# Credentials
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1

# Bucket name
export S3_BUCKET=your_bucket_name
```

### Local Development with MinIO

For local testing, MinIO provides an S3-compatible API:

```bash
# Start MinIO
docker run -p 9000:9000 -p 49384:49384 minio/minio server /data --console-address ":49384"

# Configure environment (defaults for MinIO)
export AWS_ENDPOINT_URL=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
```

### Production with AWS S3

For AWS S3, you only need credentials (no endpoint URL):

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-west-2
```

### Production with Azure ADLS

For Azure, the path format changes to `abfss://` or `wasbs://`:

```yaml
bronze:
  target_path: "abfss://container@storageaccount.dfs.core.windows.net/bronze/"

silver:
  target_path: "abfss://container@storageaccount.dfs.core.windows.net/silver/"
```

Environment variables:
```bash
export AZURE_STORAGE_ACCOUNT_NAME=your_account
export AZURE_STORAGE_ACCOUNT_KEY=your_key
# Or use Azure AD authentication
export AZURE_CLIENT_ID=...
export AZURE_CLIENT_SECRET=...
export AZURE_TENANT_ID=...
```

## Running Tests

```bash
# Run all pattern tests
pytest tests/integration/test_yaml_s3_comprehensive.py -v

# Run specific pattern
pytest tests/integration/test_yaml_s3_comprehensive.py -k "snapshot_state_scd1" -v

# Run all state patterns
pytest tests/integration/test_yaml_s3_comprehensive.py -k "state" -v

# Run all event patterns
pytest tests/integration/test_yaml_s3_comprehensive.py -k "event" -v

# Run skipped days test
pytest tests/integration/test_yaml_s3_comprehensive.py -k "skipped_days" -v

# Run with verbose output
pytest tests/integration/test_yaml_s3_comprehensive.py -v -s
```

## Test Output Structure

Each test creates files in this structure:

```
{bucket}/
└── test_pattern_{uuid}/
    ├── bronze/
    │   └── system=retail/entity=orders/dt=2025-01-15/
    │       ├── orders.parquet
    │       ├── _metadata.json
    │       └── _checksums.json
    └── silver/
        └── system=retail/entity=orders/
            ├── orders.parquet
            ├── _metadata.json
            └── _checksums.json
```

This structure is the same regardless of storage backend (S3, ADLS, local).

## Viewing Test Output

### MinIO Console (Local Development)

After running tests with MinIO, view files in the web console:

1. Open http://localhost:49384
2. Login with `minioadmin` / `minioadmin`
3. Navigate to your bucket
4. Look for directories named `test_pattern_*`

### AWS S3 Console

Use the AWS Console or CLI:

```bash
aws s3 ls s3://your-bucket/test_pattern_*/
```

### Azure Portal

Navigate to your storage account → Containers → your-container.

Note: Test cleanup is disabled by default to allow inspection.
