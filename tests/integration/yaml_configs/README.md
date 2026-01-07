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
| `{natural_key}` | Primary key column | `order_id` |
| `{change_timestamp}` | Timestamp column | `updated_at` |
| `{watermark_column}` | Watermark column (incremental) | `updated_at` |

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
    "natural_key": "order_id",
    "change_timestamp": "updated_at",
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

## MinIO Configuration

Tests use MinIO as an S3-compatible object store.

### Default Settings

| Setting | Value |
|---------|-------|
| S3 API Endpoint | `http://localhost:9000` |
| Console URL | `http://localhost:49384` |
| Access Key | `minioadmin` |
| Secret Key | `minioadmin` |
| Bucket | `mdf` |

### Starting MinIO

```bash
docker run -p 9000:9000 -p 49384:49384 minio/minio server /data --console-address ":49384"
```

### Environment Variables

Override defaults with environment variables:

```bash
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=mdf
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
mdf/
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

## Viewing Test Output

After running tests, view files in MinIO console:

1. Open http://localhost:49384
2. Login with `minioadmin` / `minioadmin`
3. Navigate to `mdf` bucket
4. Look for directories named `test_pattern_*`

Note: Test cleanup is disabled by default to allow inspection.
