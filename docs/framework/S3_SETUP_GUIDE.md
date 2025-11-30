# S3 Storage Setup Guide

This guide explains how to use S3-compatible storage (MinIO, AWS S3) with bronze-foundry.

## Quick Start with MinIO (Local Development)

### Your MinIO Configuration

- **API Endpoint**: `http://localhost:9000` (for programmatic access)
- **Browser UI**: `http://localhost:9001/browser` (for visual management)
- **Bucket**: `mdf`
- **Credentials**:
  - Username: `minioadmin`
  - Password: `minioadmin123`

### Directory Structure in MinIO

All data is stored in the `mdf` bucket at the same level:

```
mdf/
├── source_samples/          # Your source data (already exists)
│   └── sample=pattern1_full_events/
│       └── system=retail_demo/
│           └── table=orders/
│               └── dt=2025-11-13/
│                   └── full-part-0001.csv
├── bronze_samples/          # Will be created by pipeline
│   └── system=retail_demo/
│       └── entity=orders/
│           └── run_date=2025-11-13/
│               └── chunk-*.parquet
└── silver_samples/          # Will be created by pipeline
    └── domain=retail_demo/
        └── entity=orders/
            └── v1/
                └── load_date=2025-11-13/
                    └── events.parquet
```

## Configuration Files

### 1. Environment Configuration

File: `environments/dev.yaml`

```yaml
name: dev

s3:
  endpoint_url: http://localhost:9000  # MinIO API endpoint
  access_key_id: minioadmin
  secret_access_key: minioadmin123
  region: us-east-1

  buckets:
    source_data: mdf    # All three point to same bucket
    bronze_data: mdf
    silver_data: mdf
```

### 2. Pattern Configuration

File: `docs/examples/configs/patterns/pattern_s3_example.yaml`

Key settings:

```yaml
environment: dev  # References environments/dev.yaml

bronze:
  source_storage: s3
  path_pattern: s3://source_data/source_samples/sample=pattern1_full_events/...

  output_storage: s3
  output_bucket: bronze_data
  output_prefix: bronze_samples/

silver:
  input_storage: s3

  output_storage: s3
  output_bucket: silver_data
  output_prefix: silver_samples/
```

## Using the S3 Storage

### Option 1: Full S3 Pipeline

Read from S3, write Bronze to S3, write Silver to S3:

```yaml
bronze:
  source_storage: s3
  output_storage: s3

silver:
  input_storage: s3
  output_storage: s3
```

### Option 2: Mixed Storage (Debugging)

Read from S3, but write locally for inspection:

```yaml
bronze:
  source_storage: s3
  output_storage: local  # Write to local filesystem

silver:
  input_storage: local   # Read from local Bronze
  output_storage: local
```

### Option 3: Local to S3 Migration

Keep source local, but write outputs to S3:

```yaml
bronze:
  source_storage: local
  path_pattern: ./sampledata/source_samples/...
  output_storage: s3

silver:
  input_storage: s3
  output_storage: s3
```

## Running the Pipeline

### Install Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `fsspec>=2023.1.0` - Filesystem abstraction
- `s3fs>=2023.1.0` - S3 support for fsspec

### Load Configuration

```python
from pathlib import Path
from core.config.loader import load_config_with_env

# Load pattern config with environment
config_path = Path("docs/examples/configs/patterns/pattern_s3_example.yaml")
dataset, env_config = load_config_with_env(config_path)

print(f"Using environment: {env_config.name}")
print(f"S3 endpoint: {env_config.s3.endpoint_url}")
```

### Extract from S3

```python
from core.extractors.file_extractor import FileExtractor
from core.config.dataset import dataset_to_runtime_config
from datetime import date

# Create extractor with environment config
extractor = FileExtractor(env_config=env_config)

# Build runtime config
runtime_cfg = dataset_to_runtime_config(dataset)

# Fetch records from S3
records, _ = extractor.fetch_records(runtime_cfg, date.today())
print(f"Loaded {len(records)} records from S3")
```

### Process Silver from S3 Bronze

```python
from core.silver.processor import SilverProcessor

processor = SilverProcessor(
    dataset=dataset,
    bronze_path=bronze_path,
    silver_partition=silver_partition,
    run_date=run_date,
    env_config=env_config,  # Pass environment config
    write_parquet=True
)

result = processor.run()
print(f"Processed {result.metrics.rows_written} rows to Silver")
```

## Verifying S3 Access

### Test MinIO Connection

```python
from core.config.environment import EnvironmentConfig
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem

# Load environment config
env_config = EnvironmentConfig.from_yaml(Path("environments/dev.yaml"))

# Parse S3 URI
uri = StorageURI.parse("s3://mdf/source_samples")

# Create filesystem
fs = create_filesystem(uri, env_config)

# List files
files = fs.ls("mdf/source_samples", detail=False)
print(f"Found {len(files)} items in source_samples:")
for f in files[:10]:
    print(f"  - {f}")
```

## Troubleshooting

### Connection Refused

If you get connection errors:
- Verify MinIO is running on port 9000 (not 9001)
- Check endpoint URL is `http://localhost:9000` (API port)
- Port 9001 is the browser UI, not the API

### Authentication Errors

If you get 403 or authentication errors:
- Verify credentials in `environments/dev.yaml`
- Check username is `minioadmin` and password is `minioadmin123`
- Ensure bucket `mdf` exists in MinIO

### File Not Found

If source files aren't found:
- Check the exact path in MinIO browser at `http://localhost:9001/browser/mdf`
- Verify the `path_pattern` in your config matches the MinIO structure
- Use `s3://source_data/source_samples/...` where `source_data` resolves to `mdf`

## Migration Between Environments

### From Local to S3

1. **Upload files to MinIO**:
   ```bash
   # Using MinIO client (mc)
   mc cp -r ./sampledata/source_samples/ minio/mdf/source_samples/

   # Or using AWS CLI (works with MinIO too)
   aws --endpoint-url http://localhost:9000 \
       s3 sync ./sampledata/source_samples/ s3://mdf/source_samples/
   ```

2. **Update pattern config**:
   ```yaml
   bronze:
     source_storage: s3  # Change from "local"
     path_pattern: s3://source_data/source_samples/...  # Change from ./sampledata/...
   ```

### From S3 Back to Local

1. **Download files**:
   ```bash
   aws --endpoint-url http://localhost:9000 \
       s3 sync s3://mdf/source_samples/ ./sampledata/source_samples/
   ```

2. **Update pattern config**:
   ```yaml
   bronze:
     source_storage: local  # Change from "s3"
     path_pattern: ./sampledata/source_samples/...  # Change from s3://...
   ```

## AWS S3 (Production)

For AWS S3 instead of MinIO:

### Environment Configuration

```yaml
name: prod

s3:
  endpoint_url: null  # Use default AWS endpoints
  access_key_id: ${AWS_ACCESS_KEY_ID}
  secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  region: us-west-2

  buckets:
    source_data: my-prod-source-bucket
    bronze_data: my-prod-bronze-bucket
    silver_data: my-prod-silver-bucket
```

### Set Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Use Standard S3 Paths

```yaml
bronze:
  path_pattern: s3://source_data/source_samples/...
```

The `${VAR}` syntax in the environment config will automatically resolve from environment variables!
