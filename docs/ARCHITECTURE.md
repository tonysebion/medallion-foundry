# Architecture

This document covers the storage backends, path structures, and PolyBase integration.

## Storage Backends

Bronze-foundry supports multiple storage backends through a pluggable architecture:

| Backend | Implementation | Use Case |
|---------|---------------|----------|
| **S3** | `pipelines/lib/storage/s3.py` | AWS, MinIO, S3-compatible |
| **Azure** | `pipelines/lib/storage/adls.py` | Blob Storage, ADLS Gen2 |
| **Local** | `pipelines/lib/storage/local.py` | Development, testing |
| **fsspec** | `pipelines/lib/storage/fsspec_backend.py` | Generic filesystem |

### Usage

```python
from pipelines.lib.storage import get_storage

# Automatically selects backend based on path prefix
storage = get_storage("s3://my-bucket/bronze/")

# Read/write data
data = storage.read_bytes("data.parquet")
storage.write_bytes("data.parquet", parquet_bytes)
files = storage.list_files(pattern="*.parquet")
```

### Configuration

The backend is auto-detected from the path prefix, or you can configure explicitly:

```yaml
platform:
  bronze:
    storage_backend: "s3"  # or "azure", "local"
    s3_bucket: "my-bucket"
    s3_prefix: "bronze"

  s3_connection:
    endpoint_url_env: "AWS_ENDPOINT_URL"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"
```

## Path Structure

The medallion architecture uses Hive-style partitioned paths:

### Bronze Layer (Raw)

System-organized raw data landing zone:

```
bronze/
  system=retail/
    entity=orders/
      dt=2025-01-15/
        ├── entity.parquet
        ├── _metadata.json
        └── _checksums.json
```

### Silver Layer (Curated)

Domain-organized business-ready data:

```
silver/
  orders/
    ├── data.parquet
    ├── _metadata.json
    ├── _checksums.json
    └── _polybase.sql
```

### Output Artifacts

**Bronze artifacts:**
- `entity.parquet` - Raw extracted data
- `_metadata.json` - Source info, watermarks, row counts
- `_checksums.json` - SHA256 hashes for integrity

**Silver artifacts:**
- `data.parquet` - Curated data
- `_metadata.json` - Schema, entity info, columns
- `_checksums.json` - SHA256 hashes for integrity
- `_polybase.sql` - Generated DDL for SQL Server

### Path Templates

Bronze and Silver support template variables:

```python
target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/"
source_path="s3://bronze/system=retail/entity=orders/dt={run_date}/*.parquet"
```

Available variables: `{system}`, `{entity}`, `{run_date}`, `{table}`, `{domain}`

## PolyBase Integration

PolyBase allows SQL Server to query Silver layer Parquet files as external tables.

### Generate DDL

```python
from pipelines.lib.polybase import generate_from_metadata, PolyBaseConfig
from pathlib import Path

config = PolyBaseConfig(
    data_source_name="silver_source",
    data_source_location="wasbs://silver@account.blob.core.windows.net/",
)

ddl = generate_from_metadata(Path("./silver/orders/_metadata.json"), config)
print(ddl)  # CREATE EXTERNAL TABLE statements
```

### Generated Objects

1. **External Data Source** - Location of Silver artifacts
2. **External File Format** - Parquet with Snappy compression
3. **External Table** - Maps Parquet schema to SQL Server

### Temporal Queries

For STATE entities (SCD Type 2), query point-in-time state:

```sql
SELECT * FROM orders_state_external
WHERE effective_from_date <= '2025-01-15'
  AND (effective_to_date IS NULL OR effective_to_date > '2025-01-15');
```

For EVENT entities, filter by event date:

```sql
SELECT * FROM orders_events_external
WHERE event_date BETWEEN '2025-01-01' AND '2025-01-31';
```

## Checksum Verification

Silver can validate Bronze checksums before processing:

```python
silver = SilverEntity(
    validate_source="strict",  # "skip", "warn", or "strict"
)
```

- `skip` - No validation (default)
- `warn` - Log warning if checksums fail
- `strict` - Raise `ChecksumValidationError` if checksums fail

### Manual Verification

```python
from pipelines.lib.checksum import verify_checksum_manifest
from pathlib import Path

result = verify_checksum_manifest(Path("./bronze/system=retail/entity=orders/dt=2025-01-15/"))
if not result.valid:
    print(f"Corrupted: {result.mismatched_files}")
```

## Custom Backends

Implement the `StorageBackend` interface:

```python
from pipelines.lib.storage.base import StorageBackend

class MyStorage(StorageBackend):
    def upload_file(self, local_path: str, remote_path: str) -> bool: ...
    def download_file(self, remote_path: str, local_path: str) -> bool: ...
    def list_files(self, prefix: str) -> list[str]: ...
    def delete_file(self, remote_path: str) -> bool: ...
    def get_backend_type(self) -> str: ...
```

See existing implementations in `pipelines/lib/storage/` for examples.
