# medallion-foundry Documentation

Complete documentation for medallion-foundry - a production-ready, config-driven Python framework for landing data from APIs, databases, or custom sources into Bronze layer with pluggable storage backends (S3, Azure, GCS, local filesystem).

> **📚 Documentation Navigation:**
> - **README.md** - Project overview and key features
> - **QUICKSTART.md** - 2-minute quick start for first-time users
> - **QUICK_REFERENCE.md** - Command reference and common patterns
> - **INSTALLATION.md** - Detailed installation instructions
> - **CONTRIBUTING.md** - Guidelines for contributors
> - **TESTING.md** - Testing guide and CI/CD setup
> - **This file (DOCUMENTATION.md)** - Comprehensive reference documentation

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [Architecture](#architecture)
4. [Configuration Reference](#configuration-reference)
5. [Features](#features)
6. [Extending medallion-foundry](#extending-medallion-foundry)
   - [Custom Extractors](#custom-extractors)
   - [Storage Backends](#extending-storage-backends)
7. [Examples](#examples)
8. [Best Practices](#best-practices)
9. [Additional Documentation](#additional-documentation)

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/bronze-factory/medallion-foundry.git
cd medallion-foundry

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

### Basic Usage

1. **Create a configuration file**:

```bash
cp docs/examples/configs/api_example.yaml my_config.yaml
# Edit my_config.yaml with your settings
```

2. **Set environment variables**:

```bash
export MY_API_TOKEN="your-token-here"
export BRONZE_S3_ENDPOINT="https://s3.amazonaws.com"
export BRONZE_S3_ACCESS_KEY="your-access-key"
export BRONZE_S3_SECRET_KEY="your-secret-key"
```

3. **Run extraction**:

```bash
# Single extraction
python bronze_extract.py --config my_config.yaml --date 2025-01-12

# Multiple sources in parallel
python bronze_extract.py \
  --config config1.yaml,config2.yaml,config3.yaml \
  --parallel-workers 3 \
  --date 2025-01-12
```

4. **Check output**:

```bash
ls -la output/system=*/table=*/dt=*/
```

---

## Installation

### Requirements

- Python 3.8+
- pip package manager

### Standard Installation

```bash
pip install -r requirements.txt
```

### Development Installation

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=core --cov=extractors
```

### Dependencies

**Core**:
- `boto3` (≥1.26.0) - S3 operations
- `requests` (≥2.28.0) - API extraction
- `pandas` (≥1.5.0) - Data processing
- `pyarrow` (≥10.0.0) - Parquet support
- `pyyaml` (≥6.0) - Configuration parsing
- `pyodbc` (≥4.0.0) - Database extraction
- `tenacity` (≥8.0.0) - Retry logic

**Development**:
- `pytest` (≥7.0.0)
- `pytest-cov` (≥4.0.0)

---

## Architecture

### Design Principles

**Single CLI, Many Sources**  
One entrypoint (`bronze_extract.py`) reads YAML config and chooses the appropriate extractor.

**Standard Bronze Layout**  
Predictable directory structure optimized for query engines and analytics platforms:
```
bronze/system=<system>/table=<table>/dt=YYYY-MM-DD/[hour=HH/]part-0001.parquet
```

**Config-Driven Rigor**  
- **Platform section**: Owned by data platform team (buckets, prefixes, defaults)
- **Source section**: Owned by domain teams (API/DB details, queries, table names)

**Extractor Interface**  
Simple `BaseExtractor` interface for new source types while reusing Bronze writing, S3 upload, and partitioning logic.

### Directory Structure

```
medallion-foundry/
├── bronze_extract.py          # CLI entrypoint
├── core/                      # Core framework
│   ├── config.py             # Config loading & validation
│   ├── runner.py             # Orchestration & execution
│   ├── io.py                 # File I/O (CSV, Parquet)
│   ├── s3.py                 # S3 operations
│   └── parallel.py           # Parallel execution
├── extractors/               # Data extractors
│   ├── base.py              # Base extractor interface
│   ├── api_extractor.py     # REST API extraction
│   └── db_extractor.py      # Database extraction
├── docs/                     # Documentation & examples
│   ├── examples/            # Example configs & extractors
│   ├── ARCHITECTURE.md
│   ├── CONFIG_REFERENCE.md
│   └── EXTENDING_EXTRACTORS.md
├── tests/                    # Unit tests
└── DOCUMENTATION.md          # This file
```

### Data Flow

```
1. Load Config (core/config.py)
   ↓
2. Build Extractor (core/runner.py)
   ↓
3. Fetch Records (extractors/*.py)
   ↓
4. Chunk Records (core/io.py)
   ↓
5. Write Files (core/io.py) [Parallel if configured]
   ↓
6. Upload to S3 (core/s3.py) [Optional]
   ↓
7. Write Metadata (core/io.py)
```

---

## Configuration Reference

### Structure

Every config file has two top-level sections:

```yaml
platform:    # Platform team configuration
  ...
source:      # Domain team configuration
  ...
```

### Platform Section

#### Bronze Configuration

```yaml
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
    
    partitioning:
      use_dt_partition: true
      partition_strategy: "hourly"  # date | hourly | timestamp | batch_id
    
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"  # snappy | gzip | brotli | lz4 | zstd
```

**Fields**:
- `s3_bucket` - Target S3 bucket
- `s3_prefix` - Prefix for all Bronze data
- `partitioning.use_dt_partition` - Include date partition in path
- `partitioning.partition_strategy` - Partition strategy (see [Partition Strategies](#partition-strategies))
- `output_defaults.allow_csv` - Enable CSV output globally
- `output_defaults.allow_parquet` - Enable Parquet output globally
- `output_defaults.parquet_compression` - Parquet compression codec

#### S3 Connection

```yaml
platform:
  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"
```

All values are **environment variable names**, not secrets.

### Source Section

#### Common Fields

```yaml
source:
  type: "api"           # api | db | custom
  system: "salesforce"
  table: "accounts"
  
  run:
    # File size control
    max_rows_per_file: 1000000
    max_file_size_mb: 256
    
    # Parallelism
    parallel_workers: 4
    
    # Output formats
    write_csv: false
    write_parquet: true
    
    # Storage upload (supports s3_enabled for backward compatibility or storage_enabled)
    storage_enabled: true  # Upload to configured storage backend
    
    # Paths
    local_output_dir: "./output"
    
    # Error handling
    cleanup_on_failure: true
    
    # Batch tracking
    batch_id: "batch_001"  # optional
```

**Fields**:
- `type` - Extractor type: `api`, `db`, or `custom`
- `system` - Logical system identifier (→ `system=<value>` in path)
- `table` - Logical table identifier (→ `table=<value>` in path)
- `run.max_rows_per_file` - Maximum rows per file (0 = unlimited)
- `run.max_file_size_mb` - Maximum file size in MB (overrides row limit)
- `run.parallel_workers` - Concurrent chunk processing (1 = sequential, 4-8 recommended for large extractions)
- `run.write_csv` - Write CSV files
- `run.write_parquet` - Write Parquet files
- `run.storage_enabled` - Upload to configured storage backend (also supports `s3_enabled` for backward compatibility)
- `run.local_output_dir` - Local staging directory
- `run.cleanup_on_failure` - Delete files on failure (default: true)
- `run.batch_id` - Custom batch ID (auto-generated if omitted)

#### API Configuration

```yaml
source:
  type: "api"
  
  api:
    base_url: "https://api.example.com"
    endpoint: "/v1/tickets"
    method: "GET"
    
    # Authentication
    auth_type: "bearer"           # bearer | api_key | basic | none
    auth_token_env: "API_TOKEN"   # For bearer
    # auth_key_name: "X-API-Key"  # For api_key
    # auth_key_env: "API_KEY"
    # auth_username_env: "API_USER"  # For basic
    # auth_password_env: "API_PASS"
    
    # Headers
    headers:
      User-Agent: "medallion-foundry/1.0"
    
    # Query parameters
    params:
      page_size: 100
    
    # Pagination
    pagination:
      type: "offset"              # offset | page | cursor | none
      limit_param: "limit"
      offset_param: "offset"
      records_path: "data"
```

**Pagination Types**:
- `offset`: Offset-based (e.g., `?offset=0&limit=100`)
- `page`: Page-based (e.g., `?page=1&page_size=100`)
- `cursor`: Cursor-based (e.g., `?cursor=abc123`)
- `none`: No pagination (single request)

#### Database Configuration

```yaml
source:
  type: "db"
  
  db:
    conn_str_env: "POSTGRES_CONN_STR"
    base_query: |
      SELECT id, name, created_at, updated_at
      FROM accounts
      WHERE is_deleted = false
    
    # Incremental loading
    incremental_key: "updated_at"
    incremental_cursor_env: "ACCOUNTS_CURSOR"
    
    # Performance
    batch_size: 10000
```

**Fields**:
- `conn_str_env` - Environment variable with ODBC connection string
- `base_query` - Base SQL query
- `incremental_key` - Column for incremental loading
- `incremental_cursor_env` - Environment variable storing last cursor value
- `batch_size` - Rows to fetch per batch (default: 10000)

#### Custom Extractor Configuration

```yaml
source:
  type: "custom"
  
  custom_extractor:
    module: "examples.custom_extractors.salesforce_example"
    class_name: "SalesforceExtractor"
    
    # Custom parameters (passed to extractor)
    params:
      instance_url: "https://mycompany.salesforce.com"
      object_type: "Account"
```

### Partition Strategies

#### Date (Default)
```yaml
partition_strategy: "date"
# Output: dt=2025-01-12/
```
**Use case**: Daily batch loads

#### Hourly
```yaml
partition_strategy: "hourly"
# Output: dt=2025-01-12/hour=14/
```
**Use case**: Multiple loads per day (2-24 times)

#### Timestamp
```yaml
partition_strategy: "timestamp"
# Output: dt=2025-01-12/batch=20250112_1430/
```
**Use case**: High-frequency loads (>24 times per day)

#### Batch ID
```yaml
partition_strategy: "batch_id"
# Output: dt=2025-01-12/batch_id=custom_001/
```
**Use case**: Reprocessing, backfills, idempotent loads

---

## Features

### File Size Control

Optimize file sizes for analytics query engines and data processing.

**Configuration**:
```yaml
source:
  run:
    max_file_size_mb: 256  # 128-512 recommended for analytics workloads
```

**Benefits**:
- 2-5x faster queries (vectorization and parallel processing)
- Reduced metadata overhead
- Better memory management
- Improved parallelism

**Recommendations**:
- Small clusters (<10 nodes): 128 MB
- Medium clusters (10-50 nodes): 256 MB
- Small datasets (<1M records/day): 128 MB
- Medium datasets (1-10M records/day): 256 MB
- Large datasets (>10M records/day): 512 MB

### Multiple Daily Loads

Handle multiple extraction runs per day with partition strategies optimized for query engines.

**Hourly Partitions**:
```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "hourly"
```

**Example Query** (for engines supporting partition pruning):
```sql
SELECT * FROM bronze.accounts
WHERE dt = DATE '2025-01-12' AND hour = 14;
-- Only scans 1 hour of data instead of entire table!
```

**Benefits**:
- 10-1000x faster queries (partition pruning when supported)
- Support for multiple daily loads
- No data conflicts

### Two-Level Parallelism

medallion-foundry supports **two independent levels of parallelism**:

#### Level 1: Config-Level (Multiple Configs)

Run multiple config files concurrently:

```bash
bronze-extract \
  --config sales.yaml,marketing.yaml,products.yaml \
  --parallel-workers 3
```

**Use case**: Extract multiple independent sources concurrently

#### Level 2: Chunk-Level (Within Config)

Process file chunks concurrently within single extraction:

```yaml
source:
  run:
    max_file_size_mb: 256
    parallel_workers: 4
```

**Use case**: Large single-source extractions with many chunks

#### Combined (Maximum Performance)

```yaml
# Each config has chunk-level parallelism
source:
  run:
    parallel_workers: 4
```

```bash
# Run 3 configs, each with 4 chunk workers
bronze-extract \
  --config large1.yaml,large2.yaml,large3.yaml \
  --parallel-workers 3

# Total: 3 × 4 = 12 concurrent operations
```

**Performance**:
- Chunk parallel: 3-5x faster
- Config parallel: 3-4x faster
- Combined: **10x faster!**

### Batch Metadata

Automatic metadata tracking for monitoring and idempotent loads.

Each extraction creates `_metadata.json`:

```json
{
  "batch_timestamp": "2025-01-12T14:30:15.123456",
  "run_date": "2025-01-12",
  "system": "salesforce",
  "table": "accounts",
  "total_records": 2500000,
  "num_files": 10,
  "partition_path": "system=salesforce/table=accounts/dt=2025-01-12/hour=14/",
  "file_formats": {
    "csv": false,
    "parquet": true
  },
  "new_cursor": "2025-01-12T14:30:00Z"
}
```

**Use cases**:
- Monitoring & alerting
- Idempotent load validation
- Incremental cursor tracking
- Data lineage & auditing

---

## Extending medallion-foundry

### Creating Custom Extractors

1. **Create extractor class**:

```python
# docs/examples/custom_extractors/my_extractor.py
from typing import Dict, Any, List, Tuple, Optional
from datetime import date
from extractors.base import BaseExtractor

class MyCustomExtractor(BaseExtractor):
    def fetch_records(
        self, 
        cfg: Dict[str, Any], 
        run_date: date
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Extract records from custom source.
        
        Returns:
            Tuple of (records, new_cursor)
            - records: List of dictionaries
            - new_cursor: Optional cursor for incremental loads
        """
        source_cfg = cfg["source"]
        custom_cfg = source_cfg.get("custom_extractor", {})
        params = custom_cfg.get("params", {})
        
        # Your extraction logic here
        records = []
        # ... fetch data ...
        
        new_cursor = None  # Update if doing incremental loads
        
        return records, new_cursor
```

2. **Create configuration**:

```yaml
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: true
      allow_parquet: true

  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"

source:
  type: "custom"
  system: "my_system"
  table: "my_table"
  
  custom_extractor:
    module: "examples.custom_extractors.my_extractor"
    class_name: "MyCustomExtractor"
    
    params:
      api_url: "https://api.example.com"
      # Add your custom parameters
  
  run:
    max_file_size_mb: 256
    parallel_workers: 4
    write_parquet: true
    storage_enabled: true  # Upload to configured storage backend
```

3. **Run extraction**:

```bash
python bronze_extract.py --config my_custom_config.yaml
```

### BaseExtractor Interface

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Optional
from datetime import date

class BaseExtractor(ABC):
    @abstractmethod
    def fetch_records(
        self, 
        cfg: Dict[str, Any], 
        run_date: date
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetch records from the source.
        
        Args:
            cfg: Full configuration dictionary
            run_date: Logical run date for the extraction
            
        Returns:
            Tuple of (records, new_cursor)
            - records: List of record dictionaries
            - new_cursor: Optional cursor value for incremental loads
        """
        pass
```

**See also**: [EXTENDING_EXTRACTORS.md](docs/EXTENDING_EXTRACTORS.md) for detailed implementation guide.

### Extending Storage Backends

The framework provides a pluggable storage backend architecture supporting multiple storage systems out of the box.

**Supported Backends**:
- **S3** - AWS S3, MinIO, and any S3-compatible storage (default)
- **Azure** - Azure Blob Storage and ADLS Gen2 
- **GCS** - Google Cloud Storage
- **Local** - Local filesystem for development/testing

**Configuration**:
```yaml
platform:
  bronze:
    storage_backend: "s3"  # or "azure", "gcs", "local"
    # Backend-specific configuration follows...
```

**Want to add a custom storage backend?** See the complete implementation example:

The StorageBackend abstract base class provides a clean interface:
- `upload_file()` - Upload to remote storage
- `download_file()` - Download from remote storage
- `list_files()` - List files by prefix
- `delete_file()` - Delete from remote storage
- `get_backend_type()` - Return backend identifier

This architecture ensures vendor-neutrality and extensibility.

---

## Examples

### Example 1: API Extraction

Extract data from REST API with authentication and pagination.

**Config** (`configs/api_example.yaml`):
```yaml
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
      partition_strategy: "date"
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"

source:
  type: "api"
  system: "github"
  table: "issues"

  api:
    base_url: "https://api.github.com"
    endpoint: "/repos/owner/repo/issues"
    method: "GET"

    auth_type: "bearer"
    auth_token_env: "GITHUB_TOKEN"

    params:
      state: "all"
      per_page: 100

    pagination:
      type: "page"
      page_param: "page"
      page_size_param: "per_page"
      records_path: null

  run:
    max_file_size_mb: 128
    parallel_workers: 2
    write_csv: false
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
```

**Run**:
```bash
export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
python bronze_extract.py --config configs/api_example.yaml
```

### Example 2: Database Extraction with Incremental Loading

**Config** (`configs/db_example.yaml`):
```yaml
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
      partition_strategy: "hourly"
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"

source:
  type: "db"
  system: "postgres_prod"
  table: "orders"

  db:
    conn_str_env: "POSTGRES_CONN_STR"
    base_query: |
      SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        updated_at
      FROM orders
      WHERE is_deleted = false

    incremental_key: "updated_at"
    incremental_cursor_env: "ORDERS_LAST_CURSOR"
    batch_size: 10000

  run:
    max_file_size_mb: 256
    parallel_workers: 4
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output/bronze"
```

**Run**:
```bash
export POSTGRES_CONN_STR="DRIVER={PostgreSQL};SERVER=localhost;DATABASE=prod;UID=user;PWD=pass"
export ORDERS_LAST_CURSOR="2025-01-01T00:00:00Z"
python bronze_extract.py --config configs/db_example.yaml --date 2025-01-12
```

### Example 3: Parallel Extraction

Extract multiple sources concurrently.

**Run**:
```bash
# Extract 5 sources with 4 config workers
python bronze_extract.py \
  --config configs/sales.yaml,configs/marketing.yaml,configs/products.yaml,configs/customers.yaml,configs/inventory.yaml \
  --parallel-workers 4 \
  --date 2025-01-12
```

**With chunk-level parallelism** (each config has `parallel_workers: 4`):
```bash
# 4 configs × 4 chunk workers = 16 concurrent operations
python bronze_extract.py \
  --config configs/large1.yaml,configs/large2.yaml,configs/large3.yaml,configs/large4.yaml \
  --parallel-workers 4 \
  --date 2025-01-12
```

---

## Best Practices

### Query Optimization (for Future Analytics Platforms)

1. **File Sizes**: Use 128-512 MB for optimal performance
   ```yaml
   run:
     max_file_size_mb: 256
   ```

2. **Partitioning**: Enable partition pruning strategies
   ```yaml
   partitioning:
     partition_strategy: "hourly"  # For multiple daily loads
   ```

3. **Format**: Use Parquet, not CSV
   ```yaml
   run:
     write_csv: false
     write_parquet: true
   ```

4. **Compression**: Use Snappy for balance
   ```yaml
   output_defaults:
     parquet_compression: "snappy"
   ```

5. **Queries**: Always filter on partition columns first
   ```sql
   SELECT * FROM bronze.orders
   WHERE dt = DATE '2025-01-12'  -- Partition column first
     AND hour = 14
     AND status = 'completed';
   ```

### Parallelism

1. **Small extractions** (<5 chunks): `parallel_workers: 1` (sequential)
2. **Large extractions** (40+ chunks): `parallel_workers: 4-8`
3. **Multiple sources**: CLI `--parallel-workers` = number of sources
4. **Combined**: Monitor CPU/memory, start conservative

### Error Handling

1. **Enable cleanup**: `cleanup_on_failure: true`
2. **Monitor metadata**: Check `_metadata.json` files
3. **Use batch_id**: For reprocessing and idempotent loads
4. **Check logs**: Structured logging shows all operations

### Security

1. **Never commit secrets**: Use environment variables
2. **Restrict IAM**: Minimum S3 permissions (PutObject, GetObject)
3. **Rotate credentials**: Regular rotation of API tokens and DB passwords
4. **Audit access**: Monitor S3 access logs

### Monitoring

1. **Metadata files**: Validate `_metadata.json` after each run
2. **Record counts**: Compare with source system
3. **File sizes**: Monitor for consistency
4. **Partition distribution**: Monitor partition layout
5. **Error rates**: Alert on failures

### Performance Tuning

1. **Start simple**: Default settings first
2. **Measure baseline**: Time sequential extraction
3. **Add chunk parallelism**: Increase `parallel_workers` gradually
4. **Add config parallelism**: Use CLI `--parallel-workers` for multiple sources
5. **Monitor resources**: CPU, memory, I/O, network
6. **Tune based on data**: Adjust `max_file_size_mb` based on file count

---

## Future Platform Integration

> **Note**: medallion-foundry creates standard Parquet files with Hive-style partitioning that can be consumed by various analytics platforms when available.

### Example Table Creation

**For date partitions**:
```sql
CREATE TABLE bronze.orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  total_amount DECIMAL(10,2),
  status VARCHAR,
  updated_at TIMESTAMP,
  dt DATE
)
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY['dt'],
  external_location = 's3://analytics-bronze/bronze/system=postgres_prod/table=orders/'
);
```

**For hourly partitions**:
```sql
CREATE TABLE bronze.orders (
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  total_amount DECIMAL(10,2),
  status VARCHAR,
  updated_at TIMESTAMP,
  dt DATE,
  hour INTEGER
)
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY['dt', 'hour'],
  external_location = 's3://analytics-bronze/bronze/system=postgres_prod/table=orders/'
);
```

### Partition Discovery

```sql
-- Sync partitions
CALL system.sync_partition_metadata('bronze', 'orders', 'FULL');

-- List partitions
SELECT * FROM "bronze$partitions" WHERE table_name = 'orders';
```

### Query Optimization

```sql
-- Good: Partition pruning enabled
SELECT COUNT(*) FROM bronze.orders
WHERE dt = DATE '2025-01-12' AND hour = 14;

-- Bad: Full table scan
SELECT COUNT(*) FROM bronze.orders
WHERE order_date = DATE '2025-01-12';
```

---

## Troubleshooting

### Common Issues

**Issue**: Files too small (<64MB)  
**Solution**: Increase `max_file_size_mb` to 128-256

**Issue**: Files too large (>1GB)  
**Solution**: Decrease `max_file_size_mb` to 256-512

**Issue**: No speedup with parallelism  
**Solution**: Check chunk count, reduce `max_file_size_mb` to create more chunks

**Issue**: High memory usage  
**Solution**: Reduce `parallel_workers`

**Issue**: S3 throttling  
**Solution**: Reduce `parallel_workers` or add retry configuration

**Issue**: Missing metadata files  
**Solution**: Check logs for errors, ensure write permissions

### Debug Mode

Enable debug logging:

```python
# In bronze_extract.py, change:
logging.basicConfig(
    level=logging.DEBUG,  # Changed from INFO
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
```

### Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_enhanced_features.py -v

# Run with coverage
pytest tests/ --cov=core --cov=extractors --cov-report=html
```

---

## Additional Documentation

For deeper technical details, see:

- **[Storage Backend Architecture](docs/STORAGE_BACKEND_ARCHITECTURE.md)** - Comprehensive guide to pluggable storage backends
- **[Extending Extractors](docs/EXTENDING_EXTRACTORS.md)** - Guide to creating custom data source extractors
- **[Enhanced Features](docs/ENHANCED_FEATURES.md)** - Advanced features for production use
- **[Architecture](docs/ARCHITECTURE.md)** - Core framework design principles
- **[Configuration Reference](docs/CONFIG_REFERENCE.md)** - Complete YAML configuration guide

---

## License

MIT License - See LICENSE file for details

---

## Support

For issues, questions, or contributions:
- GitHub Issues: https://github.com/bronze-factory/medallion-foundry/issues
- Documentation: This file
- Examples: `docs/examples/` directory
