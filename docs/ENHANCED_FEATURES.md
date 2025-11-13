# Enhanced Features Guide

This guide covers the advanced features added to bronze-foundry for production-scale data extraction.

## Table of Contents

1. [File Size Control](#file-size-control)
2. [Multiple Daily Loads & Partitioning](#multiple-daily-loads--partitioning)
3. [Parallel Extraction](#parallel-extraction)
4. [Batch Metadata](#batch-metadata)

---

## File Size Control

Control the size of output files to optimize for analytics query engines and data processing performance.

### Configuration

```yaml
source:
  run:
    # Row-based limit
    max_rows_per_file: 1000000  # Maximum rows per file (0 = unlimited)
    
    # Size-based limit (takes precedence over row limit)
    max_file_size_mb: 256       # Maximum file size in MB
```

### Best Practices for Analytics Platforms

- **Optimal file size**: 128MB - 1GB per file
- **Too small** (<64MB): Poor vectorization, high metadata overhead
- **Too large** (>1GB): Memory pressure, slow queries
- **Recommended**: `max_file_size_mb: 256` for balanced performance

### Example

```bash
# Extract with 256MB file size limit
bronze-extract --config configs/sales.yaml

# Output structure:
# output/system=salesforce/table=accounts/dt=2025-01-12/
#   ├── part-0001.parquet  (256 MB)
#   ├── part-0002.parquet  (256 MB)
#   └── part-0003.parquet  (128 MB)
```

---

## Multiple Daily Loads & Partitioning

Handle multiple extraction runs per day with partition strategies optimized for query engines.

### Partition Strategies

#### 1. **Date Partitioning** (Default)
One partition per day - ideal for daily batch loads.

```yaml
platform:
  bronze:
    partitioning:
      use_dt_partition: true
      partition_strategy: "date"
```

**Output structure**:
```
system=salesforce/table=accounts/
  └── dt=2025-01-12/
      ├── part-0001.parquet
      └── part-0002.parquet
```

**Example query**:
```sql
SELECT * FROM bronze.accounts
WHERE dt = DATE '2025-01-12';
-- Partition pruning: reads only dt=2025-01-12/
```

#### 2. **Hourly Partitioning**
One partition per hour - ideal for multiple daily loads.

```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "hourly"
```

**Output structure**:
```
system=salesforce/table=accounts/
  └── dt=2025-01-12/
      ├── hour=08/
      │   └── part-0001.parquet
      ├── hour=14/
      │   └── part-0001.parquet
      └── hour=20/
          └── part-0001.parquet
```

**Example query**:
```sql
-- Query specific hour
SELECT * FROM bronze.accounts
WHERE dt = DATE '2025-01-12' AND hour = 14;
-- Partition pruning: reads only dt=2025-01-12/hour=14/

-- Query full day
SELECT * FROM bronze.accounts
WHERE dt = DATE '2025-01-12';
-- Partition pruning: reads all hours for 2025-01-12
```

#### 3. **Timestamp Partitioning**
Minute-level granularity - ideal for very frequent loads.

```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "timestamp"
```

**Output structure**:
```
system=salesforce/table=accounts/
  └── dt=2025-01-12/
      ├── batch=20250112_0830/
      ├── batch=20250112_1445/
      └── batch=20250112_2015/
```

#### 4. **Batch ID Partitioning**
Custom batch tracking - ideal for idempotent loads and reprocessing.

```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "batch_id"

source:
  run:
    # Optional: provide custom batch_id
    batch_id: "batch_20250112_001"
    # If omitted, auto-generated as YYYYMMDD_HHMMSS_<random>
```

**Output structure**:
```
system=salesforce/table=accounts/
  └── dt=2025-01-12/
      └── batch_id=batch_20250112_001/
          ├── part-0001.parquet
          └── _metadata.json
```

**Example query**:
```sql
SELECT * FROM bronze.accounts
WHERE dt = DATE '2025-01-12' 
  AND batch_id = 'batch_20250112_001';
```

### Partition Pruning Benefits

Query engines with cost-based optimizers uses partition pruning to skip irrelevant data:

- **Without partitions**: Scans entire table (slow, expensive)
- **With date partitions**: Scans only relevant dates (10-100x faster)
- **With hourly partitions**: Scans only relevant hours (100-1000x faster)

**Example**: Table with 365 days of data, query for 1 day:
- No partitions: Reads 100% of data
- Date partitions: Reads 0.27% of data (365x reduction)
- Hourly partitions: Reads 0.011% of data (8760x reduction)

---

## Parallel Extraction

Extract multiple data sources concurrently to reduce total runtime. Bronze-foundry supports **two levels of parallelism**:

1. **Config-level parallelism**: Run multiple config files concurrently (via CLI `--parallel-workers`)
2. **Chunk-level parallelism**: Process chunks within a single extraction concurrently (via config `parallel_workers`)

### Chunk-Level Parallelism (Within Single Extraction)

Process multiple file chunks concurrently within a single extraction job.

**Configuration:**
```yaml
source:
  run:
    max_file_size_mb: 256       # Creates multiple chunks
    parallel_workers: 4         # Process 4 chunks at a time
```

**Example:**
```bash
# Single extraction with 4 parallel chunk processors
bronze-extract --config configs/large_table.yaml --date 2025-01-12

# Log output:
# [INFO] Retrieved 10000000 records from extractor
# [INFO] Chunked 10000000 records into 40 chunks (max_file_size_mb=256)
# [INFO] Processing 40 chunks with 4 workers
# [DEBUG] Chunk 1 completed successfully
# [DEBUG] Chunk 2 completed successfully
# [DEBUG] Chunk 3 completed successfully
# [DEBUG] Chunk 4 completed successfully
# ... (continues with next batch)
```

**Benefits:**
- Faster single-source extraction (especially for large datasets)
- Efficient use of I/O bandwidth
- Reduced wall-clock time for big extractions
- Particularly effective when writing to S3 (concurrent uploads)

**Best Practices:**
- **Large extractions** (>10 chunks): Use 4-8 workers
- **Small extractions** (<5 chunks): Use 1-2 workers (default)
- **S3-enabled**: Higher workers benefit from concurrent uploads
- **Local-only**: Moderate workers (I/O may be bottleneck)

### Config-Level Parallelism (Multiple Configs)

Run multiple extraction jobs (different config files) concurrently.

### CLI Usage

```bash
# Sequential extraction (default)
bronze-extract --config configs/sales.yaml,configs/marketing.yaml

# Parallel extraction with 4 workers
bronze-extract --config configs/sales.yaml,configs/marketing.yaml --parallel-workers 4

# Extract multiple sources in parallel
bronze-extract \
  --config configs/salesforce_accounts.yaml,configs/salesforce_contacts.yaml,configs/hubspot_deals.yaml \
  --parallel-workers 3 \
  --date 2025-01-12
```

### Combining Both Levels

For maximum performance, combine both parallelism levels:

```yaml
# configs/large_table.yaml
source:
  run:
    max_file_size_mb: 256
    parallel_workers: 4    # Chunk-level parallelism
```

```bash
# Config-level parallelism (3 configs) × Chunk-level parallelism (4 workers each)
bronze-extract \
  --config configs/table1.yaml,configs/table2.yaml,configs/table3.yaml \
  --parallel-workers 3 \
  --date 2025-01-12

# Total concurrency: 3 configs × 4 chunk workers = up to 12 concurrent operations
```

**Warning**: Monitor resource usage when combining parallelism levels:
- **CPU**: Total workers = config_workers × chunk_workers
- **Memory**: Each chunk holds data in memory
- **Network**: S3 uploads multiply with worker count

### Performance Considerations

- **I/O-bound** (API/DB): Use `workers = number of sources` (max benefit)
- **CPU-bound** (large transforms): Use `workers = CPU cores`
- **Memory limits**: Monitor RAM usage, reduce workers if OOM errors occur
- **Rate limits**: Respect API rate limits, adjust workers accordingly

### Example Speedup

Sequential vs. Parallel for 5 sources:

| Workers | Runtime | Speedup |
|---------|---------|---------|
| 1 (sequential) | 50 min | 1x |
| 2 | 28 min | 1.8x |
| 4 | 16 min | 3.1x |
| 5 | 14 min | 3.6x |

**Chunk-level parallel for large single source (40 chunks):**

| Chunk Workers | Runtime | Speedup |
|---------------|---------|---------|
| 1 (sequential) | 40 min | 1x |
| 2 | 22 min | 1.8x |
| 4 | 12 min | 3.3x |
| 8 | 8 min | 5.0x |

### Monitoring

Check logs for parallel execution status:

**Config-level parallelism:**
```
[INFO] Starting parallel extraction with 4 workers for 5 configs
[INFO] Starting extraction for salesforce.accounts
[INFO] Starting extraction for salesforce.contacts
[INFO] ✓ Successfully completed extraction for salesforce.accounts
[INFO] ✓ Successfully completed extraction for hubspot.deals
[INFO] Parallel extraction complete: 5 successful, 0 failed out of 5 total
```

**Chunk-level parallelism:**
```
[INFO] Retrieved 10000000 records from extractor
[INFO] Chunked 10000000 records into 40 chunks
[INFO] Processing 40 chunks with 4 workers
[DEBUG] Chunk 1 completed successfully
[DEBUG] Chunk 3 completed successfully
[DEBUG] Chunk 2 completed successfully
[DEBUG] Chunk 4 completed successfully
[INFO] Finished Bronze extract run successfully
```

---

## Batch Metadata

Track extraction metadata for monitoring, debugging, and idempotent loads.

### Metadata File

Each extraction creates `_metadata.json` in the output directory:

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

### Use Cases

#### 1. **Monitoring & Alerting**
```python
import json
from pathlib import Path

# Check if extraction completed
metadata_path = Path("output/system=salesforce/table=accounts/dt=2025-01-12/_metadata.json")
if not metadata_path.exists():
    alert("Extraction failed or incomplete")

# Check record count
with open(metadata_path) as f:
    meta = json.load(f)
    if meta["total_records"] == 0:
        alert("No records extracted - possible data issue")
```

#### 2. **Idempotent Loads**
```python
# Check if batch already processed
batch_id = "batch_20250112_001"
metadata_path = Path(f"output/.../batch_id={batch_id}/_metadata.json")

if metadata_path.exists():
    print(f"Batch {batch_id} already processed, skipping")
else:
    run_extraction(batch_id)
```

#### 3. **Incremental Cursor Tracking**
```python
# Resume from last cursor
with open(metadata_path) as f:
    meta = json.load(f)
    last_cursor = meta.get("new_cursor")
    
# Use in next extraction
config["source"]["db"]["incremental_cursor_value"] = last_cursor
```

#### 4. **Data Quality Checks**
```sql
-- Query your analytics platform to validate record counts
SELECT 
  dt,
  hour,
  COUNT(*) as record_count,
  COUNT(DISTINCT id) as unique_ids
FROM bronze.accounts
WHERE dt = DATE '2025-01-12'
GROUP BY dt, hour;

-- Compare with _metadata.json total_records
```

---

## Complete Example

Putting it all together - production-ready configuration:

```yaml
platform:
  bronze:
    s3_bucket: "production-bronze"
    s3_prefix: "bronze"
    
    partitioning:
      use_dt_partition: true
      partition_strategy: "hourly"  # Multiple daily loads
    
    output_defaults:
      allow_csv: false              # Parquet only for production
      allow_parquet: true
      parquet_compression: "snappy" # Fast compression

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
    
    incremental_key: "updated_at"
    incremental_cursor_env: "ORDERS_LAST_CURSOR"
  
  run:
    # Query-optimized file sizes
    max_file_size_mb: 256
    
    # Parquet only
    write_csv: false
    write_parquet: true
    
    # Production settings
    s3_enabled: true
    cleanup_on_failure: true
    local_output_dir: "./staging/bronze"
```

**Run with parallel extraction**:
```bash
# Extract 10 tables in parallel with 5 workers
bronze-extract \
  --config configs/orders.yaml,configs/customers.yaml,configs/products.yaml,... \
  --parallel-workers 5 \
  --date 2025-01-12
```

**Table creation example**:
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
  external_location = 's3://production-bronze/bronze/system=postgres_prod/table=orders/'
);

-- Query with partition pruning
SELECT * 
FROM bronze.orders
WHERE dt = DATE '2025-01-12' 
  AND hour >= 8 
  AND hour <= 17;
```

---

## Performance Tuning

### Example query Optimization

1. **Always filter on partition columns first**:
   ```sql
   -- Good: Partition pruning before other filters
   WHERE dt = DATE '2025-01-12' AND hour = 14 AND status = 'completed'
   
   -- Bad: No partition pruning
   WHERE status = 'completed' AND dt = DATE '2025-01-12'
   ```

2. **Use appropriate file sizes**:
   - Small clusters (< 10 nodes): 128-256 MB
   - Large clusters (> 50 nodes): 256-512 MB
   - Very large clusters (> 100 nodes): 512 MB - 1 GB

3. **Monitor query engine metrics**:
   ```sql
   -- Check partition distribution
   SELECT dt, hour, COUNT(*) as files
   FROM information_schema.files
   WHERE table_name = 'orders'
   GROUP BY dt, hour;
   ```

### Extraction Optimization

1. **Batch sizing**: Balance between memory and parallelism
   - Too small: High overhead, many small files
   - Too large: Memory pressure, slow queries
   - Sweet spot: 256 MB files, 500K-2M rows per file

2. **Parallel workers**: Match to infrastructure
   - API-heavy: Workers = concurrent API limits
   - DB-heavy: Workers = DB connection pool size
   - Mixed: Start with 4 workers, scale up/down based on monitoring

3. **Partitioning strategy**: Match to query patterns
   - Daily batch jobs: `partition_strategy: "date"`
   - Multiple daily loads: `partition_strategy: "hourly"`
   - Real-time/streaming: `partition_strategy: "timestamp"`
   - Reprocessing/backfills: `partition_strategy: "batch_id"`

---

## Troubleshooting

### Issue: Files too small
**Symptoms**: Many small Parquet files (< 64 MB)  
**Solution**: Increase `max_file_size_mb` or `max_rows_per_file`

### Issue: Files too large
**Symptoms**: OOM errors, slow queries  
**Solution**: Decrease `max_file_size_mb` to 128-256 MB

### Issue: Too many partitions
**Symptoms**: Slow metadata queries, high S3 list operations  
**Solution**: Use coarser partitioning (hourly → daily)

### Issue: Parallel extraction failures
**Symptoms**: Some extractions succeed, others fail  
**Solution**: Check logs for specific errors, reduce `--parallel-workers`

### Issue: Missing metadata files
**Symptoms**: `_metadata.json` not created  
**Solution**: Check for extraction errors, ensure write permissions

---

## Next Steps

- See [CONFIG_REFERENCE.md](../docs/CONFIG_REFERENCE.md) for complete configuration options
- See [QUICKSTART.md](../QUICKSTART.md) for basic usage
- See [EXTENDING_EXTRACTORS.md](../docs/EXTENDING_EXTRACTORS.md) for custom extractors
