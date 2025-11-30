# Enhanced Features Quick Reference

## 🚀 Quick Start

### 1. Optimize File Sizes for Query Performance
```yaml
source:
  run:
    max_file_size_mb: 256  # 128-512 recommended for analytics
```

### 2. Enable Hourly Partitions (Multiple Daily Loads)
```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "hourly"
```

### 3. Run Parallel Extraction

**Chunk-level (within single config):**
```yaml
source:
  run:
    parallel_workers: 4  # Process chunks concurrently
```

**Config-level (multiple configs):**
```bash
bronze-extract \
  --config config1.yaml,config2.yaml,config3.yaml \
  --parallel-workers 3
```

**Combined (maximum performance):**
```yaml
# Each config has chunk-level parallelism
source:
  run:
    parallel_workers: 4
```
```bash
# Run 3 configs concurrently, each with 4 chunk workers
bronze-extract \
  --config config1.yaml,config2.yaml,config3.yaml \
  --parallel-workers 3
# Total: up to 12 concurrent operations
```

---

## 📋 Configuration Cheat Sheet

### Partition Strategies
| Strategy | Path Example | Use Case |
|----------|-------------|----------|
| `date` | `dt=2025-01-12/` | Daily batches (default) |
| `hourly` | `dt=2025-01-12/hour=14/` | 2-24 loads per day |
| `timestamp` | `dt=2025-01-12/batch=20250112_1430/` | >24 loads per day |
| `batch_id` | `dt=2025-01-12/batch_id=custom/` | Reprocessing/backfills |

### File Size Recommendations
| Data Volume | max_file_size_mb | Reason |
|-------------|------------------|---------|
| Small (< 1M records/day) | 128 | Fast query startup |
| Medium (1-10M records/day) | 256 | Balanced performance |
| Large (> 10M records/day) | 512 | Optimized for large scans |

### Parallel Workers
| Scenario | Config parallel_workers | CLI --parallel-workers |
|----------|-------------------------|------------------------|
| Single small source | 1 (default) | 1 |
| Single large source (40+ chunks) | 4-8 | 1 |
| Multiple sources (3-5) | 1-2 per config | 3-4 |
| Multiple large sources | 4 per config | 2-3 |
| Maximum throughput | 4-8 per config | 4-8 |

**Note**: Total concurrency = config_workers × chunk_workers. Monitor CPU/memory usage.

---

## 💡 Common Patterns


### Silver Chunking Behavior

`SilverProcessor` automatically chunks large Bronze partitions so you do not need `--stream` or `--resume`. Tune Bronze file sizes via `bronze.options.max_file_size_mb`, `max_rows_per_file`, or partition strategies, and rely on `_metadata.json`/`_checksums.json` for safe reruns. Legacy streaming flags remain documented in `docs/framework/operations/legacy-streaming.md`.

### Pattern 1: High-Performance Analytics
```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "hourly"
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

source:
  run:
    max_file_size_mb: 256
    write_csv: false
    write_parquet: true
```

**Example Query** (for future query engines):
```sql
SELECT * FROM bronze.table
WHERE dt = DATE '2025-01-12' AND hour = 14;
```

### Pattern 2: Idempotent Loads
```yaml
platform:
  bronze:
    partitioning:
      partition_strategy: "batch_id"

source:
  run:
    batch_id: "batch_20250112_001"  # Custom or auto-generated
```

**Check before reprocessing:**
```bash
if [ -f "output/.../batch_id=batch_20250112_001/_metadata.json" ]; then
  echo "Batch already processed"
else
  bronze-extract --config config.yaml
fi
```

### Pattern 3: Fast Multi-Source Extraction
```bash
# Create manifest file
cat > configs.txt <<EOF
configs/salesforce.yaml
configs/hubspot.yaml
configs/postgres.yaml
configs/mysql.yaml
EOF

# Extract all in parallel
bronze-extract \
  --config $(cat configs.txt | tr '\n' ',') \
  --parallel-workers 4 \
  --date $(date +%Y-%m-%d)
```

---

## 🔍 Monitoring & Validation

### Check Extraction Status
```bash
# View metadata
find output -name "_metadata.json" | while read f; do
  echo "=== $f ==="
  cat "$f" | jq '{system, table, total_records, num_files}'
done
```

### Validate File Sizes
```bash
# Check Parquet file sizes
find output -name "*.parquet" -exec ls -lh {} \; | \
  awk '{print $5, $9}' | \
  sort -h
```

### Verify Partitions
```bash
# List all partitions
find output -type d -name "hour=*" -o -name "batch_id=*" | sort
```

### Check Parallel Execution Logs
```bash
# Search for parallel execution summary
grep "Parallel extraction complete" logs/*.log
```

---

## 📊 Future Platform Integration Examples

### With Hourly Partitions (for query engines that support partition pruning)
```sql
CREATE TABLE bronze.my_table (
  id BIGINT,
  name VARCHAR,
  created_at TIMESTAMP,
  dt DATE,
  hour INTEGER
)
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY['dt', 'hour'],
  external_location = 's3://bucket/bronze/system=mysys/table=my_table/'
);
```

### With Batch ID Partitions
```sql
CREATE TABLE bronze.my_table (
  id BIGINT,
  name VARCHAR,
  created_at TIMESTAMP,
  dt DATE,
  batch_id VARCHAR
)
WITH (
  format = 'PARQUET',
  partitioned_by = ARRAY['dt', 'batch_id'],
  external_location = 's3://bucket/bronze/system=mysys/table=my_table/'
);
```

---

## 🛠️ Troubleshooting

### Problem: Files too small (<64MB)
**Solution:**
```yaml
source:
  run:
    max_file_size_mb: 256  # Increase target size
```

### Problem: Files too large (>1GB)
**Solution:**
```yaml
source:
  run:
    max_file_size_mb: 128  # Decrease target size
```

### Problem: Too many partitions
**Solution:** Use coarser strategy
```yaml
# Change from:
partition_strategy: "timestamp"
# To:
partition_strategy: "hourly"
```

### Problem: Parallel extraction fails
**Solution:** Reduce workers
```bash
# Try with fewer workers
bronze-extract --config ... --parallel-workers 2
```

### Problem: Missing metadata files
**Solution:** Check logs for errors
```bash
grep "ERROR" logs/*.log
grep "metadata" logs/*.log
```

---

## 📚 Documentation Links

- **Complete Guide:** [DOCUMENTATION.md](DOCUMENTATION.md)
- **Enhanced Features:** [ENHANCED_FEATURES.md](../../usage/patterns/ENHANCED_FEATURES.md)
- **Config Reference:** [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)
- **Quick Start:** [README](../../../README.md)

---

## ⚡ Performance Tips

1. **Always filter on partition columns first** in queries
2. **Use Parquet, not CSV** for production
3. **Enable partition pruning** with hourly/batch_id strategies
4. **Tune file sizes** to 256MB for most analytics workloads
5. **Monitor metadata files** for extraction validation
6. **Use parallel workers** for multiple sources
7. **Clean up old partitions** to reduce metadata overhead

---

## 🎯 Example Production Config

```yaml
platform:
  bronze:
    s3_bucket: "production-bronze"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
      partition_strategy: "hourly"
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

source:
  type: "db"
  system: "postgres_prod"
  table: "orders"

  db:
    conn_str_env: "POSTGRES_CONN"
    base_query: "SELECT * FROM orders"
    incremental_key: "updated_at"

  run:
    max_file_size_mb: 256
    write_csv: false
    write_parquet: true
    s3_enabled: true
    cleanup_on_failure: true
```

**Run:**
```bash
bronze-extract --config production.yaml --date $(date +%Y-%m-%d)
```

**Example Query** (for future query engine):
```sql
SELECT COUNT(*) FROM bronze.orders
WHERE dt = CURRENT_DATE AND hour = HOUR(CURRENT_TIMESTAMP);
```

---

**Need Help?** See [ENHANCED_FEATURES.md](../../usage/patterns/ENHANCED_FEATURES.md) for detailed documentation.
