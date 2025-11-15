# bronze-foundry

`bronze-foundry` is a **production-ready**, config-driven Python framework for landing data from **APIs**, **databases**, or **custom sources** into a **Bronze layer** with **pluggable storage backends** (S3, Azure, GCS, local filesystem), using conventions that support future analytics platforms and medallion-style architectures.

This framework is intentionally lightweight and orchestration-neutral: you can run it from any scheduler or workflow orchestrator that can invoke a Python CLI.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Key Features

### Core Capabilities
- **🔌 Multiple Source Types** - APIs (REST), databases (SQL), local files, or custom Python extractors
- **🔐 Authentication** - Bearer tokens, API keys, Basic auth, and custom headers
- **📄 Pagination** - Offset-based, page-based, cursor-based, or none
- **🔄 Incremental Loading** - State management for efficient delta loads
- **📊 Multiple Formats** - CSV (debugging) and Parquet (analytics) with compression
- **☁️ Pluggable Storage** - S3, Azure Blob/ADLS, Google Cloud Storage, or local filesystem
- **🔁 Retry Logic** - Automatic retries with exponential backoff for network operations
- **🧹 Error Handling** - Comprehensive error handling with automatic cleanup on failure
- **📝 Extensive Logging** - Structured logging for observability
- **✅ Production Ready** - Complete implementations, not just stubs

### 🎯 Enhanced Features
- **📏 File Size Control** - Configurable `max_file_size_mb` for query-optimized file sizes (128MB-1GB)
- **⏰ Multiple Daily Loads** - Hourly, timestamp, or batch-ID partitioning for intraday extractions
- **⚡ Parallel Extraction** - Two levels of parallelism:
  - **Config-level**: Multi-threaded processing across different config files (`--parallel-workers`)
  - **Chunk-level**: Concurrent chunk processing within single extraction (`parallel_workers` in config)
- **📊 Batch Metadata** - Automatic `_metadata.json` tracking for monitoring and idempotent loads
- **🔌 Extensible Architecture** - Clean abstractions for adding new storage backends or data sources

*See [ENHANCED_FEATURES.md](docs/ENHANCED_FEATURES.md) for detailed documentation on advanced features.*

## 🏗️ Architecture Principles

- **Single CLI, many sources**  
  One entrypoint (`bronze_extract.py`) that reads a YAML config and chooses the appropriate extractor (`api`, `db`, or `custom`).

- **Standard Bronze layout**  
  All data lands in a predictable directory structure (by default):

  ```text
  bronze/system=<system>/table=<table>/dt=YYYY-MM-DD/part-0001.parquet
  ```

- **Config-driven rigor**  
  Each config file has:
  - A **`platform`** section: owned by the data platform team (buckets, prefixes, defaults).
  - A **`source`** section: owned by product/domain teams (API/DB details, query, table name, etc.).

- **Extractor interface**  
  Implement a simple `BaseExtractor` interface to support new source types, while reusing the same Bronze writing, S3 upload, and partitioning logic.

- **Formats and file splitting**  
  - CSV for human-readable debugging (optional)
  - Parquet (with Snappy) for analytics
  - Configurable `max_rows_per_file` to split large extracts into part files (e.g., `part-0001`, `part-0002`, …).

- **Orchestration friendly**  
  - CLI takes `--config` and optional `--date`
  - Uses exit codes (0 = success, non-zero = failure)
  - Structured logging with levels and timestamps

## 🚀 Quick start

### 🎯 Testing Your API (For Product/API Teams)

**Not familiar with Python?** No problem! [QUICKSTART.md](QUICKSTART.md) has a complete step-by-step walkthrough.

**Quick summary:**

```bash
# 1. Setup (one-time, ~3 minutes)
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 2. Copy and edit the quick test config
cp docs/examples/configs/quick_test.yaml config/test.yaml
# Edit config/test.yaml with your API details

# 3. Set your token
export MY_API_TOKEN="your-token-here"  # Linux/Mac
$env:MY_API_TOKEN="your-token-here"    # Windows

# 4. Run
python bronze_extract.py --config config/test.yaml

# 5. Check ./output/ folder - if you see data, you're done!
```

**👉 See [QUICKSTART.md](QUICKSTART.md) for detailed instructions with screenshots and troubleshooting.**

### 🧪 Offline Local Quick Test

Need proof the tooling works but don’t have API credentials yet? Use the bundled sample data.

```bash
# 0. (Once) prepare the sandbox data
python scripts/generate_sample_data.py

# 1. Run Bronze with the file-based config
python bronze_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-13

# 2. Promote the same run into Silver
python silver_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-13

# 3. Inspect the outputs
tree output/system=retail_demo
tree silver_output/domain=retail_demo
```

This workflow uses only local files, so a non-Python user can validate everything end-to-end before wiring real APIs or databases.

---

### 📚 Full Setup (For Data Teams)

Complete production setup:

1. **Install dependencies:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```

2. **Create a config file:**

   ```bash
   cp docs/examples/configs/api_example.yaml config/my_api.yaml
   # Edit config/my_api.yaml with your settings
   ```

3. **Set environment variables:**

   ```bash
   export MY_API_TOKEN="your-token-here"
   ```

4. **Run an extract:**

   ```bash
   # Single extraction
   python bronze_extract.py --config config/my_api.yaml --date 2025-01-12
   
   # Multiple sources in parallel
   python bronze_extract.py \
     --config config/sales.yaml,config/marketing.yaml,config/products.yaml \
     --parallel-workers 3 \
     --date 2025-01-12
   ```

5. **Check output:**

   ```bash
   ls -la output/system=*/table=*/dt=*/
   # With hourly partitioning:
   ls -la output/system=*/table=*/dt=*/hour=*/
   ```

## 📦 What's Included

### API Extractor
- ✅ Bearer token authentication
- ✅ API key authentication
- ✅ Basic authentication
- ✅ Offset-based pagination
- ✅ Page-based pagination
- ✅ Cursor-based pagination
- ✅ Retry logic with exponential backoff
- ✅ Custom headers support
- ✅ Flexible response parsing

### Database Extractor
- ✅ SQL Server, PostgreSQL, MySQL support (via ODBC)
- ✅ Incremental loading with cursor tracking
- ✅ State file management
- ✅ Batch fetching
- ✅ Automatic WHERE clause injection
- ✅ Retry logic for failed queries


### Local File Extractor
- Read CSV, TSV, JSON, JSONL, or Parquet files that live next to your code
- Optional column selection and `limit_rows` keep fixtures tiny for tests
- Reuses the same `source.run` options so Bronze writing stays identical
- Great for demos, workshops, or development on machines without network access

```yaml
source:
  type: file
  system: local_demo
  table: offline_sample
  file:
    path: ./data/sample.csv
    format: csv          # csv, tsv, json, jsonl, parquet (auto if omitted)
    columns: ['id', 'name']
    limit_rows: 100
  run:
    write_csv: true
    write_parquet: false
```

### Load Patterns & Silver Promotion
- Configure the Bronze CLI with `--load-pattern` (or `source.run.load_pattern`) to label outputs as `full`, `cdc`, or `current_history`
- Bronze partition paths now include the load pattern (`system=foo/table=bar/pattern=current_history/...`) so downstream jobs can select data easily
- Use the new `silver_extract.py` helper to pull Bronze chunks into curated Silver tables; it mirrors the partition layout and writes metadata for later stages
- Example:

```bash
# Bronze full snapshot with explicit pattern override
python bronze_extract.py --config config/my_api.yaml --load-pattern full

# Promote a Bronze partition into Silver, building current/history views
python silver_extract.py \
  --bronze-path output/system=my/table=orders/pattern=current_history/dt=2025-11-14/ \
  --silver-base ./silver_output \
  --primary-key order_id \
  --order-column updated_at
```

### Sample Data Sets
- Regenerate fixtures anytime with `python scripts/generate_sample_data.py`
- Full snapshot (500 rows): `docs/examples/data/bronze_samples/full/system=retail_demo/table=orders/pattern=full/dt=2025-11-01/`
- CDC stream (400 events): `docs/examples/data/bronze_samples/cdc/system=retail_demo/table=orders/pattern=cdc/dt=2025-11-02/`
- Current + history mix (800 rows): `docs/examples/data/bronze_samples/current_history/system=retail_demo/table=orders/pattern=current_history/dt=2025-11-03/`
- Matching configs: `file_example.yaml` (full), `file_cdc_example.yaml` (cdc), `file_current_history_example.yaml`

### Multi-Source Pipelines (One YAML, Many Jobs)

Group related extracts into a single config by using the `sources:` array. Shared settings (storage, schema, normalization) live at the top, while each entry supplies only the bits that differ.

```yaml
platform:
  bronze: { ... }
silver:
  output_dir: ./silver_output
sources:
  - name: claims_header
    source:
      system: claims
      table: header
      type: file
      file: { path: ./data/claim_header.csv, format: csv }
      run: { load_pattern: full }
    silver:
      domain: claims
      entity: claim_header
  - name: claims_cdc
    source:
      system: claims
      table: header_cdc
      type: file
      file: { path: ./data/claim_header_cdc.csv, format: csv }
      run: { load_pattern: cdc }
```

Run both layers with the same file:

```bash
python bronze_extract.py --config pipeline.yaml
python silver_extract.py --config pipeline.yaml --source-name claims_cdc
```

This keeps medallion assets in sync without juggling dozens of CLI flags or bespoke Python scripts.

### Shared Bronze/Silver Configs
- Every example config now contains a `silver` section with the same vocabulary as `source.run` (e.g., `write_parquet`, `write_csv`, `output_dir`, `primary_keys`, `order_column`)
- Run both stages with the same file so settings stay in sync:

```bash
python bronze_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-14
python silver_extract.py --config docs/examples/configs/file_example.yaml --date 2025-11-14
```

- Silver CLI highlights:
  - `--config`, `--date`, `--dry-run`, `--validate-only`, `--pattern {full|cdc|current_history}`
  - Output controls: `--write-parquet/--no-write-parquet`, `--write-csv/--no-write-csv`, `--parquet-compression`
  - Naming overrides: `--full-output-name`, `--cdc-output-name`, `--current-output-name`, `--history-output-name`
  - Partition overrides still available via `--bronze-path`/`--silver-base` when you need to promote ad-hoc data
- Define many inputs in a single YAML by using the `sources:` list (each item holds its own `source` and optional `silver` overrides). Bronze automatically runs every entry; Silver uses `--source-name <entry>` to pick the one you want when the config contains multiple sources.

### Silver Refinement Options
- `silver.schema`: rename or reorder columns for standardized curated tables.
- `silver.normalization`: toggle `trim_strings` / `empty_strings_as_null` to keep formatting consistent across datasets.
- `silver.error_handling`: set `enabled`, `max_bad_records`, and `max_bad_percent` to quarantine bad rows into `_errors/` files instead of failing immediately (exceeds threshold → fail).
- `silver.partitioning`: add a secondary partition column (e.g., status, region) for Silver outputs while still mirroring the Bronze folder layout.
- `silver.domain` / `entity` / `version` / `load_partition_name`: describe the medallion layout so outputs land under `domain=<domain>/entity=<entity>/v<version>/<load partition>=YYYY-MM-DD/…`. Optional `include_pattern_folder: true` inserts `pattern=<load_pattern>` before the load partition.

Example Silver section:

```yaml
silver:
  domain: claims
  entity: claim_header
  version: 1
  load_partition_name: load_date
  include_pattern_folder: false
  write_parquet: true
  partitioning:
    columns: ["status", "is_current"]
  schema:
    column_order: ["claim_id", "member_id", "status", "is_current", "load_timestamp"]
  normalization:
    trim_strings: true
    empty_strings_as_null: true
  error_handling:
    enabled: true
    max_bad_records: 25
    max_bad_percent: 1.5
```

This produces a layout such as:

```
silver_output/
  domain=claims/
    entity=claim_header/
      v1/
        load_date=2025-11-01/
          status=approved/
            is_current=1/
              claim_snapshot.parquet
```

| Key | Description | Default |
| --- | --- | --- |
| `silver.domain` / `silver.entity` | Medallion folders (`domain=<domain>/entity=<entity>`) | `source.system` / `source.table` |
| `silver.version` | Version folder (e.g., `v1`) | `1` |
| `silver.load_partition_name` | Load-level partition name | `load_date` |
| `silver.partitioning.columns` | Additional partition folders (e.g., `status`, `is_current`) | `[]` |
| `silver.schema.rename_map` / `column_order` | Structural column cleanup | `None` |
| `silver.normalization.trim_strings` / `empty_strings_as_null` | Consistent string handling | `False` / `False` |
| `silver.error_handling.enabled` | Quarantine rows missing primary keys | `False` |
| `silver.error_handling.max_bad_records` / `max_bad_percent` | Threshold before failing | `0` / `0.0` |

### What Silver Will *Not* Do

- ❌ Business logic or row-level filtering—Silver only standardizes structure.
- ❌ Custom transformations per dataset beyond the declarative `schema`/`normalization` options.
- ❌ Silent drops of bad data—use `silver.error_handling` to quarantine and alert.

### Core Features
- ✅ Proper Python package structure
- ✅ Comprehensive configuration validation
- ✅ Structured logging system
- ✅ Error handling with cleanup
- ✅ CSV and Parquet output
- ✅ S3 upload with retries
- ✅ File chunking for large datasets
- ✅ Test suite with pytest
- ✅ Extensible architecture (see [Azure Storage example](docs/examples/AZURE_STORAGE_EXTENSION.md))

## 📖 Documentation

