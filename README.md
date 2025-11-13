# bronze-foundry

`bronze-foundry` is a **production-ready**, config-driven Python framework for landing data from **APIs**, **databases**, or **custom sources** into a **Bronze layer** with **pluggable storage backends** (S3, Azure, GCS, local filesystem), using conventions that support future analytics platforms and medallion-style architectures.

This framework is intentionally lightweight and orchestration-neutral: you can run it from any scheduler or workflow orchestrator that can invoke a Python CLI.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Key Features

### Core Capabilities
- **🔌 Multiple Source Types** - APIs (REST), databases (SQL), or custom Python extractors
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
