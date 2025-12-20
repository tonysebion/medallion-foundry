# medallion-foundry Examples

This directory contains complete, working examples demonstrating how to use and extend medallion-foundry.

## Quick Start: Python Templates

The fastest way to create a new pipeline is to copy a template from `pipelines/templates/`:

```bash
# Copy a template
cp pipelines/templates/csv_snapshot.py pipelines/my_team/orders.py

# Edit with your settings
code pipelines/my_team/orders.py

# Run it
python -m pipelines my_team.orders --date 2025-01-15
```

### Available Templates

| Template | Use Case |
|----------|----------|
| `pipeline_template.py` | General purpose template with all options |
| `csv_snapshot.py` | CSV files (full snapshot) |
| `mssql_dimension.py` | SQL Server dimension tables |
| `incremental_load.py` | Database with watermark tracking |
| `event_log.py` | Immutable event/fact data |
| `fixed_width.py` | Mainframe/legacy fixed-width files |

### Working Examples

See `pipelines/examples/` for complete, runnable examples:

| Example | Description |
|---------|-------------|
| `retail_orders.py` | CSV → Bronze → Silver (SCD Type 1) |
| `customer_scd2.py` | SCD Type 2 with full history |
| `mssql_incremental.py` | Database incremental load with retry |
| `github_api.py` | REST API extraction |
| `file_to_silver.py` | File processing with quality checks |
| `multi_source_parallel.py` | Parallel extraction from multiple sources |

---

## Directory Structure

```
examples/
├── extensions/                 # Optional feature extensions
│   └── azure_storage/         # Azure Blob Storage / ADLS Gen2 support
│       ├── README.md          # Setup guide
│       ├── azure_storage.py   # Implementation
│       └── azure_config.yaml  # Configuration example
│
└── custom_extractors/          # Custom data source examples
    ├── __init__.py
    └── salesforce_example.py   # Salesforce extractor template
```

---

## Extension Examples

### Storage Backends (`extensions/`)

#### Azure Blob Storage / ADLS Gen2

**Location**: `extensions/azure_storage/`

Complete, production-ready Azure storage backend:

**Files:**
- `README.md` - Complete setup documentation
- `azure_storage.py` - Full implementation
- `azure_config.yaml` - Example configuration

**Features:**
- 4 authentication methods (connection string, account key, service principal, managed identity)
- Automatic retries with exponential backoff
- Works with Blob Storage and ADLS Gen2
- Production-ready error handling

**Quick start:**
```bash
# 1. Install dependencies
pip install azure-storage-blob azure-identity

# 2. Copy backend to core
cp docs/examples/extensions/azure_storage/azure_storage.py pipelines/lib/

# 3. Run with Azure credentials
export AZURE_STORAGE_CONNECTION_STRING="..."
python -m pipelines your_pipeline --date 2025-01-15
```

### Custom Extractors (`custom_extractors/`)

**`salesforce_example.py`** - Template for Salesforce API extraction
- Shows custom extractor pattern
- SOQL query support
- Pagination handling

---

## Using Templates

### 1. Copy and Customize

```bash
# Copy a template to your team's directory
cp pipelines/templates/csv_snapshot.py pipelines/my_team/orders.py

# Edit with your settings
# Change: system, entity, source_path, natural_keys, change_timestamp
```

### 2. Set Environment Variables

```bash
# For database extractions
export DB_HOST="your-database-server"
export DB_USER="your-username"
export DB_PASSWORD="your-password"

# For S3 storage
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"

# For Azure storage
export AZURE_STORAGE_CONNECTION_STRING="..."
```

### 3. Run Pipeline

```bash
# Full pipeline (Bronze → Silver)
python -m pipelines my_team.orders --date 2025-01-15

# Bronze only
python -m pipelines my_team.orders:bronze --date 2025-01-15

# Dry run (validate without executing)
python -m pipelines my_team.orders --date 2025-01-15 --dry-run

# Pre-flight check
python -m pipelines my_team.orders --date 2025-01-15 --check
```

## Output Structure

All pipelines write data to:
```
bronze/system=<system>/entity=<entity>/dt=YYYY-MM-DD/
├── entity.parquet
├── _metadata.json
└── _checksums.json

silver/<entity>/
├── data.parquet
├── _metadata.json
└── _checksums.json
```

## Contributing Examples

Have a useful example? Contributions welcome!

1. Create your example in `pipelines/examples/`
2. Add documentation
3. Test thoroughly
4. Submit a pull request

---

Questions? Open an issue at https://github.com/anthropics/bronze-foundry/issues
