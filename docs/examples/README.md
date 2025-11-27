# medallion-foundry Examples

This directory contains complete, working examples demonstrating how to use and extend medallion-foundry.

## Directory Structure

```
examples/
├── configs/                    # Configuration file examples
│   ├── api_example.yaml       # Basic REST API extraction
│   ├── db_example.yaml        # SQL database extraction
│   ├── custom_example.yaml    # Custom Python extractor
│   ├── enhanced_example.yaml  # Production features (file size, partitioning, parallelism)
│   └── quick_test.yaml        # 2-minute quick start validation
│
├── extensions/                 # Optional feature extensions
│   └── azure_storage/         # Azure Blob Storage / ADLS Gen2 support
│       ├── README.md          # Setup guide
│       ├── azure_storage.py   # Implementation (~330 lines)
│       └── azure_config.yaml  # Configuration example
│
└── custom_extractors/          # Custom data source examples
    ├── __init__.py
    └── salesforce_example.py   # Salesforce extractor template
```

---

## Configuration Examples

### Basic Configurations (`configs/`)

**`api_example.yaml`** - Simple REST API extraction
- Bearer token authentication
- Basic pagination
- S3 upload

**`db_example.yaml`** - SQL database extraction
- ODBC connection
- Incremental loading with cursors
- Local output only

**`custom_example.yaml`** - Custom Python extractor
- Shows how to use custom extractors
- Extensibility example

**`file_example*.yaml`** - Offline/full, CDC, and current+history examples
- Point to `sampledata/source_samples/...`
- Pair with `silver_extract.py` to test medallion layouts
- Regenerate the sample input files anytime with `python scripts/generate_sample_data.py`

### Advanced Configuration

**`enhanced_example.yaml`** - Production-ready features:
- ✓ File size optimization (128-512MB for query performance)
- ✓ Hourly partitioning (multiple daily loads)
- ✓ Parallel chunk processing (4 workers)
- ✓ Batch metadata tracking

### Quick Start

**`quick_test.yaml`** - 2-minute validation (no Python experience needed):
- Minimal setup required
- Uses public JSONPlaceholder API
- Local output only (no cloud storage needed)
- Inline instructions

---

## Extension Examples

### Storage Backends (`extensions/`)

#### Azure Blob Storage / ADLS Gen2

**Location**: `extensions/azure_storage/`

Complete, production-ready Azure storage backend:

**Files:**
- `README.md` - Complete setup documentation
- `azure_storage.py` - Full implementation (~330 lines)
- `azure_config.yaml` - Example configuration

**Features:**
- 4 authentication methods (connection string, account key, service principal, managed identity)
- Automatic retries with exponential backoff
- Works with Blob Storage and ADLS Gen2
- Production-ready error handling

**Quick start:**
```bash
# 1. Install dependencies
pip install -r requirements-azure.txt

# 2. Copy backend to core
copy docs\examples\extensions\azure_storage\azure_storage.py core\azure_storage.py

# 3. Update core/storage.py factory (see extensions/azure_storage/README.md)

# 4. Run with Azure
python bronze_extract.py --config docs/examples/extensions/azure_storage/azure_config.yaml
```

### Custom Extractors (`custom_extractors/`)

**`salesforce_example.py`** - Template for Salesforce API extraction
- Shows custom extractor pattern
- SOQL query support
- Pagination handling

---

## Using Examples

### 1. Copy and Customize

```bash
# Copy example to your configs directory
copy configs\api_example.yaml ..\\..\\configs\\my_extraction.yaml

# Edit with your settings
notepad ..\\..\\configs\\my_extraction.yaml
```

### 2. Set Environment Variables

```powershell
# For API extractions
$env:EXAMPLE_API_TOKEN = "your-api-token"

# For S3 storage
$env:BRONZE_S3_ENDPOINT = "https://s3.amazonaws.com"
$env:AWS_ACCESS_KEY_ID = "your-access-key"
$env:AWS_SECRET_ACCESS_KEY = "your-secret-key"

# For Azure storage (if using Azure extension)
$env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=..."
```

### 3. Run Extraction

```bash
python bronze_extract.py --config docs/examples/configs/api_example.yaml
```

## Example Outputs

All examples write data to:
```
output/
└── system=<system>/
    └── table=<table>/
        └── dt=YYYY-MM-DD/
            ├── part-0001.parquet
            ├── part-0002.parquet
            └── _metadata.json
```

With storage enabled (S3/Azure), files are uploaded to:
```
s3://bucket/prefix/system=<system>/table=<table>/dt=YYYY-MM-DD/
azure://container/prefix/system=<system>/table=<table>/dt=YYYY-MM-DD/
```

## Testing Examples

```bash
# Run with quick test config (no cloud dependencies)
python bronze_extract.py --config docs/examples/configs/quick_test.yaml

# Expected output:
# - Local files in output/system=jsonplaceholder/table=todos/
# - No uploads (storage_enabled: false)
# - 200 sample records from public API
```

## Contributing Examples

Have a useful example? Contributions welcome!

1. Create your example files (config + optional code)
2. Add documentation
3. Test thoroughly
4. Submit a pull request

## File Organization

```
docs/examples/
├── README.md                          # This file - examples directory index
│
├── configs/                           # Configuration examples
│   ├── api_example.yaml              # Basic REST API extraction
│   ├── db_example.yaml               # SQL database extraction
│   ├── custom_example.yaml           # Custom Python extractor
│   ├── enhanced_example.yaml         # Production features (file size, partitioning, parallel)
│   └── quick_test.yaml               # 2-minute quick start
│
├── extensions/                        # Optional feature extensions
│   └── azure_storage/                # Azure storage backend
│       ├── README.md                 # Complete Azure setup guide
│       ├── azure_storage.py          # Full implementation (~330 lines)
│       └── azure_config.yaml         # Azure configuration example
│
└── custom_extractors/                 # Custom data source examples
    ├── __init__.py
    └── salesforce_example.py         # Salesforce extractor template
```

## Additional Documentation

For more details, see:

- **[Main Documentation](../framework/reference/DOCUMENTATION.md)** - Complete framework documentation
- **[Storage Backend Architecture](../STORAGE_BACKEND_ARCHITECTURE.md)** - Pluggable storage design
- **[Extending Extractors](../EXTENDING_EXTRACTORS.md)** - Creating custom data sources
- **[Enhanced Features](../ENHANCED_FEATURES.md)** - Advanced production features

---

Questions? Open an issue at https://github.com/tonysebion/medallion-foundry/issues
