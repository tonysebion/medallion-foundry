# S3 Storage Implementation - Complete

## What Was Implemented

We've successfully added optional S3 storage support to bronze-foundry with the following capabilities:

### ✅ Core Features

1. **Streaming Architecture** - No temporary file downloads, direct S3 streaming via fsspec
2. **Granular Storage Control** - Each component can independently use local or S3:
   - Source file reading (Bronze input)
   - Bronze output writing
   - Silver reading of Bronze data
   - Silver output writing

3. **Environment-Based Configuration** - Centralized credential management
4. **Backward Compatible** - Existing local filesystem configs work unchanged
5. **Bucket Flexibility** - Support for named bucket references or direct bucket names

## Your MinIO Configuration

### Connection Details
- **API Endpoint**: `http://localhost:9000`
- **Browser UI**: `http://localhost:9001/browser`
- **Bucket**: `mdf`
- **Username**: `minioadmin`
- **Password**: `minioadmin123`

### Directory Structure
```
mdf/
├── source_samples/     # Your existing source data
├── bronze_samples/     # Will be created by Bronze pipeline
└── silver_samples/     # Will be created by Silver pipeline
```

## Files Created/Modified

### New Core Modules
- ✅ `core/config/environment.py` - Environment configuration with S3 credentials
- ✅ `core/storage/uri.py` - Storage URI parser (s3://, local paths)
- ✅ `core/storage/filesystem.py` - Filesystem factory for fsspec

### Updated Core Modules
- ✅ `core/config/dataset.py` - Added storage backend fields to BronzeIntent/SilverIntent
- ✅ `core/config/loader.py` - Added `load_config_with_env()` function
- ✅ `core/extractors/file_extractor.py` - Streaming S3 reads
- ✅ `core/silver/processor.py` - S3 streaming for Bronze reading

### Configuration Files
- ✅ `environments/dev.yaml` - Your MinIO configuration
- ✅ `environments/prod.yaml` - AWS production template
- ✅ `environments/.gitignore` - Protects credentials from git
- ✅ `docs/examples/configs/patterns/pattern_s3_example.yaml` - Full S3 example

### Documentation
- ✅ `docs/S3_SETUP_GUIDE.md` - Complete setup and usage guide
- ✅ `test_s3_connection.py` - Connection test script

### Dependencies
- ✅ `fsspec>=2023.1.0` - Filesystem abstraction
- ✅ `s3fs>=2023.1.0` - S3 implementation

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Test Connection
```bash
python test_s3_connection.py
```

This will verify:
- Environment config loads correctly
- S3 connection works
- Can list files in your `mdf/source_samples` directory
- Can read CSV files from S3

### 3. Try the S3 Pattern

Use the example pattern configured for your MinIO setup:
```bash
# The pattern is already configured to:
# - Read from: s3://mdf/source_samples/
# - Write Bronze to: s3://mdf/bronze_samples/
# - Write Silver to: s3://mdf/silver_samples/
```

## Configuration Examples

### Full S3 Pipeline (Your Setup)
```yaml
environment: dev  # Uses environments/dev.yaml

bronze:
  source_storage: s3
  path_pattern: s3://source_data/source_samples/...
  output_storage: s3
  output_bucket: bronze_data
  output_prefix: bronze_samples/

silver:
  input_storage: s3
  output_storage: s3
  output_bucket: silver_data
  output_prefix: silver_samples/
```

### Mixed Storage (Debugging)
```yaml
bronze:
  source_storage: s3      # Read from MinIO
  output_storage: local   # Write Bronze locally for inspection

silver:
  input_storage: local    # Read Bronze from local
  output_storage: s3      # Write Silver to MinIO
```

## Usage in Code

### Load Config with Environment
```python
from pathlib import Path
from core.config.loader import load_config_with_env

config_path = Path("docs/examples/configs/patterns/pattern_s3_example.yaml")
dataset, env_config = load_config_with_env(config_path)
```

### Extract from S3
```python
from core.extractors.file_extractor import FileExtractor
from core.config.dataset import dataset_to_runtime_config

extractor = FileExtractor(env_config=env_config)
runtime_cfg = dataset_to_runtime_config(dataset)
records, _ = extractor.fetch_records(runtime_cfg, run_date)
```

### Process Silver from S3
```python
from core.silver.processor import SilverProcessor

processor = SilverProcessor(
    dataset=dataset,
    bronze_path=bronze_path,
    silver_partition=silver_partition,
    run_date=run_date,
    env_config=env_config,  # Required for S3
    write_parquet=True
)

result = processor.run()
```

## Migration Paths

### Local → S3
1. Upload files to MinIO
2. Change `source_storage: s3` and update `path_pattern`

### S3 → Local
1. Download files from MinIO
2. Change `source_storage: local` and update `path_pattern`

### Gradual Migration
You can migrate one component at a time:
- Keep sources local, write Bronze to S3
- Read sources from S3, write everything else local
- Any combination you need!

## Architecture Benefits

1. **No Temp Files** - Streaming eliminates disk space issues
2. **Parallel Processing** - fsspec supports concurrent reads
3. **Cloud Native** - Ready for AWS S3, MinIO, or any S3-compatible storage
4. **Flexible** - Mix and match storage backends per component
5. **Secure** - Environment variables for credentials, .gitignore protection

## Troubleshooting

### "Connection Refused"
- Verify MinIO is running
- Check you're using port 9000 (API) not 9001 (UI)

### "Access Denied"
- Verify credentials in `environments/dev.yaml`
- Check bucket `mdf` exists

### "File Not Found"
- Check exact path in MinIO browser
- Verify `path_pattern` matches your structure
- Ensure bucket reference resolves correctly

## Next Steps

1. Run `test_s3_connection.py` to verify everything works
2. Review `docs/S3_SETUP_GUIDE.md` for detailed examples
3. Try the `pattern_s3_example.yaml` pattern
4. Migrate your existing patterns to S3 as needed

## Support

- See `docs/S3_SETUP_GUIDE.md` for detailed documentation
- Check `test_s3_connection.py` output for connection issues
- Example configs in `docs/examples/configs/patterns/`

---

**Implementation Date**: 2025-11-29
**Status**: ✅ Complete and Ready for Use
