# Test YAML Pipeline Configurations

This directory contains YAML pipeline configurations used by integration tests.

## Files

### s3_orders_full.yaml

Tests the full Bronze → Silver pipeline with S3/MinIO storage using SCD Type 1 (current_only).

**Used by:** `test_yaml_s3_comprehensive.py::test_full_yaml_pipeline_with_all_artifacts`

**Validates:**
- Bronze extraction from CSV to S3 parquet
- Bronze `_metadata.json` and `_checksums.json` generation
- Silver curation with deduplication
- Silver `_metadata.json` and `_checksums.json` generation
- PolyBase DDL generation from metadata

### s3_customers_scd2.yaml

Tests SCD Type 2 (full_history) with customer data that includes historical versions.

**Used by:** `test_yaml_s3_comprehensive.py::test_scd2_history_mode_to_s3`

**Validates:**
- Full history tracking with `effective_from`, `effective_to` columns
- `is_current` flag correctly identifies latest version per customer
- Multiple historical versions preserved

## Sample Data

**The tests automatically generate sample CSV data at runtime.** No manual data setup is required.

Each test creates temporary CSV files in a pytest temp directory with deterministic test data:
- `orders_{run_date}.csv` - 5 order records including a duplicate for deduplication testing
- `customers_{run_date}.csv` - 4 customer records with historical versions for SCD2 testing

To generate sample data manually (e.g., for the existing example pipelines):

```bash
# Generate sample data for a specific date
python scripts/generate_yaml_test_data.py --date 2025-01-15

# Generate to a custom directory
python scripts/generate_yaml_test_data.py --date 2025-01-15 --output ./my_test_data

# Generate with more rows
python scripts/generate_yaml_test_data.py --date 2025-01-15 --num-orders 100 --num-customers 20
```

## Runtime Path Substitution

These YAML files use placeholder paths that are substituted at runtime:

- `{source_path}` - Replaced with the path to the test CSV file
- `{bucket}` - Replaced with MinIO bucket name (default: `mdf`)
- `{prefix}` - Replaced with test-specific prefix for isolation
- `{run_date}` - Replaced with the pipeline run date

## MinIO Configuration

Tests use MinIO as an S3-compatible object store running locally.

### Default Settings

| Setting | Value |
|---------|-------|
| S3 API Endpoint | `http://localhost:9000` |
| Console URL | `http://localhost:49384` |
| Access Key | `minioadmin` |
| Secret Key | `minioadmin` |
| Bucket | `mdf` |

### Starting MinIO

```bash
docker run -p 9000:9000 -p 49384:49384 minio/minio server /data --console-address ":49384"
```

### Viewing Test Output in MinIO Console

After running the tests, you can view the generated files in the MinIO console:

1. Open http://localhost:49384 in your browser
2. Login with `minioadmin` / `minioadmin`
3. Navigate to the `mdf` bucket
4. Look for directories named `test_comprehensive_*`

**Note:** Test cleanup removes files after each test by default. To keep files for inspection,
you can comment out the cleanup in the `minio_test_prefix` fixture in `test_yaml_s3_comprehensive.py`.

### Test Output Structure

Each test creates files in this structure:

```
mdf/
└── test_comprehensive_{uuid}/
    ├── bronze/
    │   └── system=test/entity=orders/dt=2025-01-15/
    │       ├── orders.parquet
    │       ├── _metadata.json
    │       └── _checksums.json
    └── silver/
        └── test/orders/
            ├── data.parquet
            ├── _metadata.json
            └── _checksums.json
```

### Overriding Configuration

You can override MinIO settings via environment variables:

```bash
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export MINIO_BUCKET=mdf

pytest tests/integration/test_yaml_s3_comprehensive.py -v
```

## Running the Tests

```bash
# Run all comprehensive S3 tests
pytest tests/integration/test_yaml_s3_comprehensive.py -v

# Run a specific test
pytest tests/integration/test_yaml_s3_comprehensive.py::TestYamlS3Comprehensive::test_full_yaml_pipeline_with_all_artifacts -v

# Run with output visible (useful for debugging)
pytest tests/integration/test_yaml_s3_comprehensive.py -v -s
```
