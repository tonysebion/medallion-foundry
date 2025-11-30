# Scripts Overview

This directory contains utility scripts for development, testing, debugging, and operations. Each script serves a specific purpose in the medallion-foundry workflow.

## Development Scripts

### `generate_sample_data.py`
Generates synthetic test data for Bronze and Silver layers.

**Usage:**
```bash
# Generate Bronze samples
python scripts/generate_sample_data.py --bronze --count 1000

# Generate Silver samples
python scripts/generate_sample_data.py --silver --count 500

# Upload to S3 (requires S3 config)
python scripts/generate_sample_data.py --bronze --upload-to-s3 --bucket my-bucket
```

**Options:**
- `--bronze`: Generate Bronze layer data
- `--silver`: Generate Silver layer data
- `--count`: Number of records to generate
- `--upload-to-s3`: Upload generated files to S3 bucket
- `--bucket`: S3 bucket name (required with --upload-to-s3)

### `generate_silver_samples.py`
Creates sample Silver layer configurations and data for testing.

**Usage:**
```bash
python scripts/generate_silver_samples.py --output-dir ./samples
```

### `generate_silver_join_samples.py`
Generates sample data for testing Silver join operations.

**Usage:**
```bash
python scripts/generate_silver_join_samples.py --num-records 1000
```

### `generate_polybase_ddl.py`
Generates PolyBase DDL statements for Azure SQL Data Warehouse integration.

**Usage:**
```bash
python scripts/generate_polybase_ddl.py --config config.yaml --output ddl.sql
```

## Testing and Validation Scripts

### `run_demo.py`
Runs a complete end-to-end demo of the medallion-foundry pipeline.

**Usage:**
```bash
python scripts/run_demo.py --config docs/examples/configs/examples/file_example.yaml
```

### `run_all_bronze_patterns.py`
Tests all Bronze extraction patterns (full, CDC, current_history) against sample data.

**Usage:**
```bash
python scripts/run_all_bronze_patterns.py --config config.yaml
```

### `verify_bronze_samples_s3.py`
Validates Bronze sample data uploaded to S3.

**Usage:**
```bash
python scripts/verify_bronze_samples_s3.py --bucket my-bucket --prefix bronze/
```

### `check_s3_connection.py`
Tests S3 connectivity and permissions.

**Usage:**
```bash
python scripts/check_s3_connection.py --bucket my-bucket
```

### `check_s3_pipeline.py`
End-to-end test of S3-based Bronze extraction pipeline.

**Usage:**
```bash
python scripts/check_s3_pipeline.py --config config.yaml
```

## Debugging Scripts

### `debug_concurrent_lock_run.py`
Debugs concurrent execution issues with file locking.

**Usage:**
```bash
python scripts/debug_concurrent_lock_run.py --config config.yaml --workers 4
```

### `debug_concurrent_simple.py`
Simple concurrent execution test for debugging.

**Usage:**
```bash
python scripts/debug_concurrent_simple.py --num-tasks 10
```

### `debug_inspect_lock.py`
Inspects file locks and concurrent access patterns.

**Usage:**
```bash
python scripts/debug_inspect_lock.py --path ./output/
```

### `lock_holder.py`
Utility for testing file locking mechanisms.

**Usage:**
```bash
python scripts/lock_holder.py --file test.lock --hold-time 30
```

## Operational Scripts

### `bronze_config_doctor.py`
Analyzes Bronze configurations for common issues and provides recommendations.

**Usage:**
```bash
python scripts/bronze_config_doctor.py --config config.yaml
```

### `silver_consolidate.py`
Consolidates multiple Silver layer outputs into optimized files.

**Usage:**
```bash
python scripts/silver_consolidate.py --input-dir ./silver_output --output-dir ./consolidated
```

### `expand_owner_intent.py`
Expands owner intent configurations into full extraction configs.

**Usage:**
```bash
python scripts/expand_owner_intent.py --intent intent.yaml --output config.yaml
```

### `run_intent_config.py`
Executes extraction based on owner intent specifications.

**Usage:**
```bash
python scripts/run_intent_config.py --intent intent.yaml
```

## Performance and Benchmarking

### `benchmark.py`
Benchmarks extraction performance across different configurations.

**Usage:**
```bash
# Benchmark Bronze extraction
python scripts/benchmark.py --config config.yaml --bronze

# Benchmark Silver promotion
python scripts/benchmark.py --config config.yaml --silver

# Compare storage backends
python scripts/benchmark.py --config config.yaml --compare-backends
```

**Options:**
- `--bronze`: Benchmark Bronze extraction
- `--silver`: Benchmark Silver promotion
- `--compare-backends`: Compare S3 vs Azure vs local performance
- `--iterations`: Number of benchmark iterations
- `--output`: JSON output file for results

## Utility Scripts

### `clean_generated_artifacts.py`
Removes generated test data and artifacts.

**Usage:**
```bash
# Clean all artifacts
python scripts/clean_generated_artifacts.py

# Clean specific directories
python scripts/clean_generated_artifacts.py --dirs output silver_output

# Dry run
python scripts/clean_generated_artifacts.py --dry-run
```

### `run_concurrent_small.py`
Small-scale concurrent execution test.

**Usage:**
```bash
python scripts/run_concurrent_small.py --config config.yaml
```

### `run_concurrent_test_py.py`
Concurrent execution test with Python-specific optimizations.

**Usage:**
```bash
python scripts/run_concurrent_test_py.py --workers 4
```

## Script Categories

### Data Generation
- `generate_sample_data.py`
- `generate_silver_samples.py`
- `generate_silver_join_samples.py`

### Testing & Validation
- `run_demo.py`
- `run_all_bronze_patterns.py`
- `verify_bronze_samples_s3.py`
- `check_s3_connection.py`
- `check_s3_pipeline.py`

### Debugging
- `debug_concurrent_lock_run.py`
- `debug_concurrent_simple.py`
- `debug_inspect_lock.py`
- `lock_holder.py`

### Operations
- `bronze_config_doctor.py`
- `silver_consolidate.py`
- `expand_owner_intent.py`
- `run_intent_config.py`

### Performance
- `benchmark.py`

### Utilities
- `clean_generated_artifacts.py`
- `run_concurrent_small.py`
- `run_concurrent_test_py.py`
- `generate_polybase_ddl.py`

## Best Practices

### When to Use Scripts
- **Development**: Use generation scripts to create test data
- **Testing**: Run validation scripts before production deployment
- **Debugging**: Use debug scripts to isolate concurrency issues
- **Operations**: Use doctor scripts for config analysis
- **Performance**: Run benchmarks to optimize configurations

### Script Dependencies
Most scripts require:
- Python 3.8+
- Framework dependencies (`pip install -e .`)
- AWS CLI configured (for S3 scripts)
- Sample data or test configurations

### Error Handling
Scripts include comprehensive error handling and logging. Check the console output and logs for detailed error messages. Most scripts support `--help` for usage information.

### Contributing
When adding new scripts:
1. Follow naming convention: `snake_case.py`
2. Include docstrings and `--help` support
3. Add error handling and logging
4. Update this documentation
5. Add unit tests in `tests/test_scripts.py`