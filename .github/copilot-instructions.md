# AI Coding Assistant Instructions for medallion-foundry

## Project Overview
medallion-foundry is a production-ready, config-driven Python framework for landing data from APIs, databases, or custom sources into a Bronze layer with pluggable storage backends (S3, Azure, local filesystem). It implements a medallion architecture with Bronze (raw landing) and Silver (transformed/curated) layers.

## Architecture
- **Bronze Layer**: Raw data extraction and landing in standardized Parquet/CSV format
- **Silver Layer**: Data transformation, deduplication, and curation from Bronze
- **Three CLIs**:
  - `bronze_extract.py`: Extract from sources to Bronze
  - `silver_extract.py`: Promote Bronze to Silver
  - `silver_join.py`: Join multiple Silver assets
- **Config Structure**: YAML with `platform` (storage/infra) and `sources` (extraction logic) sections
- **Extractors**: API (REST), database (SQL via ODBC), file (local/CSV/Parquet/JSON)
- **Storage Backends**: S3 (boto3), Azure Blob (azure-storage-blob), local filesystem
- **Load Patterns**: `full` (snapshots), `cdc` (change data capture), `current_history` (SCD Type 2)

## Key Components
- `core/`: Framework internals (config, context, storage, patterns, etc.)
- `extractors/`: Source-specific extraction logic
- `core/silver/`: Silver layer processing and models
- `core/bronze/`: Bronze-specific utilities
- `tests/`: Comprehensive test suite with unit/integration markers

## Developer Workflows

### Setup
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
pip install -r requirements-dev.txt  # For development
pip install -e .  # Install in editable mode
```

### Testing
```bash
# All tests
python run_tests.py

# Unit tests only
python run_tests.py --unit

# With coverage
python run_tests.py --coverage

# Integration tests
python run_tests.py --integration
```

### Code Quality
```bash
# Lint and format
make lint    # ruff check
make format  # ruff format
make fix     # Auto-fix issues

# Type checking
make type-check  # mypy

# All checks
python run_tests.py --all-checks
```

### Documentation
```bash
make docs      # Build MkDocs site
make docs-serve # Serve locally
```

### Running Extractions
```bash
# Bronze extraction
python bronze_extract.py --config config.yaml --date 2025-11-28

# Silver promotion
python silver_extract.py --config config.yaml --date 2025-11-28

# Dry run (validation only)
python bronze_extract.py --config config.yaml --dry-run

# Parallel workers
python bronze_extract.py --config config.yaml --parallel-workers 3 --date 2025-11-28
```

## Configuration Conventions

### Platform Section (Infrastructure)
```yaml
platform:
  bronze:
    storage_backend: s3  # s3, azure, local
    bucket: my-bucket
    prefix: bronze/
    region: us-east-1
  silver:
    output_dir: ./silver_output
```

### Source Section (Extraction)
```yaml
sources:
  - name: my_api_data
    source:
      system: my_system
      table: my_table
      type: api  # api, db, file
      run:
        load_pattern: full  # full, cdc, current_history
        write_parquet: true
        write_csv: false
    silver:
      domain: my_domain
      entity: my_entity
      primary_keys: [id]
      order_column: updated_at
```

## Code Patterns

### Error Handling
- Use `core.exceptions` for standardized error codes
- Implement retry logic with `tenacity` decorators
- Circuit breaker pattern in `core.retry`

### Logging
- Structured logging via `core.logging_config`
- Use logger levels appropriately (DEBUG for dev, INFO for ops)
- Include context in log messages

### Configuration Validation
- Use Pydantic models in `core.config.typed_models` for type safety
- Validate configs early with `--validate-only` flag
- Environment variable substitution in `core.config.env_substitution`

### Storage Abstraction
- Implement `core.storage.StorageBackend` interface for new backends
- Use `core.storage.policy` for scope enforcement (onprem vs cloud)
- Handle retries and rate limiting in storage operations

### Parallel Processing
- Use `core.parallel` for multi-config execution
- Chunk-level parallelism in `core.runner`
- Respect `parallel_workers` config for thread pools

### Data Processing
- Pandas DataFrames for in-memory processing
- PyArrow for Parquet I/O with Snappy compression
- Schema validation and normalization in Silver layer

## File Organization
- **Configs**: `config/` for production configs, `docs/examples/configs/` for samples
- **Output**: `output/` for Bronze, `silver_output/` for Silver
- **Scripts**: `scripts/` for utilities (generate samples, benchmarks, etc.)
- **Tests**: `tests/` with clear naming (`test_*.py`) and markers (`@pytest.mark.unit`)

## Common Patterns
- **Context Building**: Use `core.context.build_run_context()` for shared state
- **Path Construction**: `core.paths.build_*_path()` functions for consistent layouts
- **Metadata**: Write `_metadata.json` and `_checksums.json` for idempotency
- **Partitioning**: Hive-style partitioning (`key=value/`) for queryability
- **Normalization**: Trim strings, convert empty to null in Silver processing

## Dependencies
- **Core**: pandas, pyarrow, requests, tenacity, pyyaml
- **Storage**: boto3 (S3), azure-storage-blob (Azure)
- **Database**: pyodbc, pymssql, psycopg2, mysql-connector-python
- **Async**: httpx for async HTTP extraction
- **Dev**: pytest, ruff, mypy, mkdocs

## Testing Strategy
- Unit tests for core logic
- Integration tests for end-to-end flows
- Sample data generation in `scripts/generate_sample_data.py`
- Mock external services (S3, APIs) in tests
- Use `conftest.py` for shared fixtures

## Deployment
- CLI-first design for orchestrator integration
- Exit codes: 0=success, non-zero=failure
- Structured JSON logging for monitoring
- Webhook support for notifications
- Idempotent operations for safe retries

## Key Files to Reference
- `README.md`: High-level overview and quick start
- `docs/framework/reference/CONFIG_REFERENCE.md`: Exhaustive config options
- `docs/usage/onboarding/new_dataset_checklist.md`: Getting started guide
- `core/config/typed_models.py`: Pydantic models for configs
- `core/patterns.py`: Load pattern definitions
- `core/silver/models.py`: Silver asset types
- `tests/test_integration_samples.py`: End-to-end test examples</content>
<parameter name="filePath">c:\github\bronze-foundry3\.github\copilot-instructions.md
