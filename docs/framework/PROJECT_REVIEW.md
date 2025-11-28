# medallion-foundry Project Review

**Last Updated:** November 28, 2025
**Reviewer:** GitHub Copilot
**Project Version:** 1.0.0+ (Unreleased)
**Status:** Production-Ready, Active Development

---

## Executive Summary

**medallion-foundry** is a sophisticated, config-driven Python framework for implementing medallion architecture (Bronze → Silver → Gold) at enterprise scale. It provides a modular, extensible foundation for data landing, transformation, and curation with pluggable storage backends (S3, Azure, local), multiple extractor types (API, database, file), and rich patterns for different data lifecycle scenarios.

### Key Strengths
- **Production-Ready**: Exit codes, atomic operations, idempotent reruns, comprehensive error handling
- **Config-Driven**: YAML-based configurations with Pydantic validation keep intent separate from code
- **Extensible Architecture**: Plugin-based storage backends, custom extractors, Silver asset models
- **Comprehensive Testing**: Unit/integration/smoke tests with ~40+ test files, fixtures, and CI/CD ready
- **Rich Documentation**: 40+ markdown files covering architecture, patterns, operations, and tutorials
- **Enterprise Features**: Resilience (retry/breaker/rate-limit), tracing (OpenTelemetry), async HTTP, parallel processing

### Current State
- **Core Foundation**: Mature Bronze extraction with 3 extractors (API, DB, file) + async HTTP support
- **Silver Layer**: 5+ Silver asset models (SCD1, SCD2, incremental, snapshots) with normalization/error-handling
- **CLI Tooling**: 3 entrypoints (bronze_extract, silver_extract, silver_join) with comprehensive flags
- **Version Management**: semantic versioning via setuptools_scm, changelog tracking, deprecation framework
- **Quality**: Linting (ruff), formatting (ruff/black), type-checking (mypy), coverage reporting

---

## Architecture Overview

### Three-Layer Medallion Structure

```
┌──────────────────────────────────────────────────────────────┐
│                      GOLD (Analytics)                         │
│  (User-facing dashboards, reports, ML models)                 │
└──────────────────────────────────────────────────────────────┘
                           ↑
┌──────────────────────────────────────────────────────────────┐
│  SILVER (Curated / Conformed)         silver_extract.py      │
│  ├─ SCD Type 1 (current only)                                 │
│  ├─ SCD Type 2 (current + history)                            │
│  ├─ Incremental Merge (CDC deltas)                            │
│  ├─ Full Merge Dedupe (snapshots)                             │
│  └─ Periodic Snapshot (point-in-time)                         │
│                                                                │
│  Features: Normalization, schema refinement, partitioning    │
│  Outputs: domain=<domain>/entity=<entity>/v<version>/...     │
└──────────────────────────────────────────────────────────────┘
                           ↑
┌──────────────────────────────────────────────────────────────┐
│  BRONZE (Raw Landing)                 bronze_extract.py      │
│  ├─ Full snapshots (load_pattern=full)                        │
│  ├─ CDC streams (load_pattern=cdc)                            │
│  └─ Current + History (load_pattern=current_history)          │
│                                                                │
│  Features: Chunking, deduplication metadata, checksums       │
│  Outputs: system=<sys>/table=<tbl>/pattern=<pat>/dt=<date>/  │
│  Storage: S3, Azure Blob/ADLS, local filesystem               │
└──────────────────────────────────────────────────────────────┘
                           ↑
           ┌───────────────┼───────────────┐
           │               │               │
        APIs          Databases         Files
```

### Package Structure

```
core/                          # Shared framework infrastructure
├── config/                    # Config loading, validation, typed models (Pydantic)
│   ├── loader.py             # YAML parsing, environment substitution
│   ├── validation.py          # Config schema + rules
│   ├── typed_models.py        # Pydantic dataclasses for all config sections
│   ├── doctor.py              # Config validation CLI helper
│   └── models.py              # Legacy models (gradual migration to Pydantic)
│
├── extractors/               # Pluggable data source interfaces
│   ├── base.py               # BaseExtractor ABC
│   ├── api_extractor.py      # REST API with auth/pagination
│   ├── db_extractor.py       # SQL (ODBC) with cursor tracking
│   ├── file_extractor.py     # CSV/JSON/Parquet/TSV files
│   └── async_http.py         # httpx async HTTP support
│
├── bronze/                   # Bronze-specific utilities
│   ├── base.py               # Schema inference, metadata emission
│   ├── plan.py               # Chunk writer planning, format selection
│   ├── io.py                 # Chunk writing, metadata/checksum persistence
│   └── README.md             # Bronze package overview
│
├── silver/                   # Silver promotion logic
│   ├── models.py             # SilverModel enum + asset types
│   ├── artifacts.py          # Normalization, schema, partitioning
│   ├── processor.py          # SilverProcessor (chunking, CDC logic)
│   ├── writer.py             # SilverArtifactWriter interface + impl
│   └── README.md             # Silver package overview
│
├── storage/                  # Plugin-based storage backends
│   ├── backend.py            # StorageBackend ABC
│   ├── plugin_manager.py     # Registry and factory resolution
│   ├── plugin_factories.py   # Built-in backend factories
│   ├── policy.py             # Storage scope enforcement (onprem vs cloud)
│   ├── registry.py           # Plugin registration
│   ├── plugins/
│   │   ├── s3_storage.py     # AWS S3 backend (boto3)
│   │   ├── azure_storage.py  # Azure Blob/ADLS (azure-storage-blob)
│   │   └── local_storage.py  # Local filesystem
│   └── README.md             # Storage architecture
│
├── runner/                   # Bronze job orchestration
│   ├── job.py                # ExtractJob wires config → extractor → IO → storage
│   ├── chunks.py             # ChunkProcessor, ChunkWriter
│   └── README.md             # Runner package overview
│
├── context.py                # RunContext: shared state across extraction
├── catalog.py                # Catalog hooks (lineage, quality metrics)
├── hooks.py                  # Webhook notifications post-extraction
├── logging_config.py         # Structured logging (JSON/human formats)
├── exceptions.py             # Error taxonomy (CFG*, EXT*, STG*, etc.)
├── retry.py                  # Retry + circuit breaker logic
├── rate_limit.py             # Rate limiter for API/DB extractions
├── patterns.py               # LoadPattern enum (full, cdc, current_history)
├── partitioning.py           # Hive-style partition logic
├── paths.py                  # Path builders (Bronze/Silver layouts)
├── tracing.py                # OpenTelemetry instrumentation
├── run_options.py            # RunOptions dataclass
├── deprecation.py            # Deprecation warnings framework
└── README.md                 # Core package overview

bronze_extract.py            # CLI entrypoint for Bronze extraction
silver_extract.py            # CLI entrypoint for Silver promotion
silver_join.py               # CLI entrypoint for joining Silver assets

scripts/                     # Utility scripts
├── generate_sample_data.py  # Generate Bronze fixture samples
├── generate_silver_samples.py # Generate Silver output samples
├── generate_silver_join_samples.py # Silver join test data
├── run_intent_config.py     # Run Bronze + Silver for a single config
├── run_all_bronze_patterns.py # Test all 3 Bronze patterns
├── bronze_config_doctor.py  # Config validation helper
├── run_demo.py              # Interactive demo
└── benchmark.py             # Performance benchmarking

tests/                       # Test suite (~40+ files)
├── conftest.py              # pytest fixtures + sample data validation
├── test_*_extractor*.py     # Extractor-specific tests
├── test_integration_*.py    # End-to-end integration tests
├── test_silver_*.py         # Silver layer tests
├── test_*_patterns.py       # Pattern validation tests
├── test_storage_*.py        # Storage backend tests
└── test_*.py                # Miscellaneous unit tests

docs/                        # Comprehensive documentation (40+ files)
├── index.md                 # Documentation hub
├── framework/               # Architecture & operations
│   ├── architecture.md      # Visual architecture overview
│   ├── silver_patterns.md   # Silver entity kind/history/input modes
│   ├── manifesto-playbook.md # Philosophy & best practices
│   ├── STORAGE_BACKEND_ARCHITECTURE.md
│   ├── EXTENDING_EXTRACTORS.md
│   ├── pipeline_engine.md
│   ├── UPGRADING.md
│   ├── operations/          # Runbooks & governance
│   │   ├── OPERATIONS.md    # Day-2 ops, hooks, monitoring
│   │   ├── OPS_PLAYBOOK.md  # Operations runbook
│   │   ├── PERFORMANCE_TUNING.md
│   │   ├── ERROR_CODES.md
│   │   ├── CONFIG_DOCTOR.md
│   │   ├── GOLD_CONTRACTS.md
│   │   ├── TESTING.md
│   │   ├── CONTRIBUTING.md
│   │   └── legacy-streaming.md
│   └── reference/           # API & configuration reference
│       ├── CONFIG_REFERENCE.md # Exhaustive config guide
│       ├── DOCUMENTATION.md    # Architecture concepts & FAQs
│       ├── QUICK_REFERENCE.md
│       └── intent-lifecycle.md
├── usage/                   # Getting started & patterns
│   ├── index.md             # Usage hub
│   ├── beginner/            # New user onboarding
│   │   ├── QUICKSTART.md    # 10-minute tutorial
│   │   ├── COPY_AND_CUSTOMIZE.md
│   │   ├── FIRST_RUN_CHECKLIST.md
│   │   └── SAFE_EXPERIMENTATION.md
│   ├── onboarding/          # Production readiness
│   │   ├── intent-owner-guide.md
│   │   ├── new_dataset_checklist.md
│   │   └── bronze_readiness_checklist.md
│   └── patterns/            # Pattern selection
│       ├── pattern_matrix.md
│       ├── EXTRACTION_GUIDANCE.md
│       ├── ENHANCED_FEATURES.md
│       └── QUICK_REFERENCE.md
├── api/                     # API documentation
│   ├── core.md
│   └── extractors.md
├── examples/                # Working configurations & samples
│   ├── README.md
│   ├── configs/
│   │   ├── examples/        # Simple starter configs
│   │   ├── advanced/        # Complex feature configs
│   │   ├── patterns/        # Pattern-specific examples
│   │   └── templates/       # Config templates
│   ├── extensions/          # Backend/extractor examples
│   ├── REAL_WORLD_EXAMPLES.md
│   └── data/                # Sample data outputs
│       ├── silver_join_samples/
│       └── bronze_samples/
├── setup/                   # Installation & environment
│   └── INSTALLATION.md
└── examples/README.md       # Examples hub
```

### CLI Entrypoints

#### 1. `bronze_extract.py` – Data Landing
```bash
python bronze_extract.py \
  --config config/my_api.yaml \
  --date 2025-11-28 \
  --load-pattern full \
  --parallel-workers 3 \
  --dry-run
```

**Key Arguments:**
- `--config`: YAML file(s) (comma-separated for multiple)
- `--date`: Logical run date (YYYY-MM-DD)
- `--load-pattern`: full | cdc | current_history (override)
- `--parallel-workers`: Thread count for multi-config
- `--dry-run`: Validate without extraction
- `--validate-only`: Config validation only
- `--verbose`/`--quiet`: Logging control
- `--list-backends`: Show available storage backends

**Exit Codes:**
- `0` = Success
- `1` = Configuration error
- `2` = Extraction failure
- `3` = Storage failure
- `4` = Validation failure

#### 2. `silver_extract.py` – Data Curation
```bash
python silver_extract.py \
  --config config/my_api.yaml \
  --date 2025-11-28 \
  --pattern full \
  --silver-model scd_type_2 \
  --write-parquet \
  --no-write-csv
```

**Key Arguments:**
- `--config`: Same as Bronze
- `--date`: Run date
- `--pattern`: full | cdc | current_history (filter Bronze input)
- `--silver-model`: scd_type_1 | scd_type_2 | incremental_merge | full_merge_dedupe | periodic_snapshot
- `--source-name`: Select specific source from multi-source config
- Output controls: `--write-parquet`, `--write-csv`, `--parquet-compression`
- Asset naming: `--full-output-name`, `--cdc-output-name`, `--current-output-name`, `--history-output-name`

#### 3. `silver_join.py` – Asset Integration
```bash
python silver_join.py \
  --left-path silver_output/domain=a/entity=x/v1/ \
  --right-path silver_output/domain=b/entity=y/v1/ \
  --join-keys "order_id=order_id,customer_id=cust_id" \
  --strategy left
```

---

## Core Features Deep Dive

### 1. Extractors (core/extractors/)

#### BaseExtractor (Abstract Interface)
All extractors inherit from `BaseExtractor` and implement:
```python
def extract(self, config: Dict, context: RunContext) -> List[Dict]:
    """Fetch records from the source system."""
    pass
```

#### ApiExtractor
- **Auth**: Bearer token, API key, Basic auth, custom headers
- **Pagination**: Offset, page, cursor, none
- **Validation**: Response schema validation, optional
- **Retry Logic**: Exponential backoff (tenacity)
- **Async Support**: httpx-based async extraction with prefetch
- **Rate Limiting**: Per-extractor rate limiters
- **Features**:
  - JMESPath response parsing
  - Nested payload extraction
  - Flexible header/body mapping
  - Connection pooling
  - Backpressure handling

#### DbExtractor
- **Drivers**: SQL Server (pyodbc), PostgreSQL (psycopg2), MySQL (mysql-connector)
- **Incremental**: Cursor tracking via state files
- **Features**:
  - Query parameterization
  - Batch fetching
  - Automatic WHERE clause injection
  - Connection pooling
  - Transaction isolation

#### FileExtractor
- **Formats**: CSV, TSV, JSON, JSONL, Parquet
- **Features**:
  - Column selection
  - Row limiting for tests
  - Auto-format detection
  - Streaming for large files

### 2. Bronze Layer (core/bronze/)

#### Loading Patterns
```python
class LoadPattern(Enum):
    FULL = "full"              # Full snapshot replacement
    CDC = "cdc"                # Change data capture (deltas)
    CURRENT_HISTORY = "current_history"  # SCD Type 2 state
```

#### Chunking Strategy
- Files split by `max_rows_per_file` (configurable, 100K default)
- Output: `part-0001.parquet`, `part-0002.parquet`, etc.
- Enables parallel processing downstream
- Part files include row counts in metadata

#### Metadata & Checksums
- `_metadata.json`: Run summary, pattern, format, record counts, timing
- `_checksums.json`: SHA256 per chunk for idempotency validation
- **Idempotency**: Re-running same config/date produces identical checksums

#### Schema Inference
- Automatic from first 1K records
- Optional override via `source.run.schema`
- Preserves nullable/type information

### 3. Silver Layer (core/silver/)

#### Silver Asset Models

| Model | Use Case | Output |
|-------|----------|--------|
| **SCD Type 1** | Current-only state (customers, locations) | Single table with dedup by key |
| **SCD Type 2** | Current + full history (employees, accounts) | Split `current` + `history` tables with `is_current` flag |
| **Incremental Merge** | CDC deltas to be merged downstream | Change records with operation type |
| **Full Merge Dedupe** | Snapshot deduplicated by natural key | Deduplicated full snapshot |
| **Periodic Snapshot** | Point-in-time snapshots (no transformation) | 1:1 copy of Bronze with metadata |

#### Normalization
- `trim_strings`: Remove leading/trailing whitespace
- `empty_strings_as_null`: Replace empty strings with NULL
- Configurable per dataset

#### Error Handling
- `error_handling.enabled`: Quarantine bad rows
- `error_handling.max_bad_records`: Row threshold
- `error_handling.max_bad_percent`: Percentage threshold
- Bad rows written to `_errors/` subdirectory

#### Partitioning
- Load partition: `load_date=YYYY-MM-DD`
- Secondary partitions: e.g., `status=<value>/is_current=<0|1>/`
- Hive-style format for Spark/Presto/Trino compatibility

### 4. Storage Backends (core/storage/)

#### Plugin Architecture
- `StorageBackend` interface
- Registry-based factory pattern
- Built-in backends: S3, Azure, local
- Custom backends extend `StorageBackend` and register via `plugin_factories.py`

#### S3 Backend
- Uses boto3
- Supports custom endpoints (MinIO, Wasabi)
- Retry + circuit breaker
- Multi-part upload for large files

#### Azure Backend
- Blob Storage and ADLS Gen2 support
- Managed identity or SAS token auth
- Retry + circuit breaker

#### Local Backend
- Filesystem operations
- No network overhead
- Great for dev/testing

#### Policy Enforcement
- `storage_scope`: onprem | cloud (default)
- Onprem scope blocks cloud providers
- Validates `storage_metadata` (boundary, provider_type)

### 5. Resilience & Retry Logic (core/retry.py, core/rate_limit.py)

#### Retry Strategy
- Exponential backoff with jitter
- Max retries configurable
- Circuit breaker to fail fast after repeated failures
- Tenacity integration

#### Rate Limiting
- Per-extractor rate limiters
- Sliding window (last N seconds)
- Backpressure handling for async HTTP

---

## Configuration System

### Structure (YAML)

```yaml
# Config version (for deprecation tracking)
config_version: 1

# Platform: infrastructure-level settings (owned by platform team)
platform:
  environment: prod          # dev | test | prod
  bronze:
    storage_backend: s3      # s3 | azure | local
    bucket: my-bronze        # S3 bucket or Azure container
    prefix: bronze/          # Path prefix
    region: us-east-1        # S3 region
    output_dir: ./output     # Local fallback

    output_defaults:
      allow_parquet: true
      allow_csv: false
      max_file_size_mb: 256
      parquet_compression: snappy

    partitioning:
      use_dt_partition: true
      use_pattern_folder: true

  silver:
    storage_backend: s3
    bucket: my-silver
    prefix: silver/
    output_dir: ./silver_output

# Sources: data-source-specific settings (owned by domain teams)
sources:
  - name: my_api_source

    source:
      type: api                  # api | db | file | custom
      system: my_system
      table: my_table

      # API-specific config
      api:
        base_url: https://api.example.com
        endpoint: /v1/users
        auth_type: bearer          # bearer | apikey | basic | none
        auth_header: X-API-Key
        auth_env_var: MY_API_KEY
        method: GET

        pagination:
          type: offset             # offset | page | cursor | none
          offset_param: skip
          limit_param: limit
          page_param: page
          page_size: 100

          cursor:
            type: link_header       # link_header | response_field | body
            field: _next_page_url

      # OR DB-specific config
      db:
        driver: psycopg2           # psycopg2 | pymssql | mysql
        host_env_var: DB_HOST
        database: mydb
        query: "SELECT * FROM users WHERE created_at > ?"

      # OR file-specific config
      file:
        path: ./data/sample.csv
        format: csv                # csv | tsv | json | jsonl | parquet
        columns: [id, name]
        limit_rows: 1000

      run:
        # Extraction behavior
        load_pattern: full         # full | cdc | current_history
        max_rows_per_file: 10000
        timeout_seconds: 300

        # Output formats
        write_parquet: true
        write_csv: false
        parquet_compression: snappy

        # Rate limiting
        rate_limit_hz: 10          # Requests/sec

        # Async HTTP
        use_async: true
        prefetch_pages: 5

    # Silver-specific config (optional)
    silver:
      domain: my_domain
      entity: my_entity
      version: 1

      # Asset model
      model: scd_type_2           # or use model_profile
      model_profile: analytics    # analytics | operational | merge_ready | cdc_delta | snapshot

      # Output
      write_parquet: true
      write_csv: false

      # Partitioning
      load_partition_name: load_date
      include_pattern_folder: false
      partitioning:
        columns: [status, region]

      # Schema refinement
      schema:
        rename_map:
          old_col_name: new_col_name
        column_order: [id, name, status]

      # Data normalization
      normalization:
        trim_strings: true
        empty_strings_as_null: true

      # Error handling
      error_handling:
        enabled: true
        max_bad_records: 50
        max_bad_percent: 1.0
```

### Validation (Pydantic)

- **Typed Models** (`core/config/typed_models.py`):
  - `RootConfig`, `PlatformConfig`, `BronzeConfig`, `SourceConfig`, `ApiConfig`, `SilverConfig`
  - Full type checking + validation at load time
  - Clear error messages for invalid configs

- **Deprecation Warnings**:
  - Missing `config_version` → CFG004
  - Legacy `platform.bronze.local_path` → CFG001
  - Old `source.api.url` → CFG002
  - Etc.

- **Doctor CLI** (`scripts/bronze_config_doctor.py`):
  - Validates config without running extraction
  - Tests backend connectivity
  - Recommends fixes

---

## Testing Strategy

### Test Hierarchy

```
tests/
├── Unit Tests (mark=unit)
│   ├── test_extractors.py          # Extractor logic isolation
│   ├── test_config.py              # Config parsing/validation
│   ├── test_patterns.py            # Pattern enum logic
│   ├── test_silver_models.py       # SilverModel enum
│   └── ... (~20 files)
│
├── Integration Tests (mark=integration)
│   ├── test_integration_samples.py       # End-to-end Bronze→Silver
│   ├── test_integration_s3_moto.py       # S3 with moto mock
│   ├── test_integration_smoke.py         # Quick sanity checks
│   ├── test_azure_integration.py         # Azure with Azurite
│   └── ... (~10 files)
│
└── Sample Data Tests
    ├── test_silver_samples_generation.py # Silver sample gen
    ├── test_silver_samples_metadata.py   # Silver metadata checks
    ├── test_bronze_sample_integrity.py   # Bronze sample validation
    └── test_silver_formats.py            # Format consistency
```

### Key Test Files

| File | Purpose | Coverage |
|------|---------|----------|
| `test_api_extractor.py` | API extractor logic | Auth, pagination, retry, rate limit |
| `test_db_extractor.py` | Database extraction | Driver support, cursor tracking |
| `test_file_extractor.py` | File reading | CSV/JSON/Parquet formats |
| `test_integration_samples.py` | End-to-end flows | Bronze+Silver for all patterns |
| `test_silver_processor.py` | Silver promotion logic | CDC, SCD1/2, chunking |
| `test_storage_policy.py` | Storage scope enforcement | Onprem vs cloud validation |
| `test_config.py` | Configuration validation | Pydantic models, deprecation warnings |
| `run_tests.py` | Test orchestration | Unit/integration/coverage/linting |

### Test Fixtures (conftest.py)

- `sample_config`: Valid YAML config dict
- `sample_records`: 5-record list for testing
- `ensure_sample_data_available`: pytest hook for Bronze sample validation

### Running Tests

```bash
# All tests
python run_tests.py

# Unit only (fast)
python run_tests.py --unit

# Unit + type/lint checks
python run_tests.py --all-checks

# Coverage report (HTML)
python run_tests.py --html-coverage

# Integration only
python run_tests.py --integration
```

### Sample Data Management

- **Bronze Samples** (`sampledata/source_samples/`):
  - Full snapshots, CDC streams, SCD state samples
  - Regenerate: `python scripts/generate_sample_data.py`
  - Used by integration tests to validate extraction/promotion

- **Silver Samples** (`sampledata/silver_samples/`):
  - Per-pattern, per-model outputs
  - Show expected Silver asset shapes
  - Regenerate: `python scripts/generate_silver_samples.py --formats both`

---

## Development Workflows

### Setup (First-Time)

```bash
cd c:\github\bronze-foundry2

# Create venv
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .  # Editable install

# Generate sample data
python scripts/generate_sample_data.py
```

### Common Tasks

#### Run Tests
```bash
make test           # All tests
make test-fast      # Exclude slow integration tests
make test-cov       # With coverage report
```

#### Code Quality
```bash
make lint           # ruff check
make format         # ruff format (auto-fix)
make fix            # ruff check --fix + ruff format
make type-check     # mypy
make all            # Full check suite
```

#### Documentation
```bash
make docs           # Build MkDocs site
make docs-serve     # Serve locally at http://127.0.0.1:8000
```

#### Extract Data
```bash
# Dry run
python bronze_extract.py --config config/test.yaml --dry-run

# Actual extraction
python bronze_extract.py --config config/test.yaml --date 2025-11-28

# Promote to Silver
python silver_extract.py --config config/test.yaml --date 2025-11-28
```

### Pre-Commit Hooks (Optional)

```bash
make pre-commit-install
make pre-commit-run   # Lint before committing
```

---

## Deployment & Operations

### Orchestrator Integration

- **CLI-First Design**: Any job orchestrator (Airflow, Prefect, GitHub Actions, etc.) can invoke:
  ```bash
  python bronze_extract.py --config config.yaml --date $RUN_DATE
  python silver_extract.py --config config.yaml --date $RUN_DATE
  ```

- **Exit Codes**: Script reports success/failure via exit codes
  - `0` = Success
  - Non-zero = Failure (specific code indicates type)

### Monitoring & Observability

#### Logging
- **Format**: JSON (production) or human-readable (dev)
- **Level**: DEBUG | INFO | WARNING | ERROR (configurable via `BRONZE_LOG_LEVEL`)
- **Structured**: Timestamps, context, error codes

#### Catalog Integration
```python
# Report metrics post-extraction
notify_catalog(run_id, metrics)
report_run_metadata(schema, row_count, duration)
report_lineage(source → Bronze → Silver)
```

#### OpenTelemetry Tracing (Optional)
- Enable via `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable
- Traces cover:
  - Extraction phase (API calls, DB queries, file reads)
  - Writing phase (S3 uploads, Azure writes)
  - Promotion phase (Silver processing)

#### Webhooks
```python
# Fire custom webhooks on success/failure
fire_webhooks(event_type="extraction_complete", metadata=...)
```

### Idempotent Operations

- **Metadata Files**: `_metadata.json`, `_checksums.json`
- **Rerun Safety**: SHA256 checksums ensure identical outputs
- **Bronze Immutability**: Re-running same config/date = no duplicates
- **Silver Atomic**: All-or-nothing promotion to Silver

---

## Known Issues & Deprecations

### Deprecated (v1.0 → v1.3)

| Item | Alternative | Status |
|------|-------------|--------|
| `platform.bronze.local_path` | `output_dir` | CFG001 |
| `source.api.url` | `base_url` | CFG002 |
| Missing `source.api.endpoint` default | Explicit `endpoint` | CFG003 |
| `--stream` / `--resume` flags | New chunking via `SilverProcessor` | STREAM001 |
| Implicit `write_silver_outputs` wrapper | `DefaultSilverArtifactWriter` | API001 |

### Recently Removed (v1.0)

- `core.samples.bootstrap`: Automatic fixture creation
- `StreamingSilverWriter`: Replaced by `DatasetWriter` + chunking
- Legacy positional argument handling

### Future Roadmap

- [ ] Multi-config job submission (group configs, submit as batch job)
- [ ] Advanced scheduling hooks (pre/post extraction callbacks)
- [ ] Schema versioning & migration guidance
- [ ] Gold layer template library
- [ ] Custom extractor marketplace/registry

---

## Best Practices & Recommendations

### For Dataset Owners

1. **Start Simple**: Use `docs/examples/configs/examples/*.yaml` as templates
2. **Validate Early**: Run `python bronze_extract.py --config ... --validate-only`
3. **Dry Run First**: Use `--dry-run` to test without writing
4. **Document Intent**: Use intent-owner-guide for clarity on entity_kind, history_mode
5. **Monitor Runs**: Check `_metadata.json` and logs for issues

### For Platform Teams

1. **Centralize Config**: Keep `platform:` section consistent across teams
2. **Storage Policy**: Use `storage_scope: onprem` if on-premises
3. **Rate Limits**: Tune `rate_limit_hz` for API sustainability
4. **Retention**: Set up lifecycle policies for Bronze/Silver outputs
5. **Cost Control**: Monitor file splits via `max_file_size_mb`, consider compression

### For Contributors

1. **Architecture Symmetry**: Keep Bronze/Silver packages balanced
2. **Plugin Model**: New backends = implement `StorageBackend` interface + register
3. **Extractor Extensibility**: Inherit from `BaseExtractor`, test with fixtures
4. **Type Hints**: Full Pydantic models for configs, mypy compliance
5. **Documentation**: Every new feature gets a docs update + example config

---

## Quality Metrics

| Metric | Status | Notes |
|--------|--------|-------|
| **Test Coverage** | ~75% | Core modules well-covered; edge cases remaining |
| **Type Checking** | ✅ | Full Pydantic models; mypy passes |
| **Linting** | ✅ | ruff check/format compliance |
| **Documentation** | ✅ | 40+ markdown files; comprehensive |
| **CI/CD Ready** | ✅ | GitHub Actions compatible; run_tests.py works offline |
| **Backward Compatibility** | ⚠️ | Deprecation framework in place; migration path clear |

---

## Key Takeaways

1. **Production-Ready Framework**: Handles enterprise data landing at scale with pluggable backends
2. **Config-Driven Intent**: YAML configs keep concern separation (platform vs domain teams)
3. **Three-Layer Architecture**: Bronze (raw) → Silver (curated) → Gold (analytics)
4. **Extensible & Modular**: Custom extractors, storage backends, Silver asset types
5. **Rich Tooling**: CLI flags, sample data, testing harness, documentation
6. **Resilient**: Retry logic, circuit breakers, rate limiting, idempotent operations
7. **Observable**: Structured logging, tracing, catalog integration, webhooks
8. **Active Development**: Deprecation framework, version management, clear migration paths

---

## Appendix: Quick Reference

### Running an End-to-End Flow

```bash
# 1. Generate sample data
python scripts/generate_sample_data.py

# 2. Extract to Bronze
python bronze_extract.py \
  --config docs/examples/configs/examples/file_example.yaml \
  --date 2025-11-28

# 3. Promote to Silver
python silver_extract.py \
  --config docs/examples/configs/examples/file_example.yaml \
  --date 2025-11-28

# 4. Inspect outputs
ls -la output/system=*/table=*/dt=*/
ls -la silver_output/domain=*/entity=*/v*/
```

### Documentation Navigator

| Need | File |
|------|------|
| Quick start | `docs/usage/beginner/QUICKSTART.md` |
| Config reference | `docs/framework/reference/CONFIG_REFERENCE.md` |
| Architecture | `docs/framework/architecture.md` |
| Silver patterns | `docs/framework/silver_patterns.md` |
| Error codes | `docs/framework/operations/ERROR_CODES.md` |
| Operations | `docs/framework/operations/OPERATIONS.md` |
| Examples | `docs/examples/README.md` |

### Key Files to Know

- **entry points**: `bronze_extract.py`, `silver_extract.py`, `silver_join.py`
- **config models**: `core/config/typed_models.py`
- **extractors**: `core/extractors/base.py` (and api_extractor.py, db_extractor.py, file_extractor.py)
- **storage**: `core/storage/backend.py` (plugin-based architecture)
- **silver logic**: `core/silver/processor.py`, `artifacts.py`, `models.py`
- **test runner**: `run_tests.py`
- **sample generation**: `scripts/generate_sample_data.py`, `scripts/generate_silver_samples.py`

---

**Document Version**: 1.0
**Last Reviewed**: November 28, 2025
**Next Review**: Quarterly or upon major feature additions
