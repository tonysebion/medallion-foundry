# medallion-foundry

`medallion-foundry` is a **production-ready**, config-driven Python framework for landing data from **APIs**, **databases**, or **custom sources** into a **Bronze layer** with **pluggable storage backends** (S3, Azure, local filesystem), using conventions that support future analytics platforms and medallion-style architectures.

This framework is intentionally lightweight and orchestration-neutral: you can run it from any scheduler or workflow orchestrator that can invoke a Python CLI.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/tonysebion/medallion-foundry/actions/workflows/ci.yml/badge.svg)](https://github.com/tonysebion/medallion-foundry/actions)

## Key Features

### Core Capabilities
- **Multiple Source Types** – APIs (REST), databases (SQL), local files, or custom Python extractors
- **Authentication** – Bearer tokens, API keys, Basic auth, and custom headers
- **Pagination** – Offset, page, cursor, or none
- **Incremental Loading** – Bronze deltas capture source changes while metadata tracks load patterns for safe replays
- **Multiple Formats** – Parquet (Snappy) for analytics; optional CSV for debugging
- **Pluggable Storage** – S3, Azure Blob/ADLS, local filesystem
- **Resilience** – Unified retry + circuit breaker + rate limiting
- **Error Taxonomy** – Standardized error codes for routing & ops
- **Structured Logging & Tracing** – Rich logs plus optional OpenTelemetry spans
- **Production Ready** – Idempotent writes, atomic Silver promotion, consistent layout

### Enhanced Features
- **File Size Control** – Configurable `max_file_size_mb` (128MB–1GB) and row splitting
- **Intraday Partitioning** – Hourly / batch-id / timestamp partition strategies
- **Parallel Extraction** –
  - Config-level: multi-thread across configs (`--parallel-workers`)
  - Chunk-level: concurrent chunk processing (`parallel_workers`)
- **Async HTTP Path** – httpx-based extractor with prefetch pagination & backpressure
- **Batch Metadata** – Automatic `_metadata.json` for monitoring and idempotency
- **Reliable Reruns** – `_metadata.json` and `_checksums.json` ensure Bronze and Silver stay in sync so retrying a run is safe.
- **Benchmark Harness** – Performance scenarios (sync vs async, rate limit impact)
- **Extensible Architecture** – Clean interfaces for new storage backends or extractors

*See [docs/usage/patterns/ENHANCED_FEATURES.md](docs/usage/patterns/ENHANCED_FEATURES.md) and [docs/usage/PERFORMANCE_TUNING.md](docs/usage/PERFORMANCE_TUNING.md) for deep dives.*

Need a concrete intent example before touching Python? Follow `docs/usage/onboarding/intent-owner-guide.md`—it bundles the owner intent template, the generated pattern matrix, the expand-intent helper, and a safe-first dry run (`python bronze_extract.py --config ... --dry-run`) into one narrative. After the guide, the new dataset checklist and Bronze readiness checklist list every checkbox you need before declaring a dataset “ready”. Use `docs/index.md` to jump to the setup track (environment, deps, first-run check), maintainer track (`silver_patterns.md`, `pipeline_engine.md`, etc.), or the owner track depending on whether you’re prepping the install, evolving the engine, or filling out configs. For a bird’s-eye view of the usage docs, start at `docs/usage/index.md` to see how beginner, onboarding, and pattern sections fit together.

## Architecture Principles

- **Multiple CLIs, unified config-driven approach**
  Three entrypoints handle the medallion architecture:
  - `bronze_extract.py` – Extract data from APIs/databases to Bronze layer
  - `silver_extract.py` – Promote Bronze data to Silver layer with transformations
  - `silver_join.py` – Join Silver assets into curated datasets

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
  - Configurable `max_rows_per_file` to split large extracts into part files (e.g., `part-0001`, `part-0002`, ...).

- **Orchestration friendly**
  - CLI takes `--config` and optional `--date`
  - Uses exit codes (0 = success, non-zero = failure)
  - Structured logging with levels and timestamps

- **Pattern-driven guidance**
  Source and semantic owners can lean on `docs/framework/silver_patterns.md` and `docs/usage/onboarding/new_dataset_checklist.md` to describe entity_kind, history_mode, input_mode, natural keys, timestamps, attributes, and advanced controls for every dataset. These docs demonstrate how Bronze and Silver work together so configs become intent-driven rather than procedural.

## Quick Start

### Testing Your API (For Product/API Teams)

**Not familiar with Python?** No problem! The quick start sections above have complete walkthroughs.

**Quick summary:**

```bash
# 1. Setup (one-time, ~3 minutes), use a 64-bit install of python
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 2. Copy and edit the quick test config
cp docs/examples/configs/examples/file_example.yaml config/test.yaml
# Edit config/test.yaml with your API details

# 3. Set your token
export MY_API_TOKEN="your-token-here"  # Linux/Mac
$env:MY_API_TOKEN="your-token-here"    # Windows

# 4. Run
python bronze_extract.py --config config/test.yaml

# 5. Check ./output/ folder - if you see data, you're done!
```

**See the quick start sections above for detailed instructions.**

### Offline Local Quick Test

Need proof the tooling works but don't have API credentials yet? Use the bundled sample data.

```bash
# 0. (Once) prepare the sandbox data (produces the 60-day, 250K-row default with linear growth + updates)
python scripts/generate_sample_data.py

# (Optional) Customize the span/scale or disable updates via the CLI flags shown nearby.

# 1. Run Bronze with the file-based config
python bronze_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-13

# 2. Promote the same run into Silver
python silver_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-13

# 3. Inspect the outputs
tree output/system=retail_demo
tree silver_output/domain=retail_demo
```

This workflow uses only local files, so a non-Python user can validate everything end-to-end before wiring real APIs or databases.

---

### Cloud Quick Test (S3)

For cloud-native deployments, use S3-based sample data. Requires AWS credentials and an S3 bucket with sample data uploaded.

**Prerequisites:**
- AWS credentials configured (via `aws configure` or environment variables)
- S3 bucket with sample data at `source_samples/`, `bronze_samples/`, and `silver_samples/` prefixes
- Upload local samples to S3: `aws s3 cp sampledata/ s3://your-bucket/ --recursive`

```bash
# 0. (Once) prepare the sandbox data locally, then upload to S3
python scripts/generate_sample_data.py
aws s3 cp sampledata/source_samples/ s3://your-bucket/source_samples/ --recursive

# 1. Run Bronze with the S3-based config
python bronze_extract.py --config docs/examples/configs/examples/s3_example.yaml --date 2025-11-13

# 2. Promote the same run into Silver
python silver_extract.py --config docs/examples/configs/examples/s3_example.yaml --date 2025-11-13

# 3. Inspect the outputs in S3
aws s3 ls s3://your-bucket/bronze_samples/
aws s3 ls s3://your-bucket/silver_samples/
```

This workflow validates the full pipeline using cloud storage, ideal for production-like testing.

---

### Running the new intent configs

To exercise the example configurations under `docs/examples/configs`, run Bronze and Silver separately with the same YAML (each config now has both sections). For example:

```bash
python bronze_extract.py --config docs/examples/configs/advanced/enhanced_example.yaml --date 2025-11-13
python silver_extract.py --config docs/examples/configs/advanced/enhanced_example.yaml --date 2025-11-13
```

Repeat those steps for `api_example.yaml`, `db_complex.yaml`, `file_cdc_example.yaml`, etc., to cover different entity kinds/load patterns. Bronze outputs always land under `output/env=<environment>/system=<system>/table=<entity>/…` (defaulting to no `env=` when none is specified); Silver promotes into `silver/env=<environment>/domain=<domain>/entity=<entity>/v<version>/<load_partition>=<date>/…`. Setting `environment` at the top level of the intent config ensures Bronze and Silver placements follow the same hierarchy.

Need a single command to run Bronze and Silver together? Use `scripts/run_intent_config.py --config docs/examples/configs/examples/<name>.yaml --date 2025-11-13`; it shells out to the Bronze and Silver CLIs so you can treat each intent config like a runnable script bundle. Use `--skip-bronze` or `--skip-silver` if you only want one leg.

The integration tests (`tests/test_integration_samples.py`) already automate this loop; read it for how we rewrite file paths and validate the Tiered outputs. See `docs/framework/reference/intent-lifecycle.md` for the canonical Bronze→Silver flow, directory mapping, and metadata expectations that keep the manifesto-lived experience calm.

---

### Full Setup (For Data Teams)

Complete production setup:

1. **Install dependencies:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```

2. **Create a config file:**

   ```bash
   cp docs/examples/configs/examples/api_example.yaml config/my_api.yaml
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

## ?? What's Included

### API Extractor
- ? Bearer token authentication
- ? API key authentication
- ? Basic authentication
- ? Offset-based pagination
- ? Page-based pagination
- ? Cursor-based pagination
- ? Retry logic with exponential backoff
- ? Custom headers support
- ? Flexible response parsing

### Database Extractor
- ? SQL Server, PostgreSQL, MySQL support (via ODBC)
- ? Incremental loading with cursor tracking
- ? State file management
- ? Batch fetching
- ? Automatic WHERE clause injection
- ? Retry logic for failed queries


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

- ### Load Patterns & Silver Promotion
- Configure the Bronze CLI with `--load-pattern` (or `source.run.load_pattern`) to label outputs as `full`, `cdc`, or `current_history`
- Bronze partition paths now include the load pattern (`system=foo/table=bar/pattern=current_history/...`) so downstream jobs can select data easily
- Use the new `silver_extract.py` helper to pull Bronze chunks into curated Silver tables; it mirrors the partition layout and writes metadata for later stages
- When Bronze partitions are large, let `SilverProcessor` handle chunking; the retired streaming/resume flow is described in `docs/framework/operations/legacy-streaming.md`.
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
- **Local paths**: Full snapshot (500 rows): `sampledata/source_samples/pattern1_full_events/system=retail_demo/table=orders/pattern=full/dt=2025-11-01/`
- **S3 paths** (after upload): `s3://your-bucket/source_samples/pattern1_full_events/system=retail_demo/table=orders/pattern=full/dt=2025-11-01/`
- Full snapshot (500 rows): `sampledata/source_samples/pattern1_full_events/system=retail_demo/table=orders/pattern=full/dt=2025-11-01/`
- CDC stream (400 events): `sampledata/source_samples/pattern2_cdc_events/system=retail_demo/table=orders/pattern=cdc/dt=2025-11-02/`
- Current + history mix (800 rows): `sampledata/source_samples/pattern3_scd_state/system=retail_demo/table=orders/pattern=current_history/dt=2025-11-03/`
- Silver curated samples per model: `sampledata/silver_samples/<load_pattern>/<silver_model>/domain=<domain>/entity=<entity>/v<version>/load_date=<YYYY-MM-DD>/`. Run `python scripts/generate_silver_samples.py --formats both` to regenerate the Bronze -> Silver outputs for every supported Silver model.
- Bronze hybrid samples now include both point-in-time and cumulative variations per pattern (see `hybrid_cdc_point`, `hybrid_cdc_cumulative`, `hybrid_incremental_point`, `hybrid_incremental_cumulative`) plus rotated references; rerun `python scripts/generate_sample_data.py` to refresh them.
- Documentation structure:
  - **Architecture overview**: `docs/ARCHITECTURE.md` sketches Bronze/Silver/storage flows plus the plugin registry and sample references.
  - **Operations & governance**: `docs/framework/operations/OPERATIONS.md` describes validation, sample generation, storage policy, and log/metric practices so different roles know where to look.
- **Silver Join**: Join two existing Silver assets with `silver_join.py`, including configurable `join_key_pairs`, `join_strategy`, `quality_guards`, spill directories, checkpoints (`progress.json`), and richer metadata (column lineage, chunk metrics) that trace back to the Bronze inputs. Sample outputs in `docs/examples/data/silver_join_samples/v1` exercise every combination of inputs/formats and are generated with `python scripts/generate_silver_join_samples.py --version 1 --formats both`. See `docs/SILVER_JOIN.md` and `docs/examples/configs/advanced/silver_join_example.yaml` for configuration patterns.
- Matching configs: `file_example.yaml` (full), `file_cdc_example.yaml` (cdc), `file_current_history_example.yaml`, `s3_example.yaml` (S3)

### Sample Configs
- `docs/examples/configs/` contains starter configs in `examples/` plus advanced versions in `advanced/` that showcase options for each extractor type (API, DB, file, custom). Use the examples configs to get Bronze/Silver running quickly and refer to the advanced ones when you need to enable partitioning, normalization, error handling, or chunk-friendly tuning; `SilverProcessor` chunking happens automatically (legacy streaming flags are now documented in `docs/framework/operations/legacy-streaming.md`).

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
python bronze_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-14
python silver_extract.py --config docs/examples/configs/examples/file_example.yaml --date 2025-11-14
```

- Silver CLI highlights:
  - `--config`, `--date`, `--dry-run`, `--validate-only`, `--pattern {full|cdc|current_history}`
  - Output controls: `--write-parquet/--no-write-parquet`, `--write-csv/--no-write-csv`, `--parquet-compression`
  - Naming overrides: `--full-output-name`, `--cdc-output-name`, `--current-output-name`, `--history-output-name`
  - Asset model controls: `--silver-model {scd_type_1|scd_type_2|incremental_merge|full_merge_dedupe|periodic_snapshot}` to request one of the supported Silver asset types (defaults are derived from the Bronze load pattern)
  - Partition overrides still available via `--bronze-path`/`--silver-base` when you need to promote ad-hoc data
- Under the hood, the CLI flows through `SilverPromotionService` and `DatasetWriter` (see `silver_extract.py`), so extending behavior means overriding a focused class instead of editing a 600-line script. Shared defaults/validation now live in the typed dataclasses inside `core/config_models.py`, keeping Bronze and Silver configs perfectly aligned.
- Define many inputs in a single YAML by using the `sources:` list (each item holds its own `source` and optional `silver` overrides). Bronze automatically runs every entry; Silver uses `--source-name <entry>` to pick the one you want when the config contains multiple sources.

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

- ? Business logic or row-level filteringï¿½Silver only standardizes structure.
- ? Custom transformations per dataset beyond the declarative `schema`/`normalization` options.
- ? Silent drops of bad dataï¿½use `silver.error_handling` to quarantine and alert.

### Core Features
- ? Proper Python package structure
- ? Comprehensive configuration validation
- ? Structured logging system
- ? Error handling with cleanup
- ? CSV and Parquet output
- ? S3 upload with retries
- ? File chunking for large datasets
- ? Test suite with pytest
- ? Extensible architecture

## ?? Documentation

- [OPS_PLAYBOOK.md](docs/framework/operations/OPS_PLAYBOOK.md) ï¿½ day-two operations, hooks, and monitoring tips.
- [GOLD_CONTRACTS.md](docs/framework/operations/GOLD_CONTRACTS.md) ï¿½ guidance for documenting downstream contracts and expectations.
- [docs/framework/reference/DOCUMENTATION.md](docs/framework/reference/DOCUMENTATION.md) ï¿½ architecture concepts and FAQs.
- [ENHANCED_FEATURES.md](docs/ENHANCED_FEATURES.md) ï¿½ advanced configuration & features.
- [CONFIG_REFERENCE.md](docs/framework/reference/CONFIG_REFERENCE.md) ï¿½ exhaustive list of config options.
- [TESTING.md](TESTING.md) ï¿½ how to run tests and interpret results.
- [CONTRIBUTING.md](CONTRIBUTING.md) â€” how to contribute and run local checks
- [SECURITY.md](SECURITY.md) â€” how to report a security issue
### Silver Refinement Options\n- silver.schema: rename or reorder columns for standardized curated tables.\n- silver.normalization: toggle 	rim_strings / empty_strings_as_null to keep formatting consistent across datasets.\n- silver.error_handling: set enabled, max_bad_records, and max_bad_percent to quarantine bad rows into _errors/ files instead of failing immediately.\n- silver.partitioning: add a secondary partition column (e.g., status, region) for Silver outputs while still mirroring the Bronze folder layout.\n- silver.domain / entity / ersion / load_partition_name: describe the medallion layout so outputs land under domain=<domain>/entity=<entity>/v<version>/<load partition>=YYYY-MM-DD/.. Optional include_pattern_folder: true inserts pattern=<load_pattern> before the load partition.\n- silver.model: choose the Silver asset type to emit. Available options now mirror the requested asset catalogue:\n  - scd_type_1 â€“ deduplicated current view (SCD Type 1).\n  - scd_type_2 â€“ current + full history split with an is_current flag (SCD Type 2).\n  - incremental_merge â€“ incremental change set from the Bronze data (CDC/timestamp).\n  - ull_merge_dedupe â€“ full snapshot deduplicated by the configured keys/order column, ready for full merges.\n  - periodic_snapshot â€“ exact Bronze snapshot for periodic refreshes.\n  - silver.model_profile â€“ pick a named preset (analytics, operational, merge_ready, cdc_delta, snapshot) that maps to silver.model.\n- storage_metadata: classify your storage target with oundary, provider_type, and cloud_provider. Use --storage-scope onprem to force an on-prem policy.\n\nExample Silver section:

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

- ? Business logic or row-level filteringï¿½Silver only standardizes structure.
- ? Custom transformations per dataset beyond the declarative `schema`/`normalization` options.
- ? Silent drops of bad dataï¿½use `silver.error_handling` to quarantine and alert.

### Core Features
- ? Proper Python package structure
- ? Comprehensive configuration validation
- ? Structured logging system
- ? Error handling with cleanup
- ? CSV and Parquet output
- ? S3 upload with retries
- ? File chunking for large datasets
- ? Test suite with pytest
- ? Extensible architecture

## ?? Documentation

- [OPS_PLAYBOOK.md](docs/framework/operations/OPS_PLAYBOOK.md) ï¿½ day-two operations, hooks, and monitoring tips.
- [GOLD_CONTRACTS.md](docs/framework/operations/GOLD_CONTRACTS.md) ï¿½ guidance for documenting downstream contracts and expectations.
- [docs/QUICKSTART.md](docs/QUICKSTART.md) ï¿½ detailed tutorial with screenshots.
- [docs/framework/reference/DOCUMENTATION.md](docs/framework/reference/DOCUMENTATION.md) ï¿½ architecture concepts and FAQs.
- [ENHANCED_FEATURES.md](docs/ENHANCED_FEATURES.md) ï¿½ advanced configuration & features.
- [CONFIG_REFERENCE.md](docs/framework/reference/CONFIG_REFERENCE.md) ï¿½ exhaustive list of config options.
- [TESTING.md](TESTING.md) ï¿½ how to run tests and interpret results.
