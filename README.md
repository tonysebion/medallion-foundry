# Medallion Foundry

`medallion-foundry` is a YAML-first framework for building Bronze → Silver data pipelines using the medallion architecture. Define your pipelines in simple YAML files and run them through the CLI - no Python knowledge required. For advanced use cases, Python pipelines are also supported.

## Highlights

- **YAML-first configuration** — Define pipelines in declarative YAML files with JSON Schema validation for editor autocomplete. Python is available for advanced use cases requiring custom logic.
- **Declarative layers** — Bronze lands raw data with telemetry, metadata, and checksums. Silver deduplicates by natural keys and applies SCD1/SCD2 history tracking.
- **Rich source coverage** — Files (CSV, Parquet, JSON, Excel, fixed-width), databases (MSSQL, Postgres, MySQL, DB2), and REST APIs all use the same interface with pagination, retry, rate limiting, and watermark support.
- **Pluggable storage** — Target local paths, S3 buckets, or Azure Blob/ADLS via storage helpers.
- **Pipeline-first CLI** — `python -m pipelines` discovers YAML and Python pipelines, supports Bronze-only/Silver-only runs, dry runs, pre-flight checks, and interactive pipeline creation.
- **Sample data & helpers** — Scripts recreate canonical test data so you can exercise Bronze→Silver flows locally.

## Quick start

1. **Prepare the environment**

   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   pip install -e .
   ```

2. **List the bundled pipelines**

   ```powershell
   python -m pipelines --list
   ```

3. **Run a sample YAML pipeline**

   ```powershell
   python -m pipelines ./pipelines/examples/retail_orders.yaml --date 2025-01-15
   ```

4. **Validate before writing**

   ```powershell
   python -m pipelines ./pipelines/examples/retail_orders.yaml --date 2025-01-15 --dry-run
   python -m pipelines ./pipelines/examples/retail_orders.yaml --date 2025-01-15 --check
   ```

5. **Generate sample data**

   ```powershell
   python scripts/generate_bronze_samples.py --all
   python scripts/generate_silver_samples.py --all
   ```

6. **Create new pipelines**

   ```powershell
   python -m pipelines.create                     # Interactive wizard (generates YAML)
   python -m pipelines.create --format python    # Generate Python instead
   ```

## Building a pipeline

Pipelines can be defined in YAML (recommended) or Python. YAML provides editor autocomplete via JSON Schema and doesn't require Python knowledge. Use Python when you need complex transformations, retry decorators, or custom logic.

### YAML Pipeline (Recommended)

```yaml
# yaml-language-server: $schema=./pipelines/schema/pipeline.schema.json
name: retail_orders
description: Load retail orders from CSV and curate with SCD Type 1

bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders_{run_date}.csv

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  attributes:
    - customer_id
    - order_total
    - status
```

Run with: `python -m pipelines ./my_pipeline.yaml --date 2025-01-15`

### Python Pipeline (Advanced)

Use Python when you need retry logic, custom transformations, or runtime-computed configuration:

```python
from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode
from pipelines.lib.resilience import with_retry

bronze = BronzeSource(...)
silver = SilverEntity(...)

@with_retry(max_attempts=3)  # Retry on transient failures (Python only)
def run(run_date: str, **kwargs):
    return {
        "bronze": bronze.run(run_date, **kwargs),
        "silver": silver.run(run_date, **kwargs),
    }
```

Templates and examples for both formats are in `pipelines/templates/` and `pipelines/examples/`.

## CLI at a glance

| Command | Description |
| --- | --- |
| `python -m pipelines ./pipeline.yaml` | Run a YAML pipeline (Bronze + Silver). |
| `python -m pipelines <module>` | Run a Python pipeline. Module names mirror `pipelines/<path>.py`. |
| `python -m pipelines ./pipeline.yaml:bronze` | Run Bronze only. |
| `python -m pipelines ./pipeline.yaml:silver` | Run Silver only. |
| `--date YYYY-MM-DD` | Required when executing a pipeline. |
| `--dry-run` | Skip writes but still validate configuration. |
| `--check` | Validate configuration + connectivity without running. |
| `--explain` | Describe the pipeline plan without touching data. |
| `--target <path>` | Override target roots (uses `BRONZE_TARGET_ROOT` / `SILVER_TARGET_ROOT` semantics). |
| `-v`, `--json-log`, `--log-file` | Logging controls. |
| `python -m pipelines --list` | Show every discovered pipeline (YAML and Python). |
| `python -m pipelines test-connection <name>` | Validate a database connection (supports `--host`, `--database`, `--type`). |
| `python -m pipelines inspect-source --file ./data.csv` | Inspect an input file and suggest natural keys/timestamps. |
| `python -m pipelines.create` | Interactive wizard (generates YAML by default, use `--format python` for Python). |

## Sample data and helpers

- `scripts/generate_bronze_samples.py` replays every load pattern into `sampledata/bronze_samples/sample=<pattern>/dt=<date>/` with `_metadata.json` and `_checksums.json`.
- `scripts/generate_silver_samples.py` reads those Bronze samples and emits curated Silver assets in `sampledata/silver_samples/`.
- `scripts/generate_pattern_test_data.py` builds ad-hoc batch series (snapshot, incremental, CDC, SCD2) for verification or manual experiments.
- Reference sample pipelines live under `pipelines/examples/` and use `sampledata/bronze_samples` as input.

## Environment hooks

- `BRONZE_TARGET_ROOT` / `SILVER_TARGET_ROOT` — Redirect Bronze/Silver outputs for local development or testing.
- `PIPELINE_STATE_DIR` — Defaults to `.state/` and houses watermark/checkpoint files.
- `${VAR_NAME}` inside pipeline `options` respects environment expansion via `pipelines.lib.env.expand_env_vars`.
- AWS/Azure credentials (e.g., `AWS_ACCESS_KEY_ID`, `AZURE_STORAGE_ACCOUNT_KEY`) power cloud storage helpers.

## Documentation & further reading

- `docs/index.md` — Guided table of contents for beginners, owners, and developers.
- `docs/pipelines/GETTING_STARTED.md` — Pipeline-specific walkthrough with Bronze/Silver snippets and patterns.
- `pipelines/QUICKREF.md` — CLI cheatsheet and operational tips.
- `docs/scripts/README.md` — Sample data generation & tooling reference.
- `docs/usage/`, `docs/guides/`, and `docs/framework/` — Deep dives on onboarding, resilience, patterns, and operations.

## Testing & validation

```powershell
pip install -e .
python -m pytest tests
```

Pair pytest with the sample data scripts above before running integration suites so fixtures are available.
