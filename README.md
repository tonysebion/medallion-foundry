# medallion-foundry

`medallion-foundry` is an opinionated Python framework for building Bronze → Silver pipelines that stay light, declarative, and orchestration neutral. You define data sources with `BronzeSource`, curate them with `SilverEntity`, then run everything through the `pipelines` CLI or your own lightweight orchestrator.

## Highlights

- **Declarative layers** — `pipelines.lib.BronzeSource` lets you land raw data, add telemetry, enforce load patterns, and emit `_metadata.json`/`_checksums.json`. `SilverEntity` deduplicates by natural keys, applies SCD1/SCD2 history, and writes standardized Silver output.
- **Rich source coverage** — Files (CSV, Parquet, space-delimited, fixed-width), MSSQL/Postgres, and REST APIs all sit behind the same interface, work with reusable pagination & pagination state helpers, and honor retry, rate limiting, and watermark configuration.
- **Pluggable storage** — Target local paths, S3 buckets, or Azure Blob/ADLS via the storage helpers (`pipelines.lib.storage`), and optionally override `BRONZE_TARGET_ROOT`/`SILVER_TARGET_ROOT` in CI.
- **Pipeline-first CLI** — `python -m pipelines` discovers everything under `pipelines/`, supports Bronze-only (`:bronze`) and Silver-only (`:silver`) runs, dry runs, pre-flight checks, explanations, and has helpers for testing connections, generating samples, and creating new pipelines.
- **Sample data & helpers** — Scripts such as `scripts/generate_bronze_samples.py`, `scripts/generate_silver_samples.py`, and `scripts/generate_pattern_test_data.py` recreate canonical test data so you can exercise Bronze→Silver flows locally.
- **Documentation & quality** — `pipelines/QUICKREF.md`, `docs/index.md`, and `docs/pipelines/GETTING_STARTED.md` walk through usage, plus there are guides for resilience, storage, and operations across `docs/`.

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

3. **Run a sample pipeline**

   ```powershell
   python -m pipelines examples.retail_orders --date 2025-01-15
   ```

4. **Validate before writing**

   ```powershell
   python -m pipelines examples.retail_orders --date 2025-01-15 --dry-run
   python -m pipelines examples.retail_orders --date 2025-01-15 --check
   ```

5. **Generate sample data**

   ```powershell
   python scripts/generate_bronze_samples.py --all
   python scripts/generate_silver_samples.py --all
   ```

6. **Create new pipelines**

   ```powershell
   python -m pipelines create                     # Interactive wizard
   python -m pipelines new finance.invoices --source-type database_mssql
   ```

## Building a pipeline

Pipelines live under the `pipelines/` package. Use the templates in `pipelines/templates/` or copy the examples in `pipelines/examples/` as starting points. A minimal pipeline imports the core dataclasses:

```python
from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

bronze = BronzeSource(...)
silver = SilverEntity(...)

def run(run_date: str, **kwargs):
    return {
        "bronze": bronze.run(run_date, **kwargs),
        "silver": silver.run(run_date, **kwargs),
    }
```

`LoadPattern`, `SourceType`, `EntityKind`, and `HistoryMode` are typed enums, and the `pipelines.lib.runner.pipeline` decorator wires consistent logging, timing, and dry-run semantics.

## CLI at a glance

| Command | Description |
| --- | --- |
| `python -m pipelines <module>` | Run both Bronze and Silver. Module names mirror `pipelines/<path>.py`, e.g., `examples.retail_orders`. |
| `python -m pipelines <module>:bronze` | Run Bronze only. |
| `python -m pipelines <module>:silver` | Run Silver only. |
| `--date YYYY-MM-DD` | Required when executing a pipeline. |
| `--dry-run` | Skip writes but still validate configuration. |
| `--check` | Validate configuration + connectivity without running. |
| `--explain` | Describe the pipeline plan without touching data. |
| `--target <path>` | Override target roots (uses `BRONZE_TARGET_ROOT` / `SILVER_TARGET_ROOT` semantics). |
| `-v`, `--json-log`, `--log-file` | Logging controls. |
| `python -m pipelines --list` | Show every discovered pipeline along with available layers. |
| `python -m pipelines test-connection <name>` | Validate a database connection (supports `--host`, `--database`, `--type`). |
| `python -m pipelines inspect-source --file ./data.csv` | Inspect an input file and suggest natural keys/timestamps. |
| `python -m pipelines generate-sample <module>` | Generate fake rows for the named pipeline (`--rows`, `--output`). |
| `python -m pipelines new <module> --source-type ...` | Create a scaffold file with the requested source type. |
| `python -m pipelines.create` | Launch interactive wizard that prompts for system, entity, source type, and Silver settings. |

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
