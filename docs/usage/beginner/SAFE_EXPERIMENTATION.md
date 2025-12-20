# Safe Experimentation Guide

Run pipelines safely without touching production data by layering validation, dry runs, and small-scale tests.

## Experimentation levels

### Level 1 — Demo playground (recommended first step)

- Regenerate the canonical fixtures with `python scripts/generate_bronze_samples.py --all` followed by `python scripts/generate_silver_samples.py --all`.
- Run a bundled pipeline against those fixtures:  
  ```bash
  python -m pipelines examples.retail_orders --date 2025-01-15 --dry-run
  ```
- This exercises Bronze + Silver without persisting any data, and it keeps you in a safe sandbox.

### Level 2 — Validate your config

- Copy an example config to `config/my_api.yaml` and adapt the system/table names.
- Validate the configuration and connectivity:
  ```bash
  python -m pipelines myteam.orders --date 2025-01-15 --dry-run
  python -m pipelines myteam.orders --date 2025-01-15 --check
  python -m pipelines myteam.orders --date 2025-01-15 --explain
  ```
- Supplement validation with helpers:
  - `python -m pipelines inspect-source --file ./data/input.csv` (schemas, keys, timestamp guesses)
  - `python -m pipelines test-connection claims_db --host ... --database ...`

### Level 3 — Controlled extraction

- Limit the amount of data via config:
  ```yaml
  bronze:
    load_pattern: full_snapshot
    options:
      limit_rows: 10
  ```
- Override targets locally:
  ```bash
  python -m pipelines myteam.orders --date 2025-01-15 --target ./local_bronze/
  ```
- Reset incremental state when needed:
  ```python
  from pipelines.lib.state import delete_watermark
  delete_watermark("my_system", "my_entity")
  ```
- After Bronze succeeds, run Silver in isolation:
  ```bash
  python -m pipelines myteam.orders:silver --date 2025-01-15 --target ./local_silver/
  ```

## Safety features

- `--dry-run` stops before writing data but still validates schema and connectivity.
- `--check` combines validation and connectivity checks so you see issues before touching storage.
- `--explain` prints the run plan — useful in automation or when reviewing new pipelines.
- `--target` (or `BRONZE_TARGET_ROOT`/`SILVER_TARGET_ROOT`) keeps experimental output on local disk.
- Metadata, checksum, and watermark helpers guard idempotency, while structured logging makes tracing failures easy.

## Step-by-step workflow

1. **Copy a sample config** from `pipelines/examples/` or `docs/examples/configs`.
2. **Validate with a dry run** using the pipeline CLI.
3. **Run Bronze locally** with `--target` and a small `limit_rows`.
4. **Inspect metadata** under `./local_bronze/` before promoting to Silver.
5. **Run Silver separately** to ensure deduplication/history handling.

## Extra tips

- Use `python -m pipelines.create` to launch the interactive wizard before writing code.
- Generate pattern-specific fixtures with `python scripts/generate_pattern_test_data.py --pattern current_history --generate-assertions`.
- When experimenting with rate limits or retries, wrap `run_bronze` with `@with_retry(...)` from `pipelines.lib.resilience`.
- Keep `.state/` under version control or change `PIPELINE_STATE_DIR` for ephemeral experiments.
