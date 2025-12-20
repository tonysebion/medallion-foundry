# medallion-foundry

**From source → Bronze → Silver with one Python package.**

## Quick start (5 minutes)

1. **Install** (Windows shown here, adapt for macOS/Linux):

   ```powershell
   python -m venv .venv
   .venv\Scripts\activate
   pip install -e .
   ```

2. **Discover built-in pipelines**:

   ```powershell
   python -m pipelines --list
   ```

3. **Run an example**:

   ```powershell
   python -m pipelines examples.retail_orders --date 2025-01-15
   ```

4. **Validate before writing**:

   ```powershell
   python -m pipelines examples.retail_orders --date 2025-01-15 --dry-run
   python -m pipelines examples.retail_orders --date 2025-01-15 --check
   ```

5. **Generate canonical fixtures**:

   ```powershell
   python scripts/generate_bronze_samples.py --all
   python scripts/generate_silver_samples.py --all
   ```

## Common tasks

| Task | Command | Notes |
| --- | --- | --- |
| List pipelines | `python -m pipelines --list` | Includes Bronze/Silver availability and descriptions |
| Run Bronze + Silver | `python -m pipelines <module> --date YYYY-MM-DD` | Module mirrors `pipelines/<path>.py` (e.g., `examples.retail_orders`) |
| Run Bronze only | `python -m pipelines <module>:bronze --date YYYY-MM-DD` | |
| Run Silver only | `python -m pipelines <module>:silver --date YYYY-MM-DD` | |
| Dry run | Add `--dry-run` | Validates config without writing anything |
| Pre-flight checks | Add `--check` | Validates config + connectivity |
| Explain run | Add `--explain` | Shows steps without executing |
| Override targets | Add `--target ./local_output/` | Honors `BRONZE_TARGET_ROOT` / `SILVER_TARGET_ROOT` semantics |
| Test a connection | `python -m pipelines test-connection claims_db --host ... --database ...` | Supports `--type` (mssql/postgres) |
| Inspect source | `python -m pipelines inspect-source --file ./data.csv` | Prints schema + suggested keys |
| Generate pipeline data | `python -m pipelines generate-sample examples.retail_orders --rows 250 --output ./sample_data/` | |
| Bootstrap pipeline | `python -m pipelines new claims.header --source-type database_mssql` | Creates scaffolded file |
| Interactive creator | `python -m pipelines.create` | Prompts for system, source, Silver config, outputs ready-to-run file |

## Reference links

- `docs/pipelines/GETTING_STARTED.md` — Pipeline primer, Bronze/Silver snippets, and patterns tailored for pipeline authors.
- `pipelines/QUICKREF.md` — Operational cheatsheet with storage tips, environment variables, and troubleshooting topics.
- `docs/scripts/README.md` — How to regenerate `sampledata/bronze_samples` and `sampledata/silver_samples`.
- `docs/usage/` — Ownership guides, onboarding checklists, and pattern references that complement the pipeline-first workflow.
- `docs/framework/` — Architecture, operations playbooks, testing strategy, and migration guides.

## System requirements & compatibility

| Item | Notes |
| --- | --- |
| Python | 3.9+ |
| Storage backends | Local filesystem, `s3://`, `abfss://`, `wasbs://`, `az://` |
| Credentials | AWS env vars or Azure account/service principal credentials |
| Windows support | Fully supported; CLI examples above use PowerShell |

## Need help?

1. `python -m pipelines --list` to confirm your module is discoverable.
2. `python -m pipelines <module> --date YYYY-MM-DD --dry-run --check` before running anything that writes.
3. Use `scripts/generate_bronze_samples.py` + `scripts/generate_silver_samples.py` to refresh fixtures for regression tests.
4. Inspect sources via `python -m pipelines inspect-source --file ./data.csv` when onboarding new files.
5. Roll your own pipeline and then run it with `python -m pipelines myteam.orders --date YYYY-MM-DD`.

If you run into issues, the troubleshooting guides live under `docs/framework/operations/`.
