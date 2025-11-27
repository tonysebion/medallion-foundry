# First Production Run Checklist

Finish [`docs/setup/INSTALLATION.md`](../../setup/INSTALLATION.md) before working through this checklist. That guide ensures your repo, virtualenv, and dependencies are installed so the items below can focus on Bronzes, Silver, and observability.

## 1) Environment & Dependencies
- Python: Verify version (`python --version`) matches supported range.
- Virtualenv: Activated (prompt shows `.venv`).
- Packages: Installed from `docs/setup/INSTALLATION.md` so subsequent steps can assume the CLI works.

## 2) MSSQL (if applicable)
- Windows: Install the Microsoft ODBC Driver for SQL Server (e.g., ODBC Driver 18).
- Connection string: Uses installed driver name and TLS options.
- Environment variable set for config key `source.db.conn_str_env`.

## 3) Storage Access
- Local: Write permissions to `./output`.
- S3/Azure/local: Credentials present; minimal IAM/role (or local permissions) allows `list/get/put`.

## 4) Config Sanity
- `--validate-only` passes:
  - `python bronze_extract.py --config config/your_source.yaml --validate-only`
- `--dry-run` passes (no writes):
  - `python bronze_extract.py --config config/your_source.yaml --dry-run`

## 5) Logging & Observability
- Prefer JSON logs for non-interactive runs:
  - `--log-format json` or `BRONZE_LOG_FORMAT=json`.
- Capture `run_id`, `dataset_id`, and `dt` from logs.

## 6) Smoke Tests (optional but recommended)
- Unit tests:
  - `pytest -q`
- Opt-in integration smoke:
  - PowerShell: `$env:RUN_SMOKE = "1"; pytest -q -m "integration"`
  - Validates HTTP, local Parquet IO, and S3 (moto).

## 7) Small-Scale Trial
- Run against a tiny time window or a low-volume endpoint first.
- Confirm Bronze files land as expected; check `_metadata.json`.

## 8) Promotion to Silver (optional)
- Discover Bronze partition and promote with `silver_extract.py`.
- Confirm outputs and `_checksums.json` present.

## 9) Rollback/Retry Plan
- If a run fails mid-flight, re-run is idempotent.
- Clean up partial outputs and temp files if needed; investigate logs.

## 10) Automate
- Add your orchestrator job using the console entry points:
  - `bronze-extract` and `silver-extract` with your preferred flags.
