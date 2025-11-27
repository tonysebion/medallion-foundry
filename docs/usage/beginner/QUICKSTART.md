# Quick Start Guide

Start here if you are new to Bronze/Silver and want to build confidence by landing example data before wiring up your own source feed.

## 1. Finish the shared setup guide

Complete [`docs/setup/INSTALLATION.md`](../../setup/INSTALLATION.md) first. That document proves cloning, the virtual environment, and all dependencies are ready so the quickstart can concentrate on Bronze/Silver concepts.

## 2. Generate sample data

We keep realistic CSV snapshots over multiple weeks under `sampledata/source_samples/`. Run the helper script once to populate that folder so each pattern named in `docs/usage/patterns/pattern_matrix.md` has a matching directory, and each directory captures daily loads that visualize how the Bronze files shift across weeks.

```powershell
python scripts/generate_sample_data.py
```

This writes

- `sampledata/source_samples/` with CSV input for each pattern; this is the single canonical store used by the docs, tests, and quickstarts.

You can also copy the generated Bronze files into a dedicated `sampledata/` folder if you want to experiment outside the repo tree; just point your config at that path.

## 3. Create your quickstart config

```powershell
mkdir -ErrorAction SilentlyContinue config
copy docs\examples\quick_test.yaml config\test.yaml
```

Edit `config\test.yaml`:

- Set `environment`, `domain`, `system`, and `entity` to describe your dataset.  
- Leave the bronze `path_pattern` and silver defaults untouched while you learn—they are already wired to the generated sample data.  
- Update the auth block near line 30 to match your API (bearer token, API key header, or basic auth) and export the required environment variables.
- Adjust pagination only if your source actually pages.

Save the file and keep it nearby for the run.

## 4. Set secrets and optional overrides

In PowerShell (or your shell) set the credentials referenced by the config:

```powershell
$env:MY_API_TOKEN="your-token-here"
```

If you are testing a database config, set `DB_CONN_STR` or other env vars as named in the YAML.

## 5. Run Bronze (+ optional Silver) against sample data

```powershell
python bronze_extract.py --config config\test.yaml --date 2025-11-13 --dry-run
python bronze_extract.py --config config\test.yaml --date 2025-11-13
python silver_extract.py --config config\test.yaml --date 2025-11-13
```

The dry run verifies paths without writing data. The subsequent runs produce Bronze files under `output/system=<system>/table=<table>/dt=2025-11-13/` and Silver artifacts under `silver_output/domain=<domain>`.

## 6. Inspect Bronze + Silver

- Confirm `output/.../dt=2025-11-13/` contains `part-0001.csv`/`.parquet` and `_metadata.json`.  
- Open `output/.../_metadata.json` to see `run_date`, `load_pattern`, and `relative_path` recorded.  
- Use `silver_output/.../events.parquet` (or the `state_*` files for `current_history`) to verify Silver matches the pattern matrix expectations.

## 7. Troubleshooting

- Missing modules or env vars? Rerun `docs/setup/INSTALLATION.md` to reinstall Python/dependencies.  
- “No data returned”? Validate your endpoint/auth in Postman before rerunning Bronze.  
- Permission errors? Ensure `output/` and `silver_output/` are writable in your shell.

## 8. What’s next

- Run `tests/test_usage_flow.py` to continuously exercise this quickstart path and keep the docs accurate.  
- Advance to [`docs/usage/onboarding/intent-owner-guide.md`](../onboarding/intent-owner-guide.md) to define real datasets, ownership, and metadata.  
- Explore `docs/usage/patterns/pattern_matrix.md` plus `docs/examples/configs/pattern_*.yaml` for more refined Bronze/Silver patterns.
- Automate the sample run: execute `python sampledata/processes/run_pattern_bronze.py` to rebuild the bronze outputs from each pattern config and observe how Bronze handles the daily source files described in the pattern matrix.
