# Bronze Readiness Checklist

Use this checklist after your first dev extract to confirm Bronze is ready for promotion and downstream consumption.

1. **Run Mode & Safe Range**
   - Launch `python bronze_extract.py --config docs/examples/configs/owner_intent_template.yaml --date 2025-11-13 --dry-run` to preview paths and metadata before writing files.
   - For a first dev run, add `--load-pattern full` or `--load-pattern cdc` and keep the dataset narrow (use `run_mode: dev_test` or `test_range_days: 1` in your intent YAML to document the intent) so only a small slice of data lands.

2. **Inspect the Bronze layout**
   - Check that the folders follow `env=<environment>/system=<system>/table=<entity>/pattern=<pattern>/dt=YYYY-MM-DD/`.
   - Confirm each file format, chunk count, and `_metadata.json`/`_checksums.json` exist inside the partition.
   - Verify the `owner_team`/`owner_contact` fields appear in `_metadata.json`.
   - If you used `owner_intent_template.yaml`, rerun `python scripts/expand_owner_intent.py` to regenerate the resolved version after making edits so the checklist reflects the latest folder inference.

3. **Validate record counts and keys**
   - Run `python -c "import pandas as pd; print(len(pd.read_parquet('...')))"` or use `datasets`/`pyarrow` to confirm counts match expectation.
   - Ensure the natural key columns you declared (`natural_keys`, timestamps) are present and populated.

4. **Declare readiness**
   - If everything aligns, mark the dataset ready by adding a human-readable note such as `bronze_status: ready` in your intent YAML or metadata comment. This flag is informational and can feed OpenMetadata or alerting hooks.
   - Document the checked items in your ops/handbook so downstream teams can re-run the same checklist.

5. **Alert & observe**
   - Share the `_checksums.json`/`_metadata.json` paths with monitoring/alerting tools so they can fail fast if Bronze changes.
   - Place a simple ownership note (team contact, domain) in README/docs so observability systems know who to call when anomalies appear.
