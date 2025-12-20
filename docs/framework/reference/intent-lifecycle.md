# Intent Lifecycle Flow

1. **Generate or locate Bronze samples**
- Run `python scripts/generate_bronze_samples.py --all` to populate `sampledata/bronze_samples`.
   - When starting from the owner intent template, run `python scripts/expand_owner_intent.py --config docs/examples/configs/templates/owner_intent_template.yaml` to see which folders the intent resolves to before you run Bronze.
   - Each intent YAML (e.g., `file_complex.yaml`) points its `bronze.path_pattern` or `source.file.path` into one of those folders, so Bronze always reads the same layout (`system=<system>/table=<entity>/pattern=<pattern>/dt=YYYY-MM-DD/…`).

2. **Run Bronze extraction**
   - Invoke `python bronze_extract.py --config docs/examples/configs/<config>.yaml --date YYYY-MM-DD`.
   - The run writes to `output/env=<environment>/system=<system>/table=<entity>/pattern=<pattern>/dt=…/…` (use `env=` only when the config defines `environment`).
   - Bronze writes raw files to `output/system=<system>/table=<entity>/pattern=<pattern>/dt=.../` and emits `_metadata.json`/`_checksums.json` for lineage and safety.

3. **Run Silver promotion**
   - Use the same YAML to promote: `python silver_extract.py --config docs/examples/configs/<config>.yaml --date YYYY-MM-DD`.
   - Silver writes to `silver/env=<environment>/domain=<domain>/entity=<entity>/v<version>/<load_partition>=<date>/…`, mirroring the environment-aware Bronze root.
   - Silver reads the Bronze partition, cleans/normalizes it based on `entity_kind`, `history_mode`, and `partition_by`, and writes to `silver/<env?>/domain=<domain>/entity=<entity>/v<version>/<load_partition>=<date>/…`.
   - Metadata columns (`load_batch_id`, `record_source`, `bronze_owner`, `silver_owner`, etc.) accompany every row, and `write_batch_metadata` + `write_checksum_manifest` record metrics/logging for observability.

4. **Iterate or rerun safely**
   - Change the intent YAML (adjust natural keys, owner info, partition_by, etc.), rerun Bronze, and then rerun Silver to see the updated outputs—nothing else changes, so the pipeline remains calm and predictable.
   - Use `tests/test_integration_samples.py` as a script template to loop over multiple configs/ dates if you want to treat this as a regression harness.
   - After your first dev extract, continue with `docs/usage/onboarding/bronze_readiness_checklist.md` so you can inspect file paths, metadata, and counts before signaling Bronze is ready for Silver.

Append this file to your reading list whenever someone asks “what exactly happens when I run Bronze then Silver?”—it matches the new configuration format and manifest expectations.
