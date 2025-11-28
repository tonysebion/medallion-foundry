# Operations & Governance

This quick reference highlights the key operational playbooks for running Bronze/Silver pipelines safely and consistently.

## Validation & configuration

- Every CLI run starts by loading `config/*.yaml` via `core/config.py`, which enforces storage metadata, Silver models/profiles, Bronze load patterns, and backend selection.
- Use `python bronze_extract.py --config ... --validate-only` or `--dry-run` to validate configs without writing data.
- `tests/test_config.py` exercises the parsing logic; add new fields (e.g., storage metadata) there when you need additional validation rules.

## Sample generation

- Regenerate Bronze raw data with `python scripts/generate_sample_data.py`. Use `python scripts/generate_silver_samples.py --formats both` to produce curated Silver artifacts for every Bronze pattern + Silver model.
- The new `tests/test_silver_samples_generation.py` now replays this generation during the test suite so the samples never drift. When you change extraction logic, rerun the script to keep `sampledata/silver_samples` (and the derived artifacts) in sync.
- Each sample folder contains a `README.md` describing the pattern/model and the recommended regeneration command.

## Storage governance

- Storage metadata (`platform.bronze.storage_metadata`) must classify every backend as `boundary: onprem|cloud`, `provider_type`, and `cloud_provider`.
- Run either CLI (Bronze or Silver) with `--storage-scope onprem` (or the alias `--onprem-only`) to enforce that only on-prem targets are permitted; Azure/cloud backends will fail fast otherwise.
- These checks keep “local S3 endpoints” separate from cloud providers and serve as a documented alternative to hostname heuristics.

## Observability

- Logs already include Bronze/Silver metrics such as record/partition counts and runtime/delta sizes; see `silver_extract.py` for the metadata payload.
- For long-running pipelines, rotate logs via `logging` configuration (`core/logging_config.py`) and monitor `core/catalog` hooks for dataset/lineage events.

## Extendability

- Add new storage providers by registering them in `core/storage/plugin_factories.py` (or anywhere that imports `core/storage/registry.py::register_backend`).
- Silver models/profiles live in `core/silver_models.py`; update `MODEL_PROFILES` when you want new named behaviors and document them in `docs/framework/reference/CONFIG_REFERENCE.md`.
