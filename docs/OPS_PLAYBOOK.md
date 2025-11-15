# Ops Playbook

This guide captures the day-two questions platform/ops teams usually ask when onboarding new medallion jobs.

## Daily Operations

- **Bronze runs** – the `bronze_extract.py` CLI is idempotent per config+date; rerunning the same tuple will overwrite the existing Bronze partition. Use `--dry-run` to smoke test configs without writing files.
- **Silver promotions** – `silver_extract.py` reads a single Bronze partition (detected from metadata or CLI flags). `--dry-run` validates inputs and checksum requirements without materializing outputs.
- **Parallelism** – Bronze can fan out across configs via `--parallel-workers`; per-config chunking uses `source.run.parallel_workers`. Monitor CPU/disk before raising workers.

## Monitoring & Alerting

- **Webhooks** – Both CLIs accept `--on-success-webhook` / `--on-failure-webhook` (repeatable). Each URL receives a JSON payload describing the layer, config names, run date, and status. Use these hooks to trigger chat alerts or pipeline orchestrators.
- **Catalog notifications** – Each run fires a placeholder `notify_catalog` event (`core/catalog.py`). Replace this with a call into your data catalog (e.g., OpenMetadata) to capture lineage, run metadata, and ownership changes.
- **Logs** – Structured logs go to stdout; configure your scheduler or container runtime to collect them. Fatal traces bubble up to the CLI exit code.

## Checksum & Metadata Artifacts

- Bronze partitions always emit `_metadata.json` and `_checksums.json`.
- Silver partitions emit `_metadata.json` plus `_checksums.json` summarising schema snapshot, record counts, and artifact hashes. Gold consumers should validate this manifest before ingestion.

## Secrets & Credentials

- Bronze configs reference env vars for API keys/connection strings. Inject secrets via your scheduler, `.env` files, or secret managers; never commit real credentials.
- Storage backends pull credentials from standard env vars (see `docs/STORAGE_BACKEND_ARCHITECTURE.md`).

## Disaster Recovery

- Bronze/ Silver directories are deterministic (`system/table/pattern/dt`). Re-running the same config+date is the simplest recovery path.
- Keep `_metadata.json` and `_checksums.json` under version control or replicate them to your catalog in case partitions are purged.

## Extending the platform

- **Catalog integration** – Implement `core/catalog.notify_catalog` to create/update datasets, lineage, or run records in OpenMetadata.
- **Custom hooks** – Webhooks post JSON bodies; build lightweight relay services if your alerting system needs auth or retries.
- **Data quality** – Silver configs support declarative normalization and PK enforcement. Leverage OpenMetadata or a dedicated expectation engine for richer checks, and add fail-open/fail-close policies via `silver.error_handling`.
- **Schema observability stub** – `core/catalog.report_schema_snapshot` currently logs schema snapshots per Silver run. Replace it or extend it later to push to OpenMetadata so lineage/quality alerts can trigger on schema drift.
