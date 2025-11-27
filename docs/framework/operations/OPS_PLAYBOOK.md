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
- **Chunked Silver writes** - SilverProcessor automatically slices large Bronze partitions so you don’t need `--stream`. Use finer Bronze files or metadata to control chunk granularity (see `docs/framework/operations/legacy-streaming.md` for the retired CLI flow).
- **Schema observability stub** – `core/catalog.report_schema_snapshot` currently logs schema snapshots per Silver run. Replace it or extend it later to push to OpenMetadata so lineage/quality alerts can trigger on schema drift.

## Error Codes

Core exceptions include stable error codes to simplify triage (see `core/exceptions.py`). Examples:
- `CFG001`: configuration validation errors
- `EXT001`: extractor failures (API/DB)
- `STG001`: storage backend errors
- `AUTH001`: authentication failures
- `PAGE001`: pagination logic errors
- `STATE001`: invalid state transitions
- `QUAL001`: data quality related failures
- `RETRY001`: retry exhaustion

In logs, messages include the code prefix; alerting rules can route by code.

## Tracing (optional)

- Enable spans by setting `BRONZE_TRACING=1`. If OpenTelemetry is installed and a tracer provider is configured (e.g., OTLP endpoint), spans are emitted around API requests and Silver chunk processing.
- No tracing libs installed? The instrumentation safely no-ops.

## Rate Limiting

- API extractions can be rate limited. Configure via any of:
	- `source.api.rate_limit.rps: <float>`
	- `source.run.rate_limit_rps: <float>`
	- env `BRONZE_API_RPS=<float>`
- The limiter coordinates with retries and circuit breakers to reduce pressure on upstream APIs.

## Azure Emulator (Azurite)

- For local testing of Azure Blob Storage, run Azurite and point the platform to it via env vars. Example docker run:

	```powershell
	docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
	```

- Then set typical env vars (example):

	```powershell
	$env:AZURE_STORAGE_ACCOUNT = "devstoreaccount1"
	$env:AZURE_STORAGE_KEY = "Eby8vdM02xNOcqFeq...=="  # default azurite key
	$env:AZURE_STORAGE_ENDPOINT = "http://127.0.0.1:10000/devstoreaccount1"
	```

- Integration tests can be gated behind an env like `RUN_SMOKE=1` when targeting emulators.
