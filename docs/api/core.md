# Core API Reference

This reference highlights the primary public modules and classes available under the `core` package. Use these building blocks when you need to extend the framework or integrate it with other tooling.

## core.context
- `RunContext` (dataclass) captures the config, date, paths, and dataset identifiers that every Bronze run carries.
- `build_run_context(...)` builds a fully resolved `RunContext` from a config + run date so extractors and writers share the same metadata.
- `dump_run_context` / `load_run_context` serialize the context for external workflows (e.g., Silver promotions).

## core.runner
- `run_extract(context: RunContext)` orchestrates the extract lifecycle: extractor → chunking → file writes → metadata reporting.
- `ExtractJob` exposes the chunk writer/storage flow if you need to customize chunk processing or reporting hooks.
- `build_extractor(cfg)` selects the correct extractor implementation based on `source.type`.

## core.runner.chunks
- `ChunkWriter`/`ChunkProcessor` write CSV/Parquet artifacts and optionally push each file to a configured storage backend through `core.infrastructure.io.storage.plan.StoragePlan`.
- `StoragePlan` encapsulates remote upload paths and the associated backend (see `core.infrastructure.io.storage.plan`).

## core.bronze
- Utility functions such as `build_chunk_writer_config`, `compute_output_formats`, and `resolve_load_pattern` centralize Bronze-specific decisions (load pattern, compression, formats).

## core.config
- `load_config` / `load_configs` parse YAML, validate structure, and normalize Silver/run fields via the typed models in `core.config.models`.
- `build_relative_path(cfg, run_date)` resolves the Bronze directory layout (`system=…/table=…/dt=…`) so CLI and Silver can stay in sync.

## core.storage
- `get_storage_backend(platform_cfg)` boots the registered backend for S3/Azure/local storage, caching instances per config.
- `StoragePlan`/`StorageBackend` abstractions live under `core.infrastructure.io.storage` for cross-backend uploads.
- Storage metadata helpers (`validate_storage_metadata`, `enforce_storage_scope`) guard on-premise policies.

## core.logging_config
- `setup_logging(...)` configures human/JSON/simple formatters, optional file handlers, and environment-aware log levels.
- `get_logger(...)` returns a logger adapter with contextual fields when needed.

## core.parallel
- `run_parallel_extracts(contexts, max_workers)` runs multiple configs concurrently via `ThreadPoolExecutor`, reports statuses, and preserves error contexts.

Refer to the source (e.g., `core/runner/job.py`, `core/storage/backend.py`, `core/io.py`) for detailed signatures if you extend the framework.
