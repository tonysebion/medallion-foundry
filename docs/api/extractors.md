# Extractors API Reference

The `core.extractors` package exposes pluggable extractors that turn a config into JSON-like records plus an optional cursor. Use this guide when adding new source types or implementing custom authentication/pagination logic.

## core.extractors.base
- `BaseExtractor` defines `fetch_records(cfg, run_date)` which must return `(records: List[Dict[str, Any]], new_cursor: Optional[str])`.
- Implementors simply focus on reading upstream data and leave Bronze writing, chunking, and storage to the runner.

## core.extractors.api_extractor
- Built-in REST extraction with bearer/API key/basic auth plus retry and pagination.
- Supports offset/page/cursor-based pagination via `source.api.pagination`.
- Automatically derives bearings such as `cursor_field` for incremental loads and honors `source.api.headers`.

## core.extractors.db_extractor
- Runs SQL queries against SQL Server/PostgreSQL/MySQL via ODBC.
- Handles incremental state files, parameterized batching, and automatic `WHERE` injection for delta loads.
- Returns cursor/state information that `core.runner` persists in metadata.

## core.extractors.file_extractor
- Reads local CSV/TSV/JSON/JSONL/Parquet datasets (great for demos, tests, or offline validation).
- `source.file` controls path, format, column projection, and `limit_rows`.
- Reuses the same chunking/storage logic so Bronze data is identical between API/DB/file sources.

## Custom extractors
- Set `source.type: custom` with `source.custom_extractor.module`/`class_name` (must live on `PYTHONPATH`).
- Your class should inherit from `BaseExtractor` and can rely on the same `fetch_records` signature.

Keep the extractor module under `core.extractors` or in `docs/examples/custom_extractors` for inspiration when building plugin-specific integrations.
