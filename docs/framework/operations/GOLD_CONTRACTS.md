# Gold Contracts

These notes capture the minimum information that Gold (downstream) teams need to understand and trust Silver assets before they are consumed.

## Schema contract
- **Columns** – list the column names and their data types (string, numeric, timestamp). When Silver renames or reorders columns, update this section to reflect the canonical Silver view.
- **Primary key(s)** – Silver already enforces `primary_keys` by default; repeat them here so analysts know which columns uniquely identify each row.
- **Nullable columns** – highlight columns that may be `null` so Gold consumers can guard against unexpected `None` values.
- **Timestamps / offsets** – document how to identify the “latest” record (e.g., `load_date`, `updated_at`, `event_ts`), especially for CDC or current/history patterns.

## Freshness & lineage
- **Bronze partition** – include the Bronze path (system.table/pattern/load partition) that produced this Silver run.
- **Run metadata** – reference the Silver `_checksums.json` / `_metadata.json` files so downstream jobs can verify record counts, chunk counts, and load patterns.
- **Lineage** – mention any upstream dependencies (other Silver artifacts that feed this dataset) or downstream consumers that rely on it.

## Quality expectations
- **Error handling policy** – describe how `silver.error_handling` is configured: what happens when PKs are missing, how many bad rows are tolerated, etc.
- **Notable measures** – include any derived data-quality metrics (duplicate rate, null percentages, checksum status) that Gold teams should monitor.

## Consumption checklist
- ✅ Inspect `_checksums.json` before joining the Silver tables.
- ✅ Validate schema snapshot (e.g., column list/types) before using it in production.
- ✅ Re-run the webhook/catalog hook or `silver_extract.py --dry-run` when upstream configs change.
