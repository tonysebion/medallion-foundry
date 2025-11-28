# Pattern Config Guide

These YAML files under `docs/examples/configs/patterns/` demonstrate how Bronze/Silver intent configs connect to the 7 sample sources. Each config follows the same basic sections:

1. **Top-level metadata**
   - `environment`, `domain`, `system`, and `entity` describe the medallion intent.
   - `system`/`entity` must match the `sampledata/source_samples/<pattern>/system=.../table=...` folders.

2. **`bronze` section**
   - `enabled`: toggle Bronze extraction for the pattern.
   - `source_type`: `"file"` for these samples.
   - `path_pattern`: points to one `dt=YYYY-MM-DD` snapshot under `sampledata/source_samples`.
   - `partition_column`: matches the file naming (e.g., `run_date` or `load_date`).
   - `options`:
     * `format`: input format (CSV by default).
     * `load_pattern`: tells Bronze/Silver whether the data is `full`, `cdc`, or `current_history`.
     * `pattern_folder`: **overrides** the Bronze output path so every pattern writes to `pattern=<pattern_folder>`; this should mirror the source folder name (pattern1_full_events, pattern2_cdc_events, etc.).
     * Additional pattern-specific knobs like `delta_mode`, `delta_tag`, and `reference_mode` describe how hybrid data is stitched together.

3. **`silver` section**
   - `enabled`, `entity_kind`, `input_mode`, and `schema_mode` drive the Silver processor behavior.
   - `natural_keys`, `event_ts_column` / `change_ts_column`, and `partition_by` describe Silver semantics.
   - `write_parquet`/`write_csv` control which artifact formats Silver emits; these remain on by default.

4. **Pattern-specific notes**
   - `pattern1_full_events` and `pattern2_cdc_events` cover the basic full/CDC cases.
   - `pattern3_scd_state` adds SCD state behavior through `history_mode: scd2`.
   - Patterns 4–7 showcase hybrid references, incremental merges, and cumulative deltas; their `options.pattern_folder` entries keep Bronze writes aligned with those names.

Use this guide as a reference when adding new pattern configs or tuning the sample runs.
