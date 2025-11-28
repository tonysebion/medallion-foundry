# Bronze + Silver Guidance

This guide highlights when to run each Bronze extraction pattern and which Silver asset model makes sense for the downstream workload. Bronze and Silver have overlapping requirements (they both care about load patterns, latency, and integrity) but they also optimize for different things, so configure them independently when appropriate.

## Bronze extraction patterns (input)

| Pattern | When to use | Key trade-offs |
| --- | --- | --- |
| `full` | Periodic refreshes (daily/weekly) when the entire dataset is small enough to rewrite, or you need a clean snapshot for archives. | Simple to reason about and easy to validate, but becomes expensive as data grows; downstream Silver and Gold consumers read the latest snapshot. |
| `cdc` | High-volume feeds where only deltas change (inserts/updates/deletes) such as transaction tables. | Bronze stores smaller files, and metadata indicates the change set. Bronze must still preserve ordering and dedupe as needed. |
| `current_history` | SCD Type 2–style histories that split current + historical rows for every business key. | Bronze retains both sides of the business key; Silver can build the current view while keeping the rest of the history. Requires primary keys + order column. |

### Factors to weigh for Bronze config

- **Data volume**: Full snapshots scale linearly—choose CDC when throughput or file counts become unwieldy.
- **Change rate**: If only a few rows change between runs, incremental/CDC drastically reduces storage and processing time.
- **Schema stability**: When columns shift frequently, full snapshots keep Silver simple; CDC/current_history must gracefully handle new columns via Silver schema options.
- **Run latency**: Chunking, parallel workers, and smaller Bronze chunks keep runtime bounded, but SilverProcessor now handles the chunking automatically. For high-throughput APIs, enable async HTTP extraction with prefetch pagination.
- **Failure tolerance**: Bronze already writes `_checksums.json`; Silver can opt into `require_checksum` to skip promotions when Bronze integrity fails.
- **Edge-case coverage**: The Sample generator (`scripts/generate_sample_data.py`) now emits schema variations, null business keys, and skewed batches so Bronze and Silver tests continually surface new corner cases; regenerate the fixtures before onboarding new extracts.
- **Sample nav**: Each pattern/model directory under `sampledata/silver_samples/` now includes a `README.md` describing the behavior and linking back to the config/docs so users can quickly find the right artifact without digging through filenames. The upstream `sampledata/source_samples/<pattern>` tree mirrors this cadence with daily files spanning multiple weeks, making it easy to understand the expected Bronze inputs day-to-day for each pattern.

## Silver promotion assets (output)

The Silver stage works with the Bronze artifacts and lets you select **five curated models** regardless of the Bronze pattern:

| Silver model | Description | Suggested scenario |
| --- | --- | --- |
| `periodic_snapshot` | Mirror the Bronze snapshot (full dataset). | Use when downstream teams expect a one-to-one dump of the Bronze partition (e.g., nightly refresh trading off duplication for simplicity). Merits small Bronze (full) datasets. |
| `incremental_merge` | Emit the raw CDC/timestamp chunk so merge targets can apply it. | Ideal when Bronze uses CDC or timestamped deltas and downstream needs to apply only the change set (e.g., merge into a central table). |
| `full_merge_dedupe` | Deduplicate using configured PKs/order column; produce a clean snapshot for full-merge processes. | Handy when Silver itself drives `MERGE` statements or analytical joins but you still prefer periodic snapshots. Works with both full and CDC Bronze sources. |
| `scd_type_1` | Keep only the latest row for each business key (SCD Type 1). | When consumers only want the current representation (e.g., query tables) and you can tolerate dropped history. Requires PK+order column. |
| `scd_type_2` | Build both the current view and the historical timeline, annotating an `is_current` flag. | Use when you must audit changes over time (current + history split). Works best behind Bronze `current_history` so the history artifacts already include past/active rows. |

### Silver factors

- **Data size**: Writing full snapshots of large Bronze partitions can be heavy; consider `scd_type_2` or `incremental_merge` to keep downstream materializations tight and partitioned.
- **Change footprint**: For small changes over huge bases (e.g., customer tables), `scd_type_1`/`scd_type_2` with inference via `order_column` prevents reprocessing everything.
- **Consumption latency**: SilverProcessor automatically chunks large Bronze partitions and metadata/checksums keep reruns safe, so you no longer need `--stream` or `--resume`. Focus on Bronze file sizing and rely on `_metadata.json` for detecting reruns.
- **Output formats**: Choose Parquet for analytics; enable CSV when errors must be human-readable or Silver artifacts feed legacy tooling. The new `scripts/generate_silver_samples.py --formats both` helps you exercise both writers.
- **Hybrid/reference mode**: For large data sets combine a periodic `reference` full snapshot with incremental/CDC runs. Set `source.run.reference_mode`:
  ```yaml
  source:
    run:
      reference_mode:
        enabled: true
        role: reference      # or `delta` when running the smaller delta job
        cadence_days: 7
        delta_patterns: ["cdc", "incremental_merge"]
        delta_mode: cumulative  # or point_in_time
  ```
  The Bronze run annotates `_metadata.json` with the reference snapshot path + cadence so Silver jobs can automatically pick up the most recent baseline before merging the deltas.
- When a delta file arrives on the same day as a new reference snapshot, stay on that delta for the current ingestion run so you see the latest changes; the next delta (day+1) is the first one that can move to the newly landed reference. Our metadata (`reference_mode.run_date`, `reference_mode.reference_run_date`) makes it unambiguous whether a Silver join should favor the delta over the fresh full for the same calendar day.
- Because we now keep both point-in-time and cumulative delta feeds, the metadata also includes `delta_mode` so you can declare whether you want to replay every change (cumulative) or just the most recent change (point-in-time); when a new reference arrives mid-cycle, the delta metadata keeps pointing back to the previous reference until the next delta after the reference takes over.
- **Error handling**: Tune `silver.error_handling` (enabled/max_bad_records/max_bad_percent) when data has nullable keys or occasional corrupt rows to avoid electing total job failure.
- **Partitioning differences**: Bronze typically partitions by `dt`/`pattern`; Silver can add business-level partitions (e.g., `status`, `region`) even when Bronze stays coarse.
