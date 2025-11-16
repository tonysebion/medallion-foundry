# Silver Join Workflow

`silver_join.py` lets you combine two existing Silver assets into a new curated dataset (e.g., to build wide tables from separate Silver views).

## Configuration

Provide a YAML file with the following structure:

```yaml
platform:             # reused to enforce storage_policy + storage_scope
  bronze:
    storage_backend: "local"
  azure_connection: {}

silver_join:
  left:
    path: "./docs/examples/data/silver_samples/full/scd_type_2/domain=retail_demo/entity=orders/v1/load_date=2025-11-13/"
    platform:
      bronze:
        storage_backend: local
  right:
    path: "./docs/examples/data/silver_samples/cdc/scd_type_2/domain=retail_demo/entity=orders_cdc/v1/load_date=2025-11-13/"
    platform:
      bronze:
        storage_backend: local
  output:
    path: "./output/silver_joined"
    model: "scd_type_2"
    formats: ["parquet", "csv"]
    join_type: "inner"
    join_keys:
      - order_id
    chunk_size: 100000
    select_columns:
      - order_id
      - status
      - total_amount
    checkpoint_dir: "./output/silver_joined/.join_progress"

Each source entry can include a `platform` block that mirrors the Bronze configuration (`platform.bronze.storage_backend`, credentials, `storage_metadata`, the optional `azure_connection` block, etc.). If a source points to remote storage, `silver_join` downloads the files to a temporary workspace before joining. The top-level `platform` block is reused for storage policy enforcement so you can still pass `--storage-scope onprem` or `--onprem-only` to guard Azure or cloud-backed paths at runtime.
```

The CLI reads metadata from each silver asset (falling back to a minimal stub if a `_metadata.json` file is not present), joins the data, and writes the requested Silver model. If the desired model requires primary keys or an order column and the upstream metadata lacks that information, the join falls back through a deterministic order (`scd_type_2`, `full_merge_dedupe`, `scd_type_1`, `periodic_snapshot`, `incremental_merge`) until a compatible model can be produced.

The `output` block controls the join semantics:

- `formats`: choose one or both of `["parquet", "csv"]`; the join writes whichever formats are enabled and records them in the resulting `_metadata.json`.
- `join_type`: controls the SQL-style merge (`inner`, `left`, `right`, `outer`); the default is `inner`.
- `join_keys`: the business keys that appear in both inputs; at least one key is required.
- `chunk_size`: optionally break the left-hand input into chunks to mitigate memory pressure before merging; set to `0` or omit to join in a single operation.
- `select_columns`/`projection`: trim the output to a specific column order after the join so downstream Silver writers stay predictable. You can include mappings (e.g., `{"source": "old_name", "alias": "new_name"}` or `{"old_name": "new_name"}`) to rename columns as part of that projection.
- If left/right inputs share a column name, the right-hand value becomes `column_right`; reference it via `right.column` or include `{"source": "right.column", "alias": "renamed"}` to stay explicit and avoid ambiguity.
- `checkpoint_dir`: override where `progress.json` is written (defaults to `<output path>/.join_progress`). The tracker records the chunk index, row counts, and sample join keys so retries can pick up where the last successful chunk ended.
- `join_key_pairs`: when the two Silver assets use different column names, map them explicitly. Each entry can be a string (matching names) or a mapping (`left`/`source` and `right`/`target`). If you omit this field, `silver_join` will try to infer the join keys from metadata/column intersections.
- `performance_profile`: one of `auto`, `chunked`, or `single`. The default `auto` derives a chunk size from the source metadata, `chunked` forces a chunked merge even for small inputs, and `single` runs the join in a single batch regardless of size. You can still override `chunk_size` manually if you want precise control.
- `join_strategy`: choose the join execution plan (`auto`, `broadcast`, `partitioned`, `hash`). Each strategy tweaks how the tool chops the left table and whether it treats the right input as partitioned or broadcast for very large datasets.
- `spill_dir`: optional directory where right-hand partitions are spilled to disk (Parquet) while joins are running; useful for debugging heavy jobs without recomputing the same partitions.
- `quality_guards`: safeguard the merged dataset with configurable checks (`row_count`, `null_ratio`, and `unique_keys`). Guard failures abort the run with a descriptive error and do not write outputs; successful guards are listed in `_metadata.json`.

If you configure `select_columns`/`projection`, the join enforces that projection after the merge – missing column names raise an error, and the columns are written in the order you list them so downstream consumers always see a predictable output schema.

If you prefer not to specify the join keys, leave `join_keys` undefined (or set it to `"auto"`) and let the tool guess from the source metadata (primary keys or shared column names). When the column names differ, use `join_key_pairs` to map each left/right column pair so the join can rename the right-hand columns before merging. The combination of `performance_profile` and `chunk_size` gives you explicit control over how aggressively the join slices the left-hand table for streaming execution.

You can still point each asset at cloud storage by reusing `platform.bronze.storage_backend`, supply the right credentials (`s3`, Azure, etc.), and add the same `storage_metadata` fields you use for Bronze. `silver_join` enforces the same storage policy, so running with `--storage-scope onprem` (or `--onprem-only`) rejects any cloud backend that does not advertise `storage_metadata.boundary=onprem` while logging which assets contributed to the new lineage.

## Running

```bash
python silver_join.py --config docs/examples/configs/silver_join_example.yaml --storage-scope onprem
```

The CLI respects the same storage metadata policy as Bronze/Silver, so if you enable `--storage-scope onprem`, only on-prem targets (non-cloud provider types) are allowed. For remote sources you can add `platform.bronze.storage_backend: "azure"` or `"s3"` plus the relevant connection settings just like you would in Bronze configs.

## Chunking & progress

`silver_join` now partitions the right-hand asset on the join keys so each chunk only touches the minimal set of rows that match the current left-hand slice. That partition-aware execution both limits duplicated scans of the right asset and makes the join safe for very large Silver sources. Each chunk writes a checkpoint (`progress.json` under the configured `checkpoint_dir`) that captures the chunk index, record count, and a sample of the join keys processed, making restarts more predictable and easier to debug.

The emitted `_metadata.json` now also includes `quality_guards`, `join_metrics`, and chunk durations so your monitoring hooks can consume them directly rather than parsing logs when performance regressions happen.

`silver_join` also aligns any shared datetime columns between the two inputs before the join runs. When both sides contain the same column name with timezone metadata, the right-hand values are converted to match the left-hand representation so you don’t need to manually cast or respecify time formats.

## Metadata & lineage

The resulting `_metadata.json` now includes several helper sections so auditors can trace every column back to its Bronze origin:

- `inputs`: metadata about each source Silver asset plus the Bronze metadata it was derived from (`bronze_path`, `bronze_metadata`, and checksum manifest references when available).
- `progress`: the latest checkpoint summary (chunks processed, rows emitted, and the stored checkpoint path).
- `join_stats`: chunk counts, how many right-hand partitions were matched, and how many right-only rows were appended.
- `joined_sources`: the paths you supplied via the config so the run record shows what produced the data.
- `join_metrics`: duration/row counts for each chunk plus right-side statistics so operations can measure performance regressions.
- `column_lineage`: a list describing every output column (its source table, the original column name, and any alias you applied) so catalog tools can follow renamed fields back to their Bronze/Silver origins.

This richer metadata makes it easier to understand when the join had to fall back to a more permissive Silver model, which chunks contributed rows, and how each Silver input maps back to its Bronze extraction.
