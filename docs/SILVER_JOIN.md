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

Each source entry can include a `platform` block that mirrors the Bronze configuration (`platform.bronze.storage_backend`, credentials, `storage_metadata`, the optional `azure_connection` block, etc.). If a source points to remote storage, `silver_join` downloads the files to a temporary workspace before joining. The top-level `platform` block is reused for storage policy enforcement so you can still pass `--storage-scope onprem` or `--onprem-only` to guard Azure or cloud-backed paths at runtime.
```

The CLI reads metadata from each silver asset (falling back to a minimal stub if a `_metadata.json` file is not present), joins the data, and writes the requested Silver model. If the desired model requires primary keys or an order column and the upstream metadata lacks that information, the join falls back through a deterministic order (`scd_type_2`, `full_merge_dedupe`, `scd_type_1`, `periodic_snapshot`, `incremental_merge`) until a compatible model can be produced.

The `output` block controls the join semantics:

- `formats`: choose one or both of `["parquet", "csv"]`; the join writes whichever formats are enabled and records them in the resulting `_metadata.json`.
- `join_type`: controls the SQL-style merge (`inner`, `left`, `right`, `outer`); the default is `inner`.
- `join_keys`: the business keys that appear in both inputs; at least one key is required.
- `chunk_size`: optionally break the left-hand input into chunks to mitigate memory pressure before merging; set to `0` or omit to join in a single operation.
- `select_columns`/`projection`: trim the output to a specific column order after the join so downstream Silver writers stay predictable.

You can still point each asset at cloud storage by reusing `platform.bronze.storage_backend`, supply the right credentials (`s3`, Azure, etc.), and add the same `storage_metadata` fields you use for Bronze. `silver_join` enforces the same storage policy, so running with `--storage-scope onprem` (or `--onprem-only`) rejects any cloud backend that does not advertise `storage_metadata.boundary=onprem` while logging which assets contributed to the new lineage.

## Running

```bash
python silver_join.py --config docs/examples/configs/silver_join_example.yaml --storage-scope onprem
```

The CLI respects the same storage metadata policy as Bronze/Silver, so if you enable `--storage-scope onprem`, only on-prem targets (non-cloud provider types) are allowed. For remote sources you can add `platform.bronze.storage_backend: "azure"` or `"s3"` plus the relevant connection settings just like you would in Bronze configs.
