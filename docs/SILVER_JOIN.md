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
  right:
    path: "./docs/examples/data/silver_samples/cdc/scd_type_2/domain=retail_demo/entity=orders_cdc/v1/load_date=2025-11-13/"
  output:
    path: "./output/silver_joined"
    model: "scd_type_2"
    formats: ["parquet", "csv"]
```

The CLI reads metadata from each silver asset, concatenates the data, and writes the requested Silver model. If the desired model requires primary keys/order columns and upstream metadata lacks them, the join falls back through a deterministic order (`scd_type_2`, `full_merge_dedupe`, `scd_type_1`, `periodic_snapshot`, `incremental_merge`) until a compatible model is found.

## Running

```bash
python silver_join.py --config docs/examples/configs/silver_join_example.yaml --storage-scope onprem
```

The CLI respects the same storage metadata policy as Bronze/Silver, so if you enable `--storage-scope onprem`, only on-prem targets (non-cloud provider types) are allowed.
