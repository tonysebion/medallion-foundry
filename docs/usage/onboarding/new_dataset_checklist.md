# New Dataset Checklist – Medallion Foundry

Use this checklist to onboard a dataset into the Bronze → Silver pipeline. The goal is that a semantic owner or source system expert can answer these questions and write a single config file. No code changes should be required.

## 1. Basic identity

1. **System name** (source):
   - e.g., `costpoint`, `servicenow`, `facets`, `crm`, `salesapp`

2. **Entity name** (logical object):
   - e.g., `gl_detail`, `project`, `employee`, `invoice_lines`, `ticket`

3. **Environment** (optional):
   - `environment: dev | test | prod`
   - Setting this ensures Bronze/Silver outputs live under `env=<environment>` so different tiers stay separated.

4. **Intent template** (Step 1 hand-holding):
   - Start from `docs/examples/configs/owner_intent_template.yaml` when you need a concrete example of how a source owner can describe systems/entities in plain language.
   - Run `python scripts/expand_owner_intent.py --config docs/examples/configs/owner_intent_template.yaml` to produce `owner_intent_template.resolved.yaml` plus a short checklist of the inferred Bronze/Silver folders.
   - Copy the template, replace `system`, `entity`, and `bronze.path_pattern` with your own source values, and let the framework infer folders.

4. **Domain** (optional):
   - `domain: finance | claims | hr | it | ...`

## 2. Is this an EVENT or a STATE?

5. Answer: is this dataset about **events** (transactions, status changes) or **states** (current attributes)?
   - `silver.entity_kind: event` for event feeds.
   - `silver.entity_kind: state` for snapshot/state tables.
   - `silver.entity_kind: derived_state` when source emits events but you want a state table.
   - `silver.entity_kind: derived_event` when source emits snapshots but you need change events.

## 3. How much history do you need? (`history_mode`)

Applies to `entity_kind: state` or `derived_state`.

6. Do you need “what was true at a point in time” answers?
   - **Yes** → `silver.history_mode: scd2`
   - **No, current value only** → `silver.history_mode: scd1`
   - **Tiny lookup, latest only** → `silver.history_mode: latest_only`

Tip: If unsure, start with `scd2`. You can simplify later.

## 4. How does the Bronze feed behave? (`input_mode`)

Applies to `entity_kind: event` or `derived_event`.

7. Does the source send only new events or complete slices?
   - New rows only → `silver.input_mode: append_log`
   - Full daily slice → `silver.input_mode: replace_daily`

## 5. Keys and timestamps

8. What columns uniquely identify a record?
   - List them under `silver.natural_keys`.

9. Which column tells you **when the data happened or changed**?
   - For events: `silver.event_ts_column`
   - For states: `silver.change_ts_column`
   - If no change timestamp exists yet, you can start with a load time proxy.

## 6. Attributes to keep

10. Which columns matter for analysis or joins?  
    - Add them to `silver.attributes`. Keys and timestamps are specified separately.

11. **Safe-first dev run controls**
    - Document your intent with optional hints such as `run_mode: dev_test` or `test_range_days: 1` (purely declarative for humans) so it’s obvious when you are running a narrow slice.
    - Aim for a single partition (day or test identifier) when you run the first Bronze extract to keep the work small and reversible.
    - Verify the planned paths before writing anything by running:

      ```bash
      python bronze_extract.py --config docs/examples/configs/owner_intent_template.yaml --date 2025-11-13 --dry-run
      ```

      This prints the Bronze/Silver targets without creating files and confirms that your checklist and template point to the right folders.

## 7. Optional advanced controls

11. **Deletions** – what if a key disappears or is flagged deleted?
    - `silver.delete_mode: ignore` (default)
    - `silver.delete_mode: tombstone_state`
    - `silver.delete_mode: tombstone_event`

12. **Schema strictness** – how strict is Silver about schema drift?
    - `silver.schema_mode: strict` (fail on schema changes)
    - `silver.schema_mode: allow_new_columns` (permit new columns unless explicitly listed)

13. **Partitioning** – partition Silver output for performance.
    - `silver.partition_by: [dt, effective_from_dt, ...]`
    - Default behavior uses reasonable partitions derived from time columns if omitted.

14. **Ownership metadata**
    - `bronze.owner_team` + `bronze.owner_contact` describe who owns the raw partition.
    - `silver.semantic_owner` + `silver.semantic_contact` capture the semantic owner of the cleaned view.
    - These propagate into metadata columns (`bronze_owner`, `silver_owner`) and manifest extras automatically.

## 8. Skeleton config

```yaml
environment: prod
domain: finance

system: costpoint
entity: project

bronze:
  enabled: true
  source_type: db
  connection_name: COSTPOINT_PROD
  source_query: |
    SELECT
      project_id,
      project_name,
      org_id,
      status,
      start_date,
      end_date,
      last_updated
    FROM project
    WHERE last_updated >= :since
  partition_column: extract_date

silver:
  enabled: true

  entity_kind: state          # event | state | derived_state | derived_event
  history_mode: scd2          # scd2 | scd1 | latest_only
  input_mode: append_log      # for events/derived_event; ignored for pure state
  delete_mode: ignore         # advanced; default is ignore
  schema_mode: strict         # optional guardrail

  natural_keys:
    - project_id

  change_ts_column: last_updated

  attributes:
    - project_name
    - org_id
    - status
    - start_date
    - end_date

  partition_by:
    - effective_from_dt
```

Once this config is filled in, Bronze + Silver can run as a paired pipeline without changing code.
