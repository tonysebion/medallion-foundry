

---

### 1.2 `docs/usage/onboarding/new_dataset_checklist.md`

```markdown
# New Dataset Checklist – Medallion Foundry

Use this checklist when onboarding a new dataset into Bronze + Silver.

The goal is that a source system owner or semantic owner can fill this out and then create a config file. No new pipeline code should be required.

---

## 1. Basic identity

1. **System name** (source):

   - e.g., `costpoint`, `servicenow`, `facets`, `crm`, `salesapp`

2. **Entity name** (logical object):

   - e.g., `gl_detail`, `project`, `employee`, `invoice_lines`, `ticket`

3. (Optional) **Environment**:

   - `environment: dev | test | prod`

4. (Optional) **Domain**:

   - `domain: finance | claims | hr | it | ...`

---

## 2. Is this an EVENT or a STATE?

5. Is this dataset primarily about **events** or **states**?

- If it’s about discrete happenings (transactions, postings, status changes):
  - `silver.entity_kind: event`

- If it’s about attributes of something over time (employee, project, vendor, org structure):
  - `silver.entity_kind: state`

- If the source only emits events but you want a stateful table:
  - `silver.entity_kind: derived_state`

- If the source only emits snapshots but you want change events:
  - `silver.entity_kind: derived_event`

---

## 3. How much history do you need? (`history_mode`)

Applies when `entity_kind` is `state` or `derived_state`.

6. Do you need to answer “what was true at a specific time?”  

- **Yes** → `silver.history_mode: scd2`

- **No, I only need the current values** → `silver.history_mode: scd1`

- **This is a tiny lookup and I only care about the latest** → `silver.history_mode: latest_only`

If you don’t know, start with `scd2`. You can simplify later.

---

## 4. How does the Bronze feed behave? (`input_mode`)

Applies when `entity_kind` is `event` or `derived_event`.

7. How does the source send events?

- “Each run only sends **new events**”:
  - `silver.input_mode: append_log`

- “Each run sends a **complete slice** (e.g., all events for a date)”:
  - `silver.input_mode: replace_daily`

If unsure, ask the source system owner or check the extract query/files.

---

## 5. Keys and timestamps

8. What columns uniquely identify a record?

- Add them to `silver.natural_keys`.

9. Which columns represent time?

- For events: the column that says **when the event occurred**:
  - `silver.event_ts_column`

- For state: the column that says **when the record was last changed**:
  - `silver.change_ts_column`

If you don’t have a good change timestamp for state, you can initially use the load time as a proxy.

---

## 6. Attributes to keep

10. Which columns are important for analysis or joins in Gold?

- Add them to `silver.attributes`.

You don’t have to list keys or timestamps here—they are specified separately.

---

## 7. Optional advanced controls

11. **Deletions** (`delete_mode`)

- What should happen if a record:
  - disappears from a snapshot, or
  - is marked as deleted?
- Start with default behavior; consider advanced options later (`ignore`, `tombstone_state`, `tombstone_event`).

12. **Schema strictness** (`schema_mode`)

- Do you want the job to fail if the schema changes unexpectedly?
  - `strict`: fail on unexpected schema changes.
  - `allow_new_columns`: allow new columns but ignore them unless added to `attributes`.

13. **Partitioning** (`partition_by`)

- For large tables, what column should we partition by in Silver?
  - Typically a date derived from `event_ts_column` or `effective_from`.

If omitted, the system will use reasonable defaults.

---

## 8. Skeleton config

A typical full config might look like:

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
  delete_mode: ignore         # advanced; optional
  schema_mode: strict         # optional

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
    - effective_from_dt       # or dt, depending on implementation


Once this is filled in, Bronze + Silver can run as a pair for this dataset.



---

## 2. Unified agent-mode prompt for Codex / Copilot

Here’s the **single** mega-prompt you can paste into Copilot Chat / Codex in “agent” mode.

It tells it to:

- analyze your current Bronze/Silver pair  
- add these docs  
- extend the config model  
- reshape your **existing examples** into the new structure  
- implement the pattern-based Silver engine  
- and keep all the extra concerns in mind (deletes, schema, partitioning, observability, backfill posture).

---

### 🔧 Prompt to give Codex

```text
You are refactoring an existing Python project that implements a medallion-style data pipeline (Bronze/Silver/Gold) using config-driven scripts.

IMPORTANT CONTEXT

- The project is not yet in active use. It is safe to significantly simplify and reshape internals.
- The existing design includes a **Bronze Extract** and a **Silver Extract** that were intended to be a pair:
  - Bronze: talks to source systems and lands raw data.
  - Silver: reads Bronze and produces a more usable layer.
- There are already example configurations in the repo.
  - These examples should be **reshaped** into the new configuration structure, not thrown away.
- You do NOT need to preserve every legacy parameter as-is.
  - Before discarding something, check whether it’s useful.
  - Prefer repackaging existing functions/ideas to fit the new model.

When you design configs and behavior, think like a **source system owner** acting as the **semantic owner** of the data:

- They can describe:
  - whether the data is about events or states,
  - whether the feed is incremental vs full slices,
  - how much history they need.
- They should NOT need to understand internal pipeline details.

Your goal is to centralize technical complexity in shared engine code and make configs describe the **intent** of the data.

############################################################
## STEP 0 – UNDERSTAND THE CURRENT STRUCTURE (READ ONLY)
############################################################

1. Identify the **Bronze Extract**:
   - Entry points that:
     - Connect to source systems.
     - Run queries or pull from files/APIs.
     - Write to `/bronze/<system>/<entity>/...`.
   - Helpers for:
     - Building S3/SAN-style paths.
     - Partitioning (e.g., dt=YYYY-MM-DD).
   - How Bronze reads per-dataset config (system/entity, query, partition column, etc.).

2. Identify the **Silver Extract**:
   - Scripts/modules intended to read Bronze and write Silver.
   - Files named like `silver_*.py`, `*_silver.py`, `silver_foundry`, `silver_extract`, etc.
   - How Silver is currently invoked and configured.

3. Identify **existing example configs**:
   - Configuration files that serve as examples for Bronze/Silver.
   - Their current structure and fields.

Do not change anything until you understand how these parts work together.

Bronze must remain:
- a raw landing zone,
- append-only,
- partitioned (e.g., by dt),
- minimally interpreted.

Silver must remain:
- the partner layer that always reads from Bronze and serves Gold.

############################################################
## STEP 1 – ADD / UPDATE DOCUMENTATION
############################################################

Create or update the following docs in the repo:

1. `docs/framework/silver_patterns.md`
   - Use the content provided in the "Silver Patterns in Medallion Foundry" spec (entity_kind, history_mode, input_mode, delete_mode, schema_mode, partition_by, observability, env/domain, etc.).
   - Ensure it explains how Bronze and Silver work together and how a source/semantic owner thinks about this.

2. `docs/usage/onboarding/new_dataset_checklist.md`
   - Use the content provided in the "New Dataset Checklist – Medallion Foundry" spec.
   - This should guide a source/semantic owner through choosing entity_kind, history_mode, input_mode, natural_keys, timestamps, attributes, and optional advanced fields.

If a docs folder does not exist, create it. Optionally link these docs from the main README.

############################################################
## STEP 2 – UNIFY AND EXTEND THE CONFIG SCHEMA
############################################################

Refactor the per-dataset configuration so that **one config file** describes both Bronze and Silver for each dataset.

The target shape is roughly:

```yaml
environment: dev | test | prod           # optional
domain: finance | claims | hr | it | ... # optional

system: <system_name>
entity: <entity_name>

bronze:
  enabled: true
  source_type: db | file | api
  connection_name: ...
  source_query: ...        # or file/path configuration
  partition_column: ...    # dt/extract_date/etc.

silver:
  enabled: true

  entity_kind: event | state | derived_state | derived_event

  history_mode: scd2 | scd1 | latest_only       # for state/derived_state
  input_mode: append_log | replace_daily        # for event/derived_event

  delete_mode: ignore | tombstone_state | tombstone_event   # optional, advanced
  schema_mode: strict | allow_new_columns                   # optional, advanced

  natural_keys:
    - ...

  event_ts_column: ...      # for event/derived_event
  change_ts_column: ...     # for state/derived_state

  attributes:
    - ...
    - ...

  partition_by:
    - dt                    # or effective_from_dt, etc. optional

Tasks:

Locate existing configuration parsing code.

Extend it to:

Support environment, domain at the top.

Support silver.entity_kind.

Support silver.history_mode (default scd2 for state/derived_state).

Support silver.input_mode (default append_log for event/derived_event).

Support silver.delete_mode (with a sensible default, e.g., ignore).

Support silver.schema_mode (default strict).

Support silver.natural_keys, silver.event_ts_column, silver.change_ts_column, silver.attributes, silver.partition_by.

Add validation that:

state / derived_state must define natural_keys and change_ts_column.

event / derived_event must define natural_keys and event_ts_column.

history_mode is only used on state-like entity_kinds.

input_mode is only used on event-like entity_kinds.

For any existing equivalent fields in the old schema, map them into these new fields where possible, rather than forcing users to start from scratch.



############################################################

STEP 3 – RESHAPE EXISTING EXAMPLE CONFIGS

############################################################

Reuse the existing examples; do not delete them.

Find all current example configs (likely under config/, config/examples/, or similar).

For each example:

Preserve its dataset identity and intent.

Wrap its parameters into the new structure:

Move Bronze-related parameters under bronze.*.

Introduce silver.* as described above.

Choose:

silver.entity_kind (event/state/derived_state/derived_event).

For state-like examples: silver.history_mode (default scd2 unless clearly a static lookup → latest_only).

For event-like examples: silver.input_mode (append_log vs replace_daily).

Identify:

silver.natural_keys

silver.event_ts_column or silver.change_ts_column

silver.attributes

Where appropriate, also set:

environment and domain (even if just as placeholders).

Optional advanced fields (delete_mode, schema_mode, partition_by) where they clearly add value.

Add brief inline comments in each example explaining:

Why you chose entity_kind.

Why you chose history_mode or input_mode.

Any interesting advanced settings.

The goal is that these examples become realistic guides a source/semantic owner can copy and adapt.

############################################################

STEP 4 – KEEP THE BRONZE/SILVER PAIRING EXPLICIT

############################################################

Refactor code so that Bronze and Silver are clearly a paired pipeline for each dataset:

Bronze Extract:

Reads the per-dataset config.

Uses system, entity, and bronze.* to:

connect to the source,

run the configured query/read,

write to /bronze/<system>/<entity>/dt=....

Silver Extract:

Reads the same per-dataset config.

Uses system, entity, and silver.* to:

find Bronze files for that dataset,

apply the pattern-based Silver logic,

write to /silver/<system>/<entity>/....

Optionally, create a simple orchestration script (e.g., run_dataset.py) that:

loads a config,

runs Bronze (if bronze.enabled),

then runs Silver (if silver.enabled),

so the pairing is obvious.

############################################################

STEP 5 – CENTRALIZE SILVER LOGIC INTO PATTERNS

############################################################

Implement a central Silver processor, for example:

src/silver_processor.py:

class SilverProcessor(config)

method run()

This should:

Inspect:

silver.entity_kind

silver.history_mode

silver.input_mode

silver.delete_mode

silver.schema_mode

Read the appropriate Bronze partitions for the dataset:

/bronze/<system>/<entity>/....

Dispatch to pattern handlers:

entity_kind: "event" → Bronze EVENT → Silver EVENT

entity_kind: "state" → Bronze STATE → Silver STATE

entity_kind: "derived_state" → Bronze EVENT → Silver STATE

entity_kind: "derived_event" → Bronze STATE → Silver EVENT

Reuse and rename any existing functions that already implement parts of this logic. Avoid duplicating behavior.

High-level behavior:

EVENT + input_mode (append_log / replace_daily)

Clean, deduplicate, typecast events.

Append or replace the appropriate Silver partitions.

STATE + history_mode (scd2 / scd1 / latest_only)

Implement SCD2 for full history.

Overwrite-in-place for SCD1.

Keep only latest snapshot for latest_only.

Apply delete_mode where relevant (e.g., treating disappearing keys as tombstones if configured).

derived_state

Use event data to derive SCD-like state.

Honour history_mode and, if applicable, delete_mode.

derived_event

Use snapshots to derive change events and deletion events if delete_mode = tombstone_event.

Silver must not talk directly to source systems; it always consumes Bronze.

############################################################

STEP 6 – INCREMENTALITY, IDEMPOTENCY, SCHEMA, AND PATHS

############################################################

Reuse or refactor existing path helpers so that both Bronze and Silver:

derive paths from <environment?>/<system>/<entity>/partition conventions if applicable.

Ensure that running Bronze+Silver for the same dataset and batch is idempotent:

Re-running should not duplicate rows or corrupt SCD history.

Apply schema_mode:

strict: fail fast on unexpected schema changes.

allow_new_columns: tolerate additional columns but ignore them unless configured in attributes.

Make clear decisions about late-arriving events:

Initially, insert them according to event_ts_column and only adjust SCD boundaries for keys touched in the current batch.

Document this behavior in comments.

If there are existing watermark or “last processed dt” mechanisms, adapt them rather than reinvent them.

############################################################

STEP 7 – METRICS, METADATA, AND PARTITIONING

############################################################

Ensure each Silver run produces basic metrics (e.g., log or metrics object):

rows read from Bronze,

rows written to Silver,

number of keys changed (SCD2),

number of derived events emitted (for derived_event).

Ensure Silver outputs include standard metadata columns:

load_batch_id,

record_source,

optional pipeline_name or similar.

Respect silver.partition_by if set:

Partition output files accordingly (e.g., dt derived from event_ts, or effective_from_dt).

If not set, use a reasonable default consistent with the rest of the project.

These hooks will be useful for monitoring and for later OpenMetadata integration.

############################################################

STEP 8 – BACKFILL POSTURE (DESIGN-LEVEL)

############################################################

You do not need to fully implement a backfill CLI in this refactor, but:

Structure the code so that backfills are possible later, e.g.:

Ability to reprocess a date range of Bronze and rebuild corresponding Silver partitions.

Avoid design choices that make backfills impossible or extremely fragile.

Add high-level comments where relevant to indicate how backfill could be invoked in the future (e.g., rerun with a date range and replace Silver for that range).

############################################################

STEP 9 – EXAMPLES AND VALIDATION

############################################################

For all reshaped example configs:

Ensure each:

Parses correctly in the configuration loader.

Drives the correct code path based on entity_kind, history_mode, input_mode.

If the project has tests, add tests to:

Validate config parsing for representative examples (event, state, derived_state, derived_event).

Validate pattern selection logic in SilverProcessor.

############################################################

STEP 10 – SEMANTIC OWNER PERSPECTIVE & COMMENTS

############################################################

Match the behavior described in:

docs/framework/silver_patterns.md

docs/usage/onboarding/new_dataset_checklist.md

Write comments and error messages so that a source/semantic owner can roughly understand them, e.g.:

“state entities require natural_keys and change_ts_column so we can track attribute changes over time.”

“event entities require event_ts_column so we know when each event occurred.”

“schema_mode 'strict' will fail the job if the upstream schema changes unexpectedly.”

############################################################

ACCEPTANCE CRITERIA

############################################################

Bronze and Silver remain a clear paired pipeline:

Same system/entity identity.

Same config file.

Bronze writes /bronze, Silver reads /bronze and writes /silver.

Existing examples are preserved and reshaped:

They all use the new structure with entity_kind, history_mode, input_mode, natural_keys, timestamps, attributes, and (optionally) advanced flags.

They illustrate both simple and complex cases.

New documentation (silver_patterns + new_dataset_checklist) matches what the code does.

A new dataset can be onboarded by:

Creating a config file in the new structure,

No pipeline code changes.

Gold can treat Silver as the authoritative analytic layer without reading from Bronze or source systems.




