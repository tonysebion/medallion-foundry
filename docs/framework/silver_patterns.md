# Silver Patterns in Medallion Foundry

The Medallion Foundry is built around a simple idea:

> **Bronze gets data out of source systems.  
> Silver represents what the source system *intended* in a consistent, queryable way.**

Bronze + Silver are a **pair**:

- **Bronze Extract** talks to source systems and lands raw files.
- **Silver Extract** reads those files and applies a small set of patterns to create analytic-friendly data that Gold will use.

This project is pre-adoption. We are taking the opportunity to simplify patterns so that configs read like what a **source/semantic owner** would naturally say about their data.

---

## 1. Core concepts

Silver behavior is controlled by these main concepts in the per-dataset config:

- `entity_kind` – what *type* of thing is this?
- `history_mode` – how much history we keep for state-like entities.
- `input_mode` – how we interpret Bronze feeds for event-like entities.
- `delete_mode` – how we treat deletions or missing keys.
- `schema_mode` – how strict we are about schema changes.
- `partition_by` – how we physically partition Silver for performance.

Plus some global context:

- `environment` – dev/test/prod.
- `domain` – business area (finance, claims, HR, etc.).

Silver is always derived from Bronze and is the layer Gold reads.

---

## 2. `entity_kind`

`entity_kind` answers: **“What kind of dataset is this?”**

Supported values:

- `event`
- `state`
- `derived_state`
- `derived_event`

### 2.1. `event` – Bronze EVENT → Silver EVENT

**Story:**  
“The world happened, and we preserve each happening — but cleanly.”

Use this for:

- GL postings
- Invoice lines
- Claim events
- Payments
- Time entries
- Status change events

Behavior:

- Silver stays at **event grain**.
- Data is cleaned, deduplicated, typed, and partitioned.
- No aggregation, no SCD logic.

---

### 2.2. `state` – Bronze STATE → Silver STATE

**Story:**  
“The world changed — and we remember exactly how, but only when it really mattered.”

Use this for:

- Employees
- Projects
- Vendors
- Org structures
- Contracts
- Membership/eligibility records

Behavior:

- Bronze is treated as **snapshots** of current state.
- Silver maintains a stateful representation, with history controlled by `history_mode`.

---

### 2.3. `derived_state` – Bronze EVENT → Silver STATE

**Story:**  
“Events imply state, even when the source doesn’t provide it directly.”

Use this when:

- The source only emits events (status changes, audit logs, workflow events).
- There is no clean state table, but you want a stateful dimension.

Behavior:

- Silver derives an SCD-style state table from event streams.
- Effective dating logic is similar to `state`.

---

### 2.4. `derived_event` – Bronze STATE → Silver EVENT

**Story:**  
“Sometimes a change in state is itself an event.”

Use this when:

- The source only gives snapshots (e.g., daily employee file).
- You need **change events** like promotions, terminations, org changes.

Behavior:

- Silver compares consecutive snapshots.
- Emits **change events** describing what changed and when.

---

## 3. `history_mode` (for state-like entities)

`history_mode` answers: **“How much history do we keep for state-like entities?”**

Applies to:

- `entity_kind: state`
- `entity_kind: derived_state`

Supported values:

- `scd2` (default)
- `scd1`
- `latest_only`

### 3.1. `scd2` – effective-dated history

**Intent:**  
Full, compact history – “as-of friendly”.

Silver schema:

- `natural_keys` (business key)
- `effective_from`
- `effective_to` (nullable)
- `is_current`
- other attributes
- metadata (e.g., load batch, record source)

Use when:

- People care about **“what was true at that time?”**

---

### 3.2. `scd1` – overwrite-in-place

**Intent:**  
Current-only view. No historical tracking.

Behavior:

- One row per business key.
- On change, Silver updates the row in place.
- No effective dates.

Use when:

- History is not important.
- Reference data where only the current view matters.

---

### 3.3. `latest_only` – keep just the latest snapshot

**Intent:**  
Minimal storage, tiny lookups.

Behavior:

- Keep only the most recent state.
- Older partitions/snapshots are ignored or pruned.

Use when:

- Very small lookup tables.
- Static-ish codebooks (countries, simple enums).

---

## 4. `input_mode` (for event-like entities)

`input_mode` answers: **“How should we interpret Bronze event feeds?”**

Applies to:

- `entity_kind: event`
- `entity_kind: derived_event`

Supported values:

- `append_log` (default)
- `replace_daily`

### 4.1. `append_log` – Bronze is a changelog

**Intent:**  
True event log.

Behavior:

- Every new Bronze partition contains **new events**.
- Silver appends those events to its archive.
- No replacement of past data.

Use when:

- Bronze is CDC-like or append-only.
- GL transaction feeds, audit logs, etc.

---

### 4.2. `replace_daily` – Bronze is a full slice for a period

**Intent:**  
Bronze partitions represent **complete slices**, not just new events.

Behavior:

- Bronze `dt=YYYY-MM-DD` contains all events for that date.
- Silver replaces that date’s partition instead of appending.
- Avoids duplicates across reruns.

Use when:

- Daily batch extracts send “full file for that day”.

---

## 5. Deletions – `delete_mode` (advanced)

Sometimes keys disappear from snapshots or are flagged as deleted.

`delete_mode` answers: **“What do we do when a record is no longer present or marked as deleted?”**

Applies primarily to `state` and `derived_state`, but can influence derived_event.

Suggested values:

- `ignore`  
  - Do nothing special. If a key disappears from a snapshot, we leave its last Silver state open or treat soft-delete flags as normal attributes.

- `tombstone_state`  
  - When a key disappears or a soft-delete flag is set, close the SCD2 row and mark the final state as deleted.

- `tombstone_event`  
  - Emit a derived delete event (for `derived_event`) when a key disappears.

You don’t have to expose this to all users immediately. It’s an advanced control with sensible defaults.

---

## 6. Late-arriving / out-of-order data (conceptual)

Event feeds sometimes arrive out of order (an old event comes in a later batch).

Design stance:

- We **trust `event_ts_column` for semantics**, but we may not retroactively rewrite entire SCD histories unless explicitly configured.
- Initial implementation can:
  - Always insert events using `event_ts_column`.
  - Limit SCD2 adjustments to the keys touched in the current batch.

If needed later, we can introduce a `late_data_mode`, but for now this behavior is more of a documented convention than a config field.

---

## 7. `schema_mode` (advanced)

`schema_mode` answers: **“How strict should we be about schema changes?”**

Suggested values:

- `strict`  
  - If the Bronze schema deviates from what Silver expects (missing columns, type changes), fail the job and alert.

- `allow_new_columns`  
  - Allow additional columns to appear in Bronze; ignore them unless added to `attributes`.

This matters most once MSP and multiple teams start adding feeds. It protects you from silent drift.

---

## 8. `partition_by` (performance hint)

Events and SCD tables benefit from clear partitioning.

You can optionally define:

```yaml
silver:
  partition_by:
    - dt


Examples:

For events: partition by a date derived from event_ts_column.

For SCD2: partition by a date derived from effective_from or by a load dt.

If not specified, the engine can use reasonable defaults.

9. Observability & metadata

Every Silver pipeline should emit basic metrics and carry some metadata:

Metrics per run (for logging/monitoring):

rows read from Bronze.

rows written to Silver.

number of changed keys (SCD2).

number of derived events emitted.

Standard metadata columns in Silver:

load_batch_id

record_source

possibly pipeline_name or similar.

These make it easy to integrate with OpenMetadata, monitoring, and CM-12 evidence later.

10. Environments & domains

At the top level of the config, you can optionally include:

environment: dev | test | prod
domain: finance | claims | hr | it | ...


These don’t affect transformation logic directly but are useful for:

separating dev/test/prod paths or connections,

organizing assets in catalogs and governance.

11. Bronze sample directory conventions

The sample Bronze data under `sampledata/source_samples` follows the same layout the CLIs expect:

```
bronze_samples/
├── full/
│   └── system=<system>/
│       └── table=<entity>/
│           └── pattern=full/
│               └── dt=YYYY-MM-DD/
│                   └── *.csv | *.parquet
├── cdc/
│   └── ...
└── current_history/
    └── ...
```

Each intent config points its Bronze section (e.g., `path_pattern`, `options.load_pattern`) to one of those directories so the Bronze CLI always reads data organized by system/table/pattern/date. When you regenerate the samples or introduce a new feed (IoT, API slice, etc.), keep that structure and simply update the intent YAML to point at the new files; Silver keeps interpreting the same partition layout through its semantic settings.

12. How Bronze and Silver work together

Bronze and Silver are configured together in one per-dataset config.

For each dataset:

Bronze Extract:

Knows how to query/pull from the source.

Writes files into /bronze/<system>/<entity>/....

Silver Extract:

Reads those Bronze files for the same <system>/<entity>.

Uses:

entity_kind

history_mode

input_mode

delete_mode (optional, advanced)

schema_mode (optional, advanced)

partition_by (optional, advanced)

natural_keys

event_ts_column / change_ts_column

attributes

Writes /silver/<system>/<entity>/... in a consistent, queryable shape.

Gold should only read from Silver, never from Bronze or the source.

12. How a source/semantic owner thinks about this

A source system owner, acting as a semantic owner, should configure their dataset by answering:

Is this data about events or states? → entity_kind

Do I need historical “as-of” answers, or just the latest? → history_mode

Does my system send new events only or full slices? → input_mode

What columns uniquely identify a record? → natural_keys

What column tells me when something happened or changed?
→ event_ts_column or change_ts_column

Which attributes matter for analysis? → attributes

(Optional) What should happen when records disappear or schema changes?
→ delete_mode, schema_mode

The pipeline engine then handles the rest.
