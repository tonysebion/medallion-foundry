You are refactoring and extending an existing Python-based Medallion Foundry project that implements Bronze + Silver + Gold data pipelines. This project is not yet in production and it is safe to reshape major components.

The repository now includes the following reference documentation in /docs:

docs/framework/silver_patterns.md

docs/framework/pipeline_engine.md

Use these two files as the authoritative design specification for the project.
They describe:

the conceptual model (entity_kind, history_mode, input_mode, delete_mode, schema_mode, partition_by)

how Bronze and Silver work together

how the Silver engine should be pattern-selected

how a source/semantic owner configures datasets

how incremental, idempotent, and backfill-friendly behavior should work

Your task is to update the entire project to match these specifications.

Before making changes:

Read the entire repository, especially the existing Bronze and Silver extract scripts.

Identify the current config model, example datasets, and pipeline structure.

Understand how Bronze and Silver currently pair together (even if imperfect).

This project already has Bronze and Silver extract code and already has example configs.
You should reshape and repurpose that code—not delete it—unless something is provably unused.

If an existing function or object is still valuable:

repurpose it into the new pattern

rename or reorganize it to make it consistent

move it into a new module if needed

do not throw away anything that still has purpose

The goal is to create a clean, cohesive engine while preserving useful existing work.

🚧 PHASE 1 — Upgrade the Configuration Model

Create a unified, simplified, intent-driven config schema exactly as defined in the docs.

Every dataset config should follow this shape:

environment: dev | test | prod        # optional
domain: finance | claims | hr | it    # optional

system: <system_name>
entity: <entity_name>

bronze:
  enabled: true
  source_type: db | file | api
  connection_name: ...
  source_query: ...        # or path pattern
  partition_column: ...

silver:
  enabled: true

  entity_kind: event | state | derived_state | derived_event

  history_mode: scd2 | scd1 | latest_only
  input_mode: append_log | replace_daily

  delete_mode: ignore | tombstone_state | tombstone_event
  schema_mode: strict | allow_new_columns

  natural_keys:
    - ...

  event_ts_column: ...     # for events
  change_ts_column: ...    # for states

  attributes:
    - ...

  partition_by:
    - ...


Tasks:

Update config loader to support all fields, defaults, and validation rules from docs/framework/silver_patterns.md.

Map old parameters into the new parameters where possible.

Mark any deprecated fields but keep compatibility temporarily.

🚧 PHASE 2 — Reshape Existing Example Configs

All existing examples must be:

converted into the new unified Bronze+Silver shape,

restructured to use entity_kind, history_mode, input_mode, natural_keys, timestamps, attributes,

updated with brief inline comments explaining choices.

You must:

Reuse every existing dataset example.

Do not delete examples—reshape them.

Add additional example datasets if needed to illustrate the full pattern surface.

🚧 PHASE 3 — Silver Processor Engine (Core Logic)

Implement (or refactor existing code into) a single SilverProcessor that performs pattern dispatch based on:

entity_kind

history_mode

input_mode

delete_mode

schema_mode

partition_by

Follow behavior as defined in docs/framework/silver_patterns.md:

Event logic:

append_log

replace_daily

State logic:

scd2 (full as-of history)

scd1 (current-only)

latest_only (thin lookup)

Derived logic:

derived_state (events → state)

derived_event (snapshots → change events)

This architecture must be clear, modular, and easy to test.

Reuse and reorganize existing logic where possible.

🚧 PHASE 4 — Bronze Extract Cleanup & Integration

The Bronze module must:

remain raw landing only

keep partition-based writes

interoperate cleanly with Silver

not embed business logic

Silver must always read from Bronze paths derived identically from config.

🚧 PHASE 5 — Observability Hooks

Add standard metrics (as per docs):

rows read

rows written

changed keys

derived events

Bronze partitions processed

Add standard metadata columns (load_batch_id, record_source, etc.).

🚧 PHASE 6 — Backfill-Ready Architecture

Follow the backfill guidance in docs:

architect code so reprocessing ranges is supported later

do not implement CLI yet

ensure SCD2/state updates are idempotent and partition-safe

🚧 PHASE 7 — Tests and Validation

If the repo has tests, update them.
If not, create basic parsing and routing tests for:

event

state / scd2

state / latest_only

derived_state

derived_event

🚧 PHASE 8 — Project Cleanup & Developer Experience

Update README to reference the new docs.

Ensure project layout is coherent.

Add inline comments that explain design from semantic-owner perspective.

ACCEPTANCE CRITERIA

A contributor should be able to:

Pick a system/entity.

Fill out a config describing the intent (event/state, history, input mode, etc.).

Run a single CLI/script.

Bronze lands raw.

Silver rewrites clean, incremental, history-aware outputs.

Gold can read Silver without knowing anything about Bronze.

The existing repo should feel like a clean, modern, pattern-driven framework, with all previous code uplifted into the new structure described in docs/framework/silver_patterns.md

silver_patterns

and docs/framework/pipeline_engine.md

pipeline_engine

.
