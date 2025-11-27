# Bronze/Silver Pattern Matrix

This guide keeps the patterns readable by presenting one “card” per pattern. Each card
includes the behavioral cues that send you down each path, a precise checklist of
configuration fields to populate, and a pointer back to the canonical sections in
`docs/framework/silver_patterns.md` and `docs/framework/pipeline_engine.md`.

## Start here: map your answers to a pattern

Answer the questions below in order and follow the pattern number in parentheses so you
can skip directly to the appropriate recipe.

1. **Event or state data?**  
   - Event: go to Pattern 1 if the source rewrites everything, or Pattern 2 if it appends deltas.  
   - State: go to Pattern 3 (state snapshot) or to a hybrid pattern if you also need derived behavior.
2. **Full table dump or incremental feed?**  
   - Full dump → Pattern 1.  
   - Incremental / CDC → Patterns 2–7 depending on metadata and history needs.
3. **Do you need “as of” history?**  
   - Yes → Patterns 3, 5, or 7.  
   - No → Patterns 1, 2, 4, or 6.
4. **Does the source already emit reference metadata/delta tags?**  
   - Yes → go to Patterns 4–7 for hybrid flows.  
   - No → stay with Patterns 1–3.
5. **Need the minimum config before optimizing?**  
   - Start with Pattern 1 and graduate to CDC (Pattern 2) or hybrids once you understand the source.

## Pattern recipes

Each pattern below repeats the pattern name for clarity, spells out the Bronze and Silver expectations,
lists the core config fields, and ends with “See also” links to the authoritative docs.

### Pattern 1 – Full Events (simple rewrite)

- **When to use:** clean table or API export; rewrites every row but is easy to understand.  
- **Bronze assumptions:** single file per `dt`, no CDC metadata.  
- **Silver checklist:**  
  - `entity_kind: event`  
  - `input_mode: append_log`  
  - `natural_keys`: business key(s)  
  - `event_ts_column`: e.g., `updated_at`  
  - `attributes`: analytics columns (status, amount, etc.)  
  - Optional `partition_by: [event_ts_dt]` for pruning.  
- **Talk it through:** confirm rewrite cadence and whether overwriting is acceptable.  
- **See also:** `docs/framework/silver_patterns.md#event`, `docs/framework/pipeline_engine.md#bronze-full`.

### Pattern 2 – CDC Events (append-only changelog)

- **When to use:** transactional log with inserts/updates/deletes.  
- **Bronze assumptions:** delta file, `change_type`/`changed_at`, batch metadata.  
- **Silver checklist:**  
  - `entity_kind: event`  
  - `input_mode: append_log`  
  - `delete_mode: tombstone_event`  
  - `natural_keys`, `event_ts_column` (`changed_at`)  
  - `attributes`: include `change_type`, `status`, `order_total`, etc.  
- **Talk it through:** how deletes are marked? Do you need reference metadata?  
- **See also:** `docs/framework/silver_patterns.md#event`, `docs/framework/pipeline_engine.md#cdc`.

### Pattern 3 – SCD State (current/history snapshot)

- **When to use:** sources that already emit current + historical rows (`effective_from`, `effective_to`).  
- **Bronze assumptions:** `current_flag`, effective dates are present.  
- **Silver checklist:**  
  - `entity_kind: state`  
  - `history_mode: scd2`  
  - `change_ts_column`: ordering timestamp  
  - `natural_keys` + `attributes`  
  - Optional `partition_by: [effective_from_dt]`.  
- **Talk it through:** how do you mark current rows? Soft deletes?  
- **See also:** `docs/framework/silver_patterns.md#state`, `docs/framework/pipeline_engine.md#state-models`.

### Pattern 4 – Derived Events (hybrid CDC + reference)

- **When to use:** delta feed that also provides reference metadata, and you want derived change events.  
- **Bronze assumptions:** reference info + deltas, `pattern_folder: hybrid_cdc_point`.  
- **Silver checklist:**  
  - `entity_kind: derived_event`  
  - `input_mode: append_log`  
  - `event_ts_column`: change timestamp  
  - `attributes`: include `delta_tag`, `order_total`, `status`.  
- **Talk it through:** do you have reference metadata? Which columns identify the change?  
- **See also:** `docs/framework/silver_patterns.md#derived_event`, `docs/framework/pipeline_engine.md#derived-event`.

### Pattern 5 – Derived State (cumulative deltas)

- **When to use:** cumulative files that should produce current + history state.  
- **Bronze assumptions:** `pattern_folder: hybrid_cdc_cumulative`, `delta_mode: cumulative`.  
- **Silver checklist:**  
  - `entity_kind: derived_state`  
  - `history_mode: scd2`  
  - `change_ts_column` + `attributes`  
- **Talk it through:** do we reconstruct history from cumulative files?  
- **See also:** `docs/framework/silver_patterns.md#derived_state`, `docs/framework/pipeline_engine.md#derived-state`.

### Pattern 6 – Derived State (latest-only)

- **When to use:** incremental snapshots where only the newest value matters.  
- **Bronze assumptions:** point-in-time slice marked with timestamp.  
- **Silver checklist:**  
  - `entity_kind: derived_state`  
  - `history_mode: latest_only`  
  - `change_ts_column`: latest indicator  
  - `attributes`: columns you need for the current view  
- **Talk it through:** which column denotes “latest”?  
- **See also:** `docs/framework/silver_patterns.md#derived_state`, `docs/framework/pipeline_engine.md#latest-only`.

### Pattern 7 – Hybrid Incremental Cumulative

- **When to use:** merged snapshots that add cumulative deltas but still need full history.  
- **Bronze assumptions:** `pattern_folder: hybrid_incremental_cumulative`, `delta_mode: cumulative`.  
- **Silver checklist:**  
  - `entity_kind: state`  
  - `history_mode: scd2`  
  - `delete_mode: tombstone_state`  
  - `attributes`: include audit metadata (`delta_tag`, etc.)  
- **Talk it through:** how often do we merge, and can we rely on `delta_tag`?  
- **See also:** `docs/framework/silver_patterns.md#state`, `docs/framework/pipeline_engine.md#cumulative-merges`.

## Running the sample matrix

1. Regenerate fixtures so each pattern has Bronze data (`python scripts/generate_sample_data.py`).  
2. Optional: refresh the owner intent mapping (`python scripts/expand_owner_intent.py --config docs/examples/configs/owner_intent_template.yaml`).  
3. Run the integration suite covering every `pattern_*.yaml` (`pytest tests/test_integration_samples.py`).  

Each config targets an exact Bronze sample under `sampledata/source_samples/<pattern>/…`. Every pattern folder now records daily loads spanning several weeks, so you can inspect how that pattern’s files evolve day-to-day and understand what downstream Bronze or Silver operations must handle (e.g., full rewrites vs. new CDC deltas vs. refreshed current/history rows).
and the tests run against the same scenarios. When you add a new pattern, copy a nearby `pattern_*.yaml`,
adjust the Bronze/Silver options, regenerate fixtures, and rerun the suite so the router stays accurate.
