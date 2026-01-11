# Concepts Glossary

This guide explains bronze-foundry concepts in plain English. Each term includes a definition, a real-world analogy, and a YAML example.

---

## The Medallion Layers

### Bronze Layer

**What it is:** Raw data exactly as the source provides it, with technical metadata added (timestamps, source info).

**Analogy:** A dated filing cabinet. Every day you photocopy documents and file them in a folder labeled with today's date. You don't edit the documents—you just store them exactly as received.

**What Bronze does:**
- Extracts data from sources (files, databases, APIs)
- Adds technical metadata (`_load_date`, `_extracted_at`, `_source_system`)
- Saves to date-partitioned folders (`dt=2025-01-15/`)
- Generates checksums for data integrity

**What Bronze does NOT do:**
- Transform or clean data
- Apply business rules
- Filter records based on business criteria

```yaml
bronze:
  system: retail          # Source system name
  entity: orders          # What we're extracting
  source_type: file_csv   # Where data comes from
  source_path: ./data/orders.csv
```

---

### Silver Layer

**What it is:** Cleaned data with duplicates removed and history tracked. Still no business logic—just technical curation.

**Analogy:** A current address book. You take all the address cards you've collected (from Bronze) and keep only the most recent version of each person's contact info. Optionally, you keep a history of past addresses.

**What Silver does:**
- Removes duplicate records by natural key
- Tracks history (SCD Type 1 or Type 2)
- Enforces column selection (schema)
- Processes CDC operations (insert/update/delete codes)

**What Silver does NOT do:**
- Apply business rules or filtering
- Calculate derived fields
- Join data from multiple sources

```yaml
silver:
  unique_columns: [order_id]       # What makes each row unique
  last_updated_column: updated_at  # Which version is newest
  model: full_merge_dedupe         # How to handle duplicates
```

---

### Gold Layer

**What it is:** Business logic, calculations, KPIs, and joined data. This is where domain-specific rules live.

**Analogy:** Your custom reports. You take the clean address book (Silver) and create reports like "customers by region" or "orders above $1000". These rules are specific to your business.

**Note:** bronze-foundry focuses on Bronze and Silver. Gold layer logic belongs in your BI tools, SQL views, or downstream applications.

---

## Key Concepts

### Unique Columns

**What it is:** The column(s) that uniquely identify each row. Like a fingerprint for your data.

**Analogy:** An employee ID or order number. Even if someone's name changes, their employee ID stays the same and identifies them.

**Why it matters:** Silver uses unique columns to detect duplicates. If two rows have the same values in these columns, they're considered the same record, and Silver keeps the one with the latest `last_updated_column`.

**TIP:** Run `python -m pipelines inspect-source --file your_data.csv` to get suggestions for which columns to use.

```yaml
silver:
  unique_columns: [customer_id]           # Single column
  # or
  unique_columns: [order_id, line_number] # Composite key
```

---

### Last Updated Column

**What it is:** The column that shows when each row was last modified.

**Analogy:** The "last updated" date on a document. When you have two versions of the same document, you keep the one with the more recent date.

**Why it matters:** When Silver finds duplicate rows (same unique columns), it uses the last updated column to determine which version is newest.

**TIP:** Run `python -m pipelines inspect-source --file your_data.csv` to get suggestions for which column to use.

```yaml
silver:
  unique_columns: [customer_id]
  last_updated_column: updated_at  # Column showing modification date
```

---

### Watermark

**What it is:** A bookmark that tracks where you left off during incremental loads.

**Analogy:** Reading a book over several days. Each night you use a bookmark so you don't re-read pages you've already finished. The watermark is that bookmark.

**How it works:**
1. First run: No watermark → extract all records
2. Save watermark as the maximum `updated_at` value (e.g., `2025-01-15 10:30:00`)
3. Next run: Extract only records where `updated_at > 2025-01-15 10:30:00`
4. Update watermark to new maximum

```yaml
bronze:
  load_pattern: incremental
  incremental_column: updated_at  # Column to track
```

---

### Partition

**What it is:** A date-organized folder that contains one day's data.

**Analogy:** Monthly folders in a filing cabinet. Instead of one giant pile of papers, you organize by date so you can find things and manage storage.

**Structure:**
```
bronze/system=retail/entity=orders/
  ├── dt=2025-01-15/     # January 15th partition
  │   ├── orders.parquet
  │   ├── _metadata.json
  │   └── _checksums.json
  ├── dt=2025-01-16/     # January 16th partition
  └── dt=2025-01-17/     # January 17th partition
```

---

### Load Pattern

**What it is:** How Bronze extracts data from the source—all at once or just the changes.

**Analogy:** Photocopying a book. `full_snapshot` means photocopying the entire book every day. `incremental` means only photocopying new pages since your last visit.

| Pattern | Each Day Contains | Use When |
|---------|-------------------|----------|
| `full_snapshot` | Complete dataset | Small tables, no change tracking |
| `incremental` | Only new/changed records | Large tables with `updated_at` column |
| `cdc` | Insert/Update/Delete operations | Source provides CDC stream |

```yaml
# Full snapshot - complete data each run
bronze:
  load_pattern: full_snapshot

# Incremental - only changes since last run
bronze:
  load_pattern: incremental
  incremental_column: updated_at

# CDC - change operations with I/U/D codes
bronze:
  load_pattern: cdc
  cdc_operation_column: op
```

**⚠️ Important:** Once you choose a load pattern, stick with it. Changing patterns mid-stream can cause data issues. See [Pattern Transitions](#pattern-transitions) below.

---

### Input Mode

**What it is:** How Silver reads Bronze partitions—just today's or all history.

**Analogy:** Reading mail. `replace_daily` means only reading today's mail (yesterday's is irrelevant). `append_log` means reading all accumulated mail to build a complete picture.

| Mode | Silver Reads | Use With |
|------|--------------|----------|
| `replace_daily` | Only today's Bronze partition | `full_snapshot` Bronze |
| `append_log` | All Bronze partitions | `incremental` or `cdc` Bronze |

**Auto-wiring:** You usually don't need to set this. Silver automatically chooses based on Bronze's `load_pattern`:
- Bronze `full_snapshot` → Silver uses `replace_daily`
- Bronze `incremental` or `cdc` → Silver uses `append_log`

---

### SCD Type 1

**What it is:** Keep only the current version of each record. History is overwritten.

**Analogy:** Updating a phone number in your contacts. You replace the old number with the new one. You don't care what the old number was.

**Use when:** You only need current state, not historical changes.

```yaml
silver:
  model: full_merge_dedupe  # SCD Type 1 behavior
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

---

### SCD Type 2

**What it is:** Keep all versions of each record with effective dates. Full history preserved.

**Analogy:** Tracking address history. When someone moves, you don't delete their old address—you mark it with an end date and add the new address with a start date.

**Output columns added:**
- `effective_from` - When this version became active
- `effective_to` - When this version was replaced (NULL if current)
- `is_current` - Boolean flag for the active version

```yaml
silver:
  model: scd_type_2
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

---

### CDC (Change Data Capture)

**What it is:** A stream of insert, update, and delete operations from the source system.

**Analogy:** A changelog or audit log. Instead of seeing the current state, you see every change that happened: "Record 123 was inserted", "Record 456 was updated", "Record 789 was deleted".

**Operation codes:** CDC data includes a column indicating the operation type:
- `I` = Insert (new record)
- `U` = Update (modified record)
- `D` = Delete (removed record)

```yaml
bronze:
  load_pattern: cdc
  cdc_operation_column: op  # Column containing I/U/D codes

silver:
  model: cdc
  keep_history: false     # false = current only (SCD1), true = full history (SCD2)
  handle_deletes: flag    # flag = soft delete, remove = hard delete, ignore = skip
  unique_columns: [customer_id]
  last_updated_column: updated_at
```

---

### Delete Mode

**What it is:** How Silver handles delete operations in CDC data.

| Mode | Behavior | Use When |
|------|----------|----------|
| `ignore` | Filter out deletes | Deletes are corrections, not business events |
| `tombstone` | Keep with `_deleted=true` | Need audit trail of deletions |
| `hard_delete` | Remove from Silver | Deletions must propagate immediately |

---

## Pattern Transitions

Silver intelligently handles pattern transitions by detecting **partition boundaries**.

### How Silver Reads Bronze Partitions

Silver determines how to read Bronze partitions based on the **latest partition's** metadata:
- Latest is `full_snapshot` → Silver uses `replace_daily` (reads ONLY latest partition)
- Latest is `incremental` or `cdc` → Silver uses `append_log` with **boundary detection**

### Partition Boundary Detection

When Silver uses `append_log` mode, it scans backwards through partition metadata to find `full_snapshot` boundaries:

```
Partitions:  dt=01-06 (incr) → dt=01-07 (incr) → dt=01-08 (full) → dt=01-09 (incr) → dt=01-10 (incr)
                                                      ↑                                      ↑
                                               BOUNDARY FOUND                    Silver starts here
                                               (full_snapshot)                   Scans backward...
                                                                                 Stops at 01-08

Result: Silver reads ONLY dt=01-08, dt=01-09, dt=01-10 (not 01-06, 01-07)
```

**Why this works:**
- A `full_snapshot` contains **complete state** at that point in time
- Partitions before a `full_snapshot` are **obsolete** - their data is already incorporated into the snapshot
- This is both correct and efficient - no redundant data is read

| Scenario | Latest Pattern | Silver Reads | Result |
|----------|---------------|--------------|--------|
| Days 1-5 incremental, Day 6 full_snapshot (STOP) | full_snapshot | Day 6 only | Correct - snapshot has everything |
| Days 1-5 incremental, Day 6 full, Day 7+ incremental | incremental | Days 6-7+ only | Correct - boundary at day 6 |
| Days 1-5 full_snapshot, Day 6 incremental | incremental | Days 1-6 (all) | Deduplication handles overlap |
| Multiple full_snapshots in history | varies | From most recent full_snapshot | Uses latest boundary |

**Safe practices:**

1. **Consistent patterns are simplest:** Choose a load pattern and stick with it
2. **Weekly full refreshes work well:** Insert `full_snapshot` partitions weekly; Silver automatically uses them as boundaries
3. **Full_snapshot = complete data:** Only use `load_pattern: full_snapshot` when the partition truly contains complete data

---

## Related Documentation

- [Model Selection Guide](./MODEL_SELECTION.md) - Choosing the right patterns
- [Business Logic Boundaries](./BUSINESS_LOGIC_BOUNDARIES.md) - What belongs where
- [Quick Reference](../pipelines/QUICKREF.md) - Copy-paste patterns
