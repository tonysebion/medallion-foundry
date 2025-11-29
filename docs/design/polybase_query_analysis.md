# Polybase Query Analysis: Pattern-by-Pattern Deep Dive

## Purpose

This document analyzes what Polybase queries would actually look like for each of the 7 patterns, exposing potential structural misalignments between current Silver layer design and real-world Polybase query requirements.

**Key Question**: Does the current partition structure + artifact organization actually support the queries that end users would write?

---

## Pattern 1: Full Events (pattern_full.yaml)

### Current Structure
```
event_date=2025-11-13/
  events.parquet
```

### Polybase External Table
```sql
CREATE EXTERNAL TABLE [dbo].[orders_events_external] (
    [order_id] VARCHAR(255),
    [status] VARCHAR(255),
    [order_total] VARCHAR(255),
    [updated_at] VARCHAR(255),
    [event_date] VARCHAR(255)
)
WITH (
    LOCATION = 'pattern1_full_events/',
    DATA_SOURCE = [silver_pattern1_full_events_events_source],
    FILE_FORMAT = [parquet_format],
    ...
);
```

### Real-World Query Requirements

#### Query 1: "Show me all order events for a specific date"
```sql
-- User's mental model: "What happened on Nov 13?"
SELECT * FROM [dbo].[orders_events_external]
WHERE event_date = '2025-11-13'
ORDER BY updated_at;
```
✅ **Works well** - partition pruning kicks in

#### Query 2: "Show me events for orders in a date range"
```sql
-- User's mental model: "What happened between Nov 10-13?"
SELECT * FROM [dbo].[orders_events_external]
WHERE event_date >= '2025-11-10'
AND event_date <= '2025-11-13'
ORDER BY event_date, updated_at;
```
✅ **Works well** - partition pruning across multiple dates

#### Query 3: "Show me all events for a specific order (might be across dates)"
```sql
-- User's mental model: "What's the history of order ORD123?"
SELECT * FROM [dbo].[orders_events_external]
WHERE order_id = 'ORD123'
ORDER BY updated_at;
```
⚠️ **Problem**: Must scan ALL partitions - order events could be in ANY date partition
- No partition pruning possible
- Full table scan required
- Poor performance for "give me history of entity X"

#### Query 4: "Count events per order"
```sql
SELECT order_id, COUNT(*) as event_count
FROM [dbo].[orders_events_external]
GROUP BY order_id;
```
❌ **Major Problem**: Must scan ALL partitions for every single order

### Assessment: ACCEPTABLE for Query 1-2, BUT QUERY 3-4 are problematic

**Current Design Strength**: Time-based queries (What happened on date X?)
**Current Design Weakness**: Entity-based queries (What's the history of entity X?)

**Recommendation for v1.1**: Consider secondary partition by order_id bucket (e.g., order_id_hash_bucket=0-9) if entity history queries are common

---

## Pattern 2: CDC Events (pattern_cdc.yaml)

### Current Structure
```
event_date=2025-11-13/
  cdc-events.parquet
```

### Similar to Pattern 1, but with CDC-specific semantics

#### Real-World Query Requirements

#### Query 1: "Show me all changes to orders on Nov 13"
```sql
SELECT * FROM [dbo].[orders_cdc_events_external]
WHERE event_date = '2025-11-13'
ORDER BY changed_at;
```
✅ **Works well**

#### Query 2: "Show me change history for a specific order"
```sql
SELECT * FROM [dbo].[orders_cdc_events_external]
WHERE order_id = 'ORD123'
ORDER BY changed_at;
```
⚠️ **Problem**: Same as Pattern 1 - no partition pruning

#### Query 3: "Show me which orders changed between dates"
```sql
SELECT DISTINCT order_id
FROM [dbo].[orders_cdc_events_external]
WHERE event_date >= '2025-11-10'
AND event_date <= '2025-11-13'
AND change_type IN ('INSERT', 'UPDATE');
```
✅ **Works well** - partition pruning on event_date

### Assessment: ACCEPTABLE but with same entity-history weakness

---

## Pattern 3: Current/History State (SCD Type 2)

### Current Structure
```
effective_from_date=2025-11-10/
  state_history.parquet
  state_current.parquet (unpartitioned)
```

### Polybase External Tables (Two tables!)

#### Table 1: Historical State (partitioned)
```sql
CREATE EXTERNAL TABLE [dbo].[orders_state_history_external] (
    [order_id] VARCHAR(255),
    [status] VARCHAR(255),
    [customer_id] VARCHAR(255),
    [effective_from_dt] VARCHAR(255),
    [effective_to_dt] VARCHAR(255),
    [effective_from_date] VARCHAR(255)
)
WITH (
    LOCATION = 'pattern3_scd_state/state_history/',
    ...
);
```

#### Table 2: Current State (unpartitioned)
```sql
CREATE EXTERNAL TABLE [dbo].[orders_state_current_external] (
    [order_id] VARCHAR(255),
    [status] VARCHAR(255),
    [customer_id] VARCHAR(255),
    [is_current] BIT
)
WITH (
    LOCATION = 'pattern3_scd_state/state_current/',
    ...
);
```

### Real-World Query Requirements

#### Query 1: "What was order ORD123's status on Nov 11?"
```sql
-- Point-in-time query - classic temporal query
SELECT * FROM [dbo].[orders_state_history_external]
WHERE order_id = 'ORD123'
AND effective_from_date <= '2025-11-11'
AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-11')
ORDER BY effective_from_dt DESC
LIMIT 1;
```
⚠️ **Problem**: `order_id` filter doesn't help with partition pruning
- Partition is on `effective_from_date`
- Must scan multiple date partitions to find historical records for single order
- Example: Order ORD123 might have records spanning 2025-11-08 through 2025-11-13
- Query must check: effective_from_date <= Nov 11 AND effective_to_dt > Nov 11
- Could be in ANY partition from Nov 8 onwards

#### Query 2: "Show me status change history for ORD123"
```sql
SELECT effective_from_dt, status
FROM [dbo].[orders_state_history_external]
WHERE order_id = 'ORD123'
ORDER BY effective_from_dt;
```
❌ **Major Problem**: Full table scan across all date partitions
- No way to prune based on order_id
- Every entity history query must scan all dates

#### Query 3: "What's the current status of all orders?"
```sql
SELECT order_id, status, customer_id
FROM [dbo].[orders_state_current_external]
WHERE is_current = 1;
```
✅ **Works well** - unpartitioned table is small and current

#### Query 4: "Which orders changed status on Nov 13?"
```sql
SELECT DISTINCT order_id, old_status, new_status
FROM [dbo].[orders_state_history_external]
WHERE effective_from_date = '2025-11-13'
ORDER BY order_id;
```
✅ **Works well** - partition pruning on effective_from_date

### Assessment: CRITICAL DESIGN ISSUE!

**Current Design Strength**: Time-based queries (What changed on date X?)
**Current Design Weakness**: Entity-based queries (What's the history of entity X?)

**The Problem**: SCD Type 2 table is partitioned by WHEN changes happened, but most real-world queries ask "what happened TO entity X"

**Better Design Option 1**: Partition by ENTITY HASH + secondary partition by effective_from_date
```
entity_hash_bucket=order_id%10=3/
  effective_from_date=2025-11-13/
    state_history.parquet
```
This allows:
- Filter by `entity_hash_bucket = 3` to narrow search
- Then scan relevant effective_from_date partitions
- Reduces partition scan space dramatically

**Better Design Option 2**: Keep current structure but add CLUSTERED INDEX on parquet file
- Parquet files themselves internally organized by order_id
- Allows faster within-file scanning

**Better Design Option 3**: Hybrid approach - partition by EFFECTIVE_FROM_DATE_MONTH + secondary by ENTITY_HASH
```
effective_from_month=2025-11/
  entity_hash=3/
    state_history.parquet
```

### Current Temporary Workaround: Use temporal functions

The generated temporal functions help somewhat:
```sql
-- Generated function handles the boilerplate
SELECT * FROM [dbo].[get_orders_state_history_as_of]('2025-11-11');
```
But still causes full partition scan under the hood.

---

## Pattern 4: Hybrid CDC Point (Derived Event)

### Current Structure
```
event_date=2025-11-13/
  derived_events.parquet
```

### Polybase External Table
```sql
CREATE EXTERNAL TABLE [dbo].[orders_derived_events_external] (
    [order_id] VARCHAR(255),
    [status] VARCHAR(255),
    [change_type] VARCHAR(255),
    [changed_at] VARCHAR(255),
    [event_date] VARCHAR(255)
)
WITH (
    LOCATION = 'pattern4_hybrid_cdc_point/',
    ...
);
```

### Real-World Query Requirements

#### Query 1: "Derive new metric: Time between consecutive status changes"
```sql
WITH status_changes AS (
    SELECT
        order_id,
        status,
        changed_at,
        LAG(status) OVER (PARTITION BY order_id ORDER BY changed_at) as prev_status,
        LAG(changed_at) OVER (PARTITION BY order_id ORDER BY changed_at) as prev_changed_at
    FROM [dbo].[orders_derived_events_external]
    WHERE order_id = 'ORD123'
)
SELECT order_id, status, prev_status,
       DATEDIFF(minute, prev_changed_at, changed_at) as duration_minutes
FROM status_changes
ORDER BY changed_at;
```
❌ **Problem**: Window functions over SINGLE entity require scanning multiple partitions
- order_id filter can't help with partition pruning
- Must fetch all events for that order across all date partitions

#### Query 2: "Which orders had status=active on Nov 13?"
```sql
SELECT DISTINCT order_id
FROM [dbo].[orders_derived_events_external]
WHERE event_date = '2025-11-13'
AND status = 'active'
AND change_type IN ('INSERT', 'UPDATE');
```
✅ **Works well** - partition pruning on event_date

### Assessment: SAME PATTERN WEAKNESS AS #1 AND #2

Derived events have same issues:
- Good for time-based queries
- Poor for entity-history queries

---

## Pattern 5: Hybrid CDC Cumulative (Derived State)

### Current Structure
```
effective_from_date=2025-11-13/
  derived_state.parquet
```

### Polybase External Table
```sql
CREATE EXTERNAL TABLE [dbo].[orders_derived_state_external] (
    [order_id] VARCHAR(255),
    [status] VARCHAR(255),
    [delta_tag] VARCHAR(255),
    [effective_from_dt] VARCHAR(255),
    [effective_to_dt] VARCHAR(255),
    [effective_from_date] VARCHAR(255)
)
WITH (
    LOCATION = 'pattern5_hybrid_cdc_cumulative/',
    ...
);
```

### Real-World Query Requirements

#### Query 1: "What was derived state of order ORD123 on Nov 11?"
```sql
SELECT * FROM [dbo].[orders_derived_state_external]
WHERE order_id = 'ORD123'
AND effective_from_date <= '2025-11-11'
AND (effective_to_dt IS NULL OR effective_to_dt > '2025-11-11')
ORDER BY effective_from_dt DESC
LIMIT 1;
```
❌ **Same problem as Pattern 3**: Full partition scan for entity-based query

#### Query 2: "Which derived states changed on Nov 13?"
```sql
SELECT DISTINCT order_id, status
FROM [dbo].[orders_derived_state_external]
WHERE effective_from_date = '2025-11-13'
ORDER BY order_id;
```
✅ **Works well** - partition pruning on effective_from_date

### Assessment: SAME CRITICAL ISSUE AS PATTERN 3

---

## Pattern 6: Hybrid Incremental Point (Derived State Latest)

### Current Structure
```
effective_from_date=2025-11-13/
  latest_state.parquet
```

### Real-World Query Requirements

#### Query 1: "What's the latest derived state for order ORD123?"
```sql
SELECT * FROM [dbo].[orders_latest_state_external]
WHERE order_id = 'ORD123'
AND is_current = 1;
```
❌ **Problem**: Partitioned by effective_from_date, but users want current state
- Partition value for "latest" is ambiguous
- Might need to scan multiple partitions to find the actual latest
- Should this be unpartitioned like state_current?

#### Query 2: "What's the latest state of all orders as of Nov 13?"
```sql
SELECT * FROM [dbo].[orders_latest_state_external]
WHERE effective_from_date <= '2025-11-13'
AND is_current = 1;
```
⚠️ **Works but awkward**: Partition pruning on date, but is_current should be the primary filter

### Assessment: DESIGN MISMATCH

**Question**: Should latest-only state be:
- **Option A** (Current): Partitioned by effective_from_date
  - Good for: Time-based queries
  - Bad for: "Give me current state" queries

- **Option B** (Better?): Unpartitioned single file (like state_current in Pattern 3)
  - Good for: "What's the current state?" - the most common query
  - Bad for: Time-range historical queries (but this pattern is LATEST_ONLY, not historical!)

---

## Pattern 7: Hybrid Incremental Cumulative (State SCD1)

### Current Structure
```
effective_from_date=2025-11-13/
  state_scd1.parquet
```

### Real-World Query Requirements

#### Query 1: "What's the current status of order ORD123?"
```sql
SELECT * FROM [dbo].[orders_state_scd1_external]
WHERE order_id = 'ORD123';
```
❌ **Problem**: Partitioned by effective_from_date, but SCD Type 1 doesn't track history
- Latest value is scattered across all date partitions
- Full scan required to find "current" (overwritten) state

#### Query 2: "Which orders have status=active?"
```sql
SELECT order_id, status
FROM [dbo].[orders_state_scd1_external]
WHERE status = 'active';
```
❌ **Same problem**: Full partition scan

#### Query 3: "Show me SCD Type 1 updates from Nov 13"
```sql
SELECT order_id, status
FROM [dbo].[orders_state_scd1_external]
WHERE effective_from_date = '2025-11-13';
```
✅ **Works** - partition pruning on date

### Assessment: FUNDAMENTAL DESIGN MISMATCH

**The Issue**: SCD Type 1 is a CURRENT-STATE table (no history)
- Should be unpartitioned (like state_current)
- Partitioning by effective_from_date doesn't help
- Users want: "Give me current status of entity X"
- Current structure provides: "Give me updates from date X"

**Recommendation**: Pattern 7 should store state_scd1.parquet UNPARTITIONED
```
v1/
  state_scd1.parquet  -- No partitions, always latest overwritten values
```

---

## Synthesis: Common Query Pattern Problems

### Problem 1: Entity-Based Queries on Date-Partitioned Tables ❌

**Affected Patterns**: 1, 2, 3, 4, 5
- All date-partitioned tables
- Most entity-history queries must scan all partitions
- No way to quickly find "all events/state for entity X"

**Solution Options**:
1. **Composite partitioning**: entity_hash_bucket + date
2. **Materialized views**: Create pre-aggregated views for common patterns
3. **Secondary index**: Let parquet handle internal organization
4. **Accept it**: Document that entity-history queries will scan partitions

### Problem 2: Current-State Tables Partitioned by Date ❌

**Affected Patterns**: 6, 7
- Pattern 6 (latest_only): Should be unpartitioned
- Pattern 7 (SCD1): Should be unpartitioned
- Current design optimizes for "changes on date X"
- Users care about "current state of entity X"

**Solution**: Make these unpartitioned (single file)

### Problem 3: Missing Natural Query Interface 🤔

**All Patterns**:
- Polybase DDL requires explicit partition column filters
- No way to express "most recent record for entity" without boilerplate
- Temporal functions help but are still workarounds

**Solution**: Generated temporal functions (already implemented!) partially address this

---

## Recommended Changes for v1.1

### Priority 1: Fix Current-State Tables (Easy Win)
- Pattern 6: `effective_from_date=` → `unpartitioned`
- Pattern 7: `effective_from_date=` → `unpartitioned`
- Changes: Remove partition columns from external table DDL
- Testing: Verify "current state" queries perform better

### Priority 2: Add Temporal Functions (Already Done!) ✅
- Addresses the boilerplate query problem
- Generated functions like `get_orders_state_external_as_of('2025-11-13')`
- Helps with entity-history pattern somewhat

### Priority 3: Document Entity-History Query Performance (v1.1)
- Add notes to Polybase integration guide
- Explain partition scan required
- Show how to optimize (composite partitioning is v2 feature)

### Priority 4: Consider Composite Partitioning (v2)
- Add entity_hash_bucket as secondary partition
- Would require Silver processor changes
- Would require new partition column derivation logic
- Would reduce partition scan space for entity-history queries

---

## Conclusion

**Current State Assessment**:
- ✅ Polybase integration works correctly
- ✅ Generated DDL is valid SQL
- ✅ Temporal functions provide good query interface
- ⚠️ Partition structure optimized for TIME queries, not ENTITY queries
- ❌ Patterns 6 & 7 should not be partitioned by date

**Immediate Action** (should be done now):
- [ ] Change Pattern 6 & 7 to use unpartitioned artifacts
- [ ] Update partition configuration in pattern YAMLs

**Future Work** (v1.1+):
- [ ] Add composite partitioning (entity_hash + date) for better entity-history performance
- [ ] Consider materialized views for common aggregations
- [ ] Document query performance characteristics per pattern

---

## Reference: Quick Pattern Query Grid

| Pattern | Name | Primary Table | Partition | Good For | Poor For |
|---------|------|---------------|-----------|----------|----------|
| 1 | Full Events | events | event_date | Time-range queries | Entity history |
| 2 | CDC Events | cdc_events | event_date | Time-range queries | Entity history |
| 3 | SCD2 History | state_history | effective_from_date | Time-range queries | Entity history |
| 3 | SCD2 Current | state_current | UNPARTITIONED | Current state | Historical queries |
| 4 | Derived Events | derived_events | event_date | Time-range queries | Entity history |
| 5 | Derived State | derived_state | effective_from_date | Time-range queries | Entity history |
| 6 | Latest Only | latest_state | **SHOULD BE UNPARTITIONED** | Current state | ~~Time-range~~ |
| 7 | SCD1 Current | state_scd1 | **SHOULD BE UNPARTITIONED** | Current state | ~~Time-range~~ |

