# Polybase Query Analysis & Pattern Structural Corrections

## Executive Summary

By deeply analyzing what Polybase queries would actually look like in SSMS for each of the 7 patterns, we discovered critical structural misalignments in Patterns 6 and 7. These patterns store CURRENT STATE ONLY (no history), but were configured with date-partitioning that optimizes for historical queries.

**Key Finding**: Partition structure should follow the QUERY PATTERN, not the data pattern.

---

## The Discovery

### What We Found

When we asked "What would a user actually query?" for each pattern, we discovered:

| Pattern | Name | History Mode | Current Design | Issue | Fix |
|---------|------|--------------|-----------------|-------|-----|
| 1 | Full Events | N/A | Partitioned by event_date | ✅ Good for time queries | ✓ Keep |
| 2 | CDC Events | N/A | Partitioned by event_date | ✅ Good for time queries | ✓ Keep |
| 3 | SCD2 History | SCD2 | Partitioned by effective_from_date | ⚠️ Okay but heavy for entity queries | ✓ Keep (historical data) |
| 3 | SCD2 Current | SCD2 | Unpartitioned | ✅ Perfect for current state | ✓ Keep |
| 4 | Derived Events | N/A | Partitioned by event_date | ✅ Good for time queries | ✓ Keep |
| 5 | Derived State | SCD2 | Partitioned by effective_from_date | ⚠️ Okay but heavy for entity queries | ✓ Keep (historical data) |
| 6 | Latest Only | **LATEST_ONLY** | **Partitioned by effective_from_date** | ❌ **WRONG** - Stores current state only | ✅ **FIX: Unpartition** |
| 7 | SCD1 Current | **SCD1** | **Partitioned by effective_from_date** | ❌ **WRONG** - Stores current state with overwrites | ✅ **FIX: Unpartition** |

### Why Patterns 6 & 7 Were Wrong

#### Pattern 6 (Latest Only - Derived State)
```sql
-- What users actually query
SELECT * FROM [dbo].[orders_latest_state_external]
WHERE order_id = 'ORD123';  -- "Give me current state of this order"

-- But with date partition, query becomes:
SELECT * FROM [dbo].[orders_latest_state_external]
WHERE order_id = 'ORD123'
AND effective_from_date <= CAST(GETDATE() AS DATE)  -- Must filter by date!
AND is_current = 1;

-- Problems:
-- 1. Users don't think about "effective_from_date" for CURRENT state
-- 2. Latest state could be in any partition
-- 3. Partition pruning is awkward and unintuitive
```

**Better Query with Unpartitioned Structure**:
```sql
SELECT * FROM [dbo].[orders_latest_state_external]
WHERE order_id = 'ORD123';  -- Clean and simple!
```

#### Pattern 7 (SCD Type 1 - State with Overwrites)
```sql
-- SCD Type 1 OVERWRITES old values - no history!
-- What users actually query
SELECT status, customer_id
FROM [dbo].[orders_scd1_external]
WHERE order_id = 'ORD123';  -- "What's the current status?"

-- But with date partition, latest value is scattered across dates:
-- If ORD123 was updated on Nov 10, 11, 12, 13...
-- Latest value is in effective_from_date=2025-11-13
-- But query doesn't know what the "latest" date is!

-- Users would need to:
SELECT TOP 1 status, customer_id
FROM [dbo].[orders_scd1_external]
WHERE order_id = 'ORD123'
ORDER BY effective_from_date DESC;

-- Problems:
-- 1. SCD1 shouldn't have history - why partition by date?
-- 2. Latest value discovery requires full scan
-- 3. Partition structure doesn't match semantic intent
```

**Better Query with Unpartitioned Structure**:
```sql
SELECT status, customer_id
FROM [dbo].[orders_scd1_external]
WHERE order_id = 'ORD123';  -- Simple! Single value per order
```

---

## What We Changed

### Before (Incorrect)

#### pattern_hybrid_incremental_point.yaml (Pattern 6)
```yaml
silver:
  entity_kind: derived_state
  history_mode: latest_only
  partition_by:
    - change_ts_dt  # ❌ WRONG - why partition if only storing current?
  record_time_partition: "effective_from_date"  # ❌ WRONG
```

#### pattern_hybrid_incremental_cumulative.yaml (Pattern 7)
```yaml
silver:
  entity_kind: state
  history_mode: scd1
  partition_by:
    - change_ts_dt  # ❌ WRONG - SCD1 overwrites, no history
  record_time_partition: "effective_from_date"  # ❌ WRONG
```

### After (Correct)

#### pattern_hybrid_incremental_point.yaml (Pattern 6)
```yaml
silver:
  entity_kind: derived_state
  history_mode: latest_only
  partition_by: []  # ✅ CORRECT - No partitions, single unpartitioned artifact
  record_time_partition: null  # ✅ CORRECT - No partitioning
  # Comment explains WHY
  # IMPORTANT: latest_only stores CURRENT state only, not historical
  # Users query: "What's the current state of entity X?" - no partitioning needed
```

#### pattern_hybrid_incremental_cumulative.yaml (Pattern 7)
```yaml
silver:
  entity_kind: state
  history_mode: scd1
  partition_by: []  # ✅ CORRECT - SCD1 overwrites only, single file
  record_time_partition: null  # ✅ CORRECT - No partitioning
  # Comment explains WHY
  # IMPORTANT: SCD Type 1 stores CURRENT state only with overwrites
  # Users query: "What's the current status of order X?" - no partitioning needed
  # SCD1 overwrites old values, so partition by change date doesn't make sense
```

---

## Impact on Polybase DDL Generation

### Pattern 6 External Table - BEFORE
```sql
CREATE EXTERNAL TABLE [dbo].[orders_latest_state_external] (
    [status] VARCHAR(255),
    [effective_from_date] VARCHAR(255)  -- ❌ Partition column present
)
WITH (
    LOCATION = 'pattern6_hybrid_incremental_point/',
    ...
);
```

### Pattern 6 External Table - AFTER
```sql
CREATE EXTERNAL TABLE [dbo].[orders_latest_state_external] (
    [status] VARCHAR(255)  -- ✅ No partition columns
)
WITH (
    LOCATION = 'pattern6_hybrid_incremental_point/',
    ...
);
```

### Pattern 7 External Table - BEFORE
```sql
CREATE EXTERNAL TABLE [dbo].[orders_scd1_external] (
    [status] VARCHAR(255),
    [customer_id] VARCHAR(255),
    [changed_at] VARCHAR(255),
    [effective_from_date] VARCHAR(255)  -- ❌ Partition column present
)
WITH (
    ...
);
```

### Pattern 7 External Table - AFTER
```sql
CREATE EXTERNAL TABLE [dbo].[orders_scd1_external] (
    [status] VARCHAR(255),
    [customer_id] VARCHAR(255),
    [changed_at] VARCHAR(255)  -- ✅ No partition columns
)
WITH (
    ...
);
```

---

## Broader Insights: The Entity-History Query Problem

While Patterns 6 & 7 had an obvious structural issue, all patterns reveal a deeper query pattern weakness:

### Pattern Classes

**Historical Patterns** (Patterns 1, 2, 4):
- Event tables partitioned by event_date
- ✅ Good for: "What happened on date X?"
- ⚠️ Bad for: "What's the full history of entity X?" - must scan all partitions

**State History Patterns** (Patterns 3, 5):
- State history tables partitioned by effective_from_date
- ✅ Good for: "What changed on date X?"
- ⚠️ Bad for: "What's the history of entity X?" - must scan all partitions to find all records

**Current State Patterns** (Patterns 6, 7):
- Should NOT be partitioned at all
- ✅ Good for: "What's the current state of entity X?"

### The Fundamental Issue

Users think about queries in ENTITY TERMS ("Give me history of order ORD123"), but data is organized in TIME TERMS (partitioned by date).

This mismatch means:
1. Entity-history queries must scan all date partitions
2. No partition pruning benefit for single-entity analysis
3. Performance degradation for entity-focused queries

### Recommended Solution Path (v1.1+)

**v1 Status**: Acceptable - temporal functions and documentation help bridge the gap

**v1.1 Options**:
1. **Composite Partitioning**: Add entity_hash_bucket as secondary partition
   - Allows scanning relevant date ranges within entity subset
   - Would require Silver processor changes
   - Cost: More directories, complexity

2. **Materialized Views**: Pre-aggregate common entity queries
   - Orders with all their events pre-grouped
   - Orders with complete state history
   - Cost: Storage duplication, freshness management

3. **Accept & Document**:
   - Entity-history queries will scan partitions
   - Provide guidance in Polybase docs
   - Recommend temporal functions for common patterns
   - Cost: User awareness, potential performance issues for large datasets

**For now**: The analysis document (polybase_query_analysis.md) documents these tradeoffs clearly.

---

## Testing & Verification

### All Tests Pass ✅
```
tests/test_polybase_generator.py: 10/10 PASSED
tests/test_config.py: 17/17 PASSED
tests/test_silver_processor.py: 6/6 PASSED
Total: 33/33 PASSED
```

### Pattern Structure Verification
- Pattern 6 (latest_only): ✅ Correctly generates unpartitioned external table
- Pattern 7 (SCD1): ✅ Correctly generates unpartitioned external table
- Patterns 1-5: ✅ Continue to work as designed

### Generated DDL Quality
- Pattern 1-5: Partition columns correctly included in CREATE EXTERNAL TABLE
- Pattern 6-7: NO partition columns in CREATE EXTERNAL TABLE (correct!)
- Temporal functions: Still generated appropriately for state patterns

---

## Files Changed

1. **docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml**
   - Changed `partition_by: [change_ts_dt]` → `partition_by: []`
   - Changed `record_time_partition: "effective_from_date"` → `record_time_partition: null`
   - Added explanatory comments

2. **docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml**
   - Changed `partition_by: [change_ts_dt]` → `partition_by: []`
   - Changed `record_time_partition: "effective_from_date"` → `record_time_partition: null`
   - Added explanatory comments

3. **docs/design/polybase_query_analysis.md** (NEW)
   - Comprehensive analysis of all 7 patterns
   - Real-world Polybase query examples
   - Identifies strengths and weaknesses
   - Recommends future improvements

---

## Why This Matters

### Correctness
Patterns 6 & 7 now have partition structures that match their semantic intent:
- **Pattern 6 (latest_only)**: Stores CURRENT derived state → unpartitioned ✓
- **Pattern 7 (SCD1)**: Stores CURRENT dimension with overwrites → unpartitioned ✓

### Queryability
Users can now write intuitive Polybase queries without fighting partition structure:
```sql
-- Before: Awkward partition filter needed for current state
SELECT * FROM [orders_scd1_external]
WHERE order_id = 'ORD123'
AND effective_from_date = (SELECT MAX(effective_from_date) FROM [orders_scd1_external])

-- After: Natural query
SELECT * FROM [orders_scd1_external]
WHERE order_id = 'ORD123'
```

### DDL Cleanliness
Generated SQL external table definitions now correctly reflect the artifact structure:
- Partition columns in DDL only when partitioning actually exists
- Eliminates confusion about how to query current-state tables

---

## Next Steps

### Immediate (Already Complete)
- ✅ Fixed Pattern 6 & 7 partition structure
- ✅ Updated pattern YAML files with explanatory comments
- ✅ All tests passing
- ✅ Generated Polybase DDL correctly reflects new structure
- ✅ Created comprehensive analysis document

### Short-term (Documentation)
- Update Polybase integration guide to explain pattern-specific characteristics
- Add section on query performance implications
- Document the entity-history query challenge and workarounds

### Medium-term (v1.1 Planning)
- Evaluate composite partitioning (entity_hash + date)
- Consider materialized view patterns
- Measure performance of entity-history queries on large datasets

### Long-term (v2+)
- Implement composite partitioning if performance requires
- Support multiple schema versions per entity
- Potentially support schema evolution with view translation

---

## Summary of Key Insights

1. **Partition Structure = Query Interface**
   - Structure should follow how users think about the data
   - Current-state tables should not be partitioned by date
   - Historical tables can be partitioned by date, but causes entity-query overhead

2. **The 7 Patterns Fall Into 3 Classes**
   - Events (1, 2, 4): Time-partitioned, many events per entity
   - Histories (3, 5): Time-partitioned, many versions per entity
   - Current (3 current, 6, 7): Should be unpartitioned, single value per entity

3. **Polybase Queries Expose Design Issues**
   - Thinking through actual SQL queries reveals structural misalignments
   - Time-partitioning works well for "what changed on date X?"
   - But fails for "what's the history of entity X?"

4. **Temporal Functions Bridge the Gap (v1)**
   - Generated functions hide boilerplate and partition logic
   - Provides intuitive "as of" interface
   - Sufficient for v1, but not a long-term solution

5. **Documentation is Critical**
   - Users need to understand partition characteristics
   - Clear examples for each pattern type
   - Performance expectations and workarounds

---

## Conclusion

By thinking through **actual Polybase queries** that users would write in SSMS, we discovered and corrected structural issues in 2 of 7 patterns. This validates the user's key insight:

> "I'm thinking that you'll discover new layout issues when you start to think about the polybase queries that need to be created for the data structures to support them being presented accurately through the sql query interface in SSMS."

The framework is now more correctly aligned with how users will actually query the data through Polybase external tables.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
