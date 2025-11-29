# Data Validation Test Coverage Analysis

**Purpose:** Assess the current test suite's ability to validate data correctness from source → bronze → silver, with special attention to Polybase query readiness.

**Status:** Comprehensive analysis complete
**Date:** 2025-11-28

---

## Executive Summary

### ✅ What's Being Tested

Your test suite includes excellent coverage for:
- Source data → Bronze transformation (row count, column preservation)
- Business logic correctness (deduplication, CDC change tracking, timestamp handling)
- Metadata consistency (batch IDs, pipeline tracking)
- Basic Polybase structure validation (external table definitions)

### ⚠️ Gaps Identified

1. **End-to-End Data Correctness:** No tests validating Silver data actually matches source/Bronze data at the row level
2. **Partition Pruning Validation:** No tests verifying partition structures enable proper Polybase optimization
3. **Polybase Query Simulation:** No tests that actually execute SQL-like predicates against Parquet files to validate queries work
4. **Pattern-Specific Semantics:** Limited validation of pattern-specific business rules (SCD2 state transitions, CDC completeness)
5. **Temporal Correctness:** No validation of temporal predicates working correctly (point-in-time, time ranges)

---

## Current Test Files Overview

### 1. test_silver_data_correctness_patterns_1_2.py (491 lines)

**What It Tests:**
- ✅ Source → Bronze row preservation
- ✅ Timestamp parsing and date extraction
- ✅ Event deduplication (no duplicates on natural_key)
- ✅ Partition structure existence
- ✅ Metadata column presence
- ✅ CDC change_type distribution
- ✅ Append-log history preservation
- ⚠️ Basic Polybase external table column requirements

**Gap:** Tests check columns exist but NOT that they contain correct values

```python
# Current test checks columns exist:
assert set(silver_df.columns) >= required_cols
# MISSING: Validate column VALUES match source/Bronze expectations
```

**Example Missing Test:**
```python
def test_pattern1_silver_data_values_match_source(tmp_path: Path):
    """Verify each Silver row is correctly transformed from source."""
    source_df = load_source_csv(...)
    silver_df = load_silver_parquet(...)

    # Check a sample order exists with correct status/total
    sample_order = source_df.iloc[0]["order_id"]
    silver_row = silver_df[silver_df["order_id"] == sample_order]

    assert silver_row["status"].iloc[0] == source_df.iloc[0]["status"]
    assert silver_row["order_total"].iloc[0] == source_df.iloc[0]["order_total"]
    # ... validate all attributes match
```

### 2. test_bronze_sample_integrity.py (150+ lines)

**What It Tests:**
- ✅ Source → Bronze row count matching
- ✅ Column presence in Bronze
- ✅ Metadata file generation
- ✅ Value distribution matching (null counts, unique value counts)

**Gap:** Does NOT test Silver transformations, only Bronze

**Strength:** Uses `_value_counter()` to compare null handling:
```python
def _value_counter(series: pd.Series) -> Counter[str]:
    """Compare null handling between source and Bronze."""
    values = (
        NULL_SENTINEL if pd.isna(value) else str(value)
        for value in series.astype(object)
    )
    return Counter(values)
```

### 3. test_integration_samples.py (150+ lines)

**What It Tests:**
- ✅ Bronze sample path construction
- ✅ Configuration rewriting for tests
- ✅ Bronze partition metadata generation
- ⚠️ Bronze → Silver artifact creation (minimal validation)

**Gap:** Creates Silver artifacts but doesn't validate their content

### 4. test_silver_samples_generation.py (26 lines)

**What It Tests:**
- ✅ Silver samples are generated
- ✅ Output directory exists

**Gap:** Only existence check, no content validation

### 5. test_polybase_generator.py (476 lines)

**What It Tests:**
- ✅ Polybase configuration generation from DatasetConfig
- ✅ DDL statement generation
- ✅ Temporal function generation
- ✅ Partition column identification
- ✅ Sample query generation

**Gap:** Tests generation logic, NOT that queries actually work on real data

**Example:**
```python
def test_polybase_setup_event_entity(tmp_path: Path):
    """Verify DDL is generated correctly."""
    setup = generate_polybase_setup(dataset)
    # Checks: external_data_source.name, partition_columns, etc.
    assert "event_date" in ext_table.partition_columns
    # MISSING: Verify query against actual Parquet files would work
```

---

## Test Coverage Matrix

| Layer | Aspect | Testing | Status | Gap Description |
|-------|--------|---------|--------|-----------------|
| **Source→Bronze** | Row count | test_bronze_sample_integrity.py | ✅ Good | None |
| | Columns | test_bronze_sample_integrity.py | ✅ Good | None |
| | Values | test_bronze_sample_integrity.py | ⚠️ Limited | Only null/unique count comparison |
| | Nulls | test_bronze_sample_integrity.py | ✅ Good | _value_counter validates |
| **Bronze→Silver** | Row count | test_silver_data_correctness_patterns_1_2.py | ⚠️ Limited | Only dedup validation |
| | Columns | test_silver_data_correctness_patterns_1_2.py | ✅ Good | |
| | Values | test_silver_data_correctness_patterns_1_2.py | ❌ Missing | No value match testing |
| | Metadata | test_silver_data_correctness_patterns_1_2.py | ✅ Good | batch_id, pipeline_run_at |
| **Polybase** | DDL generation | test_polybase_generator.py | ✅ Good | |
| | External tables | test_polybase_generator.py | ✅ Good | |
| | Partitions | test_polybase_generator.py | ✅ Good | |
| | Query execution | ❌ Missing | No tests execute actual queries |
| **Temporal** | Point-in-time | ❌ Missing | No test validates as_of() works |
| | Time range | ❌ Missing | No test validates date_range() works |
| | CDC ordering | test_silver_data_correctness_patterns_1_2.py | ⚠️ Limited | Only timestamp parsing |
| **Patterns** | SCD2 state trans. | ❌ Missing | No test validates history chaining |
| | SCD1 overwrites | ❌ Missing | No test validates latest-only |
| | Event dedup | test_silver_data_correctness_patterns_1_2.py | ✅ Good | |
| | CDC completeness | test_silver_data_correctness_patterns_1_2.py | ⚠️ Limited | Only change_type counts |

---

## Missing Test Scenarios

### 1. End-to-End Silver Data Correctness (HIGH PRIORITY)

**What It Should Do:**
- Load source, Bronze, and Silver data
- Match specific rows across layers
- Validate transformations applied correctly

**Example:**
```python
def test_pattern1_silver_row_matches_source():
    """Validate each Silver row is correct transformation of source."""
    source = load_source_csv()        # 100 rows
    silver = load_silver_parquet()    # Should still be 100 rows (after dedup)

    for idx in range(min(10, len(source))):  # Sample 10 rows
        src_row = source.iloc[idx]
        slv_rows = silver[silver["order_id"] == src_row["order_id"]]

        # Should have exactly 1 row per order (dedup)
        assert len(slv_rows) == 1
        slv_row = slv_rows.iloc[0]

        # All attributes should match
        assert slv_row["status"] == src_row["status"]
        assert slv_row["order_total"] == src_row["order_total"]
        assert slv_row["customer_id"] == src_row["customer_id"]
```

**Impact:** Catches data corruption, transformation bugs
**Effort:** Medium (need to handle partitions, column mappings)

### 2. Polybase Query Simulation (HIGH PRIORITY)

**What It Should Do:**
- Create mock Polybase queries using Pandas
- Execute against Silver Parquet files
- Validate results match expectations

**Example:**
```python
def test_pattern1_polybase_query_events_by_date():
    """Simulate Polybase query: SELECT * WHERE event_date = '2025-11-15'"""
    silver_df = load_silver_parquet()

    # Create 'event_date' column from event_ts
    silver_df["event_date"] = pd.to_datetime(silver_df["updated_at"]).dt.date

    # Simulate Polybase query
    target_date = "2025-11-15"
    result = silver_df[silver_df["event_date"] == pd.to_datetime(target_date).date()]

    # Validate results
    assert len(result) > 0, "Query returned no rows"
    assert all(result["event_date"] == pd.to_datetime(target_date).date())
    assert all(col in result.columns for col in ["order_id", "status", "updated_at"])
```

**Impact:** Validates partition structure works for queries
**Effort:** Medium (need to handle multiple Parquet files across partitions)

### 3. Partition Pruning Validation (MEDIUM PRIORITY)

**What It Should Do:**
- Verify partition columns actually separate data correctly
- Validate that Polybase partition pruning would work

**Example:**
```python
def test_pattern1_partition_pruning_effectiveness():
    """Verify event_date partitions actually separate data."""
    # Read all Parquet files and verify partition isolation
    partition_dirs = find_partition_directories()

    for partition_dir in partition_dirs:
        date_from_path = extract_date_from_partition(partition_dir)
        parquet_files = partition_dir.glob("*.parquet")

        for file in parquet_files:
            df = pd.read_parquet(file)
            df["event_date"] = pd.to_datetime(df["updated_at"]).dt.date

            # All rows in partition should have that date
            assert all(df["event_date"] == date_from_path), \
                f"Partition {partition_dir} contains data from wrong dates"
```

**Impact:** Ensures partition strategy actually works
**Effort:** High (need to understand partition directory structure per pattern)

### 4. Temporal Predicate Testing (MEDIUM PRIORITY)

**What It Should Do:**
- Test point-in-time queries (SCD2 states)
- Test time-range queries (events)
- Test temporal ordering (CDC)

**Example for SCD2:**
```python
def test_pattern3_polybase_query_as_of_date():
    """Simulate: SELECT * FROM state WHERE ... AS OF '2025-11-15'"""
    silver_df = load_silver_parquet()

    target_date = pd.to_datetime("2025-11-15").date()

    # Point-in-time query logic
    result = silver_df[
        (pd.to_datetime(silver_df["effective_from_dt"]).dt.date <= target_date) &
        ((silver_df["effective_to_dt"].isna()) |
         (pd.to_datetime(silver_df["effective_to_dt"]).dt.date > target_date))
    ]

    # Validate: one row per entity (the active record as of that date)
    assert result.drop_duplicates(subset=["order_id"]).shape[0] == result.shape[0]

    assert len(result) > 0, "As-of query returned no rows"
```

**Impact:** Validates temporal semantics
**Effort:** Medium (need SCD2 logic understanding)

### 5. Pattern-Specific Business Rule Validation (LOW PRIORITY)

**What It Should Do:**
- SCD2: Validate state transition chains
- SCD1: Validate only current values stored
- CDC: Validate change ordering

**Example for SCD2 state chain:**
```python
def test_pattern3_scd2_state_chain_validity():
    """Validate SCD2 state history forms valid chains."""
    silver_df = load_silver_parquet()

    for order_id in silver_df["order_id"].unique():
        history = silver_df[silver_df["order_id"] == order_id].sort_values("effective_from_dt")

        # Check chain continuity
        for idx in range(1, len(history)):
            prev_row = history.iloc[idx - 1]
            curr_row = history.iloc[idx]

            # Previous record's effective_to should match current's effective_from
            prev_to = pd.to_datetime(prev_row["effective_to_dt"]).date()
            curr_from = pd.to_datetime(curr_row["effective_from_dt"]).date()

            assert prev_to == curr_from, \
                f"SCD2 chain break for order {order_id}"
```

**Impact:** Catches SCD logic bugs
**Effort:** Medium (complex state machine logic)

### 6. CDC Completeness Validation (LOW PRIORITY)

**What It Should Do:**
- Verify CDC captures all change events
- Validate change ordering is preserved
- Check no changes are lost

**Example:**
```python
def test_pattern2_cdc_all_changes_captured():
    """Validate CDC doesn't miss any state changes."""
    source_df = load_source_csv()
    silver_df = load_silver_parquet()

    # For each order, trace changes
    for order_id in source_df["order_id"].unique():
        source_changes = source_df[source_df["order_id"] == order_id].sort_values("changed_at")
        silver_changes = silver_df[silver_df["order_id"] == order_id].sort_values("changed_at")

        # Should have same number of changes
        assert len(silver_changes) >= len(source_changes), \
            f"CDC lost changes for order {order_id}"

        # All source changes should appear in silver (exact match)
        for src_change in source_changes.itertuples():
            matching = silver_changes[
                (silver_changes["status"] == src_change.status) &
                (silver_changes["changed_at"] == src_change.changed_at)
            ]
            assert len(matching) > 0, \
                f"CDC missing change: {src_change}"
```

**Impact:** Catches CDC logic bugs
**Effort:** Low (straightforward comparison)

---

## Recommended Test Implementation Roadmap

### Phase 1: Critical (Do First)
1. **End-to-End Silver Data Correctness** (HIGH)
   - Tests that Silver rows actually match source/Bronze data
   - Catches transformation bugs early
   - ~200 lines of test code

2. **Polybase Query Simulation** (HIGH)
   - Tests that Polybase queries would actually work
   - Validates partition structure
   - Most important for Polybase integration
   - ~300 lines of test code

### Phase 2: Important
3. **Partition Pruning Validation** (MEDIUM)
   - Verifies partition strategy effectiveness
   - Ensures query optimization works
   - ~200 lines of test code

4. **Temporal Predicate Testing** (MEDIUM)
   - Tests point-in-time and time-range logic
   - ~300 lines of test code

### Phase 3: Nice to Have
5. **Pattern-Specific Validation** (LOW)
   - SCD state chains, CDC completeness
   - ~200 lines of test code

---

## Code Patterns for New Tests

### Pattern A: Load and Compare Structure

```python
def _load_and_compare(source_path, silver_dir):
    """Helper: Load source CSV and Silver Parquet, return aligned DFs."""
    source_df = pd.read_csv(source_path, dtype=str, keep_default_na=False)

    # Load all Silver partitions
    all_files = list(Path(silver_dir).rglob("*.parquet"))
    silver_frames = [pd.read_parquet(f) for f in sorted(all_files)]
    silver_df = pd.concat(silver_frames, ignore_index=True) if silver_frames else pd.DataFrame()

    return source_df, silver_df
```

### Pattern B: Row-Level Validation

```python
def _validate_row_matches(source_row, silver_row, attribute_cols):
    """Helper: Validate a single source row matches its Silver equivalent."""
    for col in attribute_cols:
        src_val = str(source_row[col]) if pd.notna(source_row[col]) else None
        slv_val = str(silver_row[col]) if pd.notna(silver_row[col]) else None

        assert src_val == slv_val, \
            f"Column {col} mismatch: source={src_val}, silver={slv_val}"
```

### Pattern C: Polybase Query Simulation

```python
def _simulate_polybase_query(df, partition_col, partition_value):
    """Helper: Simulate Polybase partition pruning."""
    if partition_col and partition_col in df.columns:
        result = df[df[partition_col] == partition_value]
        return result
    return df
```

---

## Test Execution Strategy

### For CI/CD Integration

```python
# Add to pytest.ini or pyproject.toml
[pytest]
markers =
    unit: unit tests
    integration: integration tests
    data_correctness: data validation tests (slower)
    polybase: Polybase query simulation tests

# Run data validation tests before Polybase deployment
pytest tests/ -m "data_correctness or polybase" -v
```

### For Local Development

```bash
# Run just the new data correctness tests
pytest tests/test_silver_data_correctness_*.py -v

# Run with coverage
pytest tests/ -m "data_correctness" --cov=core --cov-report=term-missing

# Run specific pattern
pytest tests/test_silver_data_correctness_patterns_1_2.py::test_pattern1_silver_row_matches_source -v
```

---

## Implementation Priority Matrix

| Test | Effort | Impact | Priority |
|------|--------|--------|----------|
| End-to-End Data Correctness | Medium | Critical | 🔴 DO FIRST |
| Polybase Query Simulation | Medium | Critical | 🔴 DO FIRST |
| Partition Pruning Validation | High | High | 🟠 DO SECOND |
| Temporal Predicate Testing | Medium | Medium | 🟡 DO THIRD |
| Pattern-Specific Validation | Medium | Low | 🟢 NICE TO HAVE |
| CDC Completeness | Low | Low | 🟢 NICE TO HAVE |

---

## Conclusion

Your current test suite has **good coverage for basic transformations** but is **missing critical validation for actual Polybase query execution**.

### Immediate Action Items

1. ✅ **Add end-to-end data validation tests** that verify Silver data actually matches source
2. ✅ **Add Polybase query simulation tests** that simulate actual queries against Silver Parquet
3. ✅ **Validate partition structure effectiveness** for query optimization
4. ✅ **Test temporal predicates** (point-in-time, time-ranges)

### Why This Matters

Before deploying Silver artifacts to production Polybase:
- You need to be 100% confident data is correct (not just metadata)
- You need to validate the queries users will write actually work
- You need to ensure partition optimization actually works
- You need to test temporal semantics for temporal tables

**Recommended effort:** 2-3 days of focused test development
**Expected outcome:** Comprehensive data correctness validation before Polybase deployment

