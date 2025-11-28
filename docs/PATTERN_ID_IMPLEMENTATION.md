# Pattern ID Implementation Guide

## Overview

This document describes the implementation of **pattern_id** configuration field and the **sample=** prefix for clear traceability across source, Bronze, and Silver layers in the medallion-foundry architecture.

## Problem Statement

Previously, sample data was organized using fixed pattern folder names embedded directly in the generation scripts. This made it difficult to:
1. Evolve naming from educational patterns (pattern1_full_events) to realistic domain names (retail_pos_api)
2. Trace data lineage across layers (source → bronze → silver)
3. Maintain consistent naming across all three layers

## Solution: Configurable Pattern ID with Sample Prefix

### Key Changes

#### 1. **Added `pattern_id` to Configuration Schema** 

Updated `core/config/typed_models.py`:
```python
class SourceConfig(BaseModel):
    pattern_id: Optional[str] = None  # Configurable identifier for tracing across layers
    type: SourceType = SourceType.api
    system: str
    table: str
    # ... rest of fields
```

#### 2. **Updated All Pattern Configuration Files**

Added `pattern_id` field to all 8 pattern configs in `docs/examples/configs/patterns/`:
- `pattern_full.yaml` - pattern_id: pattern1_full_events
- `pattern_cdc.yaml` - pattern_id: pattern2_cdc_events
- `pattern_current_history.yaml` - pattern_id: pattern3_scd_state
- `pattern_hybrid_cdc_point.yaml` - pattern_id: pattern4_hybrid_cdc_point
- `pattern_hybrid_cdc_cumulative.yaml` - pattern_id: pattern5_hybrid_cdc_cumulative
- `pattern_hybrid_incremental_point.yaml` - pattern_id: pattern6_hybrid_incremental_point
- `pattern_hybrid_incremental_cumulative.yaml` - pattern_id: pattern7_hybrid_incremental_cumulative

```yaml
environment: dev
pattern_id: pattern1_full_events  # Unique identifier for tracing across source → bronze → silver
domain: retail_demo
system: retail_demo
entity: orders
# ... rest of config
```

#### 3. **Implemented Sample Prefix in Directory Structure**

All three layers now use explicit `sample={pattern_id}` prefix:

```
source_samples/
  sample=pattern1_full_events/
    system=retail_demo/
      table=orders/
        dt=YYYY-MM-DD/
          *.csv

bronze_samples/
  sample=pattern1_full_events/
    system=retail_demo/
      table=orders/
        dt=YYYY-MM-DD/
          *.csv, _metadata.json

silver_samples/
  sample=pattern1_full_events/
    silver_model=incremental_merge/
      domain=retail_demo/
        entity=orders/
          v1/
            load_date=YYYY-MM-DD/
              {partitions}/
                *.parquet, _metadata.json
```

#### 4. **Updated Sample Generation Scripts**

**`scripts/generate_sample_data.py`**:
- Removed redundant `pattern=` subfolder from path construction
- Updated all four generation functions:
  - `generate_full_snapshot()` 
  - `generate_cdc()`
  - `generate_current_history()`
  - `generate_hybrid_combinations()`
- Updated `_write_pattern_readmes()` to create pattern READMEs under `sample=` prefix
- Result: Clean source/bronze hierarchy without redundant folders

**`scripts/generate_silver_samples.py`**:
- Updated `_find_bronze_partitions()` to look for `sample=` prefix instead of `pattern=`
- Updated `_generate_for_partition()` to use `sample={pattern_id}` structure
- Enhanced `SILVER_MODEL_MAP` to include additional entity_kind mappings:
  - `("event", None)` → "incremental_merge"
  - `("derived_event", None)` → "incremental_merge"
  - `("derived_state", None)` → "scd_type_1"
- Fixed Unicode encoding issue in final output message

#### 5. **Fixed Schema Compatibility**

Updated `docs/examples/configs/patterns/pattern_cdc.yaml`:
- Changed `schema_mode: strict` → `schema_mode: allow_new_columns`
- Allows CDC pattern to handle schema evolution (extra columns like 'note' added on day 2)

## Benefits

### 1. **Traceability**
Looking at any layer, you can immediately see which pattern a sample belongs to:
```
sample=pattern1_full_events → pattern1_full_events pattern config
```

### 2. **Evolution Path**
Easy to migrate from educational to realistic naming in **one place**:

```yaml
# Before (Educational)
pattern_id: pattern1_full_events

# After (Realistic)  
pattern_id: retail_pos_api_transactions
```

All generated samples automatically use the new name without script changes.

### 3. **Clear Hierarchy**
The medallion structure is now visually consistent across layers:
- source_samples uses `sample=` prefix
- bronze_samples uses `sample=` prefix  
- silver_samples uses `sample=` prefix (plus additional silver_model level)

### 4. **Production-Ready Alignment**
Demonstrates how real production environments would structure sample data, making it easier to build on top of this framework.

## Usage Examples

### Listing All Samples by Pattern

```bash
# See all pattern samples
ls sampledata/source_samples/sample=*/

# Output:
# sample=pattern1_full_events/
# sample=pattern2_cdc_events/
# sample=pattern3_scd_state/
# ... etc
```

### Running Extraction with Pattern ID

The pattern_id is primarily for organization. Generation scripts handle it automatically:

```bash
# Generate source and bronze samples
python scripts/generate_sample_data.py

# Generate silver samples  
python scripts/generate_silver_samples.py --formats parquet
```

### Adapting to Your Domain

To customize for your domain:

1. **Update pattern config:**
```yaml
# docs/examples/configs/patterns/my_config.yaml
environment: prod
pattern_id: my_domain_my_dataset  # Your pattern ID
domain: my_domain
system: my_system
entity: my_entity
# ... rest of config
```

2. **Regenerate samples:**
```bash
python scripts/generate_sample_data.py
python scripts/generate_silver_samples.py --formats parquet
```

3. **Result:**
```
sampledata/
  source_samples/sample=my_domain_my_dataset/...
  bronze_samples/sample=my_domain_my_dataset/...
  silver_samples/sample=my_domain_my_dataset/...
```

## Migration from Old Structure

If you have existing samples using the old structure:

1. **Old structure** (flat, pattern-based):
   ```
   source_samples/pattern1_full_events/system=.../table=.../pattern=pattern1_full_events/dt=.../
   bronze_samples/pattern1_full_events/system=.../table=.../pattern=pattern1_full_events/dt=.../
   ```

2. **New structure** (hierarchical, sample-prefixed):
   ```
   source_samples/sample=pattern1_full_events/system=.../table=.../dt=.../
   bronze_samples/sample=pattern1_full_events/system=.../table=.../dt=.../
   ```

3. **Migration steps:**
   - Backup existing samples
   - Delete sampledata/ directory
   - Run `python scripts/generate_sample_data.py` to regenerate
   - Run `python scripts/generate_silver_samples.py --formats parquet` to regenerate

## Configuration Reference

### Pattern ID Field

**Location:** Top-level in config YAML  
**Type:** String (optional)  
**Default:** None (scripts derive from config filenames)  
**Purpose:** Unique identifier for tracing data across layers

```yaml
pattern_id: my_unique_pattern_identifier
```

### Sample Prefix Convention

**Format:** `sample={pattern_id}`  
**Used in:** All three layers (source, bronze, silver)  
**Example:** `sample=retail_pos_api_orders`

**Benefits:**
- Explicit: Clearly marked as sample data
- Consistent: Used across all layers
- Flexible: Easy to change by updating one field

## Testing the Implementation

```bash
# Generate sample data
python scripts/generate_sample_data.py

# Verify source structure
ls sampledata/source_samples/sample=pattern*/
# Output: 7 sample=pattern* directories

# Verify bronze structure mirrors source
ls sampledata/bronze_samples/sample=pattern*/
# Output: 7 sample=pattern* directories

# Generate silver samples
python scripts/generate_silver_samples.py --formats parquet

# Verify silver structure
ls sampledata/silver_samples/sample=pattern*/silver_model=*/
# Output: Multiple silver_model directories per pattern
```

## Implementation Details

### Files Modified

1. **Configuration Schema**
   - `core/config/typed_models.py` - Added pattern_id field

2. **Configuration Files**
   - `docs/examples/configs/patterns/*.yaml` - Added pattern_id values

3. **Generation Scripts**
   - `scripts/generate_sample_data.py` - Updated path construction
   - `scripts/generate_silver_samples.py` - Updated partition discovery and generation

### Pattern Mappings

The following entity_kind values are mapped to Silver models:

| entity_kind | history_mode | Silver Model |
|------------|--------------|--------------|
| state | scd2 | scd_type_2 |
| state | scd1 | scd_type_1 |
| state | None | scd_type_1 |
| event | incremental | incremental_merge |
| event | None | incremental_merge |
| events | None | incremental_merge |
| snapshot | None | periodic_snapshot |
| derived_event | None | incremental_merge |
| derived_state | None | scd_type_1 |
| hybrid | incremental | incremental_merge |
| hybrid | cumulative | full_merge_dedupe |

## Troubleshooting

### Issue: "Bronze partitions not found"
**Cause:** Generation script looks for `sample=` prefix  
**Solution:** Run `python scripts/generate_sample_data.py` first

### Issue: "Unexpected columns in Bronze data"  
**Cause:** Schema strictness in config vs actual data  
**Solution:** Set `schema_mode: allow_new_columns` in silver section

### Issue: Unicode errors in output
**Cause:** Terminal doesn't support emoji characters  
**Solution:** Already fixed in generate_silver_samples.py (uses ASCII indicators)

## Future Enhancements

Potential improvements for future versions:

1. **Dynamic pattern_id assignment** - Auto-generate from config metadata
2. **Pattern registry** - Central mapping of pattern IDs to configs
3. **Sample dataset documentation** - Auto-generate READMEs with lineage info
4. **Validation tools** - Verify sample=prefix consistency across layers
5. **Migration utilities** - Scripts to convert old structure to new

## Related Documentation

- [Configuration Reference](docs/framework/reference/CONFIG_REFERENCE.md)
- [Load Patterns](docs/usage/patterns/pattern_matrix.md)
- [Silver Layer Reference](docs/framework/reference/SILVER_REFERENCE.md)
- [Sample Data Overview](docs/examples/README.md)
