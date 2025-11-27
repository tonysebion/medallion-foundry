# Load Pattern Config Examples

These configurations demonstrate different data loading patterns used in the medallion architecture. Each pattern handles different types of data changes and historical tracking.

## Files

### Basic Patterns
- `pattern_full.yaml` - Full snapshot loading (complete dataset refresh)
- `pattern_cdc.yaml` - Change Data Capture (incremental changes only)
- `pattern_current_history.yaml` - Slowly Changing Dimensions (current + history)

### Hybrid Patterns
- `pattern_hybrid_cdc_point.yaml` - CDC with point-in-time views
- `pattern_hybrid_cdc_cumulative.yaml` - CDC with cumulative updates
- `pattern_hybrid_incremental_point.yaml` - Incremental loading with point-in-time
- `pattern_hybrid_incremental_cumulative.yaml` - Incremental loading with cumulative updates

### File-Based Examples
- `file_cdc_example.yaml` - File extraction with CDC pattern
- `file_current_history_example.yaml` - File extraction with SCD pattern

### Sample Data Patterns
- `sample_pattern_full.yaml` - Full pattern with sample data
- `sample_pattern_cdc.yaml` - CDC pattern with sample data
- `sample_pattern_scd_state.yaml` - SCD pattern with sample data

## Purpose

These configs illustrate:
- Different load pattern implementations
- How patterns affect Bronze and Silver processing
- Historical data management strategies
- Incremental vs full loading trade-offs

## Usage

Use these configs to understand how different data patterns work and choose the appropriate pattern for your use case. The sample data configs work with the generated test datasets.