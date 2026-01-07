# Roadmap: Future Features

This document captures aspirational features that are not yet implemented. These ideas may be implemented in future versions.

## Silver Layer Enhancements

### Additional Entity Kinds
Currently supported: `STATE`, `EVENT`

**Potential additions:**
- `derived_state` - Build state from event streams (e.g., current status from status change events)
- `derived_event` - Derive events from state changes (e.g., generate change events from snapshots)

### Delete Mode
Handle deletions/missing keys in source data:
- `ignore` - Missing keys are simply absent (default)
- `tombstone_state` - Insert tombstone records for deleted entities
- `tombstone_event` - Insert delete events for removed records

### Schema Mode
Control schema evolution behavior:
- `strict` - Fail on any schema changes
- `allow_new_columns` - Auto-add new columns from source

### Input Mode
Control how Bronze feeds are interpreted:
- `append_log` - Each Bronze partition is additive
- `replace_daily` - Each Bronze partition is a full replacement

### Silver Models
Pre-built transformation patterns:
- `periodic_snapshot` - Simple periodic refresh, no deduplication
- `full_merge_dedupe` - Deduplicated current view (SCD Type 1)
- `incremental_merge` - CDC stream processing with change tracking
- `scd_type_2` - Full history with effective dates and is_current flags

## Bronze Layer Enhancements

### CDC Load Pattern
The `CDC` load pattern enum exists but is not fully implemented. Would need:
- Insert/update/delete marker handling
- Change data capture stream processing
- Merge operations for target updates

## Query Layer Integration

### Trino/Iceberg
Phase 2 migration path from PolyBase to Trino with Iceberg tables:
- Automatic schema evolution
- Time travel queries
- Better partitioning strategies

### Enhanced PolyBase
Additional PolyBase features:
- Automatic partition management
- Schema synchronization
- Multi-table DDL generation

## Operational Features

### Config Doctor
CLI tool to validate configuration health:
- Check connection strings
- Validate paths exist
- Verify credentials
- Test source connectivity

### Silver Joins
Cross-entity joins in the Silver layer:
- Reference data enrichment
- Dimension lookup during curation
- Multi-source entity building
