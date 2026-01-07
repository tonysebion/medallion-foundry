# Roadmap: Future Features

This document captures aspirational features that are not yet implemented. These ideas may be implemented in future versions.

## Silver Layer Enhancements

### Additional Entity Kinds
Currently supported: `STATE`, `EVENT`

**Potential additions:**
- `derived_state` - Build state from event streams (e.g., current status from status change events)
- `derived_event` - Derive events from state changes (e.g., generate change events from snapshots)

### Schema Mode
Control schema evolution behavior:
- `strict` - Fail on any schema changes
- `allow_new_columns` - Auto-add new columns from source

### Silver Joins
Cross-entity joins in the Silver layer:
- Reference data enrichment
- Dimension lookup during curation
- Multi-source entity building

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

## Future Enhancements

### Gold Layer
Complete the medallion architecture with a Gold layer for business-level aggregations:
- Pre-aggregated metrics and KPIs
- Business-specific data models
- Consumption-ready datasets

### Orchestration Integration
Native integration with workflow orchestrators:
- Airflow operators/hooks
- Prefect tasks
- Dagster assets
- Scheduled pipeline execution

### Schema Registry
Centralized schema management:
- Schema versioning and history
- Compatibility checking (backward, forward, full)
- Schema evolution tracking across pipelines

### Lineage Tracking
Full data lineage capabilities:
- Pipeline-to-pipeline lineage graph
- Column-level lineage
- Impact analysis for schema changes
- Visualization of data flow

### Notifications and Alerting
Pipeline event notifications:
- Webhook callbacks on success/failure
- Email notifications
- Slack/Teams integration
- Custom alerting rules

### Pipeline Dependencies
DAG-style pipeline orchestration:
- Dependency graph between pipelines
- Automatic triggering of downstream pipelines
- Cross-pipeline scheduling
