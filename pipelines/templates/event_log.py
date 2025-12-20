"""
TEMPLATE: Event Log / Fact Table
=================================

.. deprecated::
    **YAML is now the recommended format.** See `event_log.yaml` instead.
    Use Python templates only when you need custom logic or retry decorators.

Use this for immutable event/fact data:
- Click streams, page views
- Transaction logs
- Audit trails
- IoT sensor readings

Events are immutable - once recorded, they don't change.
Silver just deduplicates exact duplicates.

To run: python -m pipelines {system}.{entity} --date 2025-01-15

File naming: Save as `pipelines/{system}_{entity}.py` or `pipelines/{system}/{entity}.py`
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import EntityKind, SilverEntity

# ============================================================
# BRONZE: Load events from source
# ============================================================

bronze = BronzeSource(
    system="your_system",           # e.g., "clickstream", "iot", "audit"
    entity="your_entity",           # e.g., "page_views", "sensor_readings"
    source_type=SourceType.FILE_PARQUET,
    source_path="./data/{entity}/{run_date}/*.parquet",
)

# ============================================================
# SILVER: Curate events (dedupe only)
# ============================================================

silver = SilverEntity(
    # Event identity - what makes an event unique?
    natural_keys=["event_id"],          # Usually a UUID or composite key
    change_timestamp="event_time",      # When did the event occur?

    # Events are immutable - just dedupe exact duplicates
    entity_kind=EntityKind.EVENT,

    # Optional: specify columns to include
    # attributes=["user_id", "session_id", "page_url", "device_type"],

    # Optional: partition for efficient querying
    # partition_by=["event_date"],
)

# ============================================================
# PIPELINE
# ============================================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
