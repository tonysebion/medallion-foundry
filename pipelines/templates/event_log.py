"""
Template: Event Log / Fact Table
================================
Use this template for immutable event/fact data:
- Click streams
- Transaction logs
- Audit trails
- IoT sensor readings

Events are immutable - once recorded, they don't change.
Silver just deduplicates exact duplicates.

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# BRONZE: Load events from source
# ============================================

bronze = BronzeSource(
    system="clickstream",  # <-- CHANGE: Source system name
    entity="page_views",  # <-- CHANGE: Event type
    source_type=SourceType.FILE_PARQUET,  # <-- CHANGE if needed
    source_path="s3://raw/clickstream/page_views/dt={run_date}/*.parquet",  # <-- CHANGE
    options={},
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate events (dedupe only)
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system=clickstream/entity=page_views/dt={run_date}/*.parquet",
    target_path="s3://silver/clickstream/page_views/",  # <-- CHANGE
    # Event identity - what makes an event unique?
    natural_keys=["event_id"],  # <-- CHANGE: Usually a UUID or composite key
    # When did the event occur?
    change_timestamp="event_timestamp",  # <-- CHANGE
    # KEY: Events are immutable
    entity_kind=EntityKind.EVENT,
    # For events, we don't need SCD2 - just dedupe exact duplicates
    history_mode=HistoryMode.CURRENT_ONLY,
    # Common event columns
    attributes=[
        "user_id",
        "session_id",
        "page_url",
        "referrer",
        "device_type",
        "event_timestamp",
    ],  # <-- CHANGE: Your event attributes
    # Partition by date for efficient querying
    partition_by=["event_date"],  # <-- CHANGE: Derived date column
)


def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
