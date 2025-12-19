"""
Template: CSV File Full Snapshot
================================
Use this template for CSV files that are full snapshots:
- Daily export files from legacy systems
- Files that contain all records each time

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# BRONZE: Load from CSV file
# ============================================

bronze = BronzeSource(
    system="legacy_system",  # <-- CHANGE: Source system name
    entity="daily_export",  # <-- CHANGE: Entity/file name
    source_type=SourceType.FILE_CSV,
    # Path to the CSV file
    # Use {run_date} for the date in the filename
    source_path="/data/exports/daily_export_{run_date}.csv",  # <-- CHANGE
    options={
        "csv_options": {
            # Uncomment and modify as needed:
            # "header": True,  # Does file have a header row?
            # "delimiter": ",",  # What's the delimiter?
            # "encoding": "utf-8",
        }
    },
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate with deduplication
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system=legacy_system/entity=daily_export/dt={run_date}/*.parquet",
    target_path="s3://silver/your_domain/your_entity/",  # <-- CHANGE
    natural_keys=["record_id"],  # <-- CHANGE: What makes a record unique?
    change_timestamp="updated_at",  # <-- CHANGE: When was the record changed?
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    # Optionally specify which columns to include
    # attributes=["col1", "col2", "col3"],
)


def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
