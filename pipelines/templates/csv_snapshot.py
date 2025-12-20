"""
TEMPLATE: CSV File Full Snapshot
================================
Use this for CSV files that are full snapshots:
- Daily export files from legacy systems
- Files that contain all records each time

To run: python -m pipelines {system}.{entity} --date 2025-01-15

File naming: Save as `pipelines/{system}_{entity}.py` or `pipelines/{system}/{entity}.py`
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import SilverEntity

# ============================================================
# BRONZE: Load from CSV file
# ============================================================

bronze = BronzeSource(
    system="your_system",       # e.g., "legacy_erp", "salesforce"
    entity="your_entity",       # e.g., "customers", "orders"
    source_type=SourceType.FILE_CSV,
    source_path="./data/{entity}_{run_date}.csv",
)

# ============================================================
# SILVER: Curate with deduplication
# ============================================================

silver = SilverEntity(
    natural_keys=["id"],            # What makes a record unique?
    change_timestamp="updated_at",  # When was the record last changed?
)

# ============================================================
# PIPELINE
# ============================================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
