"""
TEMPLATE: Incremental Load with Watermark
==========================================
Use this for incremental/delta loads:
- Only fetch new records since last run
- Uses a watermark column (e.g., LastUpdated) to track progress

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import HistoryMode, SilverEntity

# ============================================================
# BRONZE: Incremental extract from database
# ============================================================

bronze = BronzeSource(
    system="your_system",           # e.g., "orders_db", "sales_dw"
    entity="your_entity",           # e.g., "orders", "transactions"
    source_type=SourceType.DATABASE_MSSQL,

    # Database connection (top-level params)
    host="${DB_HOST}",              # Use env var for host
    database="YourDatabase",        # Database name
    query="""
        SELECT OrderID, CustomerID, Amount, Status, LastUpdated
        FROM dbo.Orders
        WHERE LastUpdated > ?
    """,                            # Query with ? placeholder for watermark

    # Incremental load settings
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="LastUpdated",  # Column to track for incremental
)

# ============================================================
# SILVER: Curate with full history (SCD Type 2)
# ============================================================

silver = SilverEntity(
    natural_keys=["OrderID"],           # Primary key column(s)
    change_timestamp="LastUpdated",     # When was the record changed?
    history_mode=HistoryMode.FULL_HISTORY,  # Keep all versions (SCD Type 2)
)

# ============================================================
# PIPELINE
# ============================================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
