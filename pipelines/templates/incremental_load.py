"""
Template: Incremental Load with Watermark
=========================================
Use this template for incremental/delta loads:
- Only fetch new records since last run
- Uses a watermark column (e.g., LastUpdated) to track progress

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode
from pipelines.lib.resilience import with_retry

# ============================================
# BRONZE: Incremental extract
# ============================================

bronze = BronzeSource(
    system="orders_db",  # <-- CHANGE: Source system name
    entity="orders",  # <-- CHANGE: Table name
    source_type=SourceType.DATABASE_MSSQL,  # <-- CHANGE if needed
    source_path="",
    options={
        "connection_name": "orders_db",  # <-- CHANGE
        "host": "${ORDERS_DB_HOST}",  # <-- CHANGE
        "database": "OrdersDB",  # <-- CHANGE
        # The query should include a placeholder for the watermark
        "query": """
            SELECT OrderID, CustomerID, OrderTotal, Status, LastUpdated
            FROM dbo.Orders
            WHERE LastUpdated > ?
        """,  # <-- CHANGE: Add your query with ? for watermark
    },
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    # KEY: Use incremental pattern with watermark
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="LastUpdated",  # <-- CHANGE: Your timestamp column
)

# ============================================
# SILVER: Curate with SCD Type 2 history
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system=orders_db/entity=orders/dt={run_date}/*.parquet",
    target_path="s3://silver/sales/orders/",  # <-- CHANGE
    natural_keys=["OrderID"],  # <-- CHANGE: Primary key column(s)
    change_timestamp="LastUpdated",  # <-- CHANGE
    entity_kind=EntityKind.STATE,
    # Keep full history for auditing
    history_mode=HistoryMode.FULL_HISTORY,  # SCD Type 2
)


# Optional: Add retry for flaky database connections
@with_retry(max_attempts=3, backoff_seconds=5.0)
def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
