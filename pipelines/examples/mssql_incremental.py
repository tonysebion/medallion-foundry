"""
Example Pipeline: MSSQL Incremental Load
=========================================
Demonstrates incremental loading from a SQL Server database with watermark tracking.

This example shows:
- Database extraction with MSSQL (SourceType.DATABASE_MSSQL)
- Incremental load pattern using watermarks (LoadPattern.INCREMENTAL_APPEND)
- Environment variable expansion for secrets (${DB_HOST} syntax)
- SCD Type 1 curation

Setup:
    1. Set environment variables:
        export DB_HOST=your-server.database.windows.net
        export DB_USER=your_username
        export DB_PASSWORD=your_password

    2. Ensure the database has an 'order_details' table with an 'UpdatedAt' column

Test connectivity:
    python -m pipelines test-connection sales_db --host $DB_HOST --database SalesDB

Run:
    python -m pipelines examples.mssql_incremental --date 2025-01-15

Dry run (validate configuration):
    python -m pipelines examples.mssql_incremental --date 2025-01-15 --dry-run

Check connectivity:
    python -m pipelines examples.mssql_incremental --date 2025-01-15 --check
"""

from pathlib import Path

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import SilverEntity
from pipelines.lib.resilience import with_retry

# Output paths
OUTPUT_DIR = Path("./output")

# ============================================
# BRONZE: Incremental load from SQL Server
# ============================================

bronze = BronzeSource(
    system="sales_dw",
    entity="order_details",
    source_type=SourceType.DATABASE_MSSQL,

    # Database connection using top-level params
    host="${DB_HOST}",
    database="SalesDB",
    query="""
        SELECT
            OrderDetailID,
            OrderID,
            ProductID,
            Quantity,
            UnitPrice,
            Discount,
            UpdatedAt
        FROM Sales.OrderDetails
        WHERE UpdatedAt > :watermark
        ORDER BY UpdatedAt
    """,

    # Incremental load settings
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="UpdatedAt",
)

# ============================================
# SILVER: Curate order details (SCD Type 1)
# ============================================

silver = SilverEntity(
    natural_keys=["OrderDetailID"],
    change_timestamp="UpdatedAt",
    attributes=["OrderID", "ProductID", "Quantity", "UnitPrice", "Discount"],
)

# ============================================
# PIPELINE with retry support
# ============================================

pipeline = Pipeline(bronze=bronze, silver=silver)


# Add retry wrapper for flaky database connections
@with_retry(max_attempts=3, backoff_seconds=2.0)
def run_bronze(run_date: str, **kwargs):
    """Extract order details from SQL Server with retry on transient failures."""
    return pipeline.run_bronze(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze order details to Silver."""
    return pipeline.run_silver(run_date, **kwargs)


def run(run_date: str, **kwargs):
    """Run full pipeline: Bronze → Silver with retry."""
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}


if __name__ == "__main__":
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Running MSSQL incremental pipeline for {run_date}...")
    print("Note: Requires DB_HOST, DB_USER, DB_PASSWORD environment variables")

    try:
        result = run(run_date)
        print("\nResults:")
        print(f"  Bronze: {result['bronze'].get('row_count', 0)} rows")
        print(f"  Silver: {result['silver'].get('row_count', 0)} rows")
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure environment variables are set and database is accessible.")
        sys.exit(1)
