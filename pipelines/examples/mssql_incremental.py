"""
Example Pipeline: MSSQL Incremental Load
=========================================
Demonstrates incremental loading from a SQL Server database with watermark tracking.

This example shows:
- Database extraction with MSSQL
- Incremental load pattern using watermarks
- Environment variable expansion for secrets
- SCD Type 1 curation

Prerequisites:
- SQL Server database accessible
- Environment variables set:
  - DB_HOST: Database host
  - DB_USER: Database user
  - DB_PASSWORD: Database password

To run:
    python -m pipelines examples.mssql_incremental --date 2025-01-15
"""

from pathlib import Path

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
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
    # Use environment variable expansion for secrets
    options={
        "host": "${DB_HOST}",
        "database": "SalesDB",
        "user": "${DB_USER}",
        "password": "${DB_PASSWORD}",
    },
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
    target_path=str(OUTPUT_DIR / "bronze" / "sales_dw" / "order_details"),
    load_pattern=LoadPattern.INCREMENTAL_APPEND,
    watermark_column="UpdatedAt",
)

# ============================================
# SILVER: Curate order details (SCD Type 1)
# ============================================

silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "sales_dw" / "order_details" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "sales" / "order_details"),
    natural_keys=["OrderDetailID"],
    change_timestamp="UpdatedAt",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["OrderID", "ProductID", "Quantity", "UnitPrice", "Discount"],
)


@with_retry(max_attempts=3, backoff_seconds=2.0)
def run_bronze(run_date: str, **kwargs):
    """Extract order details from SQL Server with retry on transient failures."""
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze order details to Silver."""
    return silver.run(run_date, **kwargs)


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
