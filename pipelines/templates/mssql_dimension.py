"""
Template: SQL Server Dimension Table (SCD Type 1)
=================================================
Use this template for dimension tables from SQL Server:
- Customer, Product, Account, Employee, etc.
- Keeps only the latest version of each record

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# BRONZE: Extract from SQL Server
# ============================================

bronze = BronzeSource(
    system="your_system",  # <-- CHANGE: Source system name
    entity="your_table",  # <-- CHANGE: Table name
    source_type=SourceType.DATABASE_MSSQL,
    source_path="",
    options={
        "connection_name": "your_db",  # <-- CHANGE: Unique name for this connection
        "host": "${YOUR_DB_HOST}",  # <-- CHANGE: Server hostname (or env var)
        "database": "YourDatabase",  # <-- CHANGE: Database name
        # Uncomment to use custom query:
        # "query": """
        #     SELECT CustomerID, Name, Email, Status, LastUpdated
        #     FROM dbo.Customers
        #     WHERE IsActive = 1
        # """,
    },
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate as SCD Type 1
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system=your_system/entity=your_table/dt={run_date}/*.parquet",
    target_path="s3://silver/your_domain/your_entity/",  # <-- CHANGE
    natural_keys=["YourPrimaryKey"],  # <-- CHANGE: What makes a record unique?
    change_timestamp="LastUpdated",  # <-- CHANGE: When was the record last changed?
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,  # SCD Type 1
)


def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
