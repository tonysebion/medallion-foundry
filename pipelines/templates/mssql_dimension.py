"""
TEMPLATE: SQL Server Dimension Table (SCD Type 1)
==================================================
Use this for dimension tables from SQL Server:
- Customer, Product, Account, Employee, etc.
- Keeps only the latest version of each record

Prerequisites:
    export DB_HOST="your-server.database.com"  # Database host

To run: python -m pipelines {system}.{entity} --date 2025-01-15

File naming: Save as `pipelines/{system}_{entity}.py` or `pipelines/{system}/{entity}.py`
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import SilverEntity

# ============================================================
# BRONZE: Extract from SQL Server
# ============================================================

bronze = BronzeSource(
    system="your_system",           # e.g., "crm", "erp", "hr_db"
    entity="your_table",            # e.g., "customers", "products"
    source_type=SourceType.DATABASE_MSSQL,

    # Database connection (top-level params)
    host="${DB_HOST}",              # Use env var for host
    database="YourDatabase",        # Database name

    # Optional: custom SQL query (uncomment if needed)
    # query="""
    #     SELECT CustomerID, Name, Email, Status, LastUpdated
    #     FROM dbo.Customers
    #     WHERE IsActive = 1
    # """,
)

# ============================================================
# SILVER: Curate as SCD Type 1
# ============================================================

silver = SilverEntity(
    natural_keys=["id"],                # What makes a record unique?
    change_timestamp="LastUpdated",     # When was the record last changed?
    # SCD Type 1 is the default (only keep latest version)
)

# ============================================================
# PIPELINE
# ============================================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
