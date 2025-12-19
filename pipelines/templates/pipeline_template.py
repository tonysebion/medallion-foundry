"""
============================================
PIPELINE TEMPLATE: Copy this file and fill in the blanks
============================================
File: pipelines/{system}/{entity}.py

To run: python -m pipelines {system}.{entity} --date 2025-01-15
============================================
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# STEP 1: BRONZE - Where does the data come from?
# ============================================

bronze = BronzeSource(
    # What system is this from?
    system="claims_dw",  # <-- CHANGE THIS
    # What table/entity?
    entity="claims_header",  # <-- CHANGE THIS
    # What type of source?
    # Options: SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES,
    #          SourceType.FILE_CSV, SourceType.FILE_PARQUET,
    #          SourceType.FILE_SPACE_DELIMITED, SourceType.API_REST
    source_type=SourceType.DATABASE_MSSQL,  # <-- CHANGE THIS
    # For file sources: path to the file(s)
    # Use {run_date} placeholder for the date
    source_path="",  # <-- CHANGE THIS (for file sources)
    # Connection details (for database sources)
    options={
        "connection_name": "claims_db",  # <-- CHANGE THIS (reuses connections)
        "host": "${CLAIMS_DB_HOST}",  # <-- CHANGE THIS (or use env var)
        "database": "ClaimsDB",  # <-- CHANGE THIS
        # Optional: custom SQL query
        # "query": "SELECT * FROM dbo.YourTable WHERE LastUpdated > ?",
    },
    # Where to land the data (usually don't change this pattern)
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    # How to load?
    # Options: LoadPattern.FULL_SNAPSHOT (replace all each run),
    #          LoadPattern.INCREMENTAL_APPEND (only new records)
    load_pattern=LoadPattern.FULL_SNAPSHOT,  # <-- CHANGE THIS
    # For incremental: which column tracks changes?
    watermark_column=None,  # <-- e.g., "LastUpdated" for incremental
)

# ============================================
# STEP 2: SILVER - How should data be curated?
# ============================================

silver = SilverEntity(
    # Where to read from (Bronze output)
    source_path="s3://bronze/system=claims_dw/entity=claims_header/dt={run_date}/*.parquet",
    # Where to write
    target_path="s3://silver/claims/header/",  # <-- CHANGE THIS
    # What makes a record unique? (the primary key)
    natural_keys=["ClaimID"],  # <-- CHANGE THIS (list of column names)
    # Which column has the "last updated" timestamp?
    change_timestamp="LastUpdated",  # <-- CHANGE THIS
    # What kind of entity is this?
    # EntityKind.STATE = Dimension (customer, product, account)
    # EntityKind.EVENT = Fact/Transaction (orders, clicks, payments)
    entity_kind=EntityKind.STATE,  # <-- CHANGE THIS
    # How to handle history?
    # HistoryMode.CURRENT_ONLY = SCD Type 1 (just keep latest)
    # HistoryMode.FULL_HISTORY = SCD Type 2 (keep all versions)
    history_mode=HistoryMode.CURRENT_ONLY,  # <-- CHANGE THIS
    # Which columns to include? (None = all columns)
    attributes=None,  # <-- Or: ["Column1", "Column2", "Column3"]
    # How to partition the output? (None = no partitioning)
    partition_by=None,  # <-- Or: ["Year", "Month"]
)

# ============================================
# STEP 3: RUN FUNCTIONS (don't change these)
# ============================================


def run_bronze(run_date: str, **kwargs):
    """Extract data from source to Bronze."""
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze data to Silver."""
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    """Run full pipeline: Bronze → Silver."""
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
