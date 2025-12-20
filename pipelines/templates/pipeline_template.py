"""
PIPELINE TEMPLATE: Copy this file and fill in the blanks
=========================================================

To run:
    python -m pipelines {system}.{entity} --date 2025-01-15

To test:
    python -m pipelines {system}.{entity} --date 2025-01-15 --dry-run

To validate:
    python -m pipelines {system}.{entity} --date 2025-01-15 --check
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity

# ============================================================
# BRONZE: Where does the data come from?
# ============================================================

bronze = BronzeSource(
    # SYSTEM: The source system name (e.g., "salesforce", "erp", "legacy_db")
    system="your_system",

    # ENTITY: The table or file name (e.g., "customers", "orders", "products")
    entity="your_entity",

    # SOURCE TYPE: What kind of source is this?
    # Options: SourceType.FILE_CSV, SourceType.FILE_PARQUET,
    #          SourceType.DATABASE_MSSQL, SourceType.DATABASE_POSTGRES,
    #          SourceType.API_REST
    source_type=SourceType.FILE_CSV,

    # SOURCE PATH: Where to find the data
    # For files: path to the file (use {run_date} for date-based files)
    # For databases: leave empty (connection info below)
    source_path="./data/your_file_{run_date}.csv",

    # DATABASE CONNECTION (only for database sources):
    # host="${DB_HOST}",           # Database host (use env var)
    # database="YourDatabase",     # Database name
    # query="SELECT * FROM table", # Optional custom SQL

    # LOAD PATTERN: How to load the data?
    # Options: LoadPattern.FULL_SNAPSHOT (replace all each run)
    #          LoadPattern.INCREMENTAL_APPEND (only new records)
    load_pattern=LoadPattern.FULL_SNAPSHOT,

    # WATERMARK: For incremental loads, which column tracks changes?
    # watermark_column="updated_at",
)

# ============================================================
# SILVER: How should data be curated?
# ============================================================

silver = SilverEntity(
    # NATURAL KEYS: What makes a record unique? (the primary key)
    # Examples: ["id"], ["customer_id"], ["order_id", "line_id"]
    natural_keys=["id"],

    # CHANGE TIMESTAMP: Which column has the "last updated" time?
    # This is used to detect changes and order records
    change_timestamp="updated_at",

    # ENTITY KIND: What type of data is this?
    # EntityKind.STATE = Dimension (customer, product, account) - can change
    # EntityKind.EVENT = Fact/Log (orders, clicks, payments) - immutable
    entity_kind=EntityKind.STATE,

    # HISTORY MODE: How to handle changes over time?
    # HistoryMode.CURRENT_ONLY = SCD Type 1 (only keep latest version)
    # HistoryMode.FULL_HISTORY = SCD Type 2 (keep all versions)
    history_mode=HistoryMode.CURRENT_ONLY,

    # OPTIONAL: Specify which columns to include (default: all)
    # attributes=["name", "email", "status"],
)

# ============================================================
# PIPELINE: Wire it all together
# ============================================================

# Pipeline auto-wires Bronze → Silver and generates run functions
pipeline = Pipeline(bronze=bronze, silver=silver)

# Export run functions for CLI discovery
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
