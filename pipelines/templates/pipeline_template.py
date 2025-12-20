"""
PIPELINE TEMPLATE: Copy this file and fill in the blanks
=========================================================

.. deprecated::
    **YAML is now the recommended format.** See `pipeline_template.yaml` instead.
    Use Python templates only when you need custom logic or retry decorators.

To run:
    python -m pipelines {system}.{entity} --date 2025-01-15

To test:
    python -m pipelines {system}.{entity} --date 2025-01-15 --dry-run

To validate:
    python -m pipelines {system}.{entity} --date 2025-01-15 --check

FILE LOCATION & NAMING
----------------------
Place pipeline files in the `pipelines/` directory (not in templates/ or examples/).
The CLI uses dot notation to find your pipeline:

  Option A: Flat file with underscores
    File: pipelines/claims_header.py
    Run:  python -m pipelines claims_header --date 2025-01-15

  Option B: Nested in subdirectory
    File: pipelines/claims/header.py
    Run:  python -m pipelines claims.header --date 2025-01-15

SECRETS & ENVIRONMENT VARIABLES
-------------------------------
NEVER hardcode passwords, API keys, or tokens in pipeline files.
Use ${VAR_NAME} syntax - values are expanded at runtime:

    host="${DB_HOST}",
    password="${DB_PASSWORD}",

Set variables before running:
    # Linux/Mac
    export DB_HOST="server.example.com"
    export DB_PASSWORD="secret"

    # Windows PowerShell
    $env:DB_HOST = "server.example.com"
    $env:DB_PASSWORD = "secret"

    # Windows Command Prompt
    set DB_HOST=server.example.com
    set DB_PASSWORD=secret

Pipeline fails with clear error if required variables are missing.
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
