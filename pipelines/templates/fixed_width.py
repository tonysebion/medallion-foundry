"""
Template: Fixed-Width File
==========================
Use this template for files where each column is at a fixed character position:
- Mainframe reports
- Bank statements with positional fields
- Legacy feeds with no delimiters

To run: python -m pipelines {system}.{entity} --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

# ============================================
# BRONZE: Load from fixed-width or character-delimited file
# ============================================

bronze = BronzeSource(
    system="legacy_mainframe",  # <-- CHANGE: Source system name
    entity="daily_transactions",  # <-- CHANGE: Entity/file name
    source_type=SourceType.FILE_FIXED_WIDTH,
    # Path to the file(s)
    # Use {run_date} for the date in the filename
    source_path="/data/mainframe/exports/txn_{run_date}.txt",  # <-- CHANGE
    options={
        "csv_options": {
            # Column names (required for headerless files)
            "columns": [
                "txn_id",
                "account_id",
                "amount",
                "txn_date",
                "description",
            ],  # <-- CHANGE
            # Fixed-width hints:
            # "field_widths": [10, 20, 8, 19, 30],
            # "column_specs": [(0, 10), (10, 30), (30, 38), (38, 57), (57, 87)],
            # As a reminder, any whitespace-delimited file can also use
            # SourceType.FILE_SPACE_DELIMITED with "delimiter" or "delim_whitespace".
        }
    },
    target_path="s3://bronze/system={system}/entity={entity}/dt={run_date}/",
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate to clean format
# ============================================

silver = SilverEntity(
    source_path="s3://bronze/system=legacy_mainframe/entity=daily_transactions/dt={run_date}/*.parquet",
    target_path="s3://silver/legacy/transactions/",  # <-- CHANGE
    natural_keys=["txn_id"],  # <-- CHANGE: Primary key
    change_timestamp="txn_date",  # <-- CHANGE: Timestamp column
    entity_kind=EntityKind.EVENT,  # Transactions are events
    history_mode=HistoryMode.CURRENT_ONLY,
    # Optionally specify columns to include
    # attributes=["account_id", "amount", "description"],
)


def run_bronze(run_date: str, **kwargs):
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
