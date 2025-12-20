"""
TEMPLATE: Fixed-Width File
===========================

.. deprecated::
    **YAML is now the recommended format.** See `fixed_width.yaml` instead.
    Use Python templates only when you need custom logic or retry decorators.

Use this for files where each column is at a fixed character position:
- Mainframe reports
- Bank statements with positional fields
- Legacy feeds with no delimiters

To run: python -m pipelines {system}.{entity} --date 2025-01-15

File naming: Save as `pipelines/{system}_{entity}.py` or `pipelines/{system}/{entity}.py`
"""

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import EntityKind, SilverEntity

# ============================================================
# BRONZE: Load from fixed-width file
# ============================================================

bronze = BronzeSource(
    system="your_system",           # e.g., "mainframe", "legacy_bank"
    entity="your_entity",           # e.g., "daily_transactions", "statements"
    source_type=SourceType.FILE_FIXED_WIDTH,
    source_path="./data/{entity}_{run_date}.txt",

    # Fixed-width file configuration
    options={
        "csv_options": {
            # Column names (required for headerless files)
            "columns": ["txn_id", "account_id", "amount", "txn_date", "description"],

            # Define column widths (characters per field)
            "field_widths": [10, 20, 12, 10, 40],

            # OR use column specifications (start, end positions)
            # "column_specs": [(0, 10), (10, 30), (30, 42), (42, 52), (52, 92)],
        }
    },
)

# ============================================================
# SILVER: Curate to clean format
# ============================================================

silver = SilverEntity(
    natural_keys=["txn_id"],            # Primary key column
    change_timestamp="txn_date",        # Timestamp column

    # Transactions/events are immutable
    entity_kind=EntityKind.EVENT,
)

# ============================================================
# PIPELINE
# ============================================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver
