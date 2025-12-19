"""
Example Pipeline: Retail Orders
================================
Demonstrates a complete Bronze → Silver pipeline using local CSV files.

This example shows:
- Loading from CSV files
- SCD Type 1 (current only) curation
- Full snapshot load pattern

To test:
1. Create sample data:
   python -c "from pipelines.examples.retail_orders import create_sample_data; \
   create_sample_data()"

2. Run pipeline:
   python -m pipelines examples.retail_orders --date 2025-01-15
"""

from pathlib import Path

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity

# Get the directory where sample data will be stored
SAMPLE_DIR = Path(__file__).parent / "sample_data"

# Build paths
_bronze_target = str(
    SAMPLE_DIR / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"
)
_silver_source = str(
    SAMPLE_DIR
    / "bronze"
    / "system=retail"
    / "entity=orders"
    / "dt={run_date}"
    / "*.parquet"
)

# ============================================
# BRONZE: Load orders from CSV
# ============================================

bronze = BronzeSource(
    system="retail",
    entity="orders",
    source_type=SourceType.FILE_CSV,
    source_path=str(SAMPLE_DIR / "orders_{run_date}.csv"),
    target_path=_bronze_target,
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate orders (SCD Type 1)
# ============================================

silver = SilverEntity(
    source_path=_silver_source,
    target_path=str(SAMPLE_DIR / "silver" / "retail" / "orders"),
    natural_keys=["order_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["customer_id", "order_total", "status"],
)


def run_bronze(run_date: str, **kwargs):
    """Extract orders from CSV to Bronze."""
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze orders to Silver."""
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    """Run full pipeline: Bronze → Silver."""
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}


def create_sample_data(run_date: str = "2025-01-15") -> Path:
    """Create sample CSV data for testing.

    Returns the path to the created file.
    """
    import csv

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    sample_orders = [
        {
            "order_id": "ORD001",
            "customer_id": "CUST01",
            "order_total": "150.00",
            "status": "completed",
            "updated_at": "2025-01-15T10:00:00",
        },
        {
            "order_id": "ORD002",
            "customer_id": "CUST02",
            "order_total": "89.99",
            "status": "pending",
            "updated_at": "2025-01-15T11:30:00",
        },
        {
            "order_id": "ORD003",
            "customer_id": "CUST01",
            "order_total": "225.50",
            "status": "shipped",
            "updated_at": "2025-01-15T14:00:00",
        },
        {
            "order_id": "ORD004",
            "customer_id": "CUST03",
            "order_total": "45.00",
            "status": "completed",
            "updated_at": "2025-01-15T09:15:00",
        },
        {
            "order_id": "ORD005",
            "customer_id": "CUST02",
            "order_total": "310.25",
            "status": "pending",
            "updated_at": "2025-01-15T16:45:00",
        },
    ]

    file_path = SAMPLE_DIR / f"orders_{run_date}.csv"

    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sample_orders[0].keys())
        writer.writeheader()
        writer.writerows(sample_orders)

    print(f"Created sample data: {file_path}")
    return file_path


if __name__ == "__main__":
    # Quick test: create sample data and run pipeline
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Creating sample data for {run_date}...")
    create_sample_data(run_date)

    print(f"\nRunning pipeline for {run_date}...")
    result = run(run_date)

    print("\nResults:")
    print(f"  Bronze: {result['bronze'].get('row_count', 0)} rows")
    print(f"  Silver: {result['silver'].get('row_count', 0)} rows")
