"""
Example Pipeline: Customer SCD Type 2
=====================================
Demonstrates SCD Type 2 (full history) curation for tracking changes over time.

This example shows:
- Loading customer data from CSV (SourceType.FILE_CSV)
- SCD Type 2 history with effective dates (HistoryMode.FULL_HISTORY)
- How records get effective_from, effective_to, and is_current columns

Setup:
    Option 1 - Generate all sample data:
        python -m pipelines generate-samples

    Option 2 - Generate data for this example only:
        python -c "from pipelines.examples.customer_scd2 import create_sample_data; create_sample_data()"

Run:
    python -m pipelines examples.customer_scd2 --date 2025-01-15

Output columns in Silver:
    - customer_id: Natural key
    - name, email, status: Tracked attributes
    - effective_from: When this version became valid
    - effective_to: When this version expired (NULL if current)
    - is_current: True for the latest version of each customer
"""

import csv
from pathlib import Path

from pipelines.lib import Pipeline
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import HistoryMode, SilverEntity

SAMPLE_DIR = Path(__file__).parent / "sample_data"

# ============================================
# BRONZE: Load customers from CSV
# ============================================

bronze = BronzeSource(
    system="crm",
    entity="customers",
    source_type=SourceType.FILE_CSV,
    source_path=str(SAMPLE_DIR / "customers_{run_date}.csv"),
)

# ============================================
# SILVER: Curate customers with SCD Type 2
# ============================================

silver = SilverEntity(
    natural_keys=["customer_id"],
    change_timestamp="updated_at",
    history_mode=HistoryMode.FULL_HISTORY,  # SCD Type 2!
    attributes=["name", "email", "tier", "status"],
)

# ============================================
# PIPELINE
# ============================================

pipeline = Pipeline(bronze=bronze, silver=silver)
run = pipeline.run
run_bronze = pipeline.run_bronze
run_silver = pipeline.run_silver


def create_sample_data(run_date: str = "2025-01-15") -> Path:
    """Create sample customer data with multiple versions.

    This simulates having multiple historical versions of the same customer,
    which is typical for SCD Type 2 scenarios.
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Sample customers with historical changes
    # Note: same customer_id appears multiple times with different timestamps
    sample_customers = [
        # Customer 1: 3 versions (address change, then tier upgrade)
        {
            "customer_id": "CUST001",
            "name": "Alice Smith",
            "email": "alice@example.com",
            "tier": "bronze",
            "status": "active",
            "updated_at": "2024-01-01T00:00:00",
        },
        {
            "customer_id": "CUST001",
            "name": "Alice Smith",
            "email": "alice.new@example.com",
            "tier": "bronze",
            "status": "active",
            "updated_at": "2024-06-15T10:30:00",
        },
        {
            "customer_id": "CUST001",
            "name": "Alice Smith",
            "email": "alice.new@example.com",
            "tier": "gold",
            "status": "active",
            "updated_at": "2025-01-10T14:00:00",
        },
        # Customer 2: 2 versions (status change)
        {
            "customer_id": "CUST002",
            "name": "Bob Jones",
            "email": "bob@example.com",
            "tier": "silver",
            "status": "active",
            "updated_at": "2024-03-01T00:00:00",
        },
        {
            "customer_id": "CUST002",
            "name": "Bob Jones",
            "email": "bob@example.com",
            "tier": "silver",
            "status": "inactive",
            "updated_at": "2024-12-01T00:00:00",
        },
        # Customer 3: 1 version (new customer)
        {
            "customer_id": "CUST003",
            "name": "Carol White",
            "email": "carol@example.com",
            "tier": "bronze",
            "status": "active",
            "updated_at": "2025-01-15T09:00:00",
        },
    ]

    file_path = SAMPLE_DIR / f"customers_{run_date}.csv"

    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sample_customers[0].keys())
        writer.writeheader()
        writer.writerows(sample_customers)

    print(f"Created sample data: {file_path}")
    print("  - CUST001: 3 versions (email change, tier upgrade)")
    print("  - CUST002: 2 versions (status change)")
    print("  - CUST003: 1 version (new customer)")
    return file_path


if __name__ == "__main__":
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Creating sample data for {run_date}...")
    create_sample_data(run_date)

    print(f"\nRunning pipeline for {run_date}...")
    result = run(run_date)

    print("\nResults:")
    print(f"  Bronze: {result['bronze'].get('row_count', 0)} rows")
    print(f"  Silver: {result['silver'].get('row_count', 0)} rows")
    print("\nSilver output includes SCD2 columns:")
    print("  - effective_from: When this version became active")
    print("  - effective_to: When this version was superseded (NULL if current)")
    print("  - is_current: 1 if this is the latest version, 0 otherwise")
