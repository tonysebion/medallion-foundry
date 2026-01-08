"""
Example Pipeline: File to Silver
================================
Demonstrates processing Parquet files through Bronze → Silver.

This example shows:
- Loading from Parquet files
- Full snapshot processing
- SCD Type 2 (full history) with effective dates
- Data quality validation

To run:
    python -m pipelines examples.file_to_silver --date 2025-01-15
"""

from pathlib import Path

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.quality import (
    not_null,
    valid_timestamp,
    non_negative,
    check_quality,
)

# Output paths
OUTPUT_DIR = Path("./output")
INPUT_DIR = Path("./input")

# ============================================
# BRONZE: Load products from Parquet
# ============================================

bronze = BronzeSource(
    system="inventory",
    entity="products",
    source_type=SourceType.FILE_PARQUET,
    source_path=str(INPUT_DIR / "products" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "bronze" / "inventory" / "products"),
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# ============================================
# SILVER: Curate products with full history
# ============================================

silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "inventory" / "products" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "inventory" / "products"),
    natural_keys=["product_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    # Full history mode adds effective_from, effective_to, is_current columns
    history_mode=HistoryMode.FULL_HISTORY,
    attributes=["name", "category", "price", "stock_quantity", "supplier_id"],
    # Validate source checksums before processing
    validate_source="warn",
)

# Data quality rules
quality_rules = (
    not_null("product_id", "name", "updated_at")
    + [
        valid_timestamp("updated_at"),
        non_negative("price"),
        non_negative("stock_quantity"),
    ]
)


def run_bronze(run_date: str, **kwargs):
    """Extract products from Parquet files."""
    return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze products to Silver with quality checks."""
    result = silver.run(run_date, **kwargs)

    if result.get("row_count", 0) > 0:
        # Silver writes to entity_name.parquet (e.g., products.parquet)
        output_dir = Path(silver.target_path)
        # Try entity-named file first, fall back to data.parquet for compatibility
        output_path = output_dir / "products.parquet"
        if not output_path.exists():
            output_path = output_dir / "data.parquet"
        if output_path.exists():
            import ibis

            conn = ibis.duckdb.connect()
            table = conn.read_parquet(str(output_path))
            quality_result = check_quality(table, quality_rules, fail_on_error=False)
            result["quality"] = {
                "passed": quality_result.passed,
                "pass_rate": quality_result.pass_rate,
                "rules_checked": quality_result.rules_checked,
            }

    return result


def run(run_date: str, **kwargs):
    """Run full pipeline: Bronze → Silver with quality validation."""
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}


def create_sample_data(run_date: str = "2025-01-15") -> Path:
    """Create sample Parquet data for testing."""
    import pandas as pd

    input_path = INPUT_DIR / "products"
    input_path.mkdir(parents=True, exist_ok=True)

    sample_products = [
        {
            "product_id": "PROD001",
            "name": "Widget Pro",
            "category": "electronics",
            "price": 99.99,
            "stock_quantity": 150,
            "supplier_id": "SUP01",
            "updated_at": f"{run_date}T10:00:00",
        },
        {
            "product_id": "PROD002",
            "name": "Gadget Plus",
            "category": "electronics",
            "price": 149.99,
            "stock_quantity": 75,
            "supplier_id": "SUP02",
            "updated_at": f"{run_date}T11:30:00",
        },
        {
            "product_id": "PROD003",
            "name": "Basic Cable",
            "category": "accessories",
            "price": 9.99,
            "stock_quantity": 500,
            "supplier_id": "SUP01",
            "updated_at": f"{run_date}T09:00:00",
        },
        {
            "product_id": "PROD004",
            "name": "Premium Stand",
            "category": "accessories",
            "price": 29.99,
            "stock_quantity": 200,
            "supplier_id": "SUP03",
            "updated_at": f"{run_date}T14:00:00",
        },
    ]

    df = pd.DataFrame(sample_products)
    file_path = input_path / f"products_{run_date}.parquet"
    df.to_parquet(file_path, index=False)

    print(f"Created sample data: {file_path}")
    return file_path


if __name__ == "__main__":
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Creating sample data for {run_date}...")
    create_sample_data(run_date)

    print(f"\nRunning file-to-silver pipeline for {run_date}...")
    result = run(run_date)

    print("\nResults:")
    print(f"  Bronze: {result['bronze'].get('row_count', 0)} rows")
    print(f"  Silver: {result['silver'].get('row_count', 0)} rows")
    if "quality" in result.get("silver", {}):
        q = result["silver"]["quality"]
        print(f"  Quality: {'PASSED' if q['passed'] else 'FAILED'} ({q['pass_rate']:.1f}%)")
