"""
Example Pipeline: Multi-Source Parallel Processing
===================================================
Demonstrates running multiple Bronze sources in parallel.

This example shows:
- Multiple source definitions in one pipeline
- Parallel extraction using concurrent.futures
- Aggregating results from multiple sources
- Error handling for partial failures

To run:
    python -m pipelines examples.multi_source_parallel --date 2025-01-15
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity

# Output paths
OUTPUT_DIR = Path("./output")
INPUT_DIR = Path("./input")

# ============================================
# BRONZE: Define multiple sources
# ============================================

# Customers from CSV
customers_bronze = BronzeSource(
    system="ecommerce",
    entity="customers",
    source_type=SourceType.FILE_CSV,
    source_path=str(INPUT_DIR / "customers" / "customers_{run_date}.csv"),
    target_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "customers"),
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# Orders from CSV
orders_bronze = BronzeSource(
    system="ecommerce",
    entity="orders",
    source_type=SourceType.FILE_CSV,
    source_path=str(INPUT_DIR / "orders" / "orders_{run_date}.csv"),
    target_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "orders"),
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# Products from Parquet
products_bronze = BronzeSource(
    system="ecommerce",
    entity="products",
    source_type=SourceType.FILE_PARQUET,
    source_path=str(INPUT_DIR / "products" / "products_{run_date}.parquet"),
    target_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "products"),
    load_pattern=LoadPattern.FULL_SNAPSHOT,
)

# List of all bronze sources for parallel processing
BRONZE_SOURCES = [
    ("customers", customers_bronze),
    ("orders", orders_bronze),
    ("products", products_bronze),
]

# ============================================
# SILVER: Define corresponding Silver entities
# ============================================

customers_silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "customers" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "ecommerce" / "customers"),
    natural_keys=["customer_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["name", "email", "tier"],
)

orders_silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "orders" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "ecommerce" / "orders"),
    natural_keys=["order_id"],
    change_timestamp="order_date",
    entity_kind=EntityKind.EVENT,  # Orders are events (immutable)
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["customer_id", "product_id", "quantity", "total"],
)

products_silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "ecommerce" / "products" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "ecommerce" / "products"),
    natural_keys=["product_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["name", "category", "price", "active"],
)

SILVER_ENTITIES = [
    ("customers", customers_silver),
    ("orders", orders_silver),
    ("products", products_silver),
]


def run_bronze_parallel(run_date: str, max_workers: int = 3, **kwargs) -> Dict[str, Any]:
    """Run all Bronze extractions in parallel.

    Args:
        run_date: The run date for extraction
        max_workers: Maximum parallel workers (default 3)
        **kwargs: Additional arguments passed to each source

    Returns:
        Dict mapping entity name to result or error
    """
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all bronze jobs
        futures = {
            executor.submit(source.run, run_date, **kwargs): name
            for name, source in BRONZE_SOURCES
        }

        # Collect results as they complete
        for future in as_completed(futures):
            entity_name = futures[future]
            try:
                results[entity_name] = future.result()
            except Exception as e:
                results[entity_name] = {"error": str(e), "row_count": 0}

    return results


def run_silver_parallel(run_date: str, max_workers: int = 3, **kwargs) -> Dict[str, Any]:
    """Run all Silver curations in parallel.

    Args:
        run_date: The run date for curation
        max_workers: Maximum parallel workers (default 3)
        **kwargs: Additional arguments passed to each entity

    Returns:
        Dict mapping entity name to result or error
    """
    results: Dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(entity.run, run_date, **kwargs): name
            for name, entity in SILVER_ENTITIES
        }

        for future in as_completed(futures):
            entity_name = futures[future]
            try:
                results[entity_name] = future.result()
            except Exception as e:
                results[entity_name] = {"error": str(e), "row_count": 0}

    return results


def run(run_date: str, max_workers: int = 3, **kwargs) -> Dict[str, Any]:
    """Run full pipeline: All Bronze sources → All Silver entities in parallel.

    Bronze extractions run in parallel, then Silver curations run in parallel.
    """
    bronze_results = run_bronze_parallel(run_date, max_workers, **kwargs)
    silver_results = run_silver_parallel(run_date, max_workers, **kwargs)

    return {
        "bronze": bronze_results,
        "silver": silver_results,
    }


def create_sample_data(run_date: str = "2025-01-15") -> List[Path]:
    """Create sample data for all sources."""
    import csv

    import pandas as pd

    paths = []

    # Customers CSV
    customers_dir = INPUT_DIR / "customers"
    customers_dir.mkdir(parents=True, exist_ok=True)
    customers_path = customers_dir / f"customers_{run_date}.csv"
    with open(customers_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "name", "email", "tier", "updated_at"])
        writer.writeheader()
        writer.writerows([
            {"customer_id": "C001", "name": "Alice Smith", "email": "alice@example.com", "tier": "gold", "updated_at": f"{run_date}T10:00:00"},
            {"customer_id": "C002", "name": "Bob Jones", "email": "bob@example.com", "tier": "silver", "updated_at": f"{run_date}T11:00:00"},
            {"customer_id": "C003", "name": "Carol White", "email": "carol@example.com", "tier": "bronze", "updated_at": f"{run_date}T09:00:00"},
        ])
    paths.append(customers_path)

    # Orders CSV
    orders_dir = INPUT_DIR / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    orders_path = orders_dir / f"orders_{run_date}.csv"
    with open(orders_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "customer_id", "product_id", "quantity", "total", "order_date"])
        writer.writeheader()
        writer.writerows([
            {"order_id": "O001", "customer_id": "C001", "product_id": "P001", "quantity": 2, "total": 199.98, "order_date": f"{run_date}T14:00:00"},
            {"order_id": "O002", "customer_id": "C002", "product_id": "P002", "quantity": 1, "total": 49.99, "order_date": f"{run_date}T15:30:00"},
            {"order_id": "O003", "customer_id": "C001", "product_id": "P003", "quantity": 3, "total": 29.97, "order_date": f"{run_date}T16:00:00"},
        ])
    paths.append(orders_path)

    # Products Parquet
    products_dir = INPUT_DIR / "products"
    products_dir.mkdir(parents=True, exist_ok=True)
    products_path = products_dir / f"products_{run_date}.parquet"
    df = pd.DataFrame([
        {"product_id": "P001", "name": "Widget Pro", "category": "electronics", "price": 99.99, "active": True, "updated_at": f"{run_date}T08:00:00"},
        {"product_id": "P002", "name": "Gadget Basic", "category": "electronics", "price": 49.99, "active": True, "updated_at": f"{run_date}T08:30:00"},
        {"product_id": "P003", "name": "Cable Standard", "category": "accessories", "price": 9.99, "active": True, "updated_at": f"{run_date}T09:00:00"},
    ])
    df.to_parquet(products_path, index=False)
    paths.append(products_path)

    print("Created sample data files:")
    for path in paths:
        print(f"  {path}")

    return paths


if __name__ == "__main__":
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Creating sample data for {run_date}...")
    create_sample_data(run_date)

    print(f"\nRunning multi-source parallel pipeline for {run_date}...")
    result = run(run_date)

    print("\nBronze Results:")
    for name, res in result["bronze"].items():
        if "error" in res:
            print(f"  {name}: ERROR - {res['error']}")
        else:
            print(f"  {name}: {res.get('row_count', 0)} rows")

    print("\nSilver Results:")
    for name, res in result["silver"].items():
        if "error" in res:
            print(f"  {name}: ERROR - {res['error']}")
        else:
            print(f"  {name}: {res.get('row_count', 0)} rows")
