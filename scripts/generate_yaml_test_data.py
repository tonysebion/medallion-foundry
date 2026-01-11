#!/usr/bin/env python
"""Generate test CSV data for YAML pipeline integration tests.

This script generates sample data files that match the schemas expected
by the example YAML pipelines (retail_orders.yaml, customer_scd2.yaml).

Examples:
    # Generate data for a specific date
    python scripts/generate_yaml_test_data.py --date 2025-01-15

    # Generate data to a custom directory
    python scripts/generate_yaml_test_data.py --date 2025-01-15 --output ./test_data

    # Generate with a specific seed for reproducibility
    python scripts/generate_yaml_test_data.py --date 2025-01-15 --seed 42

    # Generate both orders and customers
    python scripts/generate_yaml_test_data.py --date 2025-01-15 --all
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd


def generate_orders_data(
    run_date: str,
    num_orders: int = 10,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate orders data matching retail_orders.yaml schema.

    Columns:
    - order_id: Unique order identifier
    - customer_id: Customer reference
    - order_total: Order amount
    - status: Order status (pending, shipped, completed, cancelled)
    - updated_at: Last update timestamp

    Args:
        run_date: Date string (YYYY-MM-DD) for timestamp generation
        num_orders: Number of orders to generate
        seed: Random seed for reproducibility

    Returns:
        DataFrame with order data
    """
    random.seed(seed)

    statuses = ["pending", "shipped", "completed", "cancelled"]
    customers = [f"CUST{str(i).zfill(3)}" for i in range(1, 6)]

    orders: List[Dict[str, Any]] = []
    for i in range(1, num_orders + 1):
        order_id = f"ORD{str(i).zfill(4)}"
        customer_id = random.choice(customers)
        order_total = round(random.uniform(10.0, 500.0), 2)
        status = random.choice(statuses)
        # Generate timestamps within the run_date
        hour = random.randint(8, 20)
        minute = random.randint(0, 59)
        updated_at = f"{run_date}T{hour:02d}:{minute:02d}:00"

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_total": order_total,
                "status": status,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(orders)


def generate_customers_data(
    run_date: str,
    num_customers: int = 5,
    include_history: bool = True,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate customers data matching customer_scd2.yaml schema.

    Columns:
    - customer_id: Unique customer identifier (natural key)
    - name: Customer name
    - email: Customer email
    - tier: Membership tier (bronze, silver, gold)
    - status: Account status (active, inactive)
    - updated_at: Last update timestamp

    For SCD Type 2 testing, includes historical versions of some customers.

    Args:
        run_date: Date string (YYYY-MM-DD) for timestamp generation
        num_customers: Number of unique customers
        include_history: If True, include historical versions for some customers
        seed: Random seed for reproducibility

    Returns:
        DataFrame with customer data
    """
    random.seed(seed)

    tiers = ["bronze", "silver", "gold"]
    statuses = ["active", "inactive"]

    customers: List[Dict[str, Any]] = []

    for i in range(1, num_customers + 1):
        customer_id = f"CUST{str(i).zfill(3)}"
        base_name = f"Customer {chr(64 + i)}"  # Customer A, Customer B, etc.
        email_base = f"customer{chr(96 + i)}@example.com"

        if include_history and i <= 2:
            # Create historical versions for first 2 customers
            # Version 1: Original record from a year ago
            customers.append(
                {
                    "customer_id": customer_id,
                    "name": base_name,
                    "email": f"old_{email_base}",
                    "tier": "bronze",
                    "status": "active",
                    "updated_at": "2024-01-01T00:00:00",
                }
            )

            # Version 2: Email changed mid-year
            customers.append(
                {
                    "customer_id": customer_id,
                    "name": base_name,
                    "email": email_base,
                    "tier": "bronze",
                    "status": "active",
                    "updated_at": "2024-06-15T10:30:00",
                }
            )

            # Version 3: Current version with tier upgrade
            customers.append(
                {
                    "customer_id": customer_id,
                    "name": base_name,
                    "email": email_base,
                    "tier": random.choice(["silver", "gold"]),
                    "status": random.choice(statuses),
                    "updated_at": f"{run_date}T09:00:00",
                }
            )
        else:
            # Single current record
            hour = random.randint(8, 20)
            customers.append(
                {
                    "customer_id": customer_id,
                    "name": base_name,
                    "email": email_base,
                    "tier": random.choice(tiers),
                    "status": random.choice(statuses),
                    "updated_at": f"{run_date}T{hour:02d}:00:00",
                }
            )

    return pd.DataFrame(customers)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to CSV file, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"  Wrote {path} ({len(df)} rows)")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate test CSV data for YAML pipeline tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory (default: pipelines/examples/sample_data/)",
    )
    parser.add_argument(
        "--orders-only",
        action="store_true",
        help="Generate only orders data",
    )
    parser.add_argument(
        "--customers-only",
        action="store_true",
        help="Generate only customers data",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all data types (default behavior)",
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=10,
        help="Number of orders to generate (default: 10)",
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=5,
        help="Number of unique customers to generate (default: 5)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        help="Don't include historical versions for customers",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Determine output directory
    if args.output:
        output_dir = args.output
    else:
        # Default to pipelines/examples/sample_data/
        script_dir = Path(__file__).parent
        output_dir = script_dir.parent / "pipelines" / "examples" / "sample_data"

    # Determine what to generate
    generate_orders = not args.customers_only
    generate_customers = not args.orders_only

    print(f"Generating test data for {args.date}")
    print(f"Output directory: {output_dir}")
    print(f"Seed: {args.seed}")
    print()

    if generate_orders:
        print("Generating orders data...")
        orders_df = generate_orders_data(
            run_date=args.date,
            num_orders=args.num_orders,
            seed=args.seed,
        )
        orders_file = output_dir / f"orders_{args.date}.csv"
        write_csv(orders_df, orders_file)

        if args.verbose:
            print(f"\n  Sample:\n{orders_df.head()}\n")

    if generate_customers:
        print("Generating customers data...")
        customers_df = generate_customers_data(
            run_date=args.date,
            num_customers=args.num_customers,
            include_history=not args.no_history,
            seed=args.seed,
        )
        customers_file = output_dir / f"customers_{args.date}.csv"
        write_csv(customers_df, customers_file)

        if args.verbose:
            print(f"\n  Sample:\n{customers_df.head()}\n")

    print()
    print("Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
