"""Integration tests that exercise the example YAML pipelines."""

from pathlib import Path

import pandas as pd

from pipelines.lib.config_loader import load_pipeline

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "pipelines" / "examples"


def load_example(name: str):
    return load_pipeline(EXAMPLES_DIR / f"{name}.yaml")


def test_customer_scd2_full_history(tmp_path):
    run_date = "2025-01-15"
    input_dir = tmp_path / "customer_scd2_input"
    output_dir = tmp_path / "customer_scd2_output"

    input_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST001",
                "name": "Ada",
                "email": "ada@example.com",
                "tier": "gold",
                "status": "active",
                "updated_at": "2025-01-01",
            },
            {
                "customer_id": "CUST001",
                "name": "Ada",
                "email": "ada@example.com",
                "tier": "platinum",
                "status": "active",
                "updated_at": "2025-01-10",
            },
            {
                "customer_id": "CUST001",
                "name": "Ada",
                "email": "ada@example.com",
                "tier": "platinum",
                "status": "inactive",
                "updated_at": "2025-01-14",
            },
            {
                "customer_id": "CUST002",
                "name": "Ben",
                "email": "ben@example.com",
                "tier": "silver",
                "status": "active",
                "updated_at": "2025-01-03",
            },
            {
                "customer_id": "CUST003",
                "name": "Cora",
                "email": "cora@example.com",
                "tier": "gold",
                "status": "active",
                "updated_at": "2025-01-02",
            },
            {
                "customer_id": "CUST003",
                "name": "Cora",
                "email": "cora@example.com",
                "tier": "gold",
                "status": "active",
                "updated_at": "2025-01-12",
            },
        ]
    )
    df.to_csv(input_dir / f"customers_{run_date}.csv", index=False)

    pipeline = load_example("customer_scd2")
    pipeline.bronze.source_path = str(input_dir / f"customers_{run_date}.csv")
    pipeline.bronze.target_path = str(output_dir / "bronze" / "crm" / "customers")
    pipeline.silver.source_path = str(
        output_dir / "bronze" / "crm" / "customers" / "*.parquet"
    )
    pipeline.silver.target_path = str(output_dir / "silver" / "crm" / "customers")

    result = pipeline.run(run_date)

    assert result["bronze"]["row_count"] == 6
    silver_df = pd.read_parquet(
        output_dir / "silver" / "crm" / "customers" / "customers.parquet"
    )
    assert len(silver_df) == 6
    assert set(silver_df["customer_id"]) == {"CUST001", "CUST002", "CUST003"}
    assert "effective_from" in silver_df.columns
    assert "effective_to" in silver_df.columns
    assert "is_current" in silver_df.columns
    assert silver_df["is_current"].sum() == 3
    assert silver_df[silver_df["customer_id"] == "CUST001"].shape[0] == 3


def test_retail_orders_current_only(tmp_path):
    run_date = "2025-01-15"
    input_dir = tmp_path / "retail_orders_input"
    output_dir = tmp_path / "retail_orders_output"

    input_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "order_id": "ORD001",
                "customer_id": "CUST001",
                "order_total": 120.0,
                "status": "completed",
                "updated_at": "2025-01-01",
            },
            {
                "order_id": "ORD002",
                "customer_id": "CUST002",
                "order_total": 80.0,
                "status": "pending",
                "updated_at": "2025-01-02",
            },
            {
                "order_id": "ORD003",
                "customer_id": "CUST003",
                "order_total": 60.0,
                "status": "shipped",
                "updated_at": "2025-01-03",
            },
            {
                "order_id": "ORD004",
                "customer_id": "CUST003",
                "order_total": 95.0,
                "status": "completed",
                "updated_at": "2025-01-04",
            },
            {
                "order_id": "ORD005",
                "customer_id": "CUST001",
                "order_total": 40.0,
                "status": "pending",
                "updated_at": "2025-01-05",
            },
        ]
    )
    df.to_csv(input_dir / f"orders_{run_date}.csv", index=False)

    pipeline = load_example("retail_orders")
    pipeline.bronze.source_path = str(input_dir / f"orders_{run_date}.csv")
    pipeline.bronze.target_path = str(output_dir / "bronze" / "retail" / "orders")
    pipeline.silver.source_path = str(
        output_dir / "bronze" / "retail" / "orders" / "*.parquet"
    )
    pipeline.silver.target_path = str(output_dir / "silver" / "retail" / "orders")

    result = pipeline.run(run_date)

    assert result["silver"]["row_count"] == 5
    silver_df = pd.read_parquet(
        output_dir / "silver" / "retail" / "orders" / "orders.parquet"
    )
    assert len(silver_df) == 5
    assert silver_df["order_id"].nunique() == 5
    assert "_silver_run_date" in silver_df.columns
    assert set(silver_df["status"]) >= {"completed", "pending", "shipped"}


def test_file_to_silver_scd2_history(tmp_path):
    run_date = "2025-01-15"
    input_dir = tmp_path / "file_to_silver_input"
    output_dir = tmp_path / "file_to_silver_output"

    input_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "product_id": "P001",
                "name": "Widget",
                "category": "Hardware",
                "price": 10.0,
                "stock_quantity": 5,
                "supplier_id": "SUP001",
                "updated_at": "2025-01-01",
            },
            {
                "product_id": "P001",
                "name": "Widget",
                "category": "Hardware",
                "price": 12.0,
                "stock_quantity": 4,
                "supplier_id": "SUP001",
                "updated_at": "2025-01-10",
            },
            {
                "product_id": "P002",
                "name": "Gadget",
                "category": "Hardware",
                "price": 8.5,
                "stock_quantity": 10,
                "supplier_id": "SUP002",
                "updated_at": "2025-01-05",
            },
            {
                "product_id": "P003",
                "name": "Service",
                "category": "Software",
                "price": 25.0,
                "stock_quantity": 0,
                "supplier_id": "SUP003",
                "updated_at": "2025-01-07",
            },
        ]
    )
    df.to_parquet(input_dir / "products.parquet", index=False)

    pipeline = load_example("file_to_silver")
    pipeline.bronze.source_path = str(input_dir / "*.parquet")
    pipeline.bronze.target_path = str(output_dir / "bronze" / "inventory" / "products")
    pipeline.silver.source_path = str(
        output_dir / "bronze" / "inventory" / "products" / "*.parquet"
    )
    pipeline.silver.target_path = str(output_dir / "silver" / "inventory" / "products")

    result = pipeline.run(run_date)

    # Silver writes to entity_name.parquet (products.parquet)
    silver_df = pd.read_parquet(
        output_dir / "silver" / "inventory" / "products" / "products.parquet"
    )
    assert len(silver_df) == 4
    assert "effective_from" in silver_df.columns
    assert result["silver"]["row_count"] == 4


def test_multi_source_parallel_customers_yaml(tmp_path):
    run_date = "2025-01-15"
    input_dir = tmp_path / "multi_source_input"
    output_dir = tmp_path / "multi_source_output"
    input_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST001",
                "name": "Ada",
                "email": "ada@example.com",
                "tier": "gold",
                "updated_at": "2025-01-02",
            },
            {
                "customer_id": "CUST002",
                "name": "Ben",
                "email": "ben@example.com",
                "tier": "silver",
                "updated_at": "2025-01-03",
            },
            {
                "customer_id": "CUST003",
                "name": "Cora",
                "email": "cora@example.com",
                "tier": "gold",
                "updated_at": "2025-01-04",
            },
        ]
    )
    df.to_csv(input_dir / f"customers_{run_date}.csv", index=False)

    pipeline = load_example("multi_source_parallel")
    pipeline.bronze.source_path = str(input_dir / f"customers_{run_date}.csv")
    pipeline.bronze.target_path = str(output_dir / "bronze" / "ecommerce" / "customers")
    pipeline.silver.source_path = str(
        output_dir / "bronze" / "ecommerce" / "customers" / "*.parquet"
    )
    pipeline.silver.target_path = str(output_dir / "silver" / "ecommerce" / "customers")

    result = pipeline.run(run_date)

    assert result["silver"]["row_count"] == 3

    customers_df = pd.read_parquet(
        output_dir / "silver" / "ecommerce" / "customers" / "customers.parquet"
    )
    assert len(customers_df) == 3
    assert "_silver_curated_at" in customers_df.columns
