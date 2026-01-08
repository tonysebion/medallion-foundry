"""Integration tests that exercise the example Bronze/Silver pipelines.

Note: Some examples (customer_scd2, retail_orders) have been converted to YAML.
These tests are kept for backward compatibility with Python examples.
"""

import pytest
import pandas as pd

# Import only the Python examples that exist
from pipelines.examples import file_to_silver, multi_source_parallel

# These examples were converted to YAML - skip tests that require them
try:
    from pipelines.examples import customer_scd2
except ImportError:
    customer_scd2 = None

try:
    from pipelines.examples import retail_orders
except ImportError:
    retail_orders = None


@pytest.mark.skipif(customer_scd2 is None, reason="customer_scd2 example converted to YAML")
def test_customer_scd2_full_history(tmp_path, monkeypatch):
    run_date = "2025-01-15"
    sample_dir = tmp_path / "customer_scd2"
    monkeypatch.setattr(customer_scd2, "SAMPLE_DIR", sample_dir)

    monkeypatch.setattr(
        customer_scd2.bronze,
        "source_path",
        str(sample_dir / "customers_{run_date}.csv"),
    )
    monkeypatch.setattr(
        customer_scd2.bronze,
        "target_path",
        str(sample_dir / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
    )
    monkeypatch.setattr(
        customer_scd2.silver,
        "source_path",
        str(
            sample_dir
            / "bronze"
            / "system=crm"
            / "entity=customers"
            / "dt={run_date}"
            / "*.parquet"
        ),
    )
    monkeypatch.setattr(
        customer_scd2.silver,
        "target_path",
        str(sample_dir / "silver" / "crm" / "customers"),
    )

    customer_scd2.create_sample_data(run_date)
    result = customer_scd2.run(run_date)

    assert result["bronze"]["row_count"] == 6
    silver_df = pd.read_parquet(sample_dir / "silver" / "crm" / "customers" / "data.parquet")
    assert len(silver_df) == 6
    assert set(silver_df["customer_id"]) == {"CUST001", "CUST002", "CUST003"}
    assert "effective_from" in silver_df.columns
    assert "effective_to" in silver_df.columns
    assert "is_current" in silver_df.columns
    assert silver_df["is_current"].sum() == 3
    assert silver_df[silver_df["customer_id"] == "CUST001"].shape[0] == 3


@pytest.mark.skipif(retail_orders is None, reason="retail_orders example converted to YAML")
def test_retail_orders_current_only(tmp_path, monkeypatch):
    run_date = "2025-01-15"
    sample_dir = tmp_path / "retail_orders"
    monkeypatch.setattr(retail_orders, "SAMPLE_DIR", sample_dir)

    monkeypatch.setattr(
        retail_orders.bronze,
        "source_path",
        str(sample_dir / "orders_{run_date}.csv"),
    )
    monkeypatch.setattr(
        retail_orders.bronze,
        "target_path",
        str(sample_dir / "bronze" / "system={system}" / "entity={entity}" / "dt={run_date}"),
    )
    monkeypatch.setattr(
        retail_orders.silver,
        "source_path",
        str(
            sample_dir
            / "bronze"
            / "system=retail"
            / "entity=orders"
            / "dt={run_date}"
            / "*.parquet"
        ),
    )
    monkeypatch.setattr(
        retail_orders.silver,
        "target_path",
        str(sample_dir / "silver" / "retail" / "orders"),
    )

    retail_orders.create_sample_data(run_date)
    result = retail_orders.run(run_date)

    assert result["silver"]["row_count"] == 5
    silver_df = pd.read_parquet(sample_dir / "silver" / "retail" / "orders" / "data.parquet")
    assert len(silver_df) == 5
    assert silver_df["order_id"].nunique() == 5
    assert "_silver_run_date" in silver_df.columns
    assert set(silver_df["status"]) >= {"completed", "pending", "shipped"}


def test_file_to_silver_quality_checks(tmp_path, monkeypatch):
    run_date = "2025-01-15"
    input_dir = tmp_path / "file_to_silver_input"
    output_dir = tmp_path / "file_to_silver_output"
    monkeypatch.setattr(file_to_silver, "INPUT_DIR", input_dir)
    monkeypatch.setattr(file_to_silver, "OUTPUT_DIR", output_dir)

    monkeypatch.setattr(
        file_to_silver.bronze,
        "source_path",
        str(input_dir / "products" / "*.parquet"),
    )
    monkeypatch.setattr(
        file_to_silver.bronze,
        "target_path",
        str(output_dir / "bronze" / "inventory" / "products"),
    )
    monkeypatch.setattr(
        file_to_silver.silver,
        "source_path",
        str(output_dir / "bronze" / "inventory" / "products" / "*.parquet"),
    )
    monkeypatch.setattr(
        file_to_silver.silver,
        "target_path",
        str(output_dir / "silver" / "inventory" / "products"),
    )

    file_to_silver.create_sample_data(run_date)
    result = file_to_silver.run(run_date)

    silver_result = result["silver"]
    quality = silver_result["quality"]
    assert quality["passed"]
    assert quality["rules_checked"] == 6
    assert quality["pass_rate"] == 100.0

    # Silver writes to entity_name.parquet (products.parquet)
    silver_df = pd.read_parquet(output_dir / "silver" / "inventory" / "products" / "products.parquet")
    assert len(silver_df) == 4
    assert "effective_from" in silver_df.columns


def test_multi_source_parallel_handles_every_entity(tmp_path, monkeypatch):
    run_date = "2025-01-15"
    input_dir = tmp_path / "multi_source_input"
    output_dir = tmp_path / "multi_source_output"
    monkeypatch.setattr(multi_source_parallel, "INPUT_DIR", input_dir)
    monkeypatch.setattr(multi_source_parallel, "OUTPUT_DIR", output_dir)

    monkeypatch.setattr(
        multi_source_parallel.customers_bronze,
        "source_path",
        str(input_dir / "customers" / "customers_{run_date}.csv"),
    )
    monkeypatch.setattr(
        multi_source_parallel.customers_bronze,
        "target_path",
        str(output_dir / "bronze" / "ecommerce" / "customers"),
    )
    monkeypatch.setattr(
        multi_source_parallel.orders_bronze,
        "source_path",
        str(input_dir / "orders" / "orders_{run_date}.csv"),
    )
    monkeypatch.setattr(
        multi_source_parallel.orders_bronze,
        "target_path",
        str(output_dir / "bronze" / "ecommerce" / "orders"),
    )
    monkeypatch.setattr(
        multi_source_parallel.products_bronze,
        "source_path",
        str(input_dir / "products" / "products_{run_date}.parquet"),
    )
    monkeypatch.setattr(
        multi_source_parallel.products_bronze,
        "target_path",
        str(output_dir / "bronze" / "ecommerce" / "products"),
    )

    monkeypatch.setattr(
        multi_source_parallel.customers_silver,
        "source_path",
        str(output_dir / "bronze" / "ecommerce" / "customers" / "*.parquet"),
    )
    monkeypatch.setattr(
        multi_source_parallel.customers_silver,
        "target_path",
        str(output_dir / "silver" / "ecommerce" / "customers"),
    )
    monkeypatch.setattr(
        multi_source_parallel.orders_silver,
        "source_path",
        str(output_dir / "bronze" / "ecommerce" / "orders" / "*.parquet"),
    )
    monkeypatch.setattr(
        multi_source_parallel.orders_silver,
        "target_path",
        str(output_dir / "silver" / "ecommerce" / "orders"),
    )
    monkeypatch.setattr(
        multi_source_parallel.products_silver,
        "source_path",
        str(output_dir / "bronze" / "ecommerce" / "products" / "*.parquet"),
    )
    monkeypatch.setattr(
        multi_source_parallel.products_silver,
        "target_path",
        str(output_dir / "silver" / "ecommerce" / "products"),
    )

    multi_source_parallel.create_sample_data(run_date)
    result = multi_source_parallel.run(run_date)

    silver_results = result["silver"]
    assert silver_results["customers"]["row_count"] == 3
    assert silver_results["orders"]["entity_kind"] == "event"
    assert silver_results["products"]["row_count"] == 3

    # Silver writes to entity_name.parquet (orders.parquet)
    orders_df = pd.read_parquet(output_dir / "silver" / "ecommerce" / "orders" / "orders.parquet")
    assert len(orders_df) == 3
    assert "_silver_curated_at" in orders_df.columns
