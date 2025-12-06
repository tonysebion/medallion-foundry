"""Tests for Silver transformation patterns per spec Section 5.

Tests Silver transformation patterns:
- single_source: Direct Bronze-to-Silver transformation
- multi_source_join: Join multiple Bronze sources
- single_source_with_lookups: Enrich with lookup tables
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from core.pipeline.silver.lookups import LookupConfig, LookupEnricher, LookupResult, LookupJoinKey
from core.pipeline.silver.joins import (
    JoinConfig,
    JoinSource,
    JoinKeyPair,
    MultiSourceJoiner,
    JoinResult,
)
from core.adapters.schema import SchemaValidator, SchemaSpec, validate_schema


class TestSingleSourcePattern:
    """Test single_source transformation pattern."""

    def test_deduplication_by_primary_keys(self, sample_state_df):
        """Should deduplicate records by primary keys."""
        # Add duplicate records
        df = pd.concat([sample_state_df, sample_state_df.head(5)], ignore_index=True)

        # Deduplicate
        result = df.drop_duplicates(subset=["id"], keep="last")

        assert len(result) == len(sample_state_df)

    def test_canonicalization_trim_strings(self, sample_state_df):
        """Should trim whitespace from string columns."""
        df = sample_state_df.copy()
        df["name"] = df["name"].apply(lambda x: f"  {x}  ")

        # Trim strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip()

        assert not df["name"].str.startswith(" ").any()
        assert not df["name"].str.endswith(" ").any()

    def test_null_handling(self, sample_state_df):
        """Should handle null values correctly."""
        df = sample_state_df.copy()
        df.loc[0:5, "amount"] = None

        # Count nulls
        null_count = df["amount"].isna().sum()
        assert null_count == 6


class TestMultiSourceJoinPattern:
    """Test multi_source_join transformation pattern."""

    def test_join_config_from_dict(self):
        """JoinConfig should parse from dictionary."""
        config_dict = {
            "sources": [
                {"name": "orders", "path": "bronze/orders", "role": "primary"},
                {"name": "customers", "path": "bronze/customers", "role": "secondary"},
            ],
            "join_keys": [{"left": "customer_id", "right": "id"}],
            "join_type": "inner",
            "output": {
                "primary_keys": ["order_id"],
            },
        }

        config = JoinConfig.from_dict(config_dict)
        assert len(config.sources) == 2
        assert config.join_type == "inner"
        assert len(config.join_keys) == 1

    def test_join_source_validation(self):
        """JoinSource should require name and path."""
        with pytest.raises(ValueError, match="must have a 'name'"):
            JoinSource.from_dict({"path": "some/path"})

        with pytest.raises(ValueError, match="must have a 'path'"):
            JoinSource.from_dict({"name": "source"})

    def test_inner_join_execution(self, temp_dir):
        """Inner join should return matching records only."""
        # Create source data
        orders_dir = temp_dir / "orders"
        orders_dir.mkdir()
        orders = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": ["C1", "C2", "C3"],
            "amount": [100, 200, 300],
        })
        orders.to_parquet(orders_dir / "orders.parquet", index=False)

        customers_dir = temp_dir / "customers"
        customers_dir.mkdir()
        customers = pd.DataFrame({
            "id": ["C1", "C2", "C4"],  # C3 missing, C4 extra
            "name": ["Alice", "Bob", "Dave"],
        })
        customers.to_parquet(customers_dir / "customers.parquet", index=False)

        # Configure join
        config = JoinConfig(
            sources=[
                JoinSource(name="orders", path=str(orders_dir), role="primary"),
                JoinSource(name="customers", path=str(customers_dir), role="secondary"),
            ],
            join_keys=[JoinKeyPair(left="customer_id", right="id")],
            join_type="inner",
        )

        # Execute join
        joiner = MultiSourceJoiner(base_path=temp_dir)
        result = joiner.join(config)

        assert len(result.joined_df) == 2  # Only C1 and C2 match
        assert set(result.joined_df["customer_id"]) == {"C1", "C2"}

    def test_left_join_execution(self, temp_dir):
        """Left join should include all left records."""
        orders_dir = temp_dir / "orders"
        orders_dir.mkdir()
        orders = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": ["C1", "C2", "C3"],
        })
        orders.to_parquet(orders_dir / "orders.parquet", index=False)

        customers_dir = temp_dir / "customers"
        customers_dir.mkdir()
        customers = pd.DataFrame({
            "id": ["C1", "C2"],
            "name": ["Alice", "Bob"],
        })
        customers.to_parquet(customers_dir / "customers.parquet", index=False)

        config = JoinConfig(
            sources=[
                JoinSource(name="orders", path=str(orders_dir), role="primary"),
                JoinSource(name="customers", path=str(customers_dir), role="secondary"),
            ],
            join_keys=[JoinKeyPair(left="customer_id", right="id")],
            join_type="left",
        )

        joiner = MultiSourceJoiner(base_path=temp_dir)
        result = joiner.join(config)

        assert len(result.joined_df) == 3  # All orders kept
        # C3's name should be null
        c3_name = result.joined_df[result.joined_df["customer_id"] == "C3"]["name"].values[0]
        assert pd.isna(c3_name)

    def test_join_column_lineage(self, temp_dir):
        """Join should track column lineage."""
        orders_dir = temp_dir / "orders"
        orders_dir.mkdir()
        orders = pd.DataFrame({"order_id": [1], "customer_id": ["C1"], "amount": [100]})
        orders.to_parquet(orders_dir / "orders.parquet", index=False)

        customers_dir = temp_dir / "customers"
        customers_dir.mkdir()
        customers = pd.DataFrame({"id": ["C1"], "name": ["Alice"], "email": ["a@b.com"]})
        customers.to_parquet(customers_dir / "customers.parquet", index=False)

        config = JoinConfig(
            sources=[
                JoinSource(name="orders", path=str(orders_dir), role="primary"),
                JoinSource(name="customers", path=str(customers_dir), role="secondary"),
            ],
            join_keys=[JoinKeyPair(left="customer_id", right="id")],
            join_type="inner",
        )

        joiner = MultiSourceJoiner(base_path=temp_dir)
        result = joiner.join(config)

        assert result.column_lineage["order_id"] == "orders"
        assert result.column_lineage["amount"] == "orders"
        assert result.column_lineage["name"] == "customers"
        assert result.column_lineage["email"] == "customers"


class TestSingleSourceWithLookupsPattern:
    """Test single_source_with_lookups transformation pattern."""

    def test_lookup_config_from_dict(self):
        """LookupConfig should parse from dictionary."""
        config_dict = {
            "name": "status_lookup",
            "path": "reference/statuses",
            "join_keys": [{"source": "status_code", "lookup": "code"}],
            "select_columns": ["status_name", "status_description"],
        }

        config = LookupConfig.from_dict(config_dict)
        assert config.name == "status_lookup"
        assert len(config.join_keys) == 1
        assert config.join_keys[0].source == "status_code"
        assert len(config.select_columns) == 2

    def test_lookup_enrichment(self, temp_dir):
        """Should enrich main data with lookup values."""
        # Create main data
        main_df = pd.DataFrame({
            "id": [1, 2, 3],
            "status_code": ["A", "B", "A"],
            "value": [100, 200, 300],
        })

        # Create lookup table
        lookup_dir = temp_dir / "status_lookup"
        lookup_dir.mkdir()
        lookup_df = pd.DataFrame({
            "code": ["A", "B", "C"],
            "status_name": ["Active", "Blocked", "Closed"],
            "priority": [1, 2, 3],
        })
        lookup_df.to_parquet(lookup_dir / "lookup.parquet", index=False)

        # Configure lookup
        lookup_config = LookupConfig(
            name="status",
            path=str(lookup_dir),
            join_keys=[LookupJoinKey(source="status_code", lookup="code")],
            select_columns=["status_name", "priority"],
        )

        # Enrich
        enricher = LookupEnricher(base_path=temp_dir)
        result = enricher.enrich(main_df, [lookup_config])

        assert "status_name" in result.enriched_df.columns
        assert "priority" in result.enriched_df.columns
        assert result.enriched_df["status_name"].tolist() == ["Active", "Blocked", "Active"]

    def test_multiple_lookups(self, temp_dir):
        """Should support multiple lookups on same data."""
        main_df = pd.DataFrame({
            "id": [1, 2],
            "status_code": ["A", "B"],
            "region_code": ["US", "EU"],
        })

        # Status lookup
        status_dir = temp_dir / "status"
        status_dir.mkdir()
        pd.DataFrame({
            "code": ["A", "B"],
            "name": ["Active", "Blocked"],
        }).to_parquet(status_dir / "data.parquet", index=False)

        # Region lookup
        region_dir = temp_dir / "region"
        region_dir.mkdir()
        pd.DataFrame({
            "code": ["US", "EU"],
            "name": ["United States", "Europe"],
        }).to_parquet(region_dir / "data.parquet", index=False)

        configs = [
            LookupConfig(
                name="status",
                path=str(status_dir),
                join_keys=[LookupJoinKey(source="status_code", lookup="code")],
                select_columns=["name"],
                prefix="status_",
            ),
            LookupConfig(
                name="region",
                path=str(region_dir),
                join_keys=[LookupJoinKey(source="region_code", lookup="code")],
                select_columns=["name"],
                prefix="region_",
            ),
        ]

        enricher = LookupEnricher(base_path=temp_dir)
        result = enricher.enrich(main_df, configs)

        assert "status_name" in result.enriched_df.columns
        assert "region_name" in result.enriched_df.columns

    def test_lookup_handles_missing_keys(self, temp_dir):
        """Lookup should handle missing keys gracefully."""
        main_df = pd.DataFrame({
            "id": [1, 2, 3],
            "status_code": ["A", "X", "B"],  # X doesn't exist in lookup
        })

        lookup_dir = temp_dir / "status"
        lookup_dir.mkdir()
        pd.DataFrame({
            "code": ["A", "B"],
            "name": ["Active", "Blocked"],
        }).to_parquet(lookup_dir / "data.parquet", index=False)

        config = LookupConfig(
            name="status",
            path=str(lookup_dir),
            join_keys=[LookupJoinKey(source="status_code", lookup="code")],
            select_columns=["name"],
        )

        enricher = LookupEnricher(base_path=temp_dir)
        result = enricher.enrich(main_df, [config])

        # Row with X should have null for lookup column
        assert pd.isna(result.enriched_df.iloc[1]["name"])


class TestSchemaValidation:
    """Test schema validation in Silver transformations."""

    def test_validate_schema_passes_for_valid_data(self, sample_state_df):
        """Schema validation should pass for conforming data."""
        schema = SchemaSpec.from_dict({
            "expected_columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": False},
                {"name": "status", "type": "string", "nullable": True},
                {"name": "amount", "type": "float", "nullable": True},
            ],
        })

        validator = SchemaValidator(schema, strict=False)
        result = validator.validate(sample_state_df)

        assert result.valid

    def test_validate_schema_fails_for_missing_required_column(self):
        """Schema validation should fail for missing required column."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            # "name" is missing
            "value": [100, 200, 300],
        })

        schema = SchemaSpec.from_dict({
            "expected_columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": False},
            ],
        })

        validator = SchemaValidator(schema, strict=True)
        result = validator.validate(df)

        assert not result.valid
        assert "name" in result.columns_missing

    def test_validate_schema_fails_for_null_in_non_nullable(self):
        """Schema validation should fail for nulls in non-nullable column."""
        df = pd.DataFrame({
            "id": [1, None, 3],  # Null in non-nullable
            "name": ["A", "B", "C"],
        })

        schema = SchemaSpec.from_dict({
            "expected_columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": False},
            ],
        })

        validator = SchemaValidator(schema, strict=True)
        result = validator.validate(df)

        assert not result.valid
        assert any(e.error_type == "null_violation" for e in result.errors)

    def test_type_coercion(self):
        """Schema validator should coerce types when enabled."""
        df = pd.DataFrame({
            "id": ["1", "2", "3"],  # String instead of int
            "amount": ["100.5", "200.0", "300.75"],  # String instead of decimal
        })

        schema = SchemaSpec.from_dict({
            "expected_columns": [
                {"name": "id", "type": "integer"},
                {"name": "amount", "type": "decimal"},
            ],
        })

        validator = SchemaValidator(schema, coerce_types=True)
        coerced = validator.coerce(df)

        assert coerced["id"].dtype in ["int64", "Int64"]
        assert coerced["amount"].dtype == "float64"
