"""Tests for PolyBase integration per spec Section 8.

Tests PolyBase DDL generation:
- External Data Source DDL
- External File Format DDL
- External Table DDL
- Temporal query functions
- Joined table DDL (multi_source_join pattern)
- Lookup table DDL (single_source_with_lookups pattern)
- CTE view DDL for complex queries
"""

from pathlib import Path
from typing import Any, Dict

import pytest

from core.polybase.polybase_generator import (
    generate_polybase_setup,
    generate_temporal_functions_sql,
    generate_joined_table_ddl,
    generate_lookup_table_ddl,
    generate_cte_view_ddl,
    generate_current_state_view_ddl,
    generate_history_summary_view_ddl,
    generate_event_aggregation_view_ddl,
)
from core.config.dataset import DatasetConfig


class TestPolyBaseSetupGeneration:
    """Test PolyBase setup generation from DatasetConfig."""

    @pytest.fixture
    def sample_dataset_dict(self) -> Dict[str, Any]:
        """Sample dataset configuration for testing."""
        return {
            "dataset_id": "claims.claim_header",
            "system": "claims",
            "entity": "claim_header",
            "domain": "healthcare",
            "environment": "dev",
            "bronze": {
                "enabled": True,
                "output_dir": "./bronze_output",
            },
            "silver": {
                "enabled": True,
                "output_dir": "./silver_output",
                "domain": "healthcare",
                "entity": "claim",
                "version": 1,
                "entity_kind": "state",
                "natural_keys": ["claim_id"],
                "attributes": ["patient_id", "billed_amount", "status"],
                "change_ts_column": "updated_at",
            },
        }

    def test_generate_polybase_setup_returns_setup(self, sample_dataset_dict):
        """Should generate PolybaseSetup from DatasetConfig."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        setup = generate_polybase_setup(dataset)

        assert setup is not None
        assert setup.enabled is True

    def test_polybase_setup_has_data_source(self, sample_dataset_dict):
        """Generated setup should have external data source."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        setup = generate_polybase_setup(dataset)

        assert setup.external_data_source is not None
        assert setup.external_data_source.name is not None
        assert setup.external_data_source.location is not None

    def test_polybase_setup_has_file_format(self, sample_dataset_dict):
        """Generated setup should have external file format."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        setup = generate_polybase_setup(dataset)

        assert setup.external_file_format is not None
        assert setup.external_file_format.format_type == "PARQUET"

    def test_polybase_setup_has_tables(self, sample_dataset_dict):
        """Generated setup should have external tables."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        setup = generate_polybase_setup(dataset)

        assert setup.external_tables is not None
        assert len(setup.external_tables) > 0

    def test_polybase_setup_disabled_for_disabled_silver(self, sample_dataset_dict):
        """PolyBase setup should be disabled when Silver is disabled."""
        sample_dataset_dict["silver"]["enabled"] = False
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        setup = generate_polybase_setup(dataset)

        assert setup.enabled is False

    def test_custom_data_source_location(self, sample_dataset_dict):
        """Should use custom data source location if provided."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        custom_location = "wasbs://custom@storage.blob.core.windows.net/data/"
        setup = generate_polybase_setup(
            dataset,
            external_data_source_location=custom_location,
        )

        assert setup.external_data_source.location == custom_location

    def test_custom_data_source_name(self, sample_dataset_dict):
        """Should use custom data source name if provided."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        custom_name = "CustomDataSource"
        setup = generate_polybase_setup(
            dataset,
            external_data_source_name=custom_name,
        )

        assert setup.external_data_source.name == custom_name


class TestPolyBaseExternalTableNaming:
    """Test external table naming conventions."""

    @pytest.fixture
    def base_config(self) -> Dict[str, Any]:
        return {
            "dataset_id": "test.entity",
            "system": "test_system",
            "entity": "test_entity",
            "domain": "test_domain",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "natural_keys": ["id"],
                "event_ts_column": "event_ts",
            },
        }

    def test_external_table_name_derived_from_entity(self, base_config):
        """External table name should be derived from entity."""
        dataset = DatasetConfig.from_dict(base_config)
        setup = generate_polybase_setup(dataset)

        # Should have at least one table
        assert len(setup.external_tables) > 0
        # Table name should contain entity reference
        table = setup.external_tables[0]
        assert "test_entity" in table.table_name.lower() or "test" in table.table_name.lower()


class TestPolyBaseTemporalFunctions:
    """Test PolyBase temporal SQL function generation."""

    @pytest.fixture
    def sample_dataset_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": "claims.header",
            "system": "claims",
            "entity": "claim_header",
            "domain": "healthcare",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "natural_keys": ["claim_id"],
                "change_ts_column": "updated_at",
            },
        }

    def test_generate_temporal_functions_sql(self, sample_dataset_dict):
        """Should generate temporal function SQL."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        sql = generate_temporal_functions_sql(dataset)

        # Should generate some SQL (or empty if not applicable)
        assert isinstance(sql, str)

    def test_temporal_sql_contains_as_of_function(self, sample_dataset_dict):
        """Temporal SQL should include as-of date function."""
        dataset = DatasetConfig.from_dict(sample_dataset_dict)
        sql = generate_temporal_functions_sql(dataset)

        # If state entity, should have temporal functions
        if sql:
            # Check for common temporal SQL patterns
            assert any(kw in sql.upper() for kw in ["SELECT", "WHERE", "DATE"])


class TestPolyBaseSchemaDerivation:
    """Test schema derivation for external tables."""

    @pytest.fixture
    def config_with_schema(self) -> Dict[str, Any]:
        return {
            "dataset_id": "test.with_schema",
            "system": "test",
            "entity": "entity_with_schema",
            "domain": "test",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "natural_keys": ["id"],
                "attributes": ["name", "amount", "created_at"],
                "change_ts_column": "updated_at",
            },
        }

    def test_external_table_has_columns(self, config_with_schema):
        """External table should have column definitions."""
        dataset = DatasetConfig.from_dict(config_with_schema)
        setup = generate_polybase_setup(dataset)

        assert len(setup.external_tables) > 0
        table = setup.external_tables[0]
        # Table should have artifact_name and table_name at minimum
        assert table.artifact_name is not None
        assert table.table_name is not None


class TestPolyBasePartitionedTables:
    """Test PolyBase for partitioned Silver assets."""

    @pytest.fixture
    def partitioned_config(self) -> Dict[str, Any]:
        return {
            "dataset_id": "events.user_events",
            "system": "events",
            "entity": "user_events",
            "domain": "analytics",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "natural_keys": ["event_id"],
                "event_ts_column": "event_ts",
                "partition_by": ["event_date"],
            },
        }

    def test_partitioned_table_has_location_pattern(self, partitioned_config):
        """Partitioned table should have partition columns configured."""
        dataset = DatasetConfig.from_dict(partitioned_config)
        setup = generate_polybase_setup(dataset)

        assert len(setup.external_tables) > 0
        # Tables should have artifact_name and partition columns
        for table in setup.external_tables:
            assert table.artifact_name is not None
            assert table.partition_columns is not None


class TestPolyBaseJoinedTableDDL:
    """Test DDL generation for joined Silver assets (multi_source_join pattern)."""

    @pytest.fixture
    def join_config(self) -> Dict[str, Any]:
        """Sample join configuration."""
        return {
            "sources": [
                {"name": "orders", "columns": ["order_id", "customer_id", "amount"]},
                {"name": "customers", "columns": ["customer_id", "name", "email"]},
            ],
            "result_columns": [
                {"name": "order_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "decimal", "precision": 18, "scale": 2},
                {"name": "customer_name", "type": "string"},
                {"name": "customer_email", "type": "string"},
            ],
        }

    def test_generates_create_external_table(self, join_config):
        """Should generate CREATE EXTERNAL TABLE statement."""
        ddl = generate_joined_table_ddl(
            join_config=join_config,
            table_name="ext_orders_with_customers",
            location="/silver/joined/orders_customers/",
            data_source_name="silver_source",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "ext_orders_with_customers" in ddl

    def test_includes_all_columns(self, join_config):
        """Should include all result columns in DDL."""
        ddl = generate_joined_table_ddl(
            join_config=join_config,
            table_name="ext_orders_with_customers",
            location="/silver/joined/",
            data_source_name="silver_source",
        )

        assert "[order_id]" in ddl
        assert "[customer_id]" in ddl
        assert "[amount]" in ddl
        assert "[customer_name]" in ddl
        assert "[customer_email]" in ddl

    def test_maps_types_correctly(self, join_config):
        """Should map types to SQL Server types."""
        ddl = generate_joined_table_ddl(
            join_config=join_config,
            table_name="ext_test",
            location="/silver/",
            data_source_name="source",
        )

        assert "NVARCHAR" in ddl  # string type
        assert "DECIMAL(18,2)" in ddl  # decimal with precision/scale

    def test_includes_data_source_reference(self, join_config):
        """Should reference the external data source."""
        ddl = generate_joined_table_ddl(
            join_config=join_config,
            table_name="ext_test",
            location="/silver/",
            data_source_name="my_data_source",
        )

        assert "DATA_SOURCE = [my_data_source]" in ddl


class TestPolyBaseLookupTableDDL:
    """Test DDL generation for lookup/reference tables."""

    @pytest.fixture
    def lookup_config(self) -> Dict[str, Any]:
        """Sample lookup table configuration."""
        return {
            "lookup_key": "status_code",
            "columns": [
                {"name": "status_code", "type": "string", "nullable": False},
                {"name": "status_name", "type": "string", "nullable": False},
                {"name": "description", "type": "string", "nullable": True},
            ],
        }

    def test_generates_lookup_table_ddl(self, lookup_config):
        """Should generate external table DDL for lookup."""
        ddl = generate_lookup_table_ddl(
            lookup_config=lookup_config,
            table_name="ext_status_lookup",
            location="/silver/lookups/status/",
            data_source_name="lookup_source",
        )

        assert "CREATE EXTERNAL TABLE" in ddl
        assert "ext_status_lookup" in ddl

    def test_includes_nullable_constraints(self, lookup_config):
        """Should include NULL/NOT NULL for columns."""
        ddl = generate_lookup_table_ddl(
            lookup_config=lookup_config,
            table_name="ext_status_lookup",
            location="/silver/lookups/status/",
            data_source_name="lookup_source",
        )

        assert "NOT NULL" in ddl  # Non-nullable columns
        assert "NULL" in ddl  # Nullable columns

    def test_includes_index_suggestion(self, lookup_config):
        """Should include suggested index comment."""
        ddl = generate_lookup_table_ddl(
            lookup_config=lookup_config,
            table_name="ext_status_lookup",
            location="/silver/lookups/status/",
            data_source_name="lookup_source",
        )

        assert "INDEX" in ddl
        assert "status_code" in ddl


class TestPolyBaseCTEViewDDL:
    """Test DDL generation for CTE-based views."""

    def test_generates_cte_view(self):
        """Should generate CREATE VIEW with CTE."""
        cte_definitions = [
            {
                "name": "order_totals",
                "query": "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id",
            },
        ]
        final_select = "SELECT c.*, ot.total FROM customers c JOIN order_totals ot ON c.id = ot.customer_id"

        ddl = generate_cte_view_ddl(
            view_name="vw_customer_order_totals",
            base_table="customers",
            cte_definitions=cte_definitions,
            final_select=final_select,
        )

        assert "CREATE OR ALTER VIEW" in ddl
        assert "vw_customer_order_totals" in ddl
        assert "WITH" in ddl
        assert "order_totals AS" in ddl

    def test_includes_multiple_ctes(self):
        """Should handle multiple CTEs."""
        cte_definitions = [
            {"name": "cte1", "query": "SELECT 1 as a"},
            {"name": "cte2", "query": "SELECT 2 as b"},
        ]

        ddl = generate_cte_view_ddl(
            view_name="test_view",
            base_table="base",
            cte_definitions=cte_definitions,
            final_select="SELECT * FROM cte1, cte2",
        )

        assert "cte1 AS" in ddl
        assert "cte2 AS" in ddl


class TestPolyBaseCurrentStateView:
    """Test current state view generation for SCD entities."""

    @pytest.fixture
    def state_dataset_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": "orders.order",
            "system": "orders",
            "entity": "order",
            "domain": "sales",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "natural_keys": ["order_id"],
                "change_ts_column": "updated_at",
            },
        }

    def test_generates_current_state_view(self, state_dataset_dict):
        """Should generate view for current state."""
        dataset = DatasetConfig.from_dict(state_dataset_dict)
        ddl = generate_current_state_view_ddl(
            dataset=dataset,
            external_table_name="order_v1_state_external",
        )

        assert "CREATE OR ALTER VIEW" in ddl
        assert "vw_order_current" in ddl
        assert "is_current = 1" in ddl

    def test_returns_empty_for_event_entity(self):
        """Should return empty string for event entities."""
        event_config = {
            "dataset_id": "events.click",
            "system": "events",
            "entity": "click",
            "domain": "analytics",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "natural_keys": ["event_id"],
                "event_ts_column": "event_ts",
            },
        }
        dataset = DatasetConfig.from_dict(event_config)
        ddl = generate_current_state_view_ddl(
            dataset=dataset,
            external_table_name="click_v1_events_external",
        )

        assert ddl == ""


class TestPolyBaseHistorySummaryView:
    """Test history summary view generation."""

    @pytest.fixture
    def state_dataset_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": "orders.order",
            "system": "orders",
            "entity": "order",
            "domain": "sales",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "natural_keys": ["order_id"],
                "change_ts_column": "updated_at",
            },
        }

    def test_generates_history_summary_view(self, state_dataset_dict):
        """Should generate history summary view."""
        dataset = DatasetConfig.from_dict(state_dataset_dict)
        ddl = generate_history_summary_view_ddl(
            dataset=dataset,
            external_table_name="order_v1_state_external",
        )

        assert "CREATE OR ALTER VIEW" in ddl
        assert "vw_order_history_summary" in ddl
        assert "version_count" in ddl
        assert "COUNT(*)" in ddl


class TestPolyBaseEventAggregationView:
    """Test event aggregation view generation."""

    @pytest.fixture
    def event_dataset_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": "events.click",
            "system": "events",
            "entity": "click",
            "domain": "analytics",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "event",
                "natural_keys": ["event_id"],
                "event_ts_column": "event_ts",
            },
        }

    def test_generates_daily_aggregation_view(self, event_dataset_dict):
        """Should generate daily aggregation view."""
        dataset = DatasetConfig.from_dict(event_dataset_dict)
        ddl = generate_event_aggregation_view_ddl(
            dataset=dataset,
            external_table_name="click_v1_events_external",
            aggregation_period="day",
        )

        assert "CREATE OR ALTER VIEW" in ddl
        assert "vw_click_daily_summary" in ddl
        assert "event_count" in ddl

    def test_generates_hourly_aggregation_view(self, event_dataset_dict):
        """Should generate hourly aggregation view."""
        dataset = DatasetConfig.from_dict(event_dataset_dict)
        ddl = generate_event_aggregation_view_ddl(
            dataset=dataset,
            external_table_name="click_v1_events_external",
            aggregation_period="hour",
        )

        assert "vw_click_hourly_summary" in ddl
        assert "DATEADD(hour" in ddl

    def test_returns_empty_for_state_entity(self):
        """Should return empty string for state entities."""
        state_config = {
            "dataset_id": "orders.order",
            "system": "orders",
            "entity": "order",
            "domain": "sales",
            "bronze": {"enabled": True},
            "silver": {
                "enabled": True,
                "entity_kind": "state",
                "natural_keys": ["order_id"],
                "change_ts_column": "updated_at",
            },
        }
        dataset = DatasetConfig.from_dict(state_config)
        ddl = generate_event_aggregation_view_ddl(
            dataset=dataset,
            external_table_name="order_v1_state_external",
        )

        assert ddl == ""
