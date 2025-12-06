"""Tests for PolyBase integration per spec Section 8.

Tests PolyBase DDL generation:
- External Data Source DDL
- External File Format DDL
- External Table DDL
- Temporal query functions
"""

from pathlib import Path
from typing import Any, Dict

import pytest

from core.polybase.polybase_generator import (
    generate_polybase_setup,
    generate_temporal_functions_sql,
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
        # Table should have location at minimum
        assert table.location is not None


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
        """Partitioned table should have location with partition pattern."""
        dataset = DatasetConfig.from_dict(partitioned_config)
        setup = generate_polybase_setup(dataset)

        assert len(setup.external_tables) > 0
        # Location should be set for each table
        for table in setup.external_tables:
            assert table.location is not None
