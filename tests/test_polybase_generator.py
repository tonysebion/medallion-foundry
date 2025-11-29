"""Unit tests for Polybase setup generation from dataset configurations."""

import pytest

from core.config.dataset import DatasetConfig, EntityKind
from core.polybase import generate_polybase_setup


@pytest.fixture
def sample_event_dataset():
    """Create a sample event dataset config."""
    return DatasetConfig(
        system="test_system",
        entity="test_orders",
        environment="dev",
        domain="test_domain",
        bronze=None,  # Type: ignore  # Not needed for this test
        silver=None,  # Type: ignore  # Not needed for this test
    )


def test_polybase_disabled_when_silver_disabled():
    """Test that Polybase is disabled when silver is disabled."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode

    dataset = DatasetConfig(
        system="test_system",
        entity="test_orders",
        environment="dev",
        domain="test_domain",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
        ),
        silver=SilverIntent(
            enabled=False,  # Disabled!
            entity_kind=EntityKind.EVENT,
            history_mode=None,
            input_mode=None,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["id"],
            event_ts_column="ts",
            change_ts_column=None,
            attributes=["attr1"],
            partition_by=["date"],
        ),
    )

    polybase = generate_polybase_setup(dataset)
    assert polybase.enabled is False


def test_polybase_setup_event_entity():
    """Test Polybase setup generation for event entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, InputMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern1_full_events"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.EVENT,
            history_mode=None,
            input_mode=InputMode.REPLACE_DAILY,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column="updated_at",
            change_ts_column=None,
            attributes=["status", "total"],
            partition_by=["event_date"],
            record_time_column="updated_at",
            record_time_partition="event_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)

    # Check enabled
    assert polybase.enabled is True

    # Check external data source
    assert polybase.external_data_source is not None
    assert "pattern1_full_events" in polybase.external_data_source.name
    assert "events_source" in polybase.external_data_source.name
    assert polybase.external_data_source.data_source_type == "HADOOP"

    # Check external file format
    assert polybase.external_file_format is not None
    assert polybase.external_file_format.format_type == "PARQUET"
    assert polybase.external_file_format.compression == "SNAPPY"

    # Check external table
    assert len(polybase.external_tables) == 1
    ext_table = polybase.external_tables[0]
    assert "orders" in ext_table.table_name
    assert "events" in ext_table.table_name
    assert "event_date" in ext_table.partition_columns
    assert ext_table.schema_name == "dbo"
    assert ext_table.reject_type == "VALUE"
    assert ext_table.reject_value == 0
    assert len(ext_table.sample_queries) > 0


def test_polybase_setup_state_entity():
    """Test Polybase setup generation for state entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, HistoryMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern3_scd_state"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.SCD2,
            input_mode=None,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column=None,
            change_ts_column="updated_at",
            attributes=["status", "customer_id"],
            partition_by=["effective_from_date"],
            record_time_column="updated_at",
            record_time_partition="effective_from_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)

    # Check enabled
    assert polybase.enabled is True

    # Check external data source
    assert polybase.external_data_source is not None
    assert "pattern3_scd_state" in polybase.external_data_source.name
    assert "state_source" in polybase.external_data_source.name

    # Check external table
    ext_table = polybase.external_tables[0]
    assert "orders" in ext_table.table_name
    assert "state" in ext_table.table_name
    assert "effective_from_date" in ext_table.partition_columns
    assert len(ext_table.sample_queries) > 0

    # State queries should mention point-in-time concepts
    sample_sql = " ".join(ext_table.sample_queries)
    assert "effective_from_date" in sample_sql or "is_current" in sample_sql


def test_polybase_sample_queries_event():
    """Test that event entity sample queries are appropriate."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, InputMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern1"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.EVENT,
            history_mode=None,
            input_mode=InputMode.REPLACE_DAILY,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column="updated_at",
            change_ts_column=None,
            attributes=["status"],
            partition_by=["event_date"],
            record_time_partition="event_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)
    queries = polybase.external_tables[0].sample_queries

    assert len(queries) >= 2  # At least 2 sample queries

    # Query 1: Point selection with partition column
    assert "event_date" in queries[0]
    assert "2025-11-13" in queries[0]  # Date literal

    # Query 2: Aggregation
    assert "COUNT" in queries[1]


def test_polybase_sample_queries_state():
    """Test that state entity sample queries are appropriate."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, HistoryMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern3"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.SCD2,
            input_mode=None,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column=None,
            change_ts_column="updated_at",
            attributes=["status"],
            partition_by=["effective_from_date"],
            record_time_partition="effective_from_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)
    queries = polybase.external_tables[0].sample_queries

    assert len(queries) >= 3  # At least 3 sample queries for state

    # Queries should involve temporal predicates
    all_queries = " ".join(queries)
    assert "effective_from_date" in all_queries or "effective_from_dt" in all_queries or "is_current" in all_queries


def test_polybase_custom_schema_name():
    """Test that custom schema names are honored."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, InputMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern1"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.EVENT,
            history_mode=None,
            input_mode=InputMode.REPLACE_DAILY,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column="updated_at",
            change_ts_column=None,
            attributes=["status"],
            partition_by=["event_date"],
        ),
    )

    polybase = generate_polybase_setup(dataset, schema_name="silver")
    assert polybase.external_tables[0].schema_name == "silver"


def test_polybase_derived_event():
    """Test Polybase setup for derived event entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, InputMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern4"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.DERIVED_EVENT,
            history_mode=None,
            input_mode=InputMode.APPEND_LOG,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column="changed_at",
            change_ts_column="changed_at",
            attributes=["status", "change_type"],
            partition_by=["event_date"],
            record_time_partition="event_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)
    ext_table = polybase.external_tables[0]

    # Derived events should still follow event pattern (partition by event_date)
    assert "event_date" in ext_table.partition_columns


def test_polybase_derived_state():
    """Test Polybase setup for derived state entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, HistoryMode

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern5"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.DERIVED_STATE,
            history_mode=HistoryMode.SCD2,
            input_mode=None,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column=None,
            change_ts_column="changed_at",
            attributes=["status"],
            partition_by=["effective_from_date"],
            record_time_partition="effective_from_date",
        ),
    )

    polybase = generate_polybase_setup(dataset)
    ext_table = polybase.external_tables[0]

    # Derived states should follow state pattern (partition by effective_from_date)
    assert "effective_from_date" in ext_table.partition_columns


def test_temporal_functions_state_entity():
    """Test temporal function generation for state entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, HistoryMode
    from core.polybase import generate_temporal_functions_sql

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern3"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.SCD2,
            input_mode=None,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column=None,
            change_ts_column="updated_at",
            attributes=["status"],
            partition_by=["effective_from_date"],
            record_time_partition="effective_from_date",
        ),
    )

    functions_sql = generate_temporal_functions_sql(dataset)

    # Should generate two functions for state: as_of and history
    assert "get_orders_state_external_as_of" in functions_sql
    assert "get_orders_state_external_history" in functions_sql
    assert "@target_date DATE" in functions_sql
    assert "@order_id VARCHAR(255)" in functions_sql


def test_temporal_functions_event_entity():
    """Test temporal function generation for event entity."""
    from core.config.dataset import BronzeIntent, SilverIntent, DeleteMode, SchemaMode, InputMode
    from core.polybase import generate_temporal_functions_sql

    dataset = DatasetConfig(
        system="retail_demo",
        entity="orders",
        environment="dev",
        domain="retail",
        bronze=BronzeIntent(
            enabled=True,
            source_type="file",
            connection_name=None,
            source_query=None,
            path_pattern="/path",
            partition_column="run_date",
            options={"pattern_folder": "pattern1"},
        ),
        silver=SilverIntent(
            enabled=True,
            entity_kind=EntityKind.EVENT,
            history_mode=None,
            input_mode=InputMode.REPLACE_DAILY,
            delete_mode=DeleteMode.IGNORE,
            schema_mode=SchemaMode.STRICT,
            natural_keys=["order_id"],
            event_ts_column="updated_at",
            change_ts_column=None,
            attributes=["status"],
            partition_by=["event_date"],
            record_time_partition="event_date",
        ),
    )

    functions_sql = generate_temporal_functions_sql(dataset)

    # Should generate two functions for event: for_date and for_date_range
    assert "get_orders_events_external_for_date" in functions_sql
    assert "get_orders_events_external_for_date_range" in functions_sql
    assert "@target_date DATE" in functions_sql
    assert "@start_date DATE" in functions_sql
    assert "@end_date DATE" in functions_sql
