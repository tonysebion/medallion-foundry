"""Tests for pipelines.lib.polybase module."""

import json
from unittest.mock import MagicMock

import pytest

from pipelines.lib.polybase import (
    PolyBaseConfig,
    generate_data_source_ddl,
    generate_event_views,
    generate_external_table_ddl,
    generate_file_format_ddl,
    generate_from_metadata,
    generate_from_metadata_dict,
    generate_polybase_setup,
    generate_state_views,
    write_polybase_ddl_s3,
    write_polybase_script,
)


# ============================================
# PolyBaseConfig tests
# ============================================


class TestPolyBaseConfig:
    """Tests for PolyBaseConfig dataclass."""

    def test_basic_config_creation(self):
        """Creates config with required fields."""
        config = PolyBaseConfig(
            data_source_name="test_source",
            data_source_location="wasbs://container@account.blob.core.windows.net/",
        )
        assert config.data_source_name == "test_source"
        assert (
            config.data_source_location
            == "wasbs://container@account.blob.core.windows.net/"
        )

    def test_default_values(self):
        """Has sensible defaults."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="wasbs://test@test.blob.core.windows.net/",
        )
        assert config.file_format_name == "parquet_format"
        assert config.format_type == "PARQUET"
        assert config.compression == "SNAPPY"
        assert config.schema_name == "dbo"
        assert config.table_prefix == ""
        assert config.credential_name is None

    def test_external_table_name_state(self):
        """Generates correct name for STATE entity."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="wasbs://test@test.blob.core.windows.net/",
        )
        name = config.external_table_name("orders", "state")
        assert name == "orders_state_external"

    def test_external_table_name_event(self):
        """Generates correct name for EVENT entity."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="wasbs://test@test.blob.core.windows.net/",
        )
        name = config.external_table_name("audit_log", "event")
        assert name == "audit_log_events_external"

    def test_external_table_name_with_prefix(self):
        """Includes prefix in table name."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="wasbs://test@test.blob.core.windows.net/",
            table_prefix="bronze",
        )
        name = config.external_table_name("orders", "state")
        assert name == "bronze_orders_state_external"


# ============================================
# generate_external_table_ddl tests
# ============================================


class TestGenerateExternalTableDdl:
    """Tests for generate_external_table_ddl function."""

    @pytest.fixture
    def config(self):
        """Standard test config."""
        return PolyBaseConfig(
            data_source_name="silver_source",
            data_source_location="wasbs://silver@account.blob.core.windows.net/",
        )

    @pytest.fixture
    def columns(self):
        """Sample column definitions."""
        return [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
            {"name": "created_at", "sql_type": "DATETIME2", "nullable": True},
        ]

    def test_generates_create_statement(self, config, columns):
        """Generates CREATE EXTERNAL TABLE statement."""
        ddl = generate_external_table_ddl(
            "orders_external",
            columns,
            "orders/",
            config,
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "[dbo].[orders_external]" in ddl

    def test_includes_column_definitions(self, config, columns):
        """Includes all column definitions."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
        )
        assert "[id] BIGINT NOT NULL" in ddl
        assert "[name] NVARCHAR(255) NULL" in ddl
        assert "[created_at] DATETIME2 NULL" in ddl

    def test_includes_location(self, config, columns):
        """Includes LOCATION clause."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "custom/path/",
            config,
        )
        assert "LOCATION = 'custom/path/'" in ddl

    def test_includes_data_source(self, config, columns):
        """Includes DATA_SOURCE reference."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
        )
        assert "DATA_SOURCE = [silver_source]" in ddl

    def test_includes_file_format(self, config, columns):
        """Includes FILE_FORMAT reference."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
        )
        assert "FILE_FORMAT = [parquet_format]" in ddl

    def test_includes_drop_if_exists(self, config, columns):
        """Includes DROP IF EXISTS statement."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
        )
        assert "DROP EXTERNAL TABLE" in ddl

    def test_partition_columns_in_comment(self, config, columns):
        """Includes partition columns in comment."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
            partition_columns=["year", "month"],
        )
        assert "Partitioned by: year, month" in ddl

    def test_description_in_comment(self, config, columns):
        """Includes description in comment."""
        ddl = generate_external_table_ddl(
            "test_table",
            columns,
            "test/",
            config,
            description="My custom description",
        )
        assert "My custom description" in ddl


# ============================================
# generate_data_source_ddl tests
# ============================================


class TestGenerateDataSourceDdl:
    """Tests for generate_data_source_ddl function."""

    def test_generates_create_statement(self):
        """Generates CREATE EXTERNAL DATA SOURCE."""
        config = PolyBaseConfig(
            data_source_name="my_source",
            data_source_location="wasbs://container@account.blob.core.windows.net/",
        )
        ddl = generate_data_source_ddl(config)
        assert "CREATE EXTERNAL DATA SOURCE" in ddl
        assert "[my_source]" in ddl
        assert "TYPE = HADOOP" in ddl

    def test_includes_location(self):
        """Includes LOCATION clause."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://bucket/silver/",
        )
        ddl = generate_data_source_ddl(config)
        assert "LOCATION = 's3://bucket/silver/'" in ddl

    def test_includes_credential_when_specified(self):
        """Includes CREDENTIAL when provided."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://bucket/",
            credential_name="my_credential",
        )
        ddl = generate_data_source_ddl(config)
        assert "CREDENTIAL = [my_credential]" in ddl

    def test_no_credential_when_not_specified(self):
        """Omits CREDENTIAL when not provided."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://bucket/",
        )
        ddl = generate_data_source_ddl(config)
        assert "CREDENTIAL" not in ddl


# ============================================
# generate_file_format_ddl tests
# ============================================


class TestGenerateFileFormatDdl:
    """Tests for generate_file_format_ddl function."""

    def test_generates_create_statement(self):
        """Generates CREATE EXTERNAL FILE FORMAT."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://test/",
        )
        ddl = generate_file_format_ddl(config)
        assert "CREATE EXTERNAL FILE FORMAT" in ddl
        assert "[parquet_format]" in ddl

    def test_includes_format_type(self):
        """Includes FORMAT_TYPE clause."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://test/",
            format_type="PARQUET",
        )
        ddl = generate_file_format_ddl(config)
        assert "FORMAT_TYPE = PARQUET" in ddl


# ============================================
# generate_state_views tests
# ============================================


class TestGenerateStateViews:
    """Tests for generate_state_views function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://test/",
        )

    def test_generates_current_view_scd1(self, config):
        """Generates current view for SCD1."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
        )
        assert "vw_orders_state_current" in ddl
        assert "CREATE OR ALTER VIEW" in ddl

    def test_generates_current_view_scd2(self, config):
        """Generates current view with is_current filter for SCD2."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="full_history",
        )
        assert "vw_orders_state_current" in ddl
        assert "is_current = 1" in ddl

    def test_generates_point_in_time_function_scd2(self, config):
        """Generates point-in-time function for SCD2."""
        ddl = generate_state_views(
            "customers_state_external",
            ["customer_id"],
            config,
            history_mode="full_history",
        )
        assert "fn_customers_state_as_of" in ddl
        assert "effective_from <= @as_of_date" in ddl

    def test_generates_history_function_scd2(self, config):
        """Generates entity history function for SCD2."""
        ddl = generate_state_views(
            "customers_state_external",
            ["customer_id"],
            config,
            history_mode="full_history",
        )
        assert "fn_customers_state_history" in ddl

    def test_generates_history_summary_scd2(self, config):
        """Generates history summary view for SCD2."""
        ddl = generate_state_views(
            "customers_state_external",
            ["customer_id"],
            config,
            history_mode="full_history",
        )
        assert "vw_customers_state_history_summary" in ddl
        assert "version_count" in ddl

    def test_tombstone_mode_filters_deleted(self, config):
        """Includes deleted filter for tombstone mode."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
            delete_mode="tombstone",
        )
        assert "_deleted = 0" in ddl

    def test_scd2_tombstone_filters_point_in_time(self, config):
        """Point-in-time function excludes tombstones when delete_mode=tombstone."""
        ddl = generate_state_views(
            "customers_state_external",
            ["customer_id"],
            config,
            history_mode="full_history",
            delete_mode="tombstone",
        )
        # Point-in-time function should include tombstone filter
        assert "fn_customers_state_as_of" in ddl
        assert "_deleted = 0 OR _deleted IS NULL" in ddl

    def test_scd2_tombstone_current_view_filters_deleted(self, config):
        """Current view for SCD2 + tombstone excludes deleted records."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="full_history",
            delete_mode="tombstone",
        )
        # Current view should filter by is_current AND exclude deleted
        assert "is_current = 1" in ddl
        assert "_deleted = 0 OR _deleted IS NULL" in ddl

    def test_composite_key_history_function(self, config):
        """History function supports composite natural keys."""
        ddl = generate_state_views(
            "order_lines_state_external",
            ["order_id", "line_id"],
            config,
            history_mode="full_history",
        )
        # Should have multiple parameters
        assert "@key_0 NVARCHAR(255)" in ddl
        assert "@key_1 NVARCHAR(255)" in ddl
        assert "[order_id] = @key_0" in ddl
        assert "[line_id] = @key_1" in ddl

    def test_single_key_history_function(self, config):
        """History function works with single natural key."""
        ddl = generate_state_views(
            "customers_state_external",
            ["customer_id"],
            config,
            history_mode="full_history",
        )
        # Should have single parameter
        assert "@key_value NVARCHAR(255)" in ddl
        assert "[customer_id] = @key_value" in ddl


# ============================================
# generate_event_views tests
# ============================================


class TestGenerateEventViews:
    """Tests for generate_event_views function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://test/",
        )

    def test_generates_date_range_function(self, config):
        """Generates date range query function."""
        ddl = generate_event_views(
            "audit_events_external",
            ["event_id"],
            config,
        )
        assert "fn_audit_events_for_dates" in ddl
        assert "@start_date" in ddl
        assert "@end_date" in ddl

    def test_generates_single_date_function(self, config):
        """Generates single date query function."""
        ddl = generate_event_views(
            "clicks_events_external",
            ["click_id"],
            config,
        )
        assert "fn_clicks_events_for_date" in ddl
        assert "@target_date" in ddl

    def test_generates_daily_summary_view(self, config):
        """Generates daily summary view."""
        ddl = generate_event_views(
            "audit_events_external",
            ["event_id"],
            config,
            change_timestamp="event_ts",
        )
        assert "vw_audit_events_daily_summary" in ddl
        assert "event_count" in ddl
        assert "unique_entities" in ddl


# ============================================
# generate_polybase_setup tests
# ============================================


class TestGeneratePolybaseSetup:
    """Tests for generate_polybase_setup function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver_source",
            data_source_location="wasbs://silver@account.blob.core.windows.net/",
        )

    @pytest.fixture
    def columns(self):
        return [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
        ]

    def test_includes_header_comment(self, config, columns):
        """Includes descriptive header."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "PolyBase Setup for: orders" in ddl
        assert "Entity Kind: state" in ddl

    def test_includes_data_source(self, config, columns):
        """Includes data source DDL."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "CREATE EXTERNAL DATA SOURCE" in ddl

    def test_includes_file_format(self, config, columns):
        """Includes file format DDL."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "CREATE EXTERNAL FILE FORMAT" in ddl

    def test_includes_external_table(self, config, columns):
        """Includes external table DDL."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "orders_state_external" in ddl

    def test_includes_state_views(self, config, columns):
        """Includes views for STATE entity."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "vw_orders_state_current" in ddl

    def test_includes_event_views(self, config, columns):
        """Includes views for EVENT entity."""
        ddl = generate_polybase_setup(
            "events",
            columns,
            "event",
            ["id"],
            config,
        )
        assert "fn_events_events_for_dates" in ddl

    def test_credential_instructions_when_specified(self, columns):
        """Includes credential setup instructions."""
        config = PolyBaseConfig(
            data_source_name="test",
            data_source_location="s3://test/",
            credential_name="my_cred",
            s3_endpoint="http://minio:9000",
        )
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        assert "CREDENTIAL SETUP" in ddl
        assert "[my_cred]" in ddl
        assert "S3 Endpoint: http://minio:9000" in ddl


# ============================================
# generate_from_metadata tests
# ============================================


class TestGenerateFromMetadata:
    """Tests for generate_from_metadata function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="wasbs://silver@account.blob.core.windows.net/",
        )

    def test_reads_metadata_file(self, config, tmp_path):
        """Reads and parses metadata from file."""
        metadata = {
            "columns": [
                {"name": "id", "sql_type": "BIGINT", "nullable": False},
                {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
            ],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "history_mode": "current_only",
        }
        metadata_path = tmp_path / "orders" / "_metadata.json"
        metadata_path.parent.mkdir(parents=True)
        metadata_path.write_text(json.dumps(metadata))

        ddl = generate_from_metadata(metadata_path, config)

        assert "orders" in ddl  # Derived from directory name
        assert "CREATE EXTERNAL TABLE" in ddl

    def test_uses_entity_name_override(self, config, tmp_path):
        """Uses provided entity_name instead of directory name."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
        }
        metadata_path = tmp_path / "original_name" / "_metadata.json"
        metadata_path.parent.mkdir(parents=True)
        metadata_path.write_text(json.dumps(metadata))

        ddl = generate_from_metadata(metadata_path, config, entity_name="custom_name")

        assert "custom_name" in ddl


# ============================================
# generate_from_metadata_dict tests
# ============================================


class TestGenerateFromMetadataDict:
    """Tests for generate_from_metadata_dict function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    def test_generates_ddl_from_dict(self, config):
        """Generates DDL from metadata dictionary."""
        metadata = {
            "columns": [
                {"name": "id", "sql_type": "BIGINT", "nullable": False},
            ],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "history_mode": "current_only",
        }
        ddl = generate_from_metadata_dict(metadata, config, entity_name="orders")
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "orders" in ddl

    def test_requires_entity_name(self, config):
        """Raises error when entity_name not provided."""
        metadata = {"columns": [], "entity_kind": "state", "natural_keys": []}
        with pytest.raises(ValueError, match="entity_name is required"):
            generate_from_metadata_dict(metadata, config)


# ============================================
# write_polybase_ddl_s3 tests
# ============================================


class TestWritePolybaseDdlS3:
    """Tests for write_polybase_ddl_s3 function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    @pytest.fixture
    def metadata(self):
        return {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
        }

    def test_writes_to_storage(self, config, metadata):
        """Writes DDL to storage backend."""
        mock_storage = MagicMock()
        mock_storage.write_text.return_value = MagicMock(success=True)
        mock_storage.base_path = "s3://silver/orders/"

        result = write_polybase_ddl_s3(
            mock_storage,
            metadata,
            config,
            entity_name="orders",
        )

        assert result is True
        mock_storage.write_text.assert_called_once()
        call_args = mock_storage.write_text.call_args
        assert call_args[0][0] == "_polybase.sql"
        assert "CREATE EXTERNAL TABLE" in call_args[0][1]

    def test_custom_filename(self, config, metadata):
        """Uses custom filename when provided."""
        mock_storage = MagicMock()
        mock_storage.write_text.return_value = MagicMock(success=True)
        mock_storage.base_path = "s3://silver/"

        write_polybase_ddl_s3(
            mock_storage,
            metadata,
            config,
            entity_name="orders",
            filename="custom.sql",
        )

        call_args = mock_storage.write_text.call_args
        assert call_args[0][0] == "custom.sql"

    def test_returns_false_on_failure(self, config, metadata):
        """Returns False when write fails."""
        mock_storage = MagicMock()
        mock_storage.write_text.return_value = MagicMock(
            success=False, error="Write failed"
        )

        result = write_polybase_ddl_s3(
            mock_storage,
            metadata,
            config,
            entity_name="orders",
        )

        assert result is False

    def test_handles_exception(self, config, metadata):
        """Returns False on exception."""
        mock_storage = MagicMock()
        mock_storage.write_text.side_effect = Exception("Storage error")

        result = write_polybase_ddl_s3(
            mock_storage,
            metadata,
            config,
            entity_name="orders",
        )

        assert result is False


# ============================================
# write_polybase_script tests
# ============================================


class TestWritePolybaseScript:
    """Tests for write_polybase_script function."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="wasbs://silver@account.blob.core.windows.net/",
        )

    def test_writes_to_file(self, config, tmp_path):
        """Writes DDL to file."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
        }
        metadata_path = tmp_path / "orders" / "_metadata.json"
        metadata_path.parent.mkdir(parents=True)
        metadata_path.write_text(json.dumps(metadata))

        output_path = tmp_path / "output.sql"
        result = write_polybase_script(output_path, metadata_path, config)

        assert result == output_path
        assert output_path.exists()
        content = output_path.read_text()
        assert "CREATE EXTERNAL TABLE" in content


# ============================================
# Edge Case Tests - None/Empty natural_keys
# ============================================


class TestNoneNaturalKeysEdgeCases:
    """Tests for handling None/empty natural_keys (periodic_snapshot model)."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    def test_state_views_none_keys_current_only(self, config):
        """State views work with None natural_keys in current_only mode."""
        ddl = generate_state_views(
            "periodic_data_state_external",
            None,  # None keys (periodic_snapshot without deduplication)
            config,
            history_mode="current_only",
        )
        # Should generate current view without errors
        assert "vw_periodic_data_state_current" in ddl
        assert "CREATE OR ALTER VIEW" in ddl
        # Should NOT contain GROUP BY with empty columns
        assert "GROUP BY ," not in ddl

    def test_state_views_empty_keys_current_only(self, config):
        """State views work with empty natural_keys list in current_only mode."""
        ddl = generate_state_views(
            "snapshot_state_external",
            [],  # Empty list (treated as None)
            config,
            history_mode="current_only",
        )
        assert "vw_snapshot_state_current" in ddl
        assert "CREATE OR ALTER VIEW" in ddl

    def test_state_views_none_keys_full_history(self, config):
        """State views work with None keys in full_history mode."""
        ddl = generate_state_views(
            "history_state_external",
            None,
            config,
            history_mode="full_history",
        )
        # Should generate current view and point-in-time function
        assert "vw_history_state_current" in ddl
        assert "fn_history_state_as_of" in ddl
        # Should NOT generate history function (requires keys)
        assert "fn_history_state_history" not in ddl
        # Should NOT generate history summary (requires keys)
        assert "vw_history_state_history_summary" not in ddl

    def test_state_views_none_keys_tombstone(self, config):
        """State views with tombstone mode work with None keys."""
        ddl = generate_state_views(
            "tombstone_state_external",
            None,
            config,
            history_mode="current_only",
            delete_mode="tombstone",
        )
        assert "vw_tombstone_state_current" in ddl
        # Should still apply deleted filter
        assert "_deleted = 0" in ddl

    def test_event_views_none_keys(self, config):
        """Event views work with None natural_keys."""
        ddl = generate_event_views(
            "logs_events_external",
            None,  # None keys
            config,
            change_timestamp="event_ts",
        )
        # Should generate date range functions
        assert "fn_logs_events_for_dates" in ddl
        assert "fn_logs_events_for_date" in ddl
        # Daily summary still generated (counts all events)
        assert "vw_logs_events_daily_summary" in ddl

    def test_event_views_empty_keys(self, config):
        """Event views work with empty natural_keys list."""
        ddl = generate_event_views(
            "audit_events_external",
            [],  # Empty list
            config,
        )
        assert "fn_audit_events_for_dates" in ddl
        assert "fn_audit_events_for_date" in ddl

    def test_polybase_setup_none_keys_state(self, config):
        """Full polybase setup works with None keys for state entity."""
        columns = [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "value", "sql_type": "NVARCHAR(255)", "nullable": True},
        ]
        ddl = generate_polybase_setup(
            "periodic_snapshot",
            columns,
            "state",
            None,  # None natural_keys
            config,
            history_mode="current_only",
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "CREATE EXTERNAL DATA SOURCE" in ddl
        assert "periodic_snapshot_state_external" in ddl
        # Should generate valid SQL without errors
        assert "GROUP BY ," not in ddl

    def test_polybase_setup_none_keys_event(self, config):
        """Full polybase setup works with None keys for event entity."""
        columns = [
            {"name": "event_id", "sql_type": "BIGINT", "nullable": False},
            {"name": "payload", "sql_type": "NVARCHAR(MAX)", "nullable": True},
        ]
        ddl = generate_polybase_setup(
            "raw_events",
            columns,
            "event",
            None,  # None natural_keys
            config,
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "raw_events_events_external" in ddl
        assert "fn_raw_events_events_for_dates" in ddl

    def test_from_metadata_dict_none_keys(self, config):
        """generate_from_metadata_dict handles None natural_keys."""
        metadata = {
            "columns": [
                {"name": "id", "sql_type": "BIGINT", "nullable": False},
            ],
            "entity_kind": "state",
            "natural_keys": None,  # Explicitly None
            "history_mode": "current_only",
        }
        ddl = generate_from_metadata_dict(metadata, config, entity_name="snapshot")
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "snapshot" in ddl

    def test_from_metadata_dict_missing_keys(self, config):
        """generate_from_metadata_dict handles missing natural_keys field."""
        metadata = {
            "columns": [
                {"name": "id", "sql_type": "BIGINT", "nullable": False},
            ],
            "entity_kind": "state",
            # natural_keys field not present
            "history_mode": "current_only",
        }
        ddl = generate_from_metadata_dict(metadata, config, entity_name="snapshot")
        assert "CREATE EXTERNAL TABLE" in ddl

    def test_from_metadata_dict_none_change_timestamp(self, config):
        """Explicit None change_timestamp uses default, not literal 'None' in SQL."""
        metadata = {
            "columns": [
                {"name": "id", "sql_type": "BIGINT", "nullable": False},
            ],
            "entity_kind": "event",
            "natural_keys": ["id"],
            "change_timestamp": None,  # Explicitly None (periodic_snapshot model)
        }
        ddl = generate_from_metadata_dict(
            metadata, config, entity_name="snapshot_events"
        )
        # Should NOT have literal [None] in the SQL - should use default "updated_at"
        assert "[None]" not in ddl
        # Should use the default fallback instead
        assert "[updated_at]" in ddl or "updated_at" in ddl


class TestDomainSubjectNaming:
    """Tests for domain/subject-based entity naming (Category 4e, 4f)."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    def test_from_metadata_dict_uses_domain_subject(self, config):
        """generate_from_metadata_dict uses domain as schema and subject as table name."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "history_mode": "current_only",
            "domain": "sales",
            "subject": "orders",
        }
        ddl = generate_from_metadata_dict(metadata, config)  # No entity_name!
        # Should use domain as schema and subject as table base name
        assert "[sales].[orders_state_external]" in ddl
        assert "CREATE EXTERNAL TABLE" in ddl

    def test_from_metadata_dict_uses_subject_only(self, config):
        """generate_from_metadata_dict uses subject alone if domain is missing."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "subject": "customers",  # No domain
        }
        ddl = generate_from_metadata_dict(metadata, config)
        assert "customers_state_external" in ddl

    def test_from_metadata_dict_explicit_entity_name_overrides(self, config):
        """Explicit entity_name takes precedence over domain/subject for table naming."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "domain": "sales",
            "subject": "orders",
        }
        ddl = generate_from_metadata_dict(
            metadata, config, entity_name="my_custom_name"
        )
        # When domain is provided, it's still used as schema
        # But entity_name affects the internal name only (shown in header)
        # Table name uses subject as base since domain is present
        assert "[sales].[orders_state_external]" in ddl
        assert "PolyBase Setup for: my_custom_name" in ddl

    def test_from_metadata_dict_requires_name_when_no_domain_subject(self, config):
        """generate_from_metadata_dict raises when no entity_name and no domain/subject."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            # No domain, subject, or entity_name
        }
        with pytest.raises(ValueError) as exc_info:
            generate_from_metadata_dict(metadata, config)
        assert "entity_name is required" in str(exc_info.value)

    def test_from_metadata_file_uses_domain_subject(self, config, tmp_path):
        """generate_from_metadata uses domain/subject from _metadata.json."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "domain": "retail",
            "subject": "products",
        }
        metadata_path = tmp_path / "_metadata.json"
        metadata_path.write_text(json.dumps(metadata))

        ddl = generate_from_metadata(metadata_path, config)
        assert "retail_products_state_external" in ddl

    def test_from_metadata_file_sanitizes_path_fallback(self, config, tmp_path):
        """generate_from_metadata sanitizes path-derived names when no domain/subject."""
        # Create a directory with invalid chars in name (like dt=20250117)
        bad_dir = tmp_path / "dt=20250117"
        bad_dir.mkdir()

        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            # No domain or subject - will fallback to directory name
        }
        metadata_path = bad_dir / "_metadata.json"
        metadata_path.write_text(json.dumps(metadata))

        ddl = generate_from_metadata(metadata_path, config)
        # Should sanitize "dt=20250117" to "dt_20250117" (replacing =)
        assert "dt_20250117_state_external" in ddl
        assert "dt=20250117" not in ddl  # Invalid char should be removed


class TestEmptyColumnsEdgeCases:
    """Tests for handling empty columns list."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    def test_external_table_empty_columns(self, config):
        """External table DDL handles empty columns list."""
        # Empty columns is unusual but should not crash
        ddl = generate_external_table_ddl(
            "empty_table",
            [],  # Empty columns
            "empty/",
            config,
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        # Should have empty column block
        assert "[dbo].[empty_table]" in ddl

    def test_polybase_setup_empty_columns(self, config):
        """Full setup handles empty columns list."""
        ddl = generate_polybase_setup(
            "empty_entity",
            [],  # Empty columns
            "state",
            ["id"],  # Keys still specified
            config,
        )
        assert "CREATE EXTERNAL TABLE" in ddl


class TestLocationPathConstruction:
    """Tests for Hive-style location path construction with domain/subject/env_prefix."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://bucket/silver/",
        )

    def test_location_uses_domain_subject_hive_style(self, config):
        """Location should use domain=X/subject=Y/ format when both provided."""
        ddl = generate_polybase_setup(
            "sales_orders",
            [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "state",
            ["id"],
            config,
            domain="sales",
            subject="orders",
        )
        # Location should be Hive-style, not entity_name/
        assert "LOCATION = 'domain=sales/subject=orders/'" in ddl
        # Table name uses domain as schema and subject as base
        assert "[sales].[orders_state_external]" in ddl

    def test_location_with_env_prefix(self, config):
        """Location should include env_prefix when provided."""
        ddl = generate_polybase_setup(
            "sales_orders",
            [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "state",
            ["id"],
            config,
            domain="sales",
            subject="orders",
            env_prefix="production",
        )
        # Location should include env_prefix
        assert "LOCATION = 'production/domain=sales/subject=orders/'" in ddl
        # Table name uses domain as schema and subject as base
        assert "[sales].[orders_state_external]" in ddl
        assert "production_sales" not in ddl

    def test_location_fallback_without_domain_subject(self, config):
        """Location should fall back to entity_name/ when domain/subject missing."""
        ddl = generate_polybase_setup(
            "my_entity",
            [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "state",
            ["id"],
            config,
            # No domain or subject
        )
        assert "LOCATION = 'my_entity/'" in ddl

    def test_from_metadata_dict_passes_domain_subject_to_setup(self, config):
        """generate_from_metadata_dict should pass domain/subject for location."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "domain": "retail",
            "subject": "products",
        }
        ddl = generate_from_metadata_dict(metadata, config)
        # Should use Hive-style location
        assert "LOCATION = 'domain=retail/subject=products/'" in ddl
        # Table name uses domain as schema and subject as base
        assert "[retail].[products_state_external]" in ddl

    def test_from_metadata_dict_with_env_prefix(self, config):
        """generate_from_metadata_dict should accept env_prefix parameter."""
        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "domain": "retail",
            "subject": "products",
        }
        ddl = generate_from_metadata_dict(metadata, config, env_prefix="staging")
        # Should include env_prefix in location
        assert "LOCATION = 'staging/domain=retail/subject=products/'" in ddl
        # Table name uses domain as schema and subject as base
        assert "[retail].[products_state_external]" in ddl
        assert "staging_retail" not in ddl

    def test_write_polybase_ddl_s3_with_env_prefix(self, config):
        """write_polybase_ddl_s3 should pass env_prefix to DDL generation."""
        from unittest.mock import Mock

        # Mock storage backend
        mock_storage = Mock()
        mock_storage.write_text.return_value = Mock(success=True)
        mock_storage.base_path = "s3://bucket/silver/"

        metadata = {
            "columns": [{"name": "id", "sql_type": "BIGINT", "nullable": False}],
            "entity_kind": "state",
            "natural_keys": ["id"],
            "domain": "sales",
            "subject": "orders",
        }

        result = write_polybase_ddl_s3(
            mock_storage,
            metadata,
            config,
            entity_name="sales_orders",
            env_prefix="production",
        )

        assert result is True
        # Verify write was called
        assert mock_storage.write_text.called
        ddl_content = mock_storage.write_text.call_args[0][1]
        # Check env_prefix is in location
        assert "production/domain=sales/subject=orders/" in ddl_content


# ============================================
# dt Partition Column Tests
# ============================================


class TestDtPartitionColumn:
    """Tests for dt partition column in external tables (Hive partition pruning)."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    @pytest.fixture
    def columns(self):
        return [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
        ]

    def test_dt_column_added_when_enabled(self, config, columns):
        """External table includes dt DATE column when include_dt_partition=True."""
        ddl = generate_external_table_ddl(
            "orders_external",
            columns,
            "orders/",
            config,
            include_dt_partition=True,
        )
        assert "[dt] DATE NULL" in ddl
        assert "Hive partition column" in ddl

    def test_dt_column_omitted_when_disabled(self, config, columns):
        """External table excludes dt column when include_dt_partition=False."""
        ddl = generate_external_table_ddl(
            "orders_external",
            columns,
            "orders/",
            config,
            include_dt_partition=False,
        )
        assert "[dt]" not in ddl

    def test_polybase_setup_includes_dt_by_default(self, config, columns):
        """generate_polybase_setup includes dt column by default."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
        )
        # Default is include_dt_partition=True
        assert "[dt] DATE NULL" in ddl

    def test_polybase_setup_excludes_dt_when_disabled(self, config, columns):
        """generate_polybase_setup excludes dt when include_dt_partition=False."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
            include_dt_partition=False,
        )
        assert "[dt]" not in ddl

    def test_dt_column_not_duplicated_when_already_in_columns(self, config):
        """External table does not duplicate dt column if already present in columns."""
        columns_with_dt = [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
            {"name": "dt", "sql_type": "DATE", "nullable": True},  # dt already in data
        ]
        ddl = generate_external_table_ddl(
            "orders_external",
            columns_with_dt,
            "orders/",
            config,
            include_dt_partition=True,  # Would normally add dt
        )
        # Count occurrences of [dt] - should be exactly 1, not 2
        dt_count = ddl.count("[dt]")
        assert dt_count == 1, f"Expected 1 [dt] column, found {dt_count}"


# ============================================
# Date-Filtered Views Tests
# ============================================


class TestDateFilteredViews:
    """Tests for date-filtered views using dt partition column."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    def test_state_views_include_date_range_function(self, config):
        """State views include fn_*_for_dates function when include_dt_views=True."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
            include_dt_views=True,
        )
        assert "fn_orders_state_for_dates" in ddl
        assert "@start_date DATE" in ddl
        assert "@end_date DATE" in ddl
        assert "[dt] >= @start_date" in ddl
        assert "[dt] <= @end_date" in ddl

    def test_state_views_include_single_date_function(self, config):
        """State views include fn_*_for_date function when include_dt_views=True."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
            include_dt_views=True,
        )
        assert "fn_orders_state_for_date" in ddl
        assert "@target_date DATE" in ddl
        assert "[dt] = @target_date" in ddl

    def test_state_views_exclude_date_views_when_disabled(self, config):
        """State views exclude date functions when include_dt_views=False."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
            include_dt_views=False,
        )
        assert "fn_orders_state_for_dates" not in ddl
        assert "fn_orders_state_for_date" not in ddl

    def test_state_views_date_functions_respect_tombstone_filter(self, config):
        """Date functions include tombstone filter when delete_mode=tombstone."""
        ddl = generate_state_views(
            "orders_state_external",
            ["order_id"],
            config,
            history_mode="current_only",
            delete_mode="tombstone",
            include_dt_views=True,
        )
        # Date functions should include deleted filter
        assert "fn_orders_state_for_dates" in ddl
        assert "_deleted = 0 OR _deleted IS NULL" in ddl

    def test_event_views_use_dt_partition_by_default(self, config):
        """Event views use dt column for partition-aware queries by default."""
        ddl = generate_event_views(
            "audit_events_external",
            ["event_id"],
            config,
            use_dt_partition=True,
        )
        assert "[dt] >= @start_date" in ddl
        assert "[dt] <= @end_date" in ddl
        assert "[dt] = @target_date" in ddl

    def test_event_views_use_custom_partition_when_dt_disabled(self, config):
        """Event views use custom partition column when use_dt_partition=False."""
        ddl = generate_event_views(
            "audit_events_external",
            ["event_id"],
            config,
            partition_columns=["event_date"],
            use_dt_partition=False,
        )
        assert "[event_date] >= @start_date" in ddl
        assert "[event_date] <= @end_date" in ddl
        assert "[dt]" not in ddl


# ============================================
# Domain-Based Schema Naming Tests
# ============================================


class TestDomainBasedSchemaNaming:
    """Tests for using domain as schema name and subject as table name."""

    @pytest.fixture
    def config(self):
        return PolyBaseConfig(
            data_source_name="silver",
            data_source_location="s3://silver/",
        )

    @pytest.fixture
    def columns(self):
        return [
            {"name": "id", "sql_type": "BIGINT", "nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(255)", "nullable": True},
        ]

    def test_uses_domain_as_schema(self, config, columns):
        """External table uses domain as schema name when provided."""
        ddl = generate_polybase_setup(
            "sales_orders",
            columns,
            "state",
            ["id"],
            config,
            domain="sales",
            subject="orders",
        )
        # Should use sales as schema, not dbo
        assert "[sales].[orders_state_external]" in ddl
        # Views should also be in sales schema
        assert "[sales].[vw_orders_state_current]" in ddl

    def test_creates_schema_ddl_when_domain_provided(self, config, columns):
        """Generates CREATE SCHEMA statement when domain is provided."""
        ddl = generate_polybase_setup(
            "sales_orders",
            columns,
            "state",
            ["id"],
            config,
            domain="sales",
            subject="orders",
        )
        assert "CREATE SCHEMA [sales]" in ddl
        assert "IF NOT EXISTS" in ddl

    def test_uses_dbo_schema_without_domain(self, config, columns):
        """Falls back to dbo schema when domain is not provided."""
        ddl = generate_polybase_setup(
            "orders",
            columns,
            "state",
            ["id"],
            config,
            # No domain specified
        )
        assert "[dbo].[orders_state_external]" in ddl
        assert "CREATE SCHEMA" not in ddl

    def test_date_functions_use_domain_schema(self, config, columns):
        """Date-filtered functions use domain as schema."""
        ddl = generate_polybase_setup(
            "retail_products",
            columns,
            "state",
            ["id"],
            config,
            domain="retail",
            subject="products",
            include_dt_partition=True,
        )
        assert "[retail].[fn_products_state_for_dates]" in ddl
        assert "[retail].[fn_products_state_for_date]" in ddl

    def test_event_views_use_domain_schema(self, config, columns):
        """Event views use domain as schema."""
        ddl = generate_polybase_setup(
            "audit_events",
            columns,
            "event",
            ["id"],
            config,
            domain="audit",
            subject="events",
        )
        assert "[audit].[events_events_external]" in ddl
        assert "[audit].[fn_events_events_for_dates]" in ddl

    def test_header_shows_schema_info(self, config, columns):
        """DDL header includes schema information."""
        ddl = generate_polybase_setup(
            "sales_orders",
            columns,
            "state",
            ["id"],
            config,
            domain="sales",
            subject="orders",
        )
        assert "Schema: sales (domain-based)" in ddl
