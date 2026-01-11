"""Tests for YAML configuration loader."""

import pytest

from pipelines.lib.config_loader import (
    load_pipeline,
    load_bronze_from_yaml,
    load_silver_from_yaml,
    validate_yaml_config,
    YAMLConfigError,
)
from pipelines.lib.bronze import LoadPattern, SourceType
from pipelines.lib.silver import EntityKind, HistoryMode


class TestLoadBronzeFromYaml:
    """Tests for loading Bronze configuration from YAML."""

    def test_minimal_csv_config(self):
        """Minimal CSV file configuration."""
        config = {
            "system": "retail",
            "entity": "orders",
            "source_type": "file_csv",
            "source_path": "./data/orders.csv",
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.system == "retail"
        assert bronze.entity == "orders"
        assert bronze.source_type == SourceType.FILE_CSV
        assert bronze.load_pattern == LoadPattern.FULL_SNAPSHOT

    def test_all_source_types(self):
        """Test all supported source types."""
        # Simple file-based sources
        simple_file_sources = [
            ("file_csv", SourceType.FILE_CSV),
            ("file_parquet", SourceType.FILE_PARQUET),
            ("file_json", SourceType.FILE_JSON),
            ("file_jsonl", SourceType.FILE_JSONL),
            ("file_excel", SourceType.FILE_EXCEL),
            ("api_rest", SourceType.API_REST),
        ]

        for yaml_value, expected_enum in simple_file_sources:
            config = {
                "system": "test",
                "entity": "test",
                "source_type": yaml_value,
                "source_path": "./data/test.csv",
            }
            bronze = load_bronze_from_yaml(config)
            assert bronze.source_type == expected_enum, f"Failed for {yaml_value}"

        # Fixed-width requires columns and widths
        config = {
            "system": "test",
            "entity": "test",
            "source_type": "file_fixed_width",
            "source_path": "./data/test.txt",
            "options": {
                "columns": ["id", "name"],
                "widths": [10, 20],
            },
        }
        bronze = load_bronze_from_yaml(config)
        assert bronze.source_type == SourceType.FILE_FIXED_WIDTH

        # Database sources (require host and database)
        db_source_types = [
            ("database_mssql", SourceType.DATABASE_MSSQL),
            ("database_postgres", SourceType.DATABASE_POSTGRES),
            ("database_mysql", SourceType.DATABASE_MYSQL),
            ("database_db2", SourceType.DATABASE_DB2),
        ]

        for yaml_value, expected_enum in db_source_types:
            config = {
                "system": "test",
                "entity": "test",
                "source_type": yaml_value,
                "host": "localhost",
                "database": "testdb",
            }
            bronze = load_bronze_from_yaml(config)
            assert bronze.source_type == expected_enum, f"Failed for {yaml_value}"

    def test_invalid_source_type_raises(self):
        """Invalid source type should raise YAMLConfigError."""
        config = {
            "system": "test",
            "entity": "test",
            "source_type": "invalid_type",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_bronze_from_yaml(config)

        assert "invalid_type" in str(exc_info.value).lower()
        assert "source_type" in str(exc_info.value).lower()

    def test_all_load_patterns(self):
        """Test all supported load patterns."""
        # Patterns that don't require watermark_column
        simple_patterns = [
            ("full_snapshot", LoadPattern.FULL_SNAPSHOT),
        ]

        for yaml_value, expected_enum in simple_patterns:
            config = {
                "system": "test",
                "entity": "test",
                "source_type": "file_csv",
                "source_path": "./data/test.csv",
                "load_pattern": yaml_value,
            }
            bronze = load_bronze_from_yaml(config)
            assert bronze.load_pattern == expected_enum, f"Failed for {yaml_value}"

        # Patterns that require watermark_column
        incremental_patterns = [
            ("incremental", LoadPattern.INCREMENTAL_APPEND),
            ("incremental_append", LoadPattern.INCREMENTAL_APPEND),
            ("cdc", LoadPattern.CDC),
        ]

        for yaml_value, expected_enum in incremental_patterns:
            config = {
                "system": "test",
                "entity": "test",
                "source_type": "file_csv",
                "source_path": "./data/test.csv",
                "load_pattern": yaml_value,
                "watermark_column": "updated_at",
            }
            bronze = load_bronze_from_yaml(config)
            assert bronze.load_pattern == expected_enum, f"Failed for {yaml_value}"

    def test_database_config(self):
        """Database configuration with host and database."""
        config = {
            "system": "crm",
            "entity": "customers",
            "source_type": "database_mssql",
            "host": "${DB_HOST}",
            "database": "CRM_DB",
            "query": "SELECT * FROM Customers WHERE Active = 1",
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.host == "${DB_HOST}"
        assert bronze.database == "CRM_DB"
        assert bronze.query == "SELECT * FROM Customers WHERE Active = 1"

    def test_incremental_config(self):
        """Incremental load with watermark."""
        config = {
            "system": "sales",
            "entity": "orders",
            "source_type": "database_postgres",
            "host": "localhost",
            "database": "salesdb",
            "load_pattern": "incremental",
            "watermark_column": "updated_at",
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.load_pattern == LoadPattern.INCREMENTAL_APPEND
        assert bronze.watermark_column == "updated_at"

    def test_chunking_config(self):
        """Chunk size for large data."""
        config = {
            "system": "warehouse",
            "entity": "transactions",
            "source_type": "database_mssql",
            "host": "localhost",
            "database": "warehouse",
            "chunk_size": 100000,
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.chunk_size == 100000

    def test_full_refresh_days(self):
        """Periodic full refresh configuration."""
        config = {
            "system": "sales",
            "entity": "orders",
            "source_type": "database_mssql",
            "host": "localhost",
            "database": "salesdb",
            "load_pattern": "incremental",
            "watermark_column": "modified_date",
            "full_refresh_days": 7,
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.full_refresh_days == 7

    def test_options_dict(self):
        """Extra options passed through."""
        config = {
            "system": "legacy",
            "entity": "report",
            "source_type": "file_fixed_width",
            "options": {
                "columns": ["id", "name", "value"],
                "widths": [10, 30, 20],
            },
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.options["columns"] == ["id", "name", "value"]
        assert bronze.options["widths"] == [10, 30, 20]

    def test_s3_storage_options_top_level(self):
        """S3 storage options at top level (for Nutanix Objects, MinIO)."""
        config = {
            "system": "retail",
            "entity": "orders",
            "source_type": "file_parquet",
            "source_path": "s3://bucket/data/*.parquet",
            "target_path": "s3://bucket/bronze/",
            "s3_endpoint_url": "https://objects.nutanix.local:443",
            "s3_signature_version": "s3v4",
            "s3_addressing_style": "path",
            "s3_region": "us-west-2",
        }
        bronze = load_bronze_from_yaml(config)

        # S3 options should be merged into options dict
        assert bronze.options["endpoint_url"] == "https://objects.nutanix.local:443"
        assert bronze.options["s3_signature_version"] == "s3v4"
        assert bronze.options["s3_addressing_style"] == "path"
        assert bronze.options["region"] == "us-west-2"

    def test_s3_options_in_nested_options(self):
        """S3 options in nested options dict (backward compatibility)."""
        config = {
            "system": "retail",
            "entity": "orders",
            "source_type": "file_parquet",
            "source_path": "s3://bucket/data/*.parquet",
            "options": {
                "s3_signature_version": "s3v4",
                "s3_addressing_style": "path",
            },
        }
        bronze = load_bronze_from_yaml(config)

        assert bronze.options["s3_signature_version"] == "s3v4"
        assert bronze.options["s3_addressing_style"] == "path"

    def test_missing_required_field_system(self):
        """Missing system field should raise."""
        config = {
            "entity": "orders",
            "source_type": "file_csv",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_bronze_from_yaml(config)

        assert "system" in str(exc_info.value).lower()

    def test_missing_required_field_entity(self):
        """Missing entity field should raise."""
        config = {
            "system": "retail",
            "source_type": "file_csv",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_bronze_from_yaml(config)

        assert "entity" in str(exc_info.value).lower()

    def test_missing_required_field_source_type(self):
        """Missing source_type field should raise."""
        config = {
            "system": "retail",
            "entity": "orders",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_bronze_from_yaml(config)

        assert "source_type" in str(exc_info.value).lower()


class TestLoadSilverFromYaml:
    """Tests for loading Silver configuration from YAML."""

    def test_minimal_config(self):
        """Minimal Silver configuration."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
        }
        silver = load_silver_from_yaml(config)

        assert silver.unique_columns == ["id"]
        assert silver.last_updated_column == "updated_at"
        assert silver.entity_kind == EntityKind.STATE
        assert silver.history_mode == HistoryMode.CURRENT_ONLY

    def test_natural_keys_as_string(self):
        """Single natural key as string (converted to list) - tests legacy field name support."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": "order_id",  # Legacy field name
            "change_timestamp": "updated_at",  # Legacy field name
        }
        silver = load_silver_from_yaml(config)

        assert silver.unique_columns == ["order_id"]

    def test_composite_natural_keys(self):
        """Composite natural keys - tests legacy field name support."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["order_id", "line_item_id"],  # Legacy field name
            "change_timestamp": "modified_date",  # Legacy field name
        }
        silver = load_silver_from_yaml(config)

        assert silver.unique_columns == ["order_id", "line_item_id"]

    def test_entity_kind_state(self):
        """Entity kind: state."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["customer_id"],
            "change_timestamp": "updated_at",
            "entity_kind": "state",
        }
        silver = load_silver_from_yaml(config)

        assert silver.entity_kind == EntityKind.STATE

    def test_entity_kind_event(self):
        """Entity kind: event."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["event_id"],
            "change_timestamp": "event_time",
            "entity_kind": "event",
        }
        silver = load_silver_from_yaml(config)

        assert silver.entity_kind == EntityKind.EVENT

    def test_history_mode_current_only(self):
        """History mode: current_only (SCD1)."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "history_mode": "current_only",
        }
        silver = load_silver_from_yaml(config)

        assert silver.history_mode == HistoryMode.CURRENT_ONLY

    def test_history_mode_scd1_alias(self):
        """History mode: scd1 (alias for current_only)."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "history_mode": "scd1",
        }
        silver = load_silver_from_yaml(config)

        assert silver.history_mode == HistoryMode.CURRENT_ONLY

    def test_history_mode_full_history(self):
        """History mode: full_history (SCD2)."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "history_mode": "full_history",
        }
        silver = load_silver_from_yaml(config)

        assert silver.history_mode == HistoryMode.FULL_HISTORY

    def test_history_mode_scd2_alias(self):
        """History mode: scd2 (alias for full_history)."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "history_mode": "scd2",
        }
        silver = load_silver_from_yaml(config)

        assert silver.history_mode == HistoryMode.FULL_HISTORY

    def test_attributes_list(self):
        """Explicit attributes list."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "attributes": ["name", "email", "status"],
        }
        silver = load_silver_from_yaml(config)

        assert silver.attributes == ["name", "email", "status"]

    def test_exclude_columns(self):
        """Exclude columns list."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "exclude_columns": ["internal_id", "password_hash"],
        }
        silver = load_silver_from_yaml(config)

        assert silver.exclude_columns == ["internal_id", "password_hash"]

    def test_partition_by(self):
        """Partition by columns."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "partition_by": ["year", "month"],
        }
        silver = load_silver_from_yaml(config)

        assert silver.partition_by == ["year", "month"]

    def test_output_formats(self):
        """Multiple output formats."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "output_formats": ["parquet", "csv"],
        }
        silver = load_silver_from_yaml(config)

        assert silver.output_formats == ["parquet", "csv"]

    def test_s3_storage_options_top_level(self):
        """S3 storage options at top level in Silver config."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "target_path": "s3://bucket/silver/",
            "s3_endpoint_url": "https://objects.nutanix.local:443",
            "s3_signature_version": "s3v4",
            "s3_addressing_style": "path",
            "s3_region": "us-west-2",
        }
        silver = load_silver_from_yaml(config)

        # S3 options should be in storage_options
        assert silver.storage_options is not None
        assert (
            silver.storage_options["endpoint_url"]
            == "https://objects.nutanix.local:443"
        )
        assert silver.storage_options["s3_signature_version"] == "s3v4"
        assert silver.storage_options["s3_addressing_style"] == "path"
        assert silver.storage_options["region"] == "us-west-2"

    def test_s3_options_auto_wired_from_bronze(self):
        """S3 options auto-wired from Bronze to Silver."""
        from pipelines.lib.bronze import BronzeSource, SourceType

        bronze = BronzeSource(
            system="retail",
            entity="orders",
            source_type=SourceType.FILE_PARQUET,
            source_path="s3://bucket/data/*.parquet",
            target_path="s3://bucket/bronze/",
            options={
                "s3_signature_version": "s3v4",
                "s3_addressing_style": "path",
                "endpoint_url": "https://objects.nutanix.local:443",
            },
        )

        silver_config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
        }
        silver = load_silver_from_yaml(silver_config, bronze=bronze)

        # S3 options should be auto-wired from Bronze
        assert silver.storage_options is not None
        assert silver.storage_options["s3_signature_version"] == "s3v4"
        assert silver.storage_options["s3_addressing_style"] == "path"
        assert (
            silver.storage_options["endpoint_url"]
            == "https://objects.nutanix.local:443"
        )

    def test_missing_unique_columns_raises(self):
        """Missing unique_columns should raise."""
        config = {
            "last_updated_column": "updated_at",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config)

        assert "unique_columns" in str(exc_info.value).lower()

    def test_missing_last_updated_column_raises(self):
        """Missing last_updated_column should raise."""
        config = {
            "domain": "test",
            "subject": "test",
            "unique_columns": ["id"],
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config)

        assert "last_updated_column" in str(exc_info.value).lower()

    def test_invalid_entity_kind_raises(self):
        """Invalid entity_kind should raise."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "entity_kind": "invalid",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config)

        assert "entity_kind" in str(exc_info.value).lower()

    def test_invalid_history_mode_raises(self):
        """Invalid history_mode should raise."""
        config = {
            "domain": "test",
            "subject": "test",
            "natural_keys": ["id"],
            "change_timestamp": "updated_at",
            "history_mode": "invalid",
        }
        with pytest.raises(YAMLConfigError) as exc_info:
            load_silver_from_yaml(config)

        assert "history_mode" in str(exc_info.value).lower()


class TestLoadPipeline:
    """Tests for loading full pipeline from YAML file."""

    def test_load_bronze_only(self, tmp_path):
        """Load pipeline with only bronze section."""
        yaml_content = """
bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv
"""
        config_file = tmp_path / "bronze_only.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.bronze is not None
        assert pipeline.bronze.system == "retail"
        assert pipeline.silver is None

    def test_load_silver_only(self, tmp_path):
        """Load pipeline with only silver section - tests legacy field names work."""
        yaml_content = """
silver:
  domain: retail
  subject: orders
  natural_keys: [order_id]
  change_timestamp: updated_at
  source_path: ./bronze/orders/*.parquet
  target_path: ./silver/orders/
"""
        config_file = tmp_path / "silver_only.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.bronze is None
        assert pipeline.silver is not None
        assert pipeline.silver.unique_columns == ["order_id"]

    def test_load_full_pipeline(self, tmp_path):
        """Load pipeline with both bronze and silver."""
        yaml_content = """
name: retail_orders
description: Test pipeline

bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  domain: retail
  subject: orders
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "full_pipeline.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert pipeline.name == "retail_orders"
        assert pipeline.bronze is not None
        assert pipeline.silver is not None

    def test_auto_wire_silver_source(self, tmp_path):
        """Silver source_path auto-wired from Bronze target."""
        yaml_content = """
bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  domain: retail
  subject: orders
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "auto_wire.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        # Silver source should be auto-wired
        assert pipeline.silver.source_path is not None
        assert (
            "bronze" in pipeline.silver.source_path
            or ".parquet" in pipeline.silver.source_path
        )

    def test_auto_generate_silver_target(self, tmp_path):
        """Silver target_path auto-generated from Silver domain/subject."""
        yaml_content = """
bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  domain: retail
  subject: orders
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "auto_target.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        # Silver target should include entity name
        assert "orders" in pipeline.silver.target_path

    def test_file_not_found_raises(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_pipeline("/nonexistent/path/to/config.yaml")

    def test_empty_file_raises(self, tmp_path):
        """Empty YAML file should raise."""
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        with pytest.raises(YAMLConfigError):
            load_pipeline(config_file)

    def test_no_bronze_or_silver_raises(self, tmp_path):
        """YAML with neither bronze nor silver should raise."""
        yaml_content = """
name: empty_pipeline
description: No layers defined
"""
        config_file = tmp_path / "no_layers.yaml"
        config_file.write_text(yaml_content)

        with pytest.raises(YAMLConfigError) as exc_info:
            load_pipeline(config_file)

        assert (
            "bronze" in str(exc_info.value).lower()
            or "silver" in str(exc_info.value).lower()
        )

    def test_invalid_yaml_syntax_raises(self, tmp_path):
        """Invalid YAML syntax should raise."""
        yaml_content = """
bronze:
  system: retail
  entity: orders
    invalid_indent: oops
"""
        config_file = tmp_path / "bad_yaml.yaml"
        config_file.write_text(yaml_content)

        with pytest.raises(YAMLConfigError):
            load_pipeline(config_file)


class TestValidateYamlConfig:
    """Tests for validation without loading."""

    def test_valid_config_returns_empty(self, tmp_path):
        """Valid config returns empty error list."""
        yaml_content = """
bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders.csv

silver:
  domain: retail
  subject: orders
  natural_keys: [order_id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "valid.yaml"
        config_file.write_text(yaml_content)

        errors = validate_yaml_config(config_file)

        assert errors == []

    def test_invalid_config_returns_errors(self, tmp_path):
        """Invalid config returns list of errors."""
        yaml_content = """
bronze:
  system: retail
  # Missing entity and source_type
"""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml_content)

        errors = validate_yaml_config(config_file)

        assert len(errors) > 0


class TestPipelineFromYaml:
    """Tests for PipelineFromYAML wrapper class."""

    def test_run_method_exists(self, tmp_path):
        """Pipeline has run() method."""
        yaml_content = """
bronze:
  system: test
  entity: test
  source_type: file_csv
  source_path: ./data/test.csv

silver:
  domain: test
  subject: test
  natural_keys: [id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert hasattr(pipeline, "run")
        assert callable(pipeline.run)

    def test_run_bronze_method_exists(self, tmp_path):
        """Pipeline has run_bronze() method."""
        yaml_content = """
bronze:
  system: test
  entity: test
  source_type: file_csv
  source_path: ./data/test.csv
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert hasattr(pipeline, "run_bronze")
        assert callable(pipeline.run_bronze)

    def test_run_silver_method_exists(self, tmp_path):
        """Pipeline has run_silver() method."""
        yaml_content = """
silver:
  domain: test
  subject: test
  natural_keys: [id]
  change_timestamp: updated_at
  source_path: ./data/*.parquet
  target_path: ./silver/test/
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)

        assert hasattr(pipeline, "run_silver")
        assert callable(pipeline.run_silver)

    def test_explain_method(self, tmp_path):
        """Pipeline has explain() method that returns string."""
        yaml_content = """
bronze:
  system: test
  entity: test
  source_type: file_csv
  source_path: ./data/test.csv

silver:
  domain: test
  subject: test
  natural_keys: [id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)
        explanation = pipeline.explain()

        assert isinstance(explanation, str)
        assert "test" in explanation.lower()

    def test_validate_method(self, tmp_path):
        """Pipeline has validate() method."""
        yaml_content = """
bronze:
  system: test
  entity: test
  source_type: file_csv
  source_path: ./data/test.csv

silver:
  domain: test
  subject: test
  natural_keys: [id]
  change_timestamp: updated_at
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        pipeline = load_pipeline(config_file)
        issues = pipeline.validate()

        assert isinstance(issues, list)
