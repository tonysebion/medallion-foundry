"""Tests for config validation helpers.

This module provides comprehensive test coverage for the config validation
system, including:
- Top-level section validation
- Storage backend validation
- Source type validation
- Run configuration validation
- Spec fields validation (layer, environment, classification, owners, schema_evolution)
- Quality rules validation
- Schema configuration validation
- Cross-field constraint validation
- Boundary condition tests
- Error message quality tests
"""

import copy
import warnings

import pytest

from core.foundation.primitives.exceptions import (
    BronzeFoundryCompatibilityWarning,
    BronzeFoundryDeprecationWarning,
 )
from core.infrastructure.config import validate_config_dict


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_config() -> dict:
    """Minimal valid configuration for testing."""
    return {
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": "./out",
            }
        },
        "source": {
            "system": "sys",
            "table": "tbl",
            "type": "api",
            "api": {"base_url": "https://example.com", "endpoint": "/"},
            "run": {},
        },
    }


@pytest.fixture
def s3_config() -> dict:
    """Valid S3 backend configuration."""
    return {
        "platform": {
            "bronze": {
                "storage_backend": "s3",
                "s3_bucket": "my-bucket",
                "s3_prefix": "bronze/",
            },
            "s3_connection": {
                "region": "us-east-1",
            },
        },
        "source": {
            "system": "sys",
            "table": "tbl",
            "type": "api",
            "api": {"base_url": "https://example.com", "endpoint": "/data"},
            "run": {},
        },
    }


@pytest.fixture
def azure_config() -> dict:
    """Valid Azure backend configuration."""
    return {
        "platform": {
            "bronze": {
                "storage_backend": "azure",
                "azure_container": "my-container",
            },
            "azure_connection": {
                "storage_account": "myaccount",
            },
        },
        "source": {
            "system": "sys",
            "table": "tbl",
            "type": "api",
            "api": {"base_url": "https://example.com", "endpoint": "/data"},
            "run": {},
        },
    }


# =============================================================================
# Basic Validation Tests
# =============================================================================


class TestBasicValidation:
    """Tests for basic config structure validation."""

    def test_validate_config_returns_copy(self, base_config):
        """Validated config should be a deep copy, not the original."""
        validated = validate_config_dict(base_config)
        assert validated is not base_config
        assert validated["source"]["run"]["load_pattern"] == "snapshot"

    def test_validate_missing_platform(self, base_config):
        """Config without platform section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg.pop("platform")
        with pytest.raises(ValueError, match="Config must contain a 'platform' section"):
            validate_config_dict(cfg)

    def test_validate_missing_source(self, base_config):
        """Config without source section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg.pop("source")
        with pytest.raises(ValueError, match="Config must contain a 'source' section"):
            validate_config_dict(cfg)

    def test_validate_platform_not_dict(self, base_config):
        """Platform must be a dictionary."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"] = "not-a-dict"
        with pytest.raises(ValueError, match="'platform' must be a dictionary"):
            validate_config_dict(cfg)

    def test_validate_source_not_dict(self, base_config):
        """Source must be a dictionary."""
        cfg = copy.deepcopy(base_config)
        cfg["source"] = "not-a-dict"
        with pytest.raises(ValueError, match="'source' must be a dictionary"):
            validate_config_dict(cfg)

    def test_validate_missing_bronze_section(self, base_config):
        """Missing platform.bronze section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"].pop("bronze")
        with pytest.raises(ValueError, match="Missing platform.bronze section"):
            validate_config_dict(cfg)

    def test_validate_bronze_not_dict(self, base_config):
        """platform.bronze must be a dictionary."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"] = "not-a-dict"
        with pytest.raises(ValueError, match="'platform.bronze' must be a dictionary"):
            validate_config_dict(cfg)


# =============================================================================
# Storage Backend Validation Tests
# =============================================================================


class TestStorageBackendValidation:
    """Tests for storage backend validation."""

    def test_validate_invalid_storage_backend(self, base_config):
        """Invalid storage backend should raise with list of valid options."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "unknown"
        with pytest.raises(ValueError, match="platform.bronze.storage_backend must be one of"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("backend", ["s3", "azure", "local"])
    def test_valid_storage_backends(self, base_config, backend):
        """All valid storage backends should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = backend

        if backend == "s3":
            cfg["platform"]["bronze"]["s3_bucket"] = "my-bucket"
            cfg["platform"]["bronze"]["s3_prefix"] = "bronze/"
            cfg["platform"]["s3_connection"] = {"region": "us-east-1"}
        elif backend == "azure":
            cfg["platform"]["bronze"]["azure_container"] = "my-container"
            cfg["platform"]["azure_connection"] = {"storage_account": "myaccount"}

        # Should not raise
        validate_config_dict(cfg)

    def test_s3_missing_connection(self, base_config):
        """S3 backend without s3_connection section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "s3"
        cfg["platform"]["bronze"]["s3_bucket"] = "my-bucket"
        cfg["platform"]["bronze"]["s3_prefix"] = "bronze/"
        with pytest.raises(ValueError, match="Missing platform.s3_connection section"):
            validate_config_dict(cfg)

    def test_s3_missing_bucket(self, base_config):
        """S3 backend without s3_bucket should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "s3"
        cfg["platform"]["bronze"]["s3_prefix"] = "bronze/"
        cfg["platform"]["s3_connection"] = {"region": "us-east-1"}
        with pytest.raises(ValueError, match="Missing required key 'platform.bronze.s3_bucket'"):
            validate_config_dict(cfg)

    def test_s3_missing_prefix(self, base_config):
        """S3 backend without s3_prefix should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "s3"
        cfg["platform"]["bronze"]["s3_bucket"] = "my-bucket"
        cfg["platform"]["s3_connection"] = {"region": "us-east-1"}
        with pytest.raises(ValueError, match="Missing required key 'platform.bronze.s3_prefix'"):
            validate_config_dict(cfg)

    def test_azure_missing_connection(self, base_config):
        """Azure backend without azure_connection section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "azure"
        cfg["platform"]["bronze"]["azure_container"] = "my-container"
        with pytest.raises(ValueError, match="Missing platform.azure_connection section"):
            validate_config_dict(cfg)

    def test_azure_missing_container(self, base_config):
        """Azure backend without azure_container should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "azure"
        cfg["platform"]["azure_connection"] = {"storage_account": "myaccount"}
        with pytest.raises(ValueError, match="Azure backend requires platform.bronze.azure_container"):
            validate_config_dict(cfg)

    def test_local_missing_path(self, base_config):
        """Local backend without local_path should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "local"
        cfg["platform"]["bronze"].pop("local_path")
        with pytest.raises(ValueError, match="Local backend requires platform.bronze.local_path"):
            validate_config_dict(cfg)

    def test_local_legacy_output_dir_fallback(self, base_config):
        """Local backend should accept legacy output_dir with deprecation warning."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "local"
        cfg["platform"]["bronze"].pop("local_path")
        cfg["platform"]["bronze"]["output_dir"] = "./legacy_output"
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            result = validate_config_dict(cfg)

        compat_messages = [
            str(warning.message)
            for warning in recorded
            if issubclass(warning.category, BronzeFoundryCompatibilityWarning)
        ]
        assert compat_messages
        assert any(
            "[CFG001] Using platform.bronze.output_dir as local_path; add explicit local_path to silence warning"
            in message
            for message in compat_messages
        )

        deprecation_messages = [
            str(warning.message)
            for warning in recorded
            if issubclass(warning.category, BronzeFoundryDeprecationWarning)
        ]
        assert deprecation_messages
        assert any(
            "[CFG001] Implicit local_path fallback will be removed; define platform.bronze.local_path"
            in message
            for message in deprecation_messages
        )
        assert result["platform"]["bronze"]["local_path"] == "./legacy_output"


# =============================================================================
# Source Type Validation Tests
# =============================================================================


class TestSourceTypeValidation:
    """Tests for source type validation."""

    def test_validate_invalid_source_type(self, base_config):
        """Invalid source type should raise with list of valid options."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "unknown"
        with pytest.raises(ValueError, match="Invalid source.type"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize(
        "source_type",
        ["api", "db", "db_table", "db_query", "db_multi", "file", "file_batch", "custom"],
    )
    def test_valid_source_types(self, base_config, source_type):
        """All valid source types should be accepted with proper config."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = source_type

        # Add required sections for each source type
        if source_type == "api":
            cfg["source"]["api"] = {"base_url": "https://example.com", "endpoint": "/"}
        elif source_type in ("db", "db_table", "db_query"):
            cfg["source"]["db"] = {
                "conn_str_env": "DB_CONNECTION_STRING",
                "base_query": "SELECT * FROM table",
            }
        elif source_type == "db_multi":
            cfg["source"]["entities"] = [{"name": "entity1"}]
            cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        elif source_type in ("file", "file_batch"):
            cfg["source"]["file"] = {"path": "/data/file.csv"}
        elif source_type == "custom":
            cfg["source"]["custom_extractor"] = {
                "module": "my_module",
                "class_name": "MyExtractor",
            }

        # Should not raise
        validate_config_dict(cfg)

    def test_api_missing_section(self, base_config):
        """API source without api section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "api"
        cfg["source"].pop("api")
        with pytest.raises(ValueError, match="source.type='api' requires 'source.api' section"):
            validate_config_dict(cfg)

    def test_api_missing_base_url(self, base_config):
        """API source without base_url should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["api"] = {"endpoint": "/data"}
        with pytest.raises(ValueError, match="source.api requires 'base_url'"):
            validate_config_dict(cfg)

    def test_api_legacy_url_fallback(self, base_config):
        """API source with legacy 'url' field should work with deprecation warning."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["api"] = {"url": "https://example.com", "endpoint": "/data"}
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            result = validate_config_dict(cfg)

        compat_messages = [
            str(warning.message)
            for warning in recorded
            if issubclass(warning.category, BronzeFoundryCompatibilityWarning)
        ]
        assert compat_messages
        assert any(
            "[CFG002] source.api.url key is deprecated; use base_url" in message
            for message in compat_messages
        )

        deprecation_messages = [
            str(warning.message)
            for warning in recorded
            if issubclass(warning.category, BronzeFoundryDeprecationWarning)
        ]
        assert deprecation_messages
        assert any(
            "[CFG002] Legacy 'url' field will be removed; rename to base_url" in message
            for message in deprecation_messages
        )
        assert result["source"]["api"]["base_url"] == "https://example.com"

    @pytest.mark.parametrize("db_type", ["db", "db_table", "db_query"])
    def test_db_missing_section(self, base_config, db_type):
        """DB source without db section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = db_type
        cfg["source"].pop("api", None)
        with pytest.raises(
            ValueError, match=f"source.type='{db_type}' requires 'source.db' section"
        ):
            validate_config_dict(cfg)

    def test_db_missing_conn_str_env(self, base_config):
        """DB source without conn_str_env should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db"
        cfg["source"]["db"] = {"base_query": "SELECT * FROM table"}
        with pytest.raises(ValueError, match="source.db requires 'conn_str_env'"):
            validate_config_dict(cfg)

    def test_db_missing_base_query(self, base_config):
        """DB source without base_query should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db"
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match="source.db requires 'base_query'"):
            validate_config_dict(cfg)

    def test_db_multi_missing_entities(self, base_config):
        """db_multi source without entities should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match="source.type='db_multi' requires 'source.entities'"):
            validate_config_dict(cfg)

    def test_db_multi_empty_entities(self, base_config):
        """db_multi source with empty entities list should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["entities"] = []
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match="requires at least one entity"):
            validate_config_dict(cfg)

    def test_db_multi_entities_not_list(self, base_config):
        """db_multi source with non-list entities should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["entities"] = "not-a-list"
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match="source.entities must be a list"):
            validate_config_dict(cfg)

    def test_db_multi_entity_not_dict(self, base_config):
        """db_multi source with non-dict entity should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["entities"] = ["not-a-dict"]
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match=r"source.entities\[0\] must be a dictionary"):
            validate_config_dict(cfg)

    def test_db_multi_entity_missing_name(self, base_config):
        """db_multi source with entity missing name should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["entities"] = [{"query": "SELECT 1"}]
        cfg["source"]["db"] = {"conn_str_env": "DB_CONNECTION_STRING"}
        with pytest.raises(ValueError, match=r"source.entities\[0\] requires 'name'"):
            validate_config_dict(cfg)

    def test_db_multi_missing_connection(self, base_config):
        """db_multi source without connection_ref or db section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "db_multi"
        cfg["source"]["entities"] = [{"name": "entity1"}]
        cfg["source"].pop("api", None)
        with pytest.raises(
            ValueError, match="requires 'connection_ref' or 'source.db.conn_str_env'"
        ):
            validate_config_dict(cfg)

    def test_file_missing_section(self, base_config):
        """File source without file section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"].pop("api", None)
        with pytest.raises(ValueError, match="source.type='file' requires 'source.file' section"):
            validate_config_dict(cfg)

    def test_file_missing_path(self, base_config):
        """File source without path should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"format": "csv"}
        with pytest.raises(ValueError, match="source.file requires 'path'"):
            validate_config_dict(cfg)

    def test_file_not_dict(self, base_config):
        """File source with non-dict file section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = "not-a-dict"
        with pytest.raises(ValueError, match="source.file must be a dictionary"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("fmt", ["csv", "tsv", "json", "jsonl", "parquet"])
    def test_file_valid_formats(self, base_config, fmt):
        """Valid file formats should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"path": "/data/file", "format": fmt}
        # Should not raise
        validate_config_dict(cfg)

    def test_file_invalid_format(self, base_config):
        """Invalid file format should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"path": "/data/file", "format": "xlsx"}
        with pytest.raises(ValueError, match="source.file.format must be one of"):
            validate_config_dict(cfg)

    def test_file_delimiter_not_string(self, base_config):
        """Non-string delimiter should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"path": "/data/file.csv", "delimiter": 123}
        with pytest.raises(ValueError, match="source.file.delimiter must be a string"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("limit", [0, -1, -100])
    def test_file_limit_rows_invalid(self, base_config, limit):
        """Non-positive limit_rows should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"path": "/data/file.csv", "limit_rows": limit}
        with pytest.raises(ValueError, match="source.file.limit_rows must be a positive integer"):
            validate_config_dict(cfg)

    def test_file_limit_rows_not_int(self, base_config):
        """Non-integer limit_rows should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "file"
        cfg["source"]["file"] = {"path": "/data/file.csv", "limit_rows": "100"}
        with pytest.raises(ValueError, match="source.file.limit_rows must be a positive integer"):
            validate_config_dict(cfg)

    def test_custom_missing_section(self, base_config):
        """Custom source without custom_extractor section should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "custom"
        cfg["source"].pop("api", None)
        with pytest.raises(
            ValueError, match="source.type='custom' requires 'source.custom_extractor' section"
        ):
            validate_config_dict(cfg)

    def test_custom_missing_module(self, base_config):
        """Custom source without module should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "custom"
        cfg["source"]["custom_extractor"] = {"class_name": "MyExtractor"}
        with pytest.raises(ValueError, match="requires 'module' and 'class_name'"):
            validate_config_dict(cfg)

    def test_custom_missing_class_name(self, base_config):
        """Custom source without class_name should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "custom"
        cfg["source"]["custom_extractor"] = {"module": "my_module"}
        with pytest.raises(ValueError, match="requires 'module' and 'class_name'"):
            validate_config_dict(cfg)


# =============================================================================
# Source Required Fields Tests
# =============================================================================


class TestSourceRequiredFields:
    """Tests for source required field validation."""

    @pytest.mark.parametrize("missing_field", ["system", "table", "run"])
    def test_source_missing_required_field(self, base_config, missing_field):
        """Source without required field should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"].pop(missing_field)
        with pytest.raises(ValueError, match=f"Missing required key 'source.{missing_field}'"):
            validate_config_dict(cfg)


# =============================================================================
# Run Configuration Validation Tests
# =============================================================================


class TestRunConfigValidation:
    """Tests for run configuration validation."""

    @pytest.mark.parametrize(
        "pattern",
        ["snapshot", "incremental_append", "incremental_merge"],
    )
    def test_valid_load_patterns(self, base_config, pattern):
        """Valid load patterns should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["load_pattern"] = pattern
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["load_pattern"] == pattern

    def test_current_history_requires_primary_keys(self, base_config):
        """current_history load pattern requires silver.primary_keys."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["load_pattern"] = "current_history"
        # Without primary_keys, should raise
        with pytest.raises(ValueError, match="silver.primary_keys must be provided"):
            validate_config_dict(cfg)

    def test_current_history_with_primary_keys_and_order_column(self, base_config):
        """current_history load pattern should work with primary_keys and order_column."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["load_pattern"] = "current_history"
        cfg["silver"] = {"primary_keys": ["id"], "order_column": "updated_at"}
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["load_pattern"] == "current_history"

    def test_current_history_missing_order_column(self, base_config):
        """current_history load pattern requires order_column."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["load_pattern"] = "current_history"
        cfg["silver"] = {"primary_keys": ["id"]}  # Missing order_column
        with pytest.raises(ValueError, match="silver.order_column must be provided"):
            validate_config_dict(cfg)

    def test_load_pattern_default(self, base_config):
        """Default load pattern should be 'snapshot'."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"] = {}
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["load_pattern"] == "snapshot"

    @pytest.mark.parametrize("max_size", [0, -1, -100])
    def test_max_file_size_invalid(self, base_config, max_size):
        """Non-positive max_file_size_mb should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["max_file_size_mb"] = max_size
        with pytest.raises(ValueError, match="source.run.max_file_size_mb must be a positive"):
            validate_config_dict(cfg)

    def test_max_file_size_not_number(self, base_config):
        """Non-numeric max_file_size_mb should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["max_file_size_mb"] = "100"
        with pytest.raises(ValueError, match="source.run.max_file_size_mb must be a positive"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("workers", [0, -1, -10])
    def test_parallel_workers_invalid(self, base_config, workers):
        """Non-positive parallel_workers should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["parallel_workers"] = workers
        with pytest.raises(ValueError, match="source.run.parallel_workers must be a positive"):
            validate_config_dict(cfg)

    def test_parallel_workers_not_int(self, base_config):
        """Non-integer parallel_workers should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["parallel_workers"] = 1.5
        with pytest.raises(ValueError, match="source.run.parallel_workers must be a positive"):
            validate_config_dict(cfg)

    def test_parallel_workers_valid(self, base_config):
        """Valid parallel_workers should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["parallel_workers"] = 4
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["parallel_workers"] == 4


# =============================================================================
# Reference Mode Validation Tests
# =============================================================================


class TestReferenceModeValidation:
    """Tests for reference_mode validation."""

    def test_reference_mode_not_dict(self, base_config):
        """Non-dict reference_mode should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = "not-a-dict"
        with pytest.raises(ValueError, match="source.run.reference_mode must be a dictionary"):
            validate_config_dict(cfg)

    def test_reference_mode_enabled_not_bool(self, base_config):
        """Non-boolean enabled should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = {"enabled": "true"}
        with pytest.raises(ValueError, match="source.run.reference_mode.enabled must be a boolean"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("role", ["reference", "delta", "auto"])
    def test_reference_mode_valid_roles(self, base_config, role):
        """Valid roles should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = {"role": role}
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["reference_mode"]["role"] == role

    def test_reference_mode_invalid_role(self, base_config):
        """Invalid role should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = {"role": "invalid"}
        with pytest.raises(
            ValueError,
            match="source.run.reference_mode.role must be one of 'reference', 'delta', or 'auto'",
        ):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("cadence", [0, -1, -7])
    def test_reference_mode_cadence_invalid(self, base_config, cadence):
        """Non-positive cadence_days should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = {"cadence_days": cadence}
        with pytest.raises(
            ValueError, match="source.run.reference_mode.cadence_days must be a positive"
        ):
            validate_config_dict(cfg)

    def test_reference_mode_delta_patterns_not_strings(self, base_config):
        """Non-string items in delta_patterns should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["reference_mode"] = {"delta_patterns": [1, 2, 3]}
        with pytest.raises(
            ValueError, match="source.run.reference_mode.delta_patterns must be a list of strings"
        ):
            validate_config_dict(cfg)


# =============================================================================
# Late Data Validation Tests
# =============================================================================


class TestLateDataValidation:
    """Tests for late_data validation."""

    def test_late_data_not_dict(self, base_config):
        """Non-dict late_data should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = "not-a-dict"
        with pytest.raises(ValueError, match="source.run.late_data must be a dictionary"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("mode", ["allow", "reject", "quarantine"])
    def test_late_data_valid_modes(self, base_config, mode):
        """Valid late_data modes should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = {"mode": mode}
        result = validate_config_dict(cfg)
        assert result["source"]["run"]["late_data"]["mode"] == mode

    def test_late_data_invalid_mode(self, base_config):
        """Invalid late_data mode should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = {"mode": "invalid"}
        with pytest.raises(ValueError, match="source.run.late_data.mode must be one of"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("threshold", [0, -1, -7])
    def test_late_data_threshold_invalid(self, base_config, threshold):
        """Non-positive threshold_days should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = {"threshold_days": threshold}
        with pytest.raises(
            ValueError, match="source.run.late_data.threshold_days must be a positive"
        ):
            validate_config_dict(cfg)

    def test_late_data_quarantine_path_not_string(self, base_config):
        """Non-string quarantine_path should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = {"quarantine_path": 123}
        with pytest.raises(
            ValueError, match="source.run.late_data.quarantine_path must be a string"
        ):
            validate_config_dict(cfg)

    def test_late_data_timestamp_column_not_string(self, base_config):
        """Non-string timestamp_column should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["late_data"] = {"timestamp_column": 123}
        with pytest.raises(
            ValueError, match="source.run.late_data.timestamp_column must be a string"
        ):
            validate_config_dict(cfg)


# =============================================================================
# Backfill Validation Tests
# =============================================================================


class TestBackfillValidation:
    """Tests for backfill validation."""

    def test_backfill_not_dict(self, base_config):
        """Non-dict backfill should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = "not-a-dict"
        with pytest.raises(ValueError, match="source.run.backfill must be a dictionary"):
            validate_config_dict(cfg)

    def test_backfill_missing_start_date(self, base_config):
        """Backfill without start_date should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {"end_date": "2025-01-01"}
        with pytest.raises(
            ValueError, match="source.run.backfill requires both 'start_date' and 'end_date'"
        ):
            validate_config_dict(cfg)

    def test_backfill_missing_end_date(self, base_config):
        """Backfill without end_date should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {"start_date": "2025-01-01"}
        with pytest.raises(
            ValueError, match="source.run.backfill requires both 'start_date' and 'end_date'"
        ):
            validate_config_dict(cfg)

    def test_backfill_invalid_start_date_format(self, base_config):
        """Invalid start_date format should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {
            "start_date": "01-01-2025",  # Wrong format
            "end_date": "2025-01-31",
        }
        with pytest.raises(
            ValueError, match="source.run.backfill.start_date must be a valid ISO date"
        ):
            validate_config_dict(cfg)

    def test_backfill_invalid_end_date_format(self, base_config):
        """Invalid end_date format should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {
            "start_date": "2025-01-01",
            "end_date": "31-01-2025",  # Wrong format
        }
        with pytest.raises(
            ValueError, match="source.run.backfill.end_date must be a valid ISO date"
        ):
            validate_config_dict(cfg)

    def test_backfill_force_full_not_bool(self, base_config):
        """Non-boolean force_full should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "force_full": "true",
        }
        with pytest.raises(ValueError, match="source.run.backfill.force_full must be a boolean"):
            validate_config_dict(cfg)

    def test_backfill_valid(self, base_config):
        """Valid backfill config should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["backfill"] = {
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "force_full": True,
        }
        # Should not raise
        validate_config_dict(cfg)


# =============================================================================
# Spec Fields Validation Tests
# =============================================================================


class TestSpecFieldsValidation:
    """Tests for spec fields validation (layer, environment, classification, etc.)."""

    def test_validate_invalid_data_classification(self, base_config):
        """Invalid data_classification should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["data_classification"] = "ultra-secret"
        with pytest.raises(ValueError, match="data_classification must be one of"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize(
        "classification", ["public", "internal", "confidential", "restricted"]
    )
    def test_valid_data_classifications(self, base_config, classification):
        """Valid data classifications should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["data_classification"] = classification
        # Should not raise
        validate_config_dict(cfg)

    def test_data_classification_case_insensitive(self, base_config):
        """Data classification should be case-insensitive."""
        cfg = copy.deepcopy(base_config)
        cfg["data_classification"] = "INTERNAL"
        # Should not raise
        validate_config_dict(cfg)

    @pytest.mark.parametrize("layer", ["bronze", "silver"])
    def test_valid_layers(self, base_config, layer):
        """Valid layers should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["layer"] = layer
        # Should not raise
        validate_config_dict(cfg)

    def test_invalid_layer(self, base_config):
        """Invalid layer should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["layer"] = "gold"
        with pytest.raises(ValueError, match="layer must be one of"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("env", ["dev", "staging", "prod", "test", "local"])
    def test_valid_environments(self, base_config, env):
        """Valid environments should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["environment"] = env
        # Should not raise
        validate_config_dict(cfg)

    def test_invalid_environment(self, base_config):
        """Invalid environment should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["environment"] = "production"  # Should be 'prod'
        with pytest.raises(ValueError, match="environment must be one of"):
            validate_config_dict(cfg)

    def test_owners_not_dict(self, base_config):
        """Non-dict owners should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["owners"] = "not-a-dict"
        with pytest.raises(ValueError, match="owners must be a dictionary"):
            validate_config_dict(cfg)

    def test_owners_semantic_not_string(self, base_config):
        """Non-string semantic_owner should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["owners"] = {"semantic_owner": 123}
        with pytest.raises(ValueError, match="owners.semantic_owner must be a string"):
            validate_config_dict(cfg)

    def test_owners_technical_not_string(self, base_config):
        """Non-string technical_owner should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["owners"] = {"technical_owner": 123}
        with pytest.raises(ValueError, match="owners.technical_owner must be a string"):
            validate_config_dict(cfg)

    def test_owners_valid(self, base_config):
        """Valid owners should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["owners"] = {
            "semantic_owner": "data-team@example.com",
            "technical_owner": "platform-team@example.com",
        }
        # Should not raise
        validate_config_dict(cfg)

    def test_schema_evolution_not_dict(self, base_config):
        """Non-dict schema_evolution should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema_evolution"] = "not-a-dict"
        with pytest.raises(ValueError, match="schema_evolution must be a dictionary"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("mode", ["strict", "allow_new_nullable", "ignore_unknown"])
    def test_schema_evolution_valid_modes(self, base_config, mode):
        """Valid schema_evolution modes should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["schema_evolution"] = {"mode": mode}
        # Should not raise
        validate_config_dict(cfg)

    def test_schema_evolution_invalid_mode(self, base_config):
        """Invalid schema_evolution mode should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema_evolution"] = {"mode": "invalid"}
        with pytest.raises(ValueError, match="schema_evolution.mode must be one of"):
            validate_config_dict(cfg)

    def test_schema_evolution_allow_relaxation_not_bool(self, base_config):
        """Non-boolean allow_type_relaxation should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema_evolution"] = {"allow_type_relaxation": "true"}
        with pytest.raises(
            ValueError, match="schema_evolution.allow_type_relaxation must be a boolean"
        ):
            validate_config_dict(cfg)

    def test_pipeline_id_not_string(self, base_config):
        """Non-string pipeline_id should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["pipeline_id"] = 123
        with pytest.raises(ValueError, match="pipeline_id must be a string"):
            validate_config_dict(cfg)

    def test_domain_not_string(self, base_config):
        """Non-string domain should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["domain"] = 123
        with pytest.raises(ValueError, match="domain must be a string"):
            validate_config_dict(cfg)


# =============================================================================
# Partition Strategy Validation Tests
# =============================================================================


class TestPartitionStrategyValidation:
    """Tests for partition strategy validation."""

    @pytest.mark.parametrize("strategy", ["date", "hourly", "timestamp", "batch_id"])
    def test_valid_partition_strategies(self, base_config, strategy):
        """Valid partition strategies should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["partitioning"] = {"partition_strategy": strategy}
        # Should not raise
        validate_config_dict(cfg)

    def test_invalid_partition_strategy(self, base_config):
        """Invalid partition strategy should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["partitioning"] = {"partition_strategy": "weekly"}
        with pytest.raises(
            ValueError,
            match="platform.bronze.partitioning.partition_strategy must be one of",
        ):
            validate_config_dict(cfg)


# =============================================================================
# Quality Rules Validation Tests
# =============================================================================


class TestQualityRulesValidation:
    """Tests for quality_rules validation."""

    def test_quality_rules_not_list(self, base_config):
        """Non-list quality_rules should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = "not-a-list"
        with pytest.raises(ValueError, match="quality_rules must be a list"):
            validate_config_dict(cfg)

    def test_quality_rule_not_dict(self, base_config):
        """Non-dict quality rule should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = ["not-a-dict"]
        with pytest.raises(ValueError, match=r"quality_rules\[0\] must be a dictionary"):
            validate_config_dict(cfg)

    def test_quality_rule_missing_id(self, base_config):
        """Quality rule without id should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [{"expression": "col IS NOT NULL"}]
        with pytest.raises(ValueError, match=r"quality_rules\[0\] requires 'id'"):
            validate_config_dict(cfg)

    def test_quality_rule_missing_expression(self, base_config):
        """Quality rule without expression should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [{"id": "rule_1"}]
        with pytest.raises(ValueError, match=r"quality_rules\[0\] requires 'expression'"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize("level", ["error", "warn"])
    def test_quality_rule_valid_levels(self, base_config, level):
        """Valid quality rule levels should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [
            {"id": "rule_1", "expression": "col IS NOT NULL", "level": level}
        ]
        # Should not raise
        validate_config_dict(cfg)

    def test_quality_rule_invalid_level(self, base_config):
        """Invalid quality rule level should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [
            {"id": "rule_1", "expression": "col IS NOT NULL", "level": "critical"}
        ]
        with pytest.raises(ValueError, match=r"quality_rules\[0\].level must be one of"):
            validate_config_dict(cfg)

    def test_quality_rules_valid(self, base_config):
        """Valid quality rules should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [
            {"id": "not_null_id", "expression": "id IS NOT NULL", "level": "error"},
            {"id": "positive_amount", "expression": "amount > 0", "level": "warn"},
        ]
        # Should not raise
        validate_config_dict(cfg)


# =============================================================================
# Schema Configuration Validation Tests
# =============================================================================


class TestSchemaConfigValidation:
    """Tests for schema configuration validation."""

    def test_schema_not_dict(self, base_config):
        """Non-dict schema should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = "not-a-dict"
        with pytest.raises(ValueError, match="schema must be a dictionary"):
            validate_config_dict(cfg)

    def test_expected_columns_not_list(self, base_config):
        """Non-list expected_columns should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": "not-a-list"}
        with pytest.raises(ValueError, match="schema.expected_columns must be a list"):
            validate_config_dict(cfg)

    def test_expected_column_not_dict(self, base_config):
        """Non-dict expected column should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": ["not-a-dict"]}
        with pytest.raises(
            ValueError, match=r"schema.expected_columns\[0\] must be a dictionary"
        ):
            validate_config_dict(cfg)

    def test_expected_column_missing_name(self, base_config):
        """Expected column without name should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": [{"type": "string"}]}
        with pytest.raises(ValueError, match=r"schema.expected_columns\[0\] requires 'name'"):
            validate_config_dict(cfg)

    @pytest.mark.parametrize(
        "col_type",
        [
            "string",
            "integer",
            "bigint",
            "decimal",
            "float",
            "double",
            "boolean",
            "date",
            "timestamp",
            "datetime",
            "binary",
            "array",
            "map",
            "struct",
            "any",
        ],
    )
    def test_expected_column_valid_types(self, base_config, col_type):
        """Valid column types should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": [{"name": "col1", "type": col_type}]}
        # Should not raise
        validate_config_dict(cfg)

    def test_expected_column_invalid_type(self, base_config):
        """Invalid column type should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": [{"name": "col1", "type": "varchar"}]}
        with pytest.raises(
            ValueError, match=r"schema.expected_columns\[0\].type must be one of"
        ):
            validate_config_dict(cfg)

    def test_expected_column_nullable_not_bool(self, base_config):
        """Non-boolean nullable should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": [{"name": "col1", "nullable": "true"}]}
        with pytest.raises(
            ValueError, match=r"schema.expected_columns\[0\].nullable must be a boolean"
        ):
            validate_config_dict(cfg)

    def test_expected_column_primary_key_not_bool(self, base_config):
        """Non-boolean primary_key should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"expected_columns": [{"name": "col1", "primary_key": "true"}]}
        with pytest.raises(
            ValueError, match=r"schema.expected_columns\[0\].primary_key must be a boolean"
        ):
            validate_config_dict(cfg)

    def test_primary_keys_not_list(self, base_config):
        """Non-list primary_keys should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"primary_keys": "id"}
        with pytest.raises(ValueError, match="schema.primary_keys must be a list"):
            validate_config_dict(cfg)

    def test_partition_columns_not_list(self, base_config):
        """Non-list partition_columns should raise."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {"partition_columns": "date"}
        with pytest.raises(ValueError, match="schema.partition_columns must be a list"):
            validate_config_dict(cfg)

    def test_schema_valid(self, base_config):
        """Valid schema should be accepted."""
        cfg = copy.deepcopy(base_config)
        cfg["schema"] = {
            "expected_columns": [
                {"name": "id", "type": "integer", "nullable": False, "primary_key": True},
                {"name": "name", "type": "string", "nullable": True},
                {"name": "amount", "type": "decimal"},
            ],
            "primary_keys": ["id"],
            "partition_columns": ["date"],
        }
        # Should not raise
        validate_config_dict(cfg)


# =============================================================================
# Error Message Quality Tests
# =============================================================================


class TestErrorMessageQuality:
    """Tests to verify error messages are actionable and include context."""

    def test_error_includes_field_name(self, base_config):
        """Error messages should include the field name."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["run"]["parallel_workers"] = -1
        try:
            validate_config_dict(cfg)
            pytest.fail("Should have raised ValueError")
        except ValueError as e:
            assert "parallel_workers" in str(e)
            assert "positive" in str(e).lower()

    def test_error_includes_valid_options(self, base_config):
        """Error messages for enum fields should include valid options."""
        cfg = copy.deepcopy(base_config)
        cfg["platform"]["bronze"]["storage_backend"] = "gcs"
        try:
            validate_config_dict(cfg)
            pytest.fail("Should have raised ValueError")
        except ValueError as e:
            assert "s3" in str(e)
            assert "azure" in str(e)
            assert "local" in str(e)

    def test_error_includes_got_value(self, base_config):
        """Error messages should include the invalid value received."""
        cfg = copy.deepcopy(base_config)
        cfg["source"]["type"] = "bigquery"
        try:
            validate_config_dict(cfg)
            pytest.fail("Should have raised ValueError")
        except ValueError as e:
            assert "bigquery" in str(e)

    def test_error_includes_array_index(self, base_config):
        """Error messages for array items should include the index."""
        cfg = copy.deepcopy(base_config)
        cfg["quality_rules"] = [
            {"id": "rule_1", "expression": "x > 0"},
            {"expression": "y > 0"},  # Missing id
        ]
        try:
            validate_config_dict(cfg)
            pytest.fail("Should have raised ValueError")
        except ValueError as e:
            assert "[1]" in str(e)
            assert "id" in str(e)
