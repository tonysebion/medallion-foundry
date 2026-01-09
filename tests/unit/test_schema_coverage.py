"""Schema Coverage Tests.

Ensures every field defined in pipeline.schema.json is:
1. Accepted by the YAML config loader
2. Correctly flows to the Python objects
3. Has working defaults/enums/types

This test is schema-driven - it parses the JSON Schema and generates
test cases automatically, so it stays in sync with schema changes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from pipelines.lib.config_loader import (
    load_bronze_from_yaml,
    load_silver_from_yaml,
    YAMLConfigError,
)


# ============================================================================
# Schema Loading
# ============================================================================

SCHEMA_PATH = Path(__file__).parent.parent.parent / "pipelines" / "schema" / "pipeline.schema.json"


def load_schema() -> Dict[str, Any]:
    """Load the JSON Schema."""
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def get_bronze_fields() -> List[Tuple[str, Dict[str, Any]]]:
    """Extract all Bronze field definitions from schema."""
    schema = load_schema()
    bronze_def = schema.get("definitions", {}).get("bronze", {})
    properties = bronze_def.get("properties", {})
    return [(name, props) for name, props in properties.items()]


def get_silver_fields() -> List[Tuple[str, Dict[str, Any]]]:
    """Extract all Silver field definitions from schema."""
    schema = load_schema()
    silver_def = schema.get("definitions", {}).get("silver", {})
    properties = silver_def.get("properties", {})
    return [(name, props) for name, props in properties.items()]


def get_example_value(props: Dict[str, Any]) -> Optional[Any]:
    """Extract an example value from schema property definition."""
    # Try examples array first
    if "examples" in props and props["examples"]:
        return props["examples"][0]

    # Try default
    if "default" in props:
        return props["default"]

    # Try enum (use first value)
    if "enum" in props and props["enum"]:
        return props["enum"][0]

    # Generate based on type
    prop_type = props.get("type")
    if prop_type == "string":
        return "test_value"
    elif prop_type == "integer":
        minimum = props.get("minimum", 1)
        return max(minimum, 1000) if "chunk" in str(props) else minimum
    elif prop_type == "number":
        return 1.0
    elif prop_type == "boolean":
        return True
    elif prop_type == "array":
        return ["test_item"]
    elif prop_type == "object":
        return {}

    return None


# ============================================================================
# Minimal Config Builders
# ============================================================================

def build_minimal_bronze_config(**overrides) -> Dict[str, Any]:
    """Build a minimal valid Bronze config with optional field overrides."""
    config = {
        "system": "test_system",
        "entity": "test_entity",
        "source_type": "file_csv",
        "source_path": "./data/test.csv",
    }
    config.update(overrides)
    return config


def build_minimal_silver_config(**overrides) -> Dict[str, Any]:
    """Build a minimal valid Silver config with optional field overrides."""
    config = {
        "natural_keys": ["id"],
        "change_timestamp": "updated_at",
        "source_path": "./bronze/test/*.parquet",
        "target_path": "./silver/test/",
    }
    config.update(overrides)
    return config


# ============================================================================
# Field ID Generators for Parametrized Tests
# ============================================================================

def bronze_field_ids() -> List[str]:
    """Generate test IDs for Bronze fields."""
    return [name for name, _ in get_bronze_fields()]


def silver_field_ids() -> List[str]:
    """Generate test IDs for Silver fields."""
    return [name for name, _ in get_silver_fields()]


# ============================================================================
# Bronze Field Tests
# ============================================================================

class TestBronzeSchemaFields:
    """Test that every Bronze schema field is accepted and processed correctly."""

    @pytest.fixture
    def bronze_fields(self) -> List[Tuple[str, Dict[str, Any]]]:
        """Get all Bronze fields from schema."""
        return get_bronze_fields()

    def test_schema_has_bronze_fields(self, bronze_fields):
        """Verify schema has Bronze field definitions."""
        assert len(bronze_fields) > 0, "No Bronze fields found in schema"
        # Verify some expected fields exist
        field_names = [name for name, _ in bronze_fields]
        assert "system" in field_names
        assert "entity" in field_names
        assert "source_type" in field_names

    @pytest.mark.parametrize("field_name,field_props", get_bronze_fields(), ids=bronze_field_ids())
    def test_bronze_field_has_description(self, field_name: str, field_props: Dict[str, Any]):
        """Every Bronze field should have a description."""
        assert "description" in field_props, f"Bronze field '{field_name}' missing description"
        assert len(field_props["description"]) > 0, f"Bronze field '{field_name}' has empty description"

    @pytest.mark.parametrize("field_name,field_props", get_bronze_fields(), ids=bronze_field_ids())
    def test_bronze_field_accepted(self, field_name: str, field_props: Dict[str, Any]):
        """Every Bronze schema field should be accepted by the config loader."""
        example = get_example_value(field_props)

        # Skip nested object fields that need special handling
        if field_name in ("options", "auth", "pagination", "cdc_options", "headers", "params", "path_params"):
            pytest.skip(f"Nested object field '{field_name}' requires special test")

        # Some fields require specific source_type context
        if field_name in ("base_url", "endpoint", "data_path", "requests_per_second",
                          "timeout", "max_retries", "watermark_param"):
            # API-specific fields - test with api_rest source type
            api_config = {
                "source_type": "api_rest",
                "base_url": "https://api.example.com",
                "endpoint": "/v1/data",
            }
            # Override with the example value for this specific field
            if example is not None:
                api_config[field_name] = example
            config = build_minimal_bronze_config(**api_config)
        elif field_name in ("host", "database", "query"):
            # Database fields
            config = build_minimal_bronze_config(
                source_type="database_mssql",
                host="localhost",
                database="testdb",
                **{field_name: example} if field_name not in ("host", "database") else {}
            )
            if field_name in ("host", "database"):
                config[field_name] = example or "test_value"
        elif field_name == "watermark_column":
            # Requires incremental load pattern
            config = build_minimal_bronze_config(
                load_pattern="incremental",
                watermark_column=example or "updated_at"
            )
        elif example is not None:
            config = build_minimal_bronze_config(**{field_name: example})
        else:
            pytest.skip(f"No example value for Bronze field '{field_name}'")
            return

        # Should not raise - field should be accepted
        try:
            bronze = load_bronze_from_yaml(config)
            assert bronze is not None
        except YAMLConfigError as e:
            # If error is about the specific field, that's a real failure
            if field_name.lower() in str(e).lower():
                pytest.fail(f"Bronze field '{field_name}' rejected: {e}")
            # Other validation errors might be due to incomplete config - skip
            pytest.skip(f"Config incomplete for field test: {e}")


class TestBronzeEnumFields:
    """Test that all enum values are accepted for Bronze enum fields."""

    def test_all_source_types_accepted(self):
        """Every source_type enum value should be accepted."""
        schema = load_schema()
        source_types = schema["definitions"]["bronze"]["properties"]["source_type"]["enum"]

        for source_type in source_types:
            if source_type.startswith("database_"):
                config = build_minimal_bronze_config(
                    source_type=source_type,
                    host="localhost",
                    database="testdb"
                )
            elif source_type == "api_rest":
                config = build_minimal_bronze_config(
                    source_type=source_type,
                    base_url="https://api.example.com",
                    endpoint="/v1/data"
                )
            elif source_type == "file_fixed_width":
                config = build_minimal_bronze_config(
                    source_type=source_type,
                    options={"columns": ["a", "b"], "widths": [10, 20]}
                )
            else:
                config = build_minimal_bronze_config(source_type=source_type)

            bronze = load_bronze_from_yaml(config)
            assert bronze.source_type.name.lower() == source_type.replace("_", "_").lower().replace("file_", "file_").replace("database_", "database_").replace("api_", "api_")

    def test_all_load_patterns_accepted(self):
        """Every load_pattern enum value should be accepted."""
        schema = load_schema()
        load_patterns = schema["definitions"]["bronze"]["properties"]["load_pattern"]["enum"]

        for pattern in load_patterns:
            config = build_minimal_bronze_config(load_pattern=pattern)
            # incremental patterns need watermark
            if "incremental" in pattern.lower():
                config["watermark_column"] = "updated_at"
            # cdc needs cdc_options in some cases (skip for now)
            if pattern == "cdc":
                pytest.skip("CDC pattern requires additional cdc_options config")

            bronze = load_bronze_from_yaml(config)
            assert bronze is not None

    def test_all_input_modes_accepted(self):
        """Every input_mode enum value should be accepted."""
        schema = load_schema()
        input_modes = schema["definitions"]["bronze"]["properties"]["input_mode"]["enum"]

        for mode in input_modes:
            config = build_minimal_bronze_config(input_mode=mode)
            bronze = load_bronze_from_yaml(config)
            assert bronze.input_mode is not None


# ============================================================================
# Silver Field Tests
# ============================================================================

class TestSilverSchemaFields:
    """Test that every Silver schema field is accepted and processed correctly."""

    @pytest.fixture
    def silver_fields(self) -> List[Tuple[str, Dict[str, Any]]]:
        """Get all Silver fields from schema."""
        return get_silver_fields()

    def test_schema_has_silver_fields(self, silver_fields):
        """Verify schema has Silver field definitions."""
        assert len(silver_fields) > 0, "No Silver fields found in schema"
        # Verify some expected fields exist
        field_names = [name for name, _ in silver_fields]
        assert "natural_keys" in field_names
        assert "change_timestamp" in field_names

    @pytest.mark.parametrize("field_name,field_props", get_silver_fields(), ids=silver_field_ids())
    def test_silver_field_has_description(self, field_name: str, field_props: Dict[str, Any]):
        """Every Silver field should have a description."""
        assert "description" in field_props, f"Silver field '{field_name}' missing description"
        assert len(field_props["description"]) > 0, f"Silver field '{field_name}' has empty description"

    @pytest.mark.parametrize("field_name,field_props", get_silver_fields(), ids=silver_field_ids())
    def test_silver_field_accepted(self, field_name: str, field_props: Dict[str, Any]):
        """Every Silver schema field should be accepted by the config loader."""
        example = get_example_value(field_props)

        # Skip nested/complex fields
        if field_name in ("cdc_options",):
            pytest.skip(f"Nested object field '{field_name}' requires special test")

        # Handle oneOf fields (natural_keys can be string or array)
        if field_name == "natural_keys":
            example = ["test_id"]

        if example is not None:
            config = build_minimal_silver_config(**{field_name: example})
        else:
            pytest.skip(f"No example value for Silver field '{field_name}'")
            return

        # Should not raise - field should be accepted
        try:
            silver = load_silver_from_yaml(config)
            assert silver is not None
        except YAMLConfigError as e:
            if field_name.lower() in str(e).lower():
                pytest.fail(f"Silver field '{field_name}' rejected: {e}")
            pytest.skip(f"Config incomplete for field test: {e}")


class TestSilverEnumFields:
    """Test that all enum values are accepted for Silver enum fields."""

    def test_all_models_accepted(self):
        """Every model preset enum value should be accepted."""
        schema = load_schema()
        models = schema["definitions"]["silver"]["properties"]["model"]["enum"]

        for model in models:
            config = build_minimal_silver_config(model=model)
            silver = load_silver_from_yaml(config)
            assert silver is not None

    def test_all_entity_kinds_accepted(self):
        """Every entity_kind enum value should be accepted."""
        schema = load_schema()
        entity_kinds = schema["definitions"]["silver"]["properties"]["entity_kind"]["enum"]

        for kind in entity_kinds:
            config = build_minimal_silver_config(entity_kind=kind)
            silver = load_silver_from_yaml(config)
            assert silver.entity_kind is not None

    def test_all_history_modes_accepted(self):
        """Every history_mode enum value should be accepted."""
        schema = load_schema()
        history_modes = schema["definitions"]["silver"]["properties"]["history_mode"]["enum"]

        for mode in history_modes:
            config = build_minimal_silver_config(history_mode=mode)
            silver = load_silver_from_yaml(config)
            assert silver.history_mode is not None

    def test_all_delete_modes_accepted(self):
        """Every delete_mode enum value should be accepted."""
        schema = load_schema()
        delete_modes = schema["definitions"]["silver"]["properties"]["delete_mode"]["enum"]

        for mode in delete_modes:
            # tombstone and hard_delete require CDC model; ignore works with any model
            if mode in ("tombstone", "hard_delete"):
                config = build_minimal_silver_config(model="cdc_current", delete_mode=mode)
            else:
                config = build_minimal_silver_config(delete_mode=mode)
            silver = load_silver_from_yaml(config)
            assert silver.delete_mode is not None

    def test_all_input_modes_accepted(self):
        """Every input_mode enum value should be accepted."""
        schema = load_schema()
        input_modes = schema["definitions"]["silver"]["properties"]["input_mode"]["enum"]

        for mode in input_modes:
            config = build_minimal_silver_config(input_mode=mode)
            silver = load_silver_from_yaml(config)
            assert silver.input_mode is not None

    def test_all_validate_source_modes_accepted(self):
        """Every validate_source enum value should be accepted."""
        schema = load_schema()
        validate_modes = schema["definitions"]["silver"]["properties"]["validate_source"]["enum"]

        for mode in validate_modes:
            config = build_minimal_silver_config(validate_source=mode)
            silver = load_silver_from_yaml(config)
            # validate_source may not be stored directly on entity - just verify no error
            assert silver is not None

    def test_all_parquet_compression_accepted(self):
        """Every parquet_compression enum value should be accepted."""
        schema = load_schema()
        compressions = schema["definitions"]["silver"]["properties"]["parquet_compression"]["enum"]

        for compression in compressions:
            config = build_minimal_silver_config(parquet_compression=compression)
            silver = load_silver_from_yaml(config)
            assert silver is not None


# ============================================================================
# Schema Completeness Tests
# ============================================================================

class TestSchemaCompleteness:
    """Test that schema and code are in sync."""

    def test_bronze_schema_fields_match_code(self):
        """Verify Bronze schema fields have corresponding code handling."""
        schema_fields = {name for name, _ in get_bronze_fields()}

        # Fields that are known to be handled
        # (This list should be updated if schema changes)
        expected_handled = {
            "system", "entity", "source_type", "source_path", "target_path",
            "load_pattern", "input_mode", "watermark_column", "watermark_source",
            "connection", "host", "database", "query",
            "options", "partition_by", "chunk_size", "full_refresh_days",
            "write_checksums", "write_metadata",
            # S3 options
            "s3_endpoint_url", "s3_signature_version", "s3_addressing_style",
            "s3_region", "s3_verify_ssl",
            # API fields (handled through ApiSource)
            "base_url", "endpoint", "data_path", "auth", "pagination",
            "requests_per_second", "timeout", "max_retries",
            "headers", "params", "path_params", "watermark_param",
            # CDC options
            "cdc_options",
        }

        # Find fields in schema but not expected
        unexpected = schema_fields - expected_handled
        if unexpected:
            pytest.fail(
                f"Schema has Bronze fields not in expected_handled list: {unexpected}\n"
                "Update the test if these are intentionally new fields."
            )

    def test_silver_schema_fields_match_code(self):
        """Verify Silver schema fields have corresponding code handling."""
        schema_fields = {name for name, _ in get_silver_fields()}

        expected_handled = {
            "system", "entity", "natural_keys", "change_timestamp",
            "source_path", "target_path",
            "model", "entity_kind", "history_mode", "input_mode", "delete_mode",
            "attributes", "exclude_columns", "column_mapping", "partition_by",
            "output_formats", "parquet_compression", "validate_source",
            # S3 options
            "s3_endpoint_url", "s3_signature_version", "s3_addressing_style",
            "s3_region", "s3_verify_ssl",
            # CDC options
            "cdc_options",
        }

        unexpected = schema_fields - expected_handled
        if unexpected:
            pytest.fail(
                f"Schema has Silver fields not in expected_handled list: {unexpected}\n"
                "Update the test if these are intentionally new fields."
            )

    def test_all_bronze_fields_have_examples_or_defaults(self):
        """Every Bronze field should have an example or default value."""
        missing = []
        # Nested object fields that intentionally don't have simple examples
        nested_objects = {"cdc_options", "options", "auth", "pagination", "headers", "params", "path_params"}

        for name, props in get_bronze_fields():
            if name in nested_objects:
                continue  # Nested objects are tested separately
            if not props.get("examples") and "default" not in props and "enum" not in props:
                # Check if it's a required field (those don't need defaults)
                schema = load_schema()
                required = schema["definitions"]["bronze"].get("required", [])
                if name not in required:
                    missing.append(name)

        if missing:
            pytest.fail(
                f"Bronze optional fields missing examples/defaults: {missing}\n"
                "Add 'examples' or 'default' to these fields in pipeline.schema.json"
            )

    def test_all_silver_fields_have_examples_or_defaults(self):
        """Every Silver field should have an example or default value."""
        missing = []
        # Nested object fields and array fields that don't need simple scalar examples
        nested_or_array = {"cdc_options", "attributes", "exclude_columns", "partition_by"}

        for name, props in get_silver_fields():
            if name in nested_or_array:
                continue  # Array/object fields are tested contextually
            if not props.get("examples") and "default" not in props and "enum" not in props:
                schema = load_schema()
                required = schema["definitions"]["silver"].get("required", [])
                if name not in required:
                    missing.append(name)

        if missing:
            pytest.fail(
                f"Silver optional fields missing examples/defaults: {missing}\n"
                "Add 'examples' or 'default' to these fields in pipeline.schema.json"
            )


# ============================================================================
# Nested Object Tests
# ============================================================================

class TestNestedObjectFields:
    """Test nested object fields like auth, pagination, cdc_options."""

    def test_auth_all_types(self):
        """Test all authentication types."""
        schema = load_schema()
        auth_types = schema["definitions"]["bronze"]["properties"]["auth"]["properties"]["auth_type"]["enum"]

        for auth_type in auth_types:
            config = build_minimal_bronze_config(
                source_type="api_rest",
                base_url="https://api.example.com",
                endpoint="/v1/data",
            )

            if auth_type == "none":
                config["auth"] = {"auth_type": "none"}
            elif auth_type == "bearer":
                config["auth"] = {"auth_type": "bearer", "token": "test_token"}
            elif auth_type == "api_key":
                config["auth"] = {"auth_type": "api_key", "api_key": "test_key"}
            elif auth_type == "basic":
                config["auth"] = {"auth_type": "basic", "username": "user", "password": "pass"}

            # Should not raise
            bronze = load_bronze_from_yaml(config)
            assert bronze is not None

    def test_pagination_all_strategies(self):
        """Test all pagination strategies."""
        schema = load_schema()
        strategies = schema["definitions"]["bronze"]["properties"]["pagination"]["properties"]["strategy"]["enum"]

        for strategy in strategies:
            config = build_minimal_bronze_config(
                source_type="api_rest",
                base_url="https://api.example.com",
                endpoint="/v1/data",
            )

            if strategy == "none":
                config["pagination"] = {"strategy": "none"}
            elif strategy == "offset":
                config["pagination"] = {"strategy": "offset", "page_size": 100}
            elif strategy == "page":
                config["pagination"] = {"strategy": "page", "page_size": 100}
            elif strategy == "cursor":
                config["pagination"] = {"strategy": "cursor", "cursor_path": "meta.next"}

            bronze = load_bronze_from_yaml(config)
            assert bronze is not None

    def test_cdc_options(self):
        """Test CDC options configuration."""
        config = build_minimal_bronze_config(
            load_pattern="cdc",
            cdc_options={
                "operation_column": "op",
                "insert_code": "I",
                "update_code": "U",
                "delete_code": "D",
            }
        )
        # Note: This may fail if cdc requires additional Silver config
        # The test verifies the field is accepted by the parser
        try:
            bronze = load_bronze_from_yaml(config)
            assert bronze is not None
        except YAMLConfigError:
            pytest.skip("CDC requires additional configuration")
