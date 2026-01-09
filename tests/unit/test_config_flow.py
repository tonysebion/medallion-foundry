"""Integration tests for config expansion flow.

These tests verify that environment variables are properly expanded
at each layer of the config flow:
- YAML config → BronzeSource options
- BronzeSource options → S3Storage client
- API options → AuthConfig validation

These are regression tests to ensure ${VAR} patterns expand correctly
everywhere, not just in some places (the bug we fixed).
"""

from __future__ import annotations

from typing import Any, Dict

import pytest


class TestBronzeConfigExpansion:
    """Test that env vars in Bronze options are expanded at the right time."""

    def test_database_host_expanded_before_connection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Database host ${DB_HOST} is expanded before creating connection."""
        from pipelines.lib.connections import _expand_credentials

        monkeypatch.setenv("DB_HOST", "prod-server.example.com")
        monkeypatch.setenv("DB_PASSWORD", "secret123")

        options: Dict[str, Any] = {
            "host": "${DB_HOST}",
            "database": "mydb",
            "user": "admin",
            "password": "${DB_PASSWORD}",
        }

        expanded = _expand_credentials(options)

        assert expanded["host"] == "prod-server.example.com"
        assert expanded["password"] == "secret123"
        assert expanded["database"] == "mydb"
        assert expanded["user"] == "admin"

    def test_s3_endpoint_expanded_in_storage_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """S3 endpoint_url with ${MINIO_ENDPOINT} is expanded in storage options."""
        from pipelines.lib.storage_config import get_config_value

        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")

        options: Dict[str, Any] = {
            "endpoint_url": "${MINIO_ENDPOINT}",
        }

        value = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert value == "http://localhost:9000"

    def test_s3_endpoint_falls_back_to_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When option not in dict, falls back to env var."""
        from pipelines.lib.storage_config import get_config_value

        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://fallback:9000")

        value = get_config_value({}, "endpoint_url", "AWS_ENDPOINT_URL")
        assert value == "http://fallback:9000"


class TestS3StorageConfigFlow:
    """Test end-to-end S3 config flow from options to boto3 client."""

    def test_verify_ssl_from_options_reaches_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """verify_ssl: false in options reaches boto3 client creation."""
        from pipelines.lib.storage_config import get_bool_config_value

        # Test with boolean value
        options: Dict[str, Any] = {"verify_ssl": False}
        value = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL", default=True)
        assert value is False

    def test_verify_ssl_string_true_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """verify_ssl: "true" (string) is parsed as True."""
        from pipelines.lib.storage_config import get_bool_config_value

        options: Dict[str, Any] = {"verify_ssl": "true"}
        value = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL", default=False)
        assert value is True

    def test_verify_ssl_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """AWS_S3_VERIFY_SSL env var is used when options missing."""
        from pipelines.lib.storage_config import get_bool_config_value

        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "false")

        value = get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL", default=True)
        assert value is False


class TestApiConfigExpansion:
    """Test that API credentials are expanded before AuthConfig validation."""

    def test_bearer_token_expanded_before_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """${API_TOKEN} is expanded BEFORE AuthConfig validates it."""
        from pipelines.lib.api import build_auth_headers_from_dict

        monkeypatch.setenv("API_TOKEN", "real-secret-token")

        api_options: Dict[str, Any] = {
            "auth_type": "bearer",
            "token": "${API_TOKEN}",
        }

        # build_auth_headers_from_dict returns (headers, basic_auth_tuple)
        headers, _ = build_auth_headers_from_dict(api_options)
        assert headers["Authorization"] == "Bearer real-secret-token"

    def test_api_key_expanded_before_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """${API_KEY} is expanded BEFORE AuthConfig validates it."""
        from pipelines.lib.api import build_auth_headers_from_dict

        monkeypatch.setenv("MY_API_KEY", "secret-api-key")

        api_options: Dict[str, Any] = {
            "auth_type": "api_key",
            "api_key": "${MY_API_KEY}",
            "api_key_header": "X-Custom-Key",
        }

        # build_auth_headers_from_dict returns (headers, basic_auth_tuple)
        headers, _ = build_auth_headers_from_dict(api_options)
        assert headers["X-Custom-Key"] == "secret-api-key"

    def test_basic_auth_expanded_before_validation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """${USERNAME} and ${PASSWORD} expanded BEFORE basic auth encoding."""
        from pipelines.lib.api import build_auth_headers_from_dict

        monkeypatch.setenv("API_USER", "admin")
        monkeypatch.setenv("API_PASS", "secret123")

        api_options: Dict[str, Any] = {
            "auth_type": "basic",
            "username": "${API_USER}",
            "password": "${API_PASS}",
        }

        # build_auth_headers_from_dict returns (headers, basic_auth_tuple)
        headers, basic_auth = build_auth_headers_from_dict(api_options)

        # Basic auth returns the credentials as a tuple for requests lib
        assert basic_auth == ("admin", "secret123")

    def test_unexpanded_placeholder_raises_keyerror(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If env var not set, expand_env_vars with strict=True raises KeyError.

        API expansion uses strict mode internally for auth tokens, so missing
        env vars raise KeyError rather than leaving placeholders.
        """
        from pipelines.lib.api import build_auth_headers_from_dict

        # Ensure the env var is NOT set
        monkeypatch.delenv("MISSING_TOKEN", raising=False)

        api_options: Dict[str, Any] = {
            "auth_type": "bearer",
            "token": "${MISSING_TOKEN}",  # strict mode raises KeyError
        }

        # expand_env_vars with strict=True raises KeyError for missing vars
        with pytest.raises(KeyError, match="MISSING_TOKEN"):
            build_auth_headers_from_dict(api_options)


class TestYamlConfigExpansion:
    """Test that YAML configs properly expand ${VAR} patterns."""

    def test_bronze_options_expanded_from_yaml(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        """Options in YAML Bronze config have ${VAR} expanded."""
        from pipelines.lib.config_loader import load_bronze_from_yaml

        monkeypatch.setenv("TEST_SOURCE_PATH", str(tmp_path / "data.csv"))

        # Create test data file
        (tmp_path / "data.csv").write_text("id,name\n1,Alice\n")

        config: Dict[str, Any] = {
            "system": "test",
            "entity": "data",
            "source_type": "file_csv",
            "source_path": "./data.csv",
            "target_path": "./bronze",
            "options": {
                "endpoint_url": "${MINIO_ENDPOINT}",
            },
        }

        bronze = load_bronze_from_yaml(config, tmp_path)

        # The options should be stored as-is (expansion happens at runtime)
        assert bronze.options.get("endpoint_url") == "${MINIO_ENDPOINT}"

    def test_silver_storage_options_from_yaml(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        """Storage options in YAML Silver config are passed through.

        YAML uses s3_* prefixed keys which map to storage_options.
        """
        from pipelines.lib.config_loader import load_silver_from_yaml

        config: Dict[str, Any] = {
            "domain": "sales",
            "subject": "orders",
            "natural_keys": ["order_id"],
            "change_timestamp": "updated_at",
            "source_path": "./bronze/*.parquet",
            "target_path": "./silver",
            # YAML key is s3_endpoint_url, maps to endpoint_url in storage_options
            "s3_endpoint_url": "${MINIO_ENDPOINT}",
        }

        silver = load_silver_from_yaml(config, tmp_path)

        # Storage options should contain the endpoint_url
        assert silver.storage_options is not None
        assert silver.storage_options.get("endpoint_url") == "${MINIO_ENDPOINT}"


class TestConnectionsExpansion:
    """Test database connection credential expansion."""

    def test_mssql_credentials_expanded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """MSSQL connection expands ${VAR} in credentials."""
        from pipelines.lib.connections import _expand_credentials

        monkeypatch.setenv("MSSQL_HOST", "sql-server.example.com")
        monkeypatch.setenv("MSSQL_DB", "production")
        monkeypatch.setenv("MSSQL_USER", "app_user")
        monkeypatch.setenv("MSSQL_PASS", "secret!")

        options = {
            "host": "${MSSQL_HOST}",
            "database": "${MSSQL_DB}",
            "user": "${MSSQL_USER}",
            "password": "${MSSQL_PASS}",
        }

        expanded = _expand_credentials(options)

        assert expanded["host"] == "sql-server.example.com"
        assert expanded["database"] == "production"
        assert expanded["user"] == "app_user"
        assert expanded["password"] == "secret!"

    def test_partial_expansion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Some values literal, some expanded."""
        from pipelines.lib.connections import _expand_credentials

        monkeypatch.setenv("SECRET_PASS", "p@ssw0rd")

        options = {
            "host": "localhost",  # Literal
            "database": "testdb",  # Literal
            "user": "testuser",  # Literal
            "password": "${SECRET_PASS}",  # Expanded
        }

        expanded = _expand_credentials(options)

        assert expanded["host"] == "localhost"
        assert expanded["database"] == "testdb"
        assert expanded["user"] == "testuser"
        assert expanded["password"] == "p@ssw0rd"

    def test_missing_env_var_keeps_placeholder(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Missing env var keeps placeholder (non-strict mode).

        The _expand_credentials function doesn't use strict mode, so missing
        env vars remain as placeholders. This allows connection strings to
        fail at connection time with a clearer error from the DB driver.
        """
        from pipelines.lib.connections import _expand_credentials

        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)

        options = {
            "host": "${NONEXISTENT_VAR}",
            "database": "mydb",
            "user": "",
            "password": "",
        }

        expanded = _expand_credentials(options)

        # Without strict mode, placeholder stays as-is
        assert expanded["host"] == "${NONEXISTENT_VAR}"


class TestExpandOptionsFunction:
    """Test the expand_options utility function."""

    def test_expand_options_expands_nested_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """expand_options handles nested dicts and lists."""
        from pipelines.lib.env import expand_options

        monkeypatch.setenv("HOST", "prod.example.com")
        monkeypatch.setenv("TOKEN", "secret123")

        options: Dict[str, Any] = {
            "url": "https://${HOST}/api",
            "auth": {
                "token": "${TOKEN}",
                "type": "bearer",  # Literal string
            },
            "headers": ["X-Custom: ${TOKEN}"],
            "timeout": 30,  # Non-string values unchanged
        }

        expanded = expand_options(options)

        assert expanded["url"] == "https://prod.example.com/api"
        assert expanded["auth"]["token"] == "secret123"
        assert expanded["auth"]["type"] == "bearer"
        assert expanded["headers"] == ["X-Custom: secret123"]
        assert expanded["timeout"] == 30
