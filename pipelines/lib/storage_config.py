"""Storage configuration utilities shared across Bronze and Silver layers.

This module provides common storage-related functionality used by both
Bronze and Silver layers, avoiding circular dependencies and duplication.
"""

from __future__ import annotations

import os
from enum import Enum
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import ibis

from pipelines.lib.env import expand_env_vars
from pipelines.lib.observability import get_structlog_logger

logger = get_structlog_logger(__name__)

__all__ = [
    "InputMode",
    "S3_YAML_TO_STORAGE_OPTIONS",
    "_configure_duckdb_s3",
    "_extract_storage_options",
    "get_bool_config_value",
    "get_config_value",
]

# S3 storage option mappings (YAML key -> storage backend key)
# Used by both Bronze and Silver YAML loaders
S3_YAML_TO_STORAGE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("s3_endpoint_url", "endpoint_url"),
    ("s3_signature_version", "s3_signature_version"),
    ("s3_addressing_style", "s3_addressing_style"),
    ("s3_region", "region"),
    ("s3_verify_ssl", "s3_verify_ssl"),
)


class InputMode(Enum):
    """How Silver should interpret Bronze partitions.

    This controls how multiple Bronze date partitions are combined when
    processing in the Silver layer:

    - REPLACE_DAILY: Each Bronze partition is a complete snapshot. Silver
      reads only the latest partition and replaces the target.

    - APPEND_LOG: Bronze partitions are additive (e.g., CDC events, logs).
      Silver reads all partitions and unions them before processing.
    """

    REPLACE_DAILY = "replace_daily"  # Each partition is complete snapshot
    APPEND_LOG = "append_log"  # Partitions are additive (CDC/events)


def get_config_value(
    options: Optional[Dict[str, Any]],
    key: str,
    env_var: str,
    default: str = "",
) -> str:
    """Get a configuration value from options dict or environment variable.

    Handles ${VAR} expansion for values from YAML configs, then falls back
    to environment variable, then to default value.

    This is the canonical way to read config values that may contain
    environment variable references from YAML (like ${AWS_ENDPOINT_URL}).

    Args:
        options: Dict of options (may be None)
        key: Key to look up in options dict
        env_var: Environment variable to fall back to
        default: Default value if neither options nor env var provides a value

    Returns:
        The resolved configuration value

    Example:
        >>> options = {"endpoint_url": "${AWS_ENDPOINT_URL}"}
        >>> os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
        >>> get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        'http://localhost:9000'
    """
    value = options.get(key) if options else None
    if value and isinstance(value, str):
        value = expand_env_vars(value)
    if not value:
        value = os.environ.get(env_var, default)
    return value


def get_bool_config_value(
    options: Optional[Dict[str, Any]],
    key: str,
    env_var: str,
    default: bool = False,
) -> bool:
    """Get a boolean configuration value from options dict or environment variable.

    Handles truthy strings ('true', '1', 'yes') case-insensitively.
    Returns options value if it's already a bool, otherwise parses string.

    Args:
        options: Dict of options (may be None)
        key: Key to look up in options dict
        env_var: Environment variable to fall back to
        default: Default value if neither options nor env var provides a value

    Returns:
        The resolved boolean configuration value

    Example:
        >>> options = {"verify_ssl": "true"}
        >>> get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL", False)
        True

        >>> os.environ["AWS_S3_VERIFY_SSL"] = "yes"
        >>> get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL", False)
        True
    """
    value = options.get(key) if options else None

    # If value is already a boolean, return it
    if isinstance(value, bool):
        return value

    # If value is a string, parse it
    if value is not None and isinstance(value, str):
        return value.lower() in ("true", "1", "yes")

    # Fall back to environment variable
    env_value = os.environ.get(env_var, "").lower()
    if env_value:
        return env_value in ("true", "1", "yes")

    # Return default
    return default


def _extract_storage_options(options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Extract S3/ADLS storage options from pipeline options.

    Maps YAML option names to storage backend option names:
    - s3_signature_version -> signature_version
    - s3_addressing_style -> addressing_style
    - s3_verify_ssl -> verify_ssl
    - endpoint_url, key, secret, region -> passed through

    Args:
        options: Pipeline options dict (from YAML config or Python)

    Returns:
        Dict of storage backend options
    """
    if not options:
        return {}

    storage_opts: Dict[str, Any] = {}

    # Map YAML option names to storage backend option names
    yaml_to_storage = {
        "s3_signature_version": "signature_version",
        "s3_addressing_style": "addressing_style",
        "s3_verify_ssl": "verify_ssl",
    }

    for yaml_key, storage_key in yaml_to_storage.items():
        if yaml_key in options:
            storage_opts[storage_key] = options[yaml_key]

    # Get verify_ssl using shared helper (handles bool/string/env var)
    # Default to False for self-signed certificates
    if "verify_ssl" not in storage_opts:
        storage_opts["verify_ssl"] = get_bool_config_value(
            options, "verify_ssl", "AWS_S3_VERIFY_SSL", default=False
        )

    # Pass through standard S3 options (also check environment variables)
    pass_through = ["endpoint_url", "key", "secret", "region"]
    env_mapping = {
        "endpoint_url": "AWS_ENDPOINT_URL",
        "key": "AWS_ACCESS_KEY_ID",
        "secret": "AWS_SECRET_ACCESS_KEY",
        "region": "AWS_REGION",
    }

    for key in pass_through:
        if key in options:
            value = options[key]
            # Expand ${VAR} patterns in string values
            if isinstance(value, str):
                value = expand_env_vars(value)
            storage_opts[key] = value
        elif env_mapping.get(key) and os.environ.get(env_mapping[key]):
            storage_opts[key] = os.environ[env_mapping[key]]

    return storage_opts


def _configure_duckdb_s3(con: ibis.BaseBackend, options: Optional[Dict[str, Any]] = None) -> None:
    """Configure DuckDB's httpfs extension for S3/MinIO access.

    DuckDB does not automatically pick up AWS_ENDPOINT_URL environment variable,
    so we need to explicitly configure S3 settings when using custom endpoints
    like MinIO or LocalStack.

    Args:
        con: Ibis DuckDB connection
        options: Optional dict with endpoint_url, key, secret, region
    """
    # Get endpoint URL using shared helper (handles ${VAR} expansion)
    endpoint_url = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
    if not endpoint_url:
        # No custom endpoint - DuckDB will use AWS defaults
        return

    # Install and load httpfs extension
    try:
        con.raw_sql("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        # May already be installed/loaded
        pass

    # Parse endpoint URL
    parsed = urlparse(endpoint_url)
    host = parsed.hostname or "localhost"
    port = parsed.port
    use_ssl = parsed.scheme == "https"

    # Build endpoint string
    if port:
        endpoint = f"{host}:{port}"
    else:
        endpoint = host

    # Get credentials using shared helper (handles ${VAR} expansion)
    access_key = get_config_value(options, "key", "AWS_ACCESS_KEY_ID")
    secret_key = get_config_value(options, "secret", "AWS_SECRET_ACCESS_KEY")
    region = get_config_value(options, "region", "AWS_REGION", "us-east-1")

    # Get verify_ssl using shared helper (handles bool/string/env var)
    # Default to False for self-signed certificates
    verify_ssl = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL", default=False)

    # Configure DuckDB S3 settings
    settings = [
        f"SET s3_endpoint = '{endpoint}';",
        f"SET s3_access_key_id = '{access_key}';",
        f"SET s3_secret_access_key = '{secret_key}';",
        f"SET s3_region = '{region}';",
        f"SET s3_use_ssl = {str(use_ssl).lower()};",
        "SET s3_url_style = 'path';",  # Use path-style URLs for MinIO compatibility
    ]

    # Disable SSL certificate verification for self-signed certificates
    if not verify_ssl:
        settings.append("SET enable_server_cert_verification = false;")
        settings.append("SET enable_curl_server_cert_verification = false;")

    for setting in settings:
        try:
            con.raw_sql(setting)
        except Exception as e:
            logger.debug("duckdb_s3_setting_warning", setting=setting, error=str(e))
