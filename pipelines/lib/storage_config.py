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

from pipelines.lib.observability import get_structlog_logger

logger = get_structlog_logger(__name__)

__all__ = [
    "InputMode",
    "S3_YAML_TO_STORAGE_OPTIONS",
    "_configure_duckdb_s3",
    "_extract_storage_options",
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

    # Check environment variable for verify_ssl if not in options
    # Default to False for self-signed certificates
    if "verify_ssl" not in storage_opts:
        env_verify = os.environ.get("AWS_S3_VERIFY_SSL", "").lower()
        # Default to False unless explicitly set to true
        if env_verify in ("true", "1", "yes"):
            storage_opts["verify_ssl"] = True
        else:
            storage_opts["verify_ssl"] = False

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
            storage_opts[key] = options[key]
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
    options = options or {}

    # Get endpoint URL from options or environment
    endpoint_url = options.get("endpoint_url") or os.environ.get("AWS_ENDPOINT_URL")
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

    # Get credentials from options or environment
    access_key = options.get("key") or os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key = options.get("secret") or os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = options.get("region") or os.environ.get("AWS_REGION", "us-east-1")

    # Handle SSL certificate verification
    # Check options first, then environment variable, default to False for self-signed certs
    verify_ssl = options.get("verify_ssl")
    if verify_ssl is None:
        env_verify = os.environ.get("AWS_S3_VERIFY_SSL", "").lower()
        verify_ssl = env_verify in ("true", "1", "yes")

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
