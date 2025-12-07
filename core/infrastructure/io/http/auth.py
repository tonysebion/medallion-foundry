"""Authentication helpers for HTTP extractors."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def build_api_auth(
    api_cfg: Dict[str, Any]
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build headers and auth tuple for API extractors."""

    headers: Dict[str, str] = {"Accept": "application/json"}
    auth_type = api_cfg.get("auth_type", "none")
    auth_tuple: Optional[Tuple[str, str]] = None

    if auth_type == "bearer":
        token_env = api_cfg.get("auth_token_env")
        if not token_env:
            raise ValueError("auth_type='bearer' requires 'auth_token_env' in config")
        token = os.environ.get(token_env)
        if not token:
            raise ValueError(
                f"Environment variable '{token_env}' not set for bearer token"
            )
        headers["Authorization"] = f"Bearer {token}"
        logger.debug("Added bearer token authentication")

    elif auth_type == "api_key":
        key_env = api_cfg.get("auth_key_env")
        key_header = api_cfg.get("auth_key_header", "X-API-Key")
        if not key_env:
            raise ValueError("auth_type='api_key' requires 'auth_key_env' in config")
        api_key = os.environ.get(key_env)
        if not api_key:
            raise ValueError(
                f"Environment variable '{key_env}' not set for API key"
            )
        headers[key_header] = api_key
        logger.debug("Added API key authentication in header '%s'", key_header)

    elif auth_type == "basic":
        username_env = api_cfg.get("auth_username_env")
        password_env = api_cfg.get("auth_password_env")
        if not username_env or not password_env:
            raise ValueError(
                "auth_type='basic' requires 'auth_username_env' and 'auth_password_env'"
            )
        username = os.environ.get(username_env)
        password = os.environ.get(password_env)
        if not (username and password):
            raise ValueError("Basic auth requires both username and password env vars")
        auth_tuple = (username, password)
        logger.debug("Prepared basic authentication tuple")

    elif auth_type != "none":
        raise ValueError(
            "Unsupported auth_type: '%s'. Use 'bearer', 'api_key', 'basic', or 'none'"
            % auth_type
        )

    # Additional headers from config
    custom_headers = api_cfg.get("headers", {})
    headers.update(custom_headers)

    return headers, auth_tuple
