"""API authentication utilities for pipeline sources.

Provides authentication configuration and header building for REST API
extraction. Supports bearer tokens, API keys, and basic authentication.

All credential values should use environment variable references (${VAR_NAME})
which are expanded at runtime using the env module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple

from pipelines.lib.env import expand_env_vars

logger = logging.getLogger(__name__)

__all__ = [
    "AuthType",
    "AuthConfig",
    "build_auth_headers",
]


class AuthType(Enum):
    """Supported API authentication methods."""

    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"


@dataclass
class AuthConfig:
    """Configuration for API authentication.

    All credential fields support environment variable expansion using
    ${VAR_NAME} syntax. Use this instead of hardcoding secrets.

    Examples:
        # No authentication
        auth = AuthConfig(auth_type=AuthType.NONE)

        # Bearer token
        auth = AuthConfig(
            auth_type=AuthType.BEARER,
            token="${API_TOKEN}",
        )

        # API key in header
        auth = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="${API_KEY}",
            api_key_header="X-API-Key",
        )

        # Basic auth
        auth = AuthConfig(
            auth_type=AuthType.BASIC,
            username="${API_USER}",
            password="${API_PASSWORD}",
        )
    """

    auth_type: AuthType = AuthType.NONE

    # Bearer token authentication
    token: Optional[str] = None

    # API key authentication
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"

    # Basic authentication
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate configuration based on auth type."""
        if self.auth_type == AuthType.BEARER and not self.token:
            raise ValueError("Bearer authentication requires 'token' to be set")
        if self.auth_type == AuthType.API_KEY and not self.api_key:
            raise ValueError("API key authentication requires 'api_key' to be set")
        if self.auth_type == AuthType.BASIC and not (self.username and self.password):
            raise ValueError(
                "Basic authentication requires both 'username' and 'password'"
            )


def build_auth_headers(
    config: Optional[AuthConfig],
    *,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build HTTP headers and auth tuple from authentication config.

    Args:
        config: Authentication configuration (None = no auth)
        extra_headers: Additional headers to include

    Returns:
        Tuple of (headers dict, optional basic auth tuple)

    Raises:
        ValueError: If environment variables cannot be resolved

    Example:
        auth = AuthConfig(auth_type=AuthType.BEARER, token="${MY_TOKEN}")
        headers, auth_tuple = build_auth_headers(auth)
        response = requests.get(url, headers=headers, auth=auth_tuple)
    """
    headers: Dict[str, str] = {"Accept": "application/json"}
    auth_tuple: Optional[Tuple[str, str]] = None

    if config is None or config.auth_type == AuthType.NONE:
        logger.debug("No authentication configured")

    elif config.auth_type == AuthType.BEARER:
        token = expand_env_vars(config.token or "", strict=True)
        if not token:
            raise ValueError("Bearer token resolved to empty string")
        headers["Authorization"] = f"Bearer {token}"
        logger.debug("Added bearer token authentication")

    elif config.auth_type == AuthType.API_KEY:
        api_key = expand_env_vars(config.api_key or "", strict=True)
        if not api_key:
            raise ValueError("API key resolved to empty string")
        headers[config.api_key_header] = api_key
        logger.debug("Added API key authentication in header '%s'", config.api_key_header)

    elif config.auth_type == AuthType.BASIC:
        username = expand_env_vars(config.username or "", strict=True)
        password = expand_env_vars(config.password or "", strict=True)
        if not (username and password):
            raise ValueError("Basic auth username or password resolved to empty string")
        auth_tuple = (username, password)
        logger.debug("Prepared basic authentication")

    # Add any extra headers
    if extra_headers:
        # Expand env vars in extra headers too
        for key, value in extra_headers.items():
            headers[key] = expand_env_vars(value, strict=False)

    return headers, auth_tuple


def build_auth_headers_from_dict(
    api_options: Dict[str, str],
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build auth headers from a dictionary of options.

    This is a convenience function for building AuthConfig from
    the options dict commonly used in BronzeSource.

    Expected keys:
        - auth_type: "none", "bearer", "api_key", "basic"
        - token: Bearer token (for auth_type=bearer)
        - api_key: API key value (for auth_type=api_key)
        - api_key_header: Header name for API key (default: X-API-Key)
        - username: Username (for auth_type=basic)
        - password: Password (for auth_type=basic)

    Args:
        api_options: Dictionary with auth configuration

    Returns:
        Tuple of (headers dict, optional basic auth tuple)
    """
    auth_type_str = api_options.get("auth_type", "none").lower()

    try:
        auth_type = AuthType(auth_type_str)
    except ValueError:
        raise ValueError(
            f"Unsupported auth_type: '{auth_type_str}'. "
            f"Use 'bearer', 'api_key', 'basic', or 'none'"
        )

    config = AuthConfig(
        auth_type=auth_type,
        token=api_options.get("token"),
        api_key=api_options.get("api_key"),
        api_key_header=api_options.get("api_key_header", "X-API-Key"),
        username=api_options.get("username"),
        password=api_options.get("password"),
    )

    extra_headers = api_options.get("headers", {})
    if isinstance(extra_headers, dict):
        return build_auth_headers(config, extra_headers=extra_headers)
    return build_auth_headers(config)
