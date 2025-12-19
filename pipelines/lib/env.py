"""Environment variable utilities.

Provides expansion of ${VAR_NAME} patterns in configuration values.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict

__all__ = ["expand_env_vars", "expand_options"]

# Pattern for ${VAR_NAME} or $VAR_NAME
ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def expand_env_vars(value: str, *, strict: bool = False) -> str:
    """Expand environment variables in a string.

    Supports both ${VAR_NAME} and $VAR_NAME syntax.

    Args:
        value: String potentially containing env var references
        strict: If True, raise KeyError for missing variables

    Returns:
        String with environment variables expanded

    Example:
        >>> os.environ["DB_HOST"] = "localhost"
        >>> expand_env_vars("${DB_HOST}:5432")
        'localhost:5432'
    """

    def replacer(match: re.Match) -> str:
        var_name = match.group(1) or match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is None:
            if strict:
                raise KeyError(f"Environment variable not set: {var_name}")
            # Return original if not strict
            return match.group(0)
        return env_value

    return ENV_VAR_PATTERN.sub(replacer, value)


def expand_options(options: Dict[str, Any], *, strict: bool = False) -> Dict[str, Any]:
    """Recursively expand environment variables in an options dict.

    Args:
        options: Dictionary of options
        strict: If True, raise KeyError for missing variables

    Returns:
        New dictionary with env vars expanded in string values

    Example:
        >>> os.environ["DB_HOST"] = "prod-server.com"
        >>> expand_options({"host": "${DB_HOST}", "port": 5432})
        {'host': 'prod-server.com', 'port': 5432}
    """
    result: Dict[str, Any] = {}

    for key, value in options.items():
        if isinstance(value, str):
            result[key] = expand_env_vars(value, strict=strict)
        elif isinstance(value, dict):
            result[key] = expand_options(value, strict=strict)
        elif isinstance(value, list):
            result[key] = [
                expand_env_vars(item, strict=strict)
                if isinstance(item, str)
                else item
                for item in value
            ]
        else:
            result[key] = value

    return result
