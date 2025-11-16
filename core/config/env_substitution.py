"""Environment variable substitution for configuration values.

Supports ${VAR_NAME} and ${VAR_NAME:default_value} syntax.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict


_ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def substitute_env_vars(value: Any) -> Any:
    """Recursively substitute environment variables in config values.

    Supports:
    - ${VAR_NAME} - fails if VAR_NAME not set
    - ${VAR_NAME:default} - uses default if VAR_NAME not set

    Args:
        value: Config value (str, dict, list, or primitive)

    Returns:
        Value with environment variables substituted

    Raises:
        ValueError: If required environment variable is not set
    """
    if isinstance(value, str):

        def replacer(match: re.Match) -> str:
            var_name = match.group(1)
            default_value = match.group(2)

            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            if default_value is not None:
                return default_value
            raise ValueError(
                f"Environment variable '{var_name}' is not set and no default provided"
            )

        return _ENV_VAR_PATTERN.sub(replacer, value)

    elif isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}

    elif isinstance(value, list):
        return [substitute_env_vars(item) for item in value]

    else:
        return value


def apply_env_substitution(config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply environment variable substitution to entire config.

    Args:
        config: Configuration dictionary

    Returns:
        Config with all ${VAR} references substituted
    """
    return substitute_env_vars(config)
