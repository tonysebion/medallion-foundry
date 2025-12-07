"""Placeholder utilities used by config loaders."""

from __future__ import annotations

import os
import re
from typing import Any

_ENV_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def substitute_env_vars(value: Any) -> Any:
    """Recursively substitute ${VAR} references in config values."""

    if isinstance(value, str):
        def replacer(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default_value = match.group(2)
            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            if default_value is not None:
                return str(default_value)
            raise ValueError(
                f"Environment variable '{var_name}' is not set and no default provided"
            )

        return _ENV_VAR_PATTERN.sub(replacer, value)

    if isinstance(value, dict):
        return {k: substitute_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [substitute_env_vars(item) for item in value]

    return value


def resolve_env_vars(value: Any) -> Any:
    """Resolve ${VAR} placeholders but keep missing vars intact."""

    if isinstance(value, str):
        def replacer(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is None:
                return match.group(0)
            return env_value

        return _ENV_VAR_PATTERN.sub(replacer, value)

    if isinstance(value, dict):
        return {k: resolve_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [resolve_env_vars(item) for item in value]

    return value


def apply_env_substitution(config: dict[str, Any]) -> dict[str, Any]:
    """Apply strict env substitution for an entire config tree."""

    return substitute_env_vars(config)
