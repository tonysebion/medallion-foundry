"""Environment variable utilities.

Provides expansion of ${VAR_NAME} patterns in configuration values
and loading of .env files.

Uses python-dotenv for .env file loading.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

from dotenv import load_dotenv

__all__ = [
    "expand_env_vars",
    "expand_options",
    "extract_nested_value",
    "load_env_file",
    "parse_iso_datetime",
    "utc_now_iso",
]


def extract_nested_value(
    data: Any, path: str, *, default: Any = None, raise_on_missing: bool = False
) -> Any:
    """Extract value from nested dict using dot-notation path.

    Navigates through nested dictionaries using a dot-separated path string.
    Handles list indexing with numeric path components.

    Args:
        data: Dict or nested structure to navigate
        path: Dot-separated path like "data.customers.items" or "items.0.name"
        default: Value to return if path not found (default: None)
        raise_on_missing: If True, raise KeyError for missing paths

    Returns:
        Value at path, or default if not found

    Raises:
        KeyError: If raise_on_missing=True and path not found

    Example:
        >>> data = {"response": {"items": [{"id": 1}, {"id": 2}]}}
        >>> extract_nested_value(data, "response.items")
        [{'id': 1}, {'id': 2}]
        >>> extract_nested_value(data, "response.items.0.id")
        1
        >>> extract_nested_value(data, "missing.path", default=[])
        []
    """
    for key in path.split("."):
        if isinstance(data, dict):
            if key in data:
                data = data[key]
            elif raise_on_missing:
                raise KeyError(f"Path '{path}' not found in structure")
            else:
                return default
        elif isinstance(data, list) and key.isdigit():
            idx = int(key)
            if idx < len(data):
                data = data[idx]
            elif raise_on_missing:
                raise KeyError(f"Path '{path}' not found in structure")
            else:
                return default
        elif raise_on_missing:
            raise KeyError(f"Path '{path}' not found in structure")
        else:
            return default
    return data


def parse_iso_datetime(value: str) -> datetime:
    """Parse ISO format datetime string, handling 'Z' timezone marker.

    Converts 'Z' suffix to '+00:00' for compatibility with Python's
    datetime.fromisoformat() which doesn't handle 'Z' directly.

    Args:
        value: ISO format datetime string (may end with 'Z' or '+00:00')

    Returns:
        Timezone-aware datetime object

    Example:
        >>> parse_iso_datetime("2025-01-15T10:30:00Z")
        datetime.datetime(2025, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
        >>> parse_iso_datetime("2025-01-15T10:30:00+00:00")
        datetime.datetime(2025, 1, 15, 10, 30, tzinfo=datetime.timezone.utc)
    """
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 formatted string.

    This is a convenience function to standardize timestamp generation
    across the codebase.

    Returns:
        ISO 8601 formatted timestamp string (e.g., "2025-01-15T10:30:00+00:00")

    Example:
        >>> now = utc_now_iso()
        >>> # Returns something like "2025-01-15T10:30:00.123456+00:00"
    """
    return datetime.now(timezone.utc).isoformat()

# Pattern for ${VAR_NAME} or $VAR_NAME
ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def load_env_file(
    path: Optional[Union[str, Path]] = None,
    *,
    override: bool = False,
) -> bool:
    """Load environment variables from a .env file.

    Args:
        path: Path to .env file. If None, searches for .env in current
              directory and parent directories.
        override: If True, override existing environment variables.

    Returns:
        True if a .env file was found and loaded, False otherwise.

    Example:
        >>> load_env_file()  # Loads from .env in current dir
        True
        >>> load_env_file(".env.production")  # Load specific file
        True
    """
    return load_dotenv(dotenv_path=path, override=override)


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

    def replacer(match: re.Match[str]) -> str:
        var_name = match.group(1) or match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is None:
            if strict:
                raise KeyError(f"Environment variable not set: {var_name}")
            # Return original if not strict
            return str(match.group(0))
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
