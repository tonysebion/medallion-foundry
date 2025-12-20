"""Parent-child YAML configuration inheritance.

Supports extending parent configurations via the 'extends' key:

    # child.yaml
    extends: ./base.yaml

    bronze:
      entity: specific_table  # Override parent's entity
      # system, host, database inherited from parent

The child config is deep-merged with the parent, with child values
taking precedence over parent values.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_with_inheritance(
    config_path: Path | str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Load a YAML config, resolving parent inheritance.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Tuple of (merged_config, parent_config_or_none)

    Raises:
        FileNotFoundError: If config or parent file doesn't exist
        ValueError: If parent path is invalid
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, encoding="utf-8") as f:
        child_config = yaml.safe_load(f) or {}

    if "extends" not in child_config:
        return child_config, None

    # Resolve parent path relative to child config file
    parent_ref = child_config["extends"]
    parent_path = (config_path.parent / parent_ref).resolve()

    if not parent_path.exists():
        raise FileNotFoundError(
            f"Parent config not found: {parent_path} "
            f"(referenced from {config_path})"
        )

    with open(parent_path, encoding="utf-8") as f:
        parent_config = yaml.safe_load(f) or {}

    # Deep merge: child values override parent values
    merged = deep_merge(parent_config, child_config)

    # Remove the 'extends' key from the final merged config
    merged.pop("extends", None)

    return merged, parent_config


def deep_merge(
    base: dict[str, Any],
    override: dict[str, Any],
) -> dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence.

    Args:
        base: Base dictionary (parent config)
        override: Override dictionary (child config)

    Returns:
        New dictionary with merged values

    Example:
        >>> base = {"bronze": {"system": "a", "entity": "b"}}
        >>> override = {"bronze": {"entity": "c"}}
        >>> deep_merge(base, override)
        {"bronze": {"system": "a", "entity": "c"}}
    """
    result = dict(base)

    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            # Recursively merge nested dicts
            result[key] = deep_merge(result[key], value)
        else:
            # Override the value
            result[key] = value

    return result


def identify_field_sources(
    parent: dict[str, Any],
    child: dict[str, Any],
    merged: dict[str, Any],
    prefix: str = "",
) -> dict[str, str]:
    """Identify whether each field comes from parent or child.

    Useful for TUI to show inherited vs overridden values visually.

    Args:
        parent: Parent configuration
        child: Child configuration (before merge)
        merged: Merged configuration
        prefix: Current key path prefix (for recursion)

    Returns:
        Dict mapping field paths to source ("parent" or "child")

    Example:
        >>> parent = {"bronze": {"system": "a", "entity": "b"}}
        >>> child = {"bronze": {"entity": "c"}}
        >>> merged = deep_merge(parent, child)
        >>> identify_field_sources(parent, child, merged)
        {"bronze.system": "parent", "bronze.entity": "child"}
    """
    sources: dict[str, str] = {}

    for key, value in merged.items():
        path = f"{prefix}.{key}" if prefix else key

        if isinstance(value, dict):
            # Recurse into nested dicts
            parent_nested = parent.get(key, {}) if isinstance(parent.get(key), dict) else {}
            child_nested = child.get(key, {}) if isinstance(child.get(key), dict) else {}
            nested_sources = identify_field_sources(
                parent_nested, child_nested, value, path
            )
            sources.update(nested_sources)
        else:
            # Determine source: check if child explicitly set this value
            child_value = _get_nested_value(child, key, prefix)
            if child_value is not None:
                sources[path] = "child"
            else:
                sources[path] = "parent"

    return sources


def _get_nested_value(
    config: dict[str, Any],
    key: str,
    prefix: str,
) -> Any | None:
    """Get a value from a potentially nested config structure."""
    if not prefix:
        return config.get(key)

    # Navigate to the nested location
    parts = prefix.split(".")
    current = config
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]

    if not isinstance(current, dict):
        return None
    return current.get(key)


def create_child_template(
    parent_path: str,
    child_name: str,
    child_description: str = "",
) -> dict[str, Any]:
    """Create a minimal child config template that extends a parent.

    Args:
        parent_path: Relative path to parent config
        child_name: Name for the child pipeline
        child_description: Description for the child pipeline

    Returns:
        Minimal config dict with extends reference
    """
    return {
        "extends": parent_path,
        "name": child_name,
        "description": child_description or f"Child pipeline extending {parent_path}",
    }


def validate_inheritance_chain(config_path: Path | str) -> list[str]:
    """Validate the inheritance chain for a config file.

    Checks:
    - Parent file exists
    - No circular references
    - Parent is valid YAML

    Returns:
        List of error messages (empty if valid)
    """
    errors: list[str] = []
    config_path = Path(config_path)
    visited: set[Path] = set()

    current_path = config_path
    while True:
        if current_path in visited:
            errors.append(f"Circular inheritance detected: {current_path}")
            break
        visited.add(current_path)

        if not current_path.exists():
            errors.append(f"File not found: {current_path}")
            break

        try:
            with open(current_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            errors.append(f"Invalid YAML in {current_path}: {e}")
            break

        if not config or "extends" not in config:
            break  # End of chain

        parent_ref = config["extends"]
        current_path = (current_path.parent / parent_ref).resolve()

    return errors
