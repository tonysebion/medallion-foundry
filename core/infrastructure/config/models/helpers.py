"""Helper functions for configuration validation.

These functions are used across multiple config model files.
"""

from __future__ import annotations

from typing import Any, List, Optional


def require_list_of_strings(values: Any, field_name: str) -> List[str]:
    """Validate and normalize a list of strings."""
    if values is None:
        return []
    if not isinstance(values, list) or any(
        not isinstance(item, str) for item in values
    ):
        raise ValueError(f"{field_name} must be a list of strings")
    return [item.strip() for item in values if item and item.strip()]


def require_optional_str(value: Any, field_name: str) -> Optional[str]:
    """Validate and normalize an optional string."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    value = value.strip()
    return value or None


def require_bool(value: Any, field_name: str, default: bool) -> bool:
    """Validate a boolean with a default value."""
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def ensure_bucket_reference(storage: Any, default_ref: str) -> None:
    """Ensure S3 storage config has a bucket reference."""
    if not isinstance(storage, dict):
        return
    backend = storage.get("backend")
    if backend != "s3":
        return
    if "bucket" not in storage or not storage["bucket"]:
        storage["bucket"] = default_ref


__all__ = [
    "require_list_of_strings",
    "require_optional_str",
    "require_bool",
    "ensure_bucket_reference",
]
