"""Helper utilities shared across storage backends."""

from __future__ import annotations

import os
from typing import Optional


def normalize_prefix(prefix: Optional[str]) -> str:
    """Normalize storage prefix strings by stripping slashes/spaces."""
    if not prefix:
        return ""
    return str(prefix).strip("/ ")


def get_env_value(key: Optional[str]) -> Optional[str]:
    """Return the environment value for the requested key if present."""
    if not key:
        return None
    return os.environ.get(key)
