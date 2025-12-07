"""Path sanitization and partition utilities for storage operations.

This module provides utilities for safely constructing storage paths,
particularly for partitioned data layouts.
"""

from __future__ import annotations

import re
from typing import Any


def sanitize_partition_value(value: Any) -> str:
    """Sanitize a value for use in partition paths.

    Replaces any characters that are not alphanumeric, dot, underscore,
    or hyphen with underscores. This ensures the value is safe for use
    in filesystem paths and S3 keys.

    Args:
        value: Value to sanitize (will be converted to string)

    Returns:
        Sanitized string safe for filesystem paths

    Example:
        >>> sanitize_partition_value("2024-01-15")
        '2024-01-15'
        >>> sanitize_partition_value("US/East")
        'US_East'
        >>> sanitize_partition_value("foo bar@baz")
        'foo_bar_baz'
    """
    return re.sub(r"[^0-9A-Za-z._-]", "_", str(value))


def build_partition_path(*parts: str) -> str:
    """Build a partition path from key=value parts.

    Args:
        *parts: Partition path segments (e.g., "year=2024", "month=01")

    Returns:
        Combined path string with forward slashes

    Example:
        >>> build_partition_path("year=2024", "month=01")
        'year=2024/month=01'
    """
    return "/".join(parts)
