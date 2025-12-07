"""Domain catalog utilities.

This module contains catalog-related utilities that have dependencies on
higher layers (L1+), such as the YAML skeleton generator that uses the
OpenMetadata client.
"""

from __future__ import annotations

from core.domain.catalog.yaml_generator import (
    generate_yaml_skeleton,
    generate_multi_table_skeletons,
)

__all__ = [
    "generate_yaml_skeleton",
    "generate_multi_table_skeletons",
]
