"""Fixed-width file parsing utilities.

Provides parsing for fixed-width files, including parent-child record patterns
commonly found in mainframe data extracts and legacy systems.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import ibis
import pandas as pd

__all__ = [
    "parse_fixed_width_line",
    "read_parent_child_fixed_width",
]


def parse_fixed_width_line(line: str, widths: List[int]) -> List[str]:
    """Parse a single fixed-width line into column values.

    Args:
        line: The line content (already stripped of type indicator)
        widths: List of column widths

    Returns:
        List of stripped string values

    Example:
        >>> parse_fixed_width_line("John      Doe       ", [10, 10])
        ['John', 'Doe']
    """
    values: List[str] = []
    pos = 0
    for width in widths:
        values.append(line[pos : pos + width].strip())
        pos += width
    return values


def read_parent_child_fixed_width(
    source_path: str,
    type_position: List[int],
    record_types: List[Dict[str, Any]],
    *,
    output_mode: str = "flatten",
) -> ibis.Table:
    """Parse fixed-width file with parent-child record relationships.

    Supports ABABBB, ABBAB patterns where:
    - Parent (A) lines define a master record
    - Child (B) lines belong to the most recent parent
    - Output: flattened rows with parent columns repeated on each child

    Args:
        source_path: Path to the fixed-width file
        type_position: [start, end] character positions for type indicator
        record_types: List of record type definitions with type, role, columns, widths
        output_mode: How to output records - "flatten", "parent_only", or "child_only"

    Returns:
        ibis.Table with parsed records

    Raises:
        ValueError: If parent/child config missing or orphan child found

    Example:
        >>> record_types = [
        ...     {"type": "H", "role": "parent", "columns": ["id", "name"], "widths": [5, 20]},
        ...     {"type": "D", "role": "child", "columns": ["item", "qty"], "widths": [10, 5]},
        ... ]
        >>> table = read_parent_child_fixed_width("data.txt", [0, 1], record_types)
    """
    start_pos, end_pos = type_position

    # Build lookup: type_code -> config
    type_configs = {rt["type"]: rt for rt in record_types}

    # Identify parent and child configs
    parent_config = next(
        (rt for rt in record_types if rt.get("role") == "parent"), None
    )
    child_config = next(
        (rt for rt in record_types if rt.get("role") == "child"), None
    )

    if not parent_config or not child_config:
        raise ValueError(
            "Parent-child pattern requires one 'parent' and one 'child' record type"
        )

    parent_columns = parent_config.get("columns", [])
    parent_widths = parent_config.get("widths", [])
    child_columns = child_config.get("columns", [])
    child_widths = child_config.get("widths", [])

    # Determine output columns based on mode
    if output_mode == "flatten":
        all_columns = parent_columns + child_columns
    elif output_mode == "parent_only":
        all_columns = parent_columns
    elif output_mode == "child_only":
        all_columns = child_columns
    else:
        all_columns = parent_columns + child_columns

    rows: List[List[str]] = []
    current_parent: Optional[List[str]] = None

    with open(source_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip("\n\r")
            if not line:
                continue

            type_code = line[start_pos:end_pos]
            config = type_configs.get(type_code)

            if config is None or config.get("role") == "skip":
                continue

            data_portion = line[end_pos:]

            if config.get("role") == "parent":
                # Parse and store parent values
                current_parent = parse_fixed_width_line(data_portion, parent_widths)

                if output_mode == "parent_only":
                    rows.append(current_parent)

            elif config.get("role") == "child":
                if current_parent is None:
                    raise ValueError(
                        f"Child record at line {line_num} has no parent"
                    )

                child_values = parse_fixed_width_line(data_portion, child_widths)

                if output_mode == "flatten":
                    # Combine parent + child into single row
                    row = current_parent + child_values
                    rows.append(row)
                elif output_mode == "child_only":
                    rows.append(child_values)

    df = pd.DataFrame(rows, columns=all_columns)
    return ibis.memtable(df)
