"""Low-level curation helpers for Silver layer.

These functions provide the Ibis operations for common curation patterns.
They are used internally by SilverEntity but can also be used directly
for advanced use cases.
"""

from __future__ import annotations

from typing import List

import ibis

__all__ = [
    "build_history",
    "coalesce_columns",
    "dedupe_earliest",
    "dedupe_exact",
    "dedupe_latest",
    "filter_incremental",
    "rank_by_keys",
    "union_dedupe",
]


def dedupe_latest(t: ibis.Table, keys: List[str], order_by: str) -> ibis.Table:
    """Keep only the latest record per natural key combination.

    This is SCD Type 1 behavior - no history preserved.
    The most recent record (by order_by column) wins.

    Args:
        t: Input table
        keys: List of columns that form the natural key
        order_by: Column to order by (most recent value kept)

    Returns:
        Table with duplicates removed, keeping latest record per key

    Example:
        >>> t = con.read_parquet("bronze/customers/*.parquet")
        >>> deduped = dedupe_latest(t, ["customer_id"], "updated_at")
    """
    original_cols = t.columns
    window = ibis.window(group_by=keys, order_by=ibis.desc(order_by))
    return (
        t.mutate(_rn=ibis.row_number().over(window))
        .filter(lambda tbl: tbl._rn == 0)  # row_number() starts at 0 in DuckDB
        .select(*original_cols)  # Use select to drop _rn, works better with lambda filter
    )


def dedupe_earliest(t: ibis.Table, keys: List[str], order_by: str) -> ibis.Table:
    """Keep only the earliest record per natural key combination.

    Useful for keeping the first occurrence of an event.

    Args:
        t: Input table
        keys: List of columns that form the natural key
        order_by: Column to order by (earliest value kept)

    Returns:
        Table with duplicates removed, keeping earliest record per key
    """
    original_cols = t.columns
    window = ibis.window(group_by=keys, order_by=order_by)
    return (
        t.mutate(_rn=ibis.row_number().over(window))
        .filter(lambda tbl: tbl._rn == 0)  # row_number() starts at 0 in DuckDB
        .select(*original_cols)  # Use select to drop _rn, works better with lambda filter
    )


def build_history(
    t: ibis.Table,
    keys: List[str],
    ts_col: str,
    *,
    effective_from_name: str = "effective_from",
    effective_to_name: str = "effective_to",
    is_current_name: str = "is_current",
) -> ibis.Table:
    """Build SCD Type 2 history with effective dates.

    Adds temporal columns to track when each version was valid:
    - effective_from: When this version became active
    - effective_to: When this version was superseded (NULL if current)
    - is_current: 1 if this is the current version, 0 otherwise

    Args:
        t: Input table
        keys: List of columns that form the natural key
        ts_col: Timestamp column indicating when the change occurred
        effective_from_name: Name for the effective_from column
        effective_to_name: Name for the effective_to column
        is_current_name: Name for the is_current flag column

    Returns:
        Table with SCD2 temporal columns added

    Example:
        >>> t = con.read_parquet("bronze/products/*.parquet")
        >>> with_history = build_history(t, ["product_id"], "updated_at")
    """
    window = ibis.window(group_by=keys, order_by=ts_col)

    # Get the next record's timestamp (will be NULL for latest record)
    next_ts = t[ts_col].lead().over(window)

    return t.mutate(
        **{
            effective_from_name: t[ts_col],
            effective_to_name: next_ts,
            is_current_name: next_ts.isnull().cast("int"),
        }
    )


def dedupe_exact(t: ibis.Table) -> ibis.Table:
    """Remove exact duplicate rows.

    For event logs where we want to eliminate truly identical records
    but preserve records that differ in any column.

    Args:
        t: Input table

    Returns:
        Table with exact duplicate rows removed
    """
    return t.distinct()


def filter_incremental(
    t: ibis.Table,
    watermark_col: str,
    last_watermark: str,
) -> ibis.Table:
    """Filter to records after last watermark.

    Used for incremental loading patterns where we only want new records.

    Args:
        t: Input table
        watermark_col: Column to compare against watermark
        last_watermark: Last processed watermark value

    Returns:
        Table filtered to records after the watermark
    """
    return t.filter(t[watermark_col] > last_watermark)


def rank_by_keys(
    t: ibis.Table,
    keys: List[str],
    order_by: str,
    *,
    descending: bool = True,
    rank_column: str = "_rank",
) -> ibis.Table:
    """Add a rank column partitioned by keys.

    Useful for selecting top-N per group or other ranking operations.

    Args:
        t: Input table
        keys: List of columns to partition by
        order_by: Column to order by within each partition
        descending: If True, rank 1 is highest value; if False, lowest
        rank_column: Name for the rank column

    Returns:
        Table with rank column added
    """
    order_expr = ibis.desc(order_by) if descending else order_by
    window = ibis.window(group_by=keys, order_by=order_expr)
    return t.mutate(**{rank_column: ibis.row_number().over(window)})


def coalesce_columns(t: ibis.Table, column: str, *fallbacks: str) -> ibis.Table:
    """Create a coalesced column from multiple source columns.

    Useful for handling schema evolution where a column may have
    different names across sources.

    Args:
        t: Input table
        column: Primary column name
        *fallbacks: Additional column names to try if primary is null

    Returns:
        Table with coalesced values in the primary column
    """
    cols = [t[column]] + [t[f] for f in fallbacks if f in t.columns]
    return t.mutate(**{column: ibis.coalesce(*cols)})


def union_dedupe(
    tables: List[ibis.Table],
    keys: List[str],
    order_by: str,
) -> ibis.Table:
    """Union multiple tables and dedupe by keys.

    Useful for combining multiple Bronze partitions and getting
    the latest version of each record.

    Args:
        tables: List of tables to union
        keys: Natural key columns for deduplication
        order_by: Column to order by (latest wins)

    Returns:
        Unioned and deduplicated table
    """
    if not tables:
        raise ValueError("At least one table required")

    unioned = tables[0]
    for t in tables[1:]:
        unioned = unioned.union(t)

    return dedupe_latest(unioned, keys, order_by)
