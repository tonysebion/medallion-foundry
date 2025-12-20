"""Helper functions for pattern combination tests."""

from __future__ import annotations

from typing import List

import ibis

from pipelines.lib.curate import dedupe_latest, build_history, dedupe_exact


def simulate_snapshot_to_scd1(
    batches: List[ibis.Table],
    keys: List[str],
    order_by: str,
) -> ibis.Table:
    """Simulate FULL_SNAPSHOT -> STATE (SCD1) transformation.

    For FULL_SNAPSHOT, we take the latest batch and dedupe.
    """
    # Take last batch (full replacement)
    latest_batch = batches[-1]
    return dedupe_latest(latest_batch, keys, order_by)


def simulate_snapshot_to_scd2(
    batches: List[ibis.Table],
    keys: List[str],
    ts_col: str,
) -> ibis.Table:
    """Simulate FULL_SNAPSHOT -> STATE (SCD2) transformation.

    For FULL_SNAPSHOT with SCD2, we union all batches and build history.
    """
    # Union all batches
    unioned = batches[0]
    for batch in batches[1:]:
        unioned = unioned.union(batch)

    # Dedupe exact (same record in multiple batches)
    deduped = dedupe_exact(unioned)

    # Build SCD2 history
    return build_history(deduped, keys, ts_col)


def simulate_incremental_to_scd1(
    batches: List[ibis.Table],
    keys: List[str],
    order_by: str,
) -> ibis.Table:
    """Simulate INCREMENTAL_APPEND -> STATE (SCD1) transformation.

    Union incremental batches and keep latest per key.
    """
    unioned = batches[0]
    for batch in batches[1:]:
        unioned = unioned.union(batch)

    return dedupe_latest(unioned, keys, order_by)


def simulate_incremental_to_scd2(
    batches: List[ibis.Table],
    keys: List[str],
    ts_col: str,
) -> ibis.Table:
    """Simulate INCREMENTAL_APPEND -> STATE (SCD2) transformation.

    Union incremental batches and build full history.
    """
    unioned = batches[0]
    for batch in batches[1:]:
        unioned = unioned.union(batch)

    return build_history(unioned, keys, ts_col)


def simulate_incremental_to_event(
    batches: List[ibis.Table],
) -> ibis.Table:
    """Simulate INCREMENTAL_APPEND -> EVENT transformation.

    Union all batches and remove exact duplicates.
    """
    unioned = batches[0]
    for batch in batches[1:]:
        unioned = unioned.union(batch)

    return dedupe_exact(unioned)


def simulate_cdc_to_scd1(
    cdc_table: ibis.Table,
    keys: List[str],
    order_by: str,
    op_col: str = "op",
) -> ibis.Table:
    """Simulate CDC -> STATE (SCD1) transformation.

    Apply CDC operations to get current state.
    Keys where the latest operation is DELETE are excluded.
    """
    # First, get the latest operation per key
    latest = dedupe_latest(cdc_table, keys, order_by)

    # Filter to keep only keys where latest operation is not DELETE
    active = latest.filter(latest[op_col] != "D")

    return active


def simulate_cdc_to_scd2(
    cdc_table: ibis.Table,
    keys: List[str],
    ts_col: str,
    op_col: str = "op",
) -> ibis.Table:
    """Simulate CDC -> STATE (SCD2) transformation.

    Build history from CDC events, excluding deletes from final state.
    """
    # Include all operations in history
    return build_history(cdc_table, keys, ts_col)
