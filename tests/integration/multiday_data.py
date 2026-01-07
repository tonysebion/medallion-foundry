"""Multi-day test data generators for pattern combination testing.

This module provides functions to generate test data for multi-day evolution
scenarios, simulating how data changes over time in Bronze and Silver layers.

Data Evolution Scenarios:
- Day 1: Initial load (5 records, IDs 1-5)
- Day 2: Updates (IDs 1, 3 modified) + New record (ID 6)
- Day 3: Deletes (ID 2) + Updates (ID 4)
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Literal

import pandas as pd


# =============================================================================
# Constants
# =============================================================================

# Base dates for multi-day testing
DAY1_DATE = datetime(2025, 1, 10)
DAY2_DATE = datetime(2025, 1, 11)
DAY3_DATE = datetime(2025, 1, 12)

# CDC operation codes
OP_INSERT = "I"
OP_UPDATE = "U"
OP_DELETE = "D"


# =============================================================================
# Snapshot Data Generators
# =============================================================================


def generate_snapshot_day1(entity: str = "customers") -> pd.DataFrame:
    """Generate Day 1 snapshot data - initial 5 records.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 5 initial records
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v1", "value": 100, "status": "active", "ts": DAY1_DATE},
        {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE},
        {"id": 3, "name": f"{entity}_3_v1", "value": 300, "status": "active", "ts": DAY1_DATE},
        {"id": 4, "name": f"{entity}_4_v1", "value": 400, "status": "active", "ts": DAY1_DATE},
        {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
    ])


def generate_snapshot_day2(entity: str = "customers") -> pd.DataFrame:
    """Generate Day 2 snapshot data - updates to IDs 1,3 + new ID 6.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 6 records (full snapshot for day 2)
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},  # Updated
        {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE},  # Unchanged
        {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},  # Updated
        {"id": 4, "name": f"{entity}_4_v1", "value": 400, "status": "active", "ts": DAY1_DATE},  # Unchanged
        {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},  # Unchanged
        {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},  # New
    ])


def generate_snapshot_day3(entity: str = "customers") -> pd.DataFrame:
    """Generate Day 3 snapshot data - ID 2 deleted, ID 4 updated.

    Note: For snapshot pattern, deletes are implicit (record not in snapshot).

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 5 records (ID 2 no longer present)
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},  # Unchanged
        # ID 2 is deleted (not in snapshot)
        {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},  # Unchanged
        {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},  # Updated
        {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},  # Unchanged
        {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},  # Unchanged
    ])


def generate_all_snapshot_days(entity: str = "customers") -> List[pd.DataFrame]:
    """Generate snapshot data for all 3 days.

    Args:
        entity: Entity name for test identification

    Returns:
        List of 3 DataFrames, one per day
    """
    return [
        generate_snapshot_day1(entity),
        generate_snapshot_day2(entity),
        generate_snapshot_day3(entity),
    ]


# =============================================================================
# Incremental Data Generators
# =============================================================================


def generate_incremental_day1(entity: str = "events") -> pd.DataFrame:
    """Generate Day 1 incremental data - initial 5 records.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 5 initial records
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v1", "value": 100, "status": "active", "ts": DAY1_DATE},
        {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE},
        {"id": 3, "name": f"{entity}_3_v1", "value": 300, "status": "active", "ts": DAY1_DATE},
        {"id": 4, "name": f"{entity}_4_v1", "value": 400, "status": "active", "ts": DAY1_DATE},
        {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
    ])


def generate_incremental_day2(entity: str = "events") -> pd.DataFrame:
    """Generate Day 2 incremental data - only new/changed records.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 3 records (2 updates + 1 new)
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},  # Update
        {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},  # Update
        {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},  # New
    ])


def generate_incremental_day3(entity: str = "events") -> pd.DataFrame:
    """Generate Day 3 incremental data - only new/changed records.

    Note: Incremental pattern cannot express deletes directly.
    Deletes would require CDC or a separate delete marker.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 1 record (1 update)
    """
    return pd.DataFrame([
        {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},  # Update
    ])


def generate_all_incremental_days(entity: str = "events") -> List[pd.DataFrame]:
    """Generate incremental data for all 3 days.

    Args:
        entity: Entity name for test identification

    Returns:
        List of 3 DataFrames, one per day
    """
    return [
        generate_incremental_day1(entity),
        generate_incremental_day2(entity),
        generate_incremental_day3(entity),
    ]


# =============================================================================
# CDC Data Generators
# =============================================================================


def generate_cdc_day1(entity: str = "accounts") -> pd.DataFrame:
    """Generate Day 1 CDC data - all inserts.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 5 insert operations
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v1", "value": 100, "status": "active", "ts": DAY1_DATE, "op": OP_INSERT},
        {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE, "op": OP_INSERT},
        {"id": 3, "name": f"{entity}_3_v1", "value": 300, "status": "active", "ts": DAY1_DATE, "op": OP_INSERT},
        {"id": 4, "name": f"{entity}_4_v1", "value": 400, "status": "active", "ts": DAY1_DATE, "op": OP_INSERT},
        {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE, "op": OP_INSERT},
    ])


def generate_cdc_day2(entity: str = "accounts") -> pd.DataFrame:
    """Generate Day 2 CDC data - updates and new insert.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 2 update operations + 1 insert
    """
    return pd.DataFrame([
        {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE, "op": OP_UPDATE},
        {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE, "op": OP_UPDATE},
        {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE, "op": OP_INSERT},
    ])


def generate_cdc_day3(entity: str = "accounts") -> pd.DataFrame:
    """Generate Day 3 CDC data - delete and update.

    Args:
        entity: Entity name for test identification

    Returns:
        DataFrame with 1 delete + 1 update operation
    """
    return pd.DataFrame([
        {"id": 2, "name": f"{entity}_2_deleted", "value": 0, "status": "deleted", "ts": DAY3_DATE, "op": OP_DELETE},
        {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE, "op": OP_UPDATE},
    ])


def generate_all_cdc_days(entity: str = "accounts") -> List[pd.DataFrame]:
    """Generate CDC data for all 3 days.

    Args:
        entity: Entity name for test identification

    Returns:
        List of 3 DataFrames, one per day
    """
    return [
        generate_cdc_day1(entity),
        generate_cdc_day2(entity),
        generate_cdc_day3(entity),
    ]


def generate_cdc_combined(entity: str = "accounts") -> pd.DataFrame:
    """Generate all CDC data as a single combined DataFrame.

    Useful for testing full CDC processing in one pass.

    Args:
        entity: Entity name for test identification

    Returns:
        Single DataFrame with all 3 days of CDC operations
    """
    days = generate_all_cdc_days(entity)
    return pd.concat(days, ignore_index=True)


# =============================================================================
# Expected Outcomes Generators
# =============================================================================


def expected_scd1_after_day3(
    pattern: Literal["snapshot", "incremental", "cdc"],
    delete_mode: Literal["ignore", "tombstone", "hard_delete"] = "ignore",
    entity: str = "test",
) -> pd.DataFrame:
    """Generate expected SCD1 state after Day 3 processing.

    Args:
        pattern: Bronze pattern type
        delete_mode: How deletes are handled (CDC only)
        entity: Entity name for test identification

    Returns:
        DataFrame with expected final SCD1 state
    """
    if pattern == "snapshot":
        # Snapshot SCD1: Latest snapshot deduplicated (ID 2 implicitly deleted)
        return pd.DataFrame([
            {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},
            {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},
            {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},
            {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
            {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},
        ])

    elif pattern == "incremental":
        # Incremental SCD1: Union all batches, dedupe by id keeping latest ts
        return pd.DataFrame([
            {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},
            {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE},  # No delete
            {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},
            {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},
            {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
            {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},
        ])

    elif pattern == "cdc":
        if delete_mode == "hard_delete":
            # CDC with hard delete: ID 2 removed
            return pd.DataFrame([
                {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},
                # ID 2 hard deleted
                {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},
                {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},
                {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
                {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},
            ])
        elif delete_mode == "tombstone":
            # CDC with tombstone: ID 2 marked as deleted
            return pd.DataFrame([
                {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE, "_deleted": False},
                {"id": 2, "name": f"{entity}_2_deleted", "value": 0, "status": "deleted", "ts": DAY3_DATE, "_deleted": True},
                {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE, "_deleted": False},
                {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE, "_deleted": False},
                {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE, "_deleted": False},
                {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE, "_deleted": False},
            ])
        else:  # ignore
            # CDC with ignore: ID 2 delete ignored, record remains
            return pd.DataFrame([
                {"id": 1, "name": f"{entity}_1_v2", "value": 150, "status": "active", "ts": DAY2_DATE},
                {"id": 2, "name": f"{entity}_2_v1", "value": 200, "status": "active", "ts": DAY1_DATE},  # Delete ignored
                {"id": 3, "name": f"{entity}_3_v2", "value": 350, "status": "pending", "ts": DAY2_DATE},
                {"id": 4, "name": f"{entity}_4_v2", "value": 450, "status": "closed", "ts": DAY3_DATE},
                {"id": 5, "name": f"{entity}_5_v1", "value": 500, "status": "active", "ts": DAY1_DATE},
                {"id": 6, "name": f"{entity}_6_v1", "value": 600, "status": "active", "ts": DAY2_DATE},
            ])

    raise ValueError(f"Unknown pattern: {pattern}")


def expected_event_count_after_day3(pattern: Literal["snapshot", "incremental", "cdc"]) -> int:
    """Get expected total event count after Day 3 for event entity.

    For event entities, all records are preserved as immutable events.

    Args:
        pattern: Bronze pattern type

    Returns:
        Expected total record count
    """
    if pattern == "snapshot":
        # Day 1: 5, Day 2: 6, Day 3: 5 = 16 total (with deduplication for exact matches)
        # After exact dedupe: depends on how many are truly unique
        # Assuming no exact duplicates: 5 + 6 + 5 = 16
        return 16

    elif pattern == "incremental":
        # Day 1: 5, Day 2: 3, Day 3: 1 = 9 total
        return 9

    elif pattern == "cdc":
        # Day 1: 5 (I), Day 2: 3 (U,U,I), Day 3: 2 (D,U) = 10 total
        return 10

    raise ValueError(f"Unknown pattern: {pattern}")


# =============================================================================
# Utility Functions
# =============================================================================


def save_test_data_to_csv(
    df: pd.DataFrame,
    path: str,
    include_header: bool = True,
) -> None:
    """Save DataFrame to CSV for use as Bronze source.

    Args:
        df: DataFrame to save
        path: Output file path
        include_header: Whether to include column headers
    """
    df.to_csv(path, index=False, header=include_header)


def get_day_date(day: int) -> datetime:
    """Get the datetime for a specific test day.

    Args:
        day: Day number (1, 2, or 3)

    Returns:
        datetime for that day
    """
    if day == 1:
        return DAY1_DATE
    elif day == 2:
        return DAY2_DATE
    elif day == 3:
        return DAY3_DATE
    else:
        raise ValueError(f"Invalid day: {day}. Must be 1, 2, or 3")


def get_day_date_str(day: int) -> str:
    """Get the date string for a specific test day (YYYY-MM-DD format).

    Args:
        day: Day number (1, 2, or 3)

    Returns:
        Date string in YYYY-MM-DD format
    """
    return get_day_date(day).strftime("%Y-%m-%d")
