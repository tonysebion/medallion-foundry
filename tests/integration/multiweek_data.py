"""Multi-week test data generation for realistic pipeline lifecycle testing.

This module provides data generators for simulating 2-3 weeks of production
pipeline operation, including:
- Schema evolution (v1 → v2 → v3)
- Pattern transitions (full → incremental → full refresh)
- Weekend gaps and late-arriving data
- CDC delete cycles
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


class LoadType(Enum):
    """Type of load for a given day."""

    FULL_SNAPSHOT = "full_snapshot"
    INCREMENTAL = "incremental"
    CDC = "cdc"
    SKIP = "skip"  # No run (weekend, holiday)


class SchemaVersion(Enum):
    """Schema versions for evolution testing."""

    V1 = 1  # Base schema: id, name, value, ts
    V2 = 2  # Added: category column
    V3 = 3  # Added: priority column, removed: value → amount rename


@dataclass
class DayConfig:
    """Configuration for a single day's pipeline run."""

    day_number: int
    run_date: date
    load_type: LoadType
    schema_version: SchemaVersion
    is_weekend: bool
    has_late_data: bool = False
    late_data_for_day: Optional[int] = None
    is_full_refresh: bool = False
    notes: str = ""


@dataclass
class RecordState:
    """Track the state of a single record across days."""

    id: int
    current_values: Dict
    history: List[Tuple[int, Dict]]  # (day_number, values)
    is_deleted: bool = False
    deleted_on_day: Optional[int] = None


class MultiWeekScenario:
    """Generate realistic multi-week data patterns.

    Simulates 3 weeks (21 days) of pipeline operation with:
    - Week 1: Full load Day 1, incremental Days 2-5, weekend
    - Week 2: Incremental with schema change, late data
    - Week 3: Full refresh, more incremental, final schema change
    """

    def __init__(self, start_date: date = date(2025, 1, 6), weeks: int = 3):
        """Initialize scenario.

        Args:
            start_date: First day (should be a Monday for realistic week patterns)
            weeks: Number of weeks to simulate
        """
        self.start_date = start_date
        self.weeks = weeks
        self.days = weeks * 7

        # Track record states across days
        self._record_states: Dict[int, RecordState] = {}
        self._next_id = 1

        # Generate the schedule
        self._schedule = self._generate_schedule()

    def _generate_schedule(self) -> List[DayConfig]:
        """Generate the complete 3-week schedule."""
        schedule = []

        for day in range(1, self.days + 1):
            run_date = self.start_date + timedelta(days=day - 1)
            weekday = run_date.weekday()  # 0=Monday, 6=Sunday
            is_weekend = weekday >= 5

            # Determine schema version
            if day >= 20:
                schema = SchemaVersion.V3
            elif day >= 9:
                schema = SchemaVersion.V2
            else:
                schema = SchemaVersion.V1

            # Determine load type
            if is_weekend:
                load_type = LoadType.SKIP
            elif day == 1:
                load_type = LoadType.FULL_SNAPSHOT
            elif day == 15:
                load_type = LoadType.FULL_SNAPSHOT  # Full refresh
            else:
                load_type = LoadType.INCREMENTAL

            # Late data on day 13 for day 10
            has_late = day == 13
            late_for = 10 if has_late else None

            config = DayConfig(
                day_number=day,
                run_date=run_date,
                load_type=load_type,
                schema_version=schema,
                is_weekend=is_weekend,
                has_late_data=has_late,
                late_data_for_day=late_for,
                is_full_refresh=(day == 15),
                notes=self._get_day_notes(day, schema, load_type, is_weekend),
            )
            schedule.append(config)

        return schedule

    def _get_day_notes(
        self, day: int, schema: SchemaVersion, load_type: LoadType, is_weekend: bool
    ) -> str:
        """Generate human-readable notes for a day."""
        notes = []
        if day == 1:
            notes.append("Initial full load")
        if day == 9:
            notes.append("Schema v1→v2 (add category)")
        if day == 15:
            notes.append("Full refresh reconciliation")
        if day == 20:
            notes.append("Schema v2→v3 (add priority, rename value→amount)")
        if day == 13:
            notes.append("Late data for Day 10")
        if is_weekend:
            notes.append("Weekend - no run")
        return "; ".join(notes) if notes else ""

    @property
    def schedule(self) -> List[DayConfig]:
        """Get the complete schedule."""
        return self._schedule

    def get_run_dates(self) -> List[date]:
        """Get list of dates where pipelines actually run (excludes weekends)."""
        return [d.run_date for d in self._schedule if d.load_type != LoadType.SKIP]

    def get_schema_columns(self, version: SchemaVersion) -> List[str]:
        """Get column names for a schema version."""
        if version == SchemaVersion.V1:
            return ["id", "name", "value", "ts"]
        elif version == SchemaVersion.V2:
            return ["id", "name", "value", "category", "ts"]
        else:  # V3
            return ["id", "name", "amount", "category", "priority", "ts"]

    def generate_bronze_data(
        self, day: int, pattern: str = "incremental"
    ) -> pd.DataFrame:
        """Generate Bronze layer data for a specific day.

        Args:
            day: Day number (1-21)
            pattern: "snapshot", "incremental", or "cdc"

        Returns:
            DataFrame with appropriate data for the day
        """
        config = self._schedule[day - 1]

        if config.load_type == LoadType.SKIP:
            return pd.DataFrame()

        schema_cols = self.get_schema_columns(config.schema_version)

        if pattern == "snapshot" or config.is_full_refresh:
            return self._generate_snapshot_data(day, schema_cols)
        elif pattern == "incremental":
            return self._generate_incremental_data(day, schema_cols)
        elif pattern == "cdc":
            return self._generate_cdc_data(day, schema_cols)
        else:
            raise ValueError(f"Unknown pattern: {pattern}")

    def _generate_snapshot_data(self, day: int, columns: List[str]) -> pd.DataFrame:
        """Generate full snapshot data for a day."""
        config = self._schedule[day - 1]
        ts = datetime.combine(config.run_date, datetime.min.time().replace(hour=10))

        # Build current state of all active records
        records = []
        for rec in self._get_active_records(day):
            row = self._record_to_row(rec, columns, ts)
            records.append(row)

        # Add new records for this day
        new_records = self._generate_new_records(day, columns, ts)
        records.extend(new_records)

        return pd.DataFrame(records)

    def _generate_incremental_data(self, day: int, columns: List[str]) -> pd.DataFrame:
        """Generate incremental data for a day (only changes)."""
        config = self._schedule[day - 1]
        ts = datetime.combine(config.run_date, datetime.min.time().replace(hour=10))

        records = []

        # Day 1 is always full
        if day == 1:
            for i in range(5):
                rec = self._create_record(day, columns, ts)
                records.append(self._record_to_row(rec, columns, ts))
            return pd.DataFrame(records)

        # Generate updates for some existing records
        update_ids = self._get_update_ids(day)
        for rec_id in update_ids:
            if rec_id in self._record_states:
                rec = self._record_states[rec_id]
                self._update_record(rec, day, columns, ts)
                records.append(self._record_to_row(rec, columns, ts))

        # Generate new records
        new_count = self._get_new_record_count(day)
        for _ in range(new_count):
            rec = self._create_record(day, columns, ts)
            records.append(self._record_to_row(rec, columns, ts))

        # Late data handling
        if config.has_late_data and config.late_data_for_day:
            late_ts = datetime.combine(
                self._schedule[config.late_data_for_day - 1].run_date,
                datetime.min.time().replace(hour=10),
            )
            late_rec = self._create_record(config.late_data_for_day, columns, late_ts)
            records.append(self._record_to_row(late_rec, columns, late_ts))

        return pd.DataFrame(records)

    def _generate_cdc_data(self, day: int, columns: List[str]) -> pd.DataFrame:
        """Generate CDC data for a day (with I/U/D operations)."""
        config = self._schedule[day - 1]
        ts = datetime.combine(config.run_date, datetime.min.time().replace(hour=10))

        records = []

        # Day 1: All inserts
        if day == 1:
            for i in range(5):
                rec = self._create_record(day, columns, ts)
                row = self._record_to_row(rec, columns, ts)
                row["op"] = "I"
                records.append(row)
            return pd.DataFrame(records)

        # Generate updates (op=U)
        update_ids = self._get_update_ids(day)
        for rec_id in update_ids:
            if rec_id in self._record_states:
                rec = self._record_states[rec_id]
                self._update_record(rec, day, columns, ts)
                row = self._record_to_row(rec, columns, ts)
                row["op"] = "U"
                records.append(row)

        # Generate inserts (op=I)
        new_count = self._get_new_record_count(day)
        for _ in range(new_count):
            rec = self._create_record(day, columns, ts)
            row = self._record_to_row(rec, columns, ts)
            row["op"] = "I"
            records.append(row)

        # Generate deletes (op=D) - on specific days
        if day in [10, 11, 17]:  # Delete days
            delete_ids = self._get_delete_ids(day)
            for rec_id in delete_ids:
                if (
                    rec_id in self._record_states
                    and not self._record_states[rec_id].is_deleted
                ):
                    rec = self._record_states[rec_id]
                    row = self._record_to_row(rec, columns, ts)
                    row["op"] = "D"
                    records.append(row)
                    rec.is_deleted = True
                    rec.deleted_on_day = day

        return pd.DataFrame(records)

    def _create_record(self, day: int, columns: List[str], ts: datetime) -> RecordState:
        """Create a new record and track it."""
        rec_id = self._next_id
        self._next_id += 1

        values = {
            "id": rec_id,
            "name": f"Entity_{rec_id}",
            "ts": ts,
        }

        if "value" in columns:
            values["value"] = rec_id * 100
        if "amount" in columns:
            values["amount"] = rec_id * 100
        if "category" in columns:
            values["category"] = ["A", "B", "C"][rec_id % 3]
        if "priority" in columns:
            values["priority"] = ["low", "medium", "high"][rec_id % 3]

        rec = RecordState(
            id=rec_id,
            current_values=values,
            history=[(day, values.copy())],
        )
        self._record_states[rec_id] = rec
        return rec

    def _update_record(
        self, rec: RecordState, day: int, columns: List[str], ts: datetime
    ) -> None:
        """Update an existing record."""
        rec.current_values["ts"] = ts

        # Update value/amount
        if "value" in columns:
            rec.current_values["value"] = (
                rec.current_values.get("value", rec.id * 100) + 10
            )
        if "amount" in columns:
            rec.current_values["amount"] = (
                rec.current_values.get("amount", rec.id * 100) + 10
            )

        rec.history.append((day, rec.current_values.copy()))

    def _record_to_row(
        self, rec: RecordState, columns: List[str], ts: datetime
    ) -> Dict:
        """Convert a RecordState to a row dict."""
        row = {}
        for col in columns:
            if col in rec.current_values:
                row[col] = rec.current_values[col]
            elif col == "ts":
                row[col] = ts
            elif col == "category":
                row[col] = ["A", "B", "C"][rec.id % 3]
            elif col == "priority":
                row[col] = ["low", "medium", "high"][rec.id % 3]
            else:
                row[col] = None
        return row

    def _get_active_records(self, day: int) -> List[RecordState]:
        """Get all records that are active (not deleted) as of a day."""
        return [
            rec
            for rec in self._record_states.values()
            if not rec.is_deleted or (rec.deleted_on_day and rec.deleted_on_day > day)
        ]

    def _get_update_ids(self, day: int) -> Set[int]:
        """Determine which record IDs to update on a given day."""
        # Update different records on different days for realistic churn
        existing_ids = list(self._record_states.keys())
        if not existing_ids:
            return set()

        # Update 1-2 records per day
        update_count = min(2, len(existing_ids))
        start_idx = (day - 1) % len(existing_ids)
        return set(existing_ids[start_idx : start_idx + update_count])

    def _get_new_record_count(self, day: int) -> int:
        """Determine how many new records to create on a given day."""
        # More inserts early, fewer later
        if day <= 5:
            return 2
        elif day <= 10:
            return 1
        else:
            return 1 if day % 3 == 0 else 0

    def _get_delete_ids(self, day: int) -> Set[int]:
        """Determine which record IDs to delete on a given day."""
        existing_ids = [
            rec.id for rec in self._record_states.values() if not rec.is_deleted
        ]
        if not existing_ids or len(existing_ids) <= 3:
            return set()

        # Delete 1 record on delete days
        return {existing_ids[(day * 7) % len(existing_ids)]}

    def expected_silver_state(
        self, day: int, pattern: str, history_mode: str = "current_only"
    ) -> pd.DataFrame:
        """Get expected Silver state after processing through day N.

        Args:
            day: Day number to get state for
            pattern: Load pattern used
            history_mode: "current_only" (SCD1) or "full_history" (SCD2)

        Returns:
            Expected Silver DataFrame
        """
        config = self._schedule[day - 1]
        columns = self.get_schema_columns(config.schema_version)

        if history_mode == "current_only":
            # SCD1: Just current values of non-deleted records
            records = []
            for rec in self._record_states.values():
                if not rec.is_deleted or (
                    rec.deleted_on_day and rec.deleted_on_day > day
                ):
                    row = self._record_to_row(
                        rec, columns, rec.current_values.get("ts")
                    )
                    records.append(row)
            return pd.DataFrame(records)
        else:
            # SCD2: All historical versions
            records = []
            for rec in self._record_states.values():
                for hist_day, values in rec.history:
                    if hist_day <= day:
                        row = {col: values.get(col) for col in columns}
                        row["effective_from"] = values.get("ts")
                        records.append(row)
            return pd.DataFrame(records)

    def get_expected_record_count(self, day: int, include_deleted: bool = False) -> int:
        """Get expected number of records as of day N."""
        count = 0
        for rec in self._record_states.values():
            if include_deleted:
                count += 1
            elif not rec.is_deleted:
                count += 1
            elif rec.deleted_on_day and rec.deleted_on_day > day:
                count += 1
        return count


class CDCDeleteCycleScenario(MultiWeekScenario):
    """Scenario focused on CDC delete patterns over 3 weeks.

    Week 1: Steady inserts + updates
    Week 2: Mass deletes (cleanup simulation)
    Week 3: Recovery with new inserts
    """

    def _generate_cdc_data(self, day: int, columns: List[str]) -> pd.DataFrame:
        """Override to create delete-heavy patterns."""
        config = self._schedule[day - 1]
        ts = datetime.combine(config.run_date, datetime.min.time().replace(hour=10))

        records = []

        # Week 1 (days 1-7): Steady inserts
        if day <= 5:
            for _ in range(3 if day == 1 else 2):
                rec = self._create_record(day, columns, ts)
                row = self._record_to_row(rec, columns, ts)
                row["op"] = "I"
                records.append(row)

        # Week 2 (days 8-14): Mass deletes
        elif 8 <= day <= 12:
            # Delete multiple records
            active_ids = [
                r.id for r in self._record_states.values() if not r.is_deleted
            ]
            delete_count = min(2, len(active_ids))
            for i in range(delete_count):
                rec = self._record_states[active_ids[i]]
                row = self._record_to_row(rec, columns, ts)
                row["op"] = "D"
                records.append(row)
                rec.is_deleted = True
                rec.deleted_on_day = day

        # Week 3 (days 15-21): Recovery inserts
        elif day >= 15:
            for _ in range(2):
                rec = self._create_record(day, columns, ts)
                row = self._record_to_row(rec, columns, ts)
                row["op"] = "I"
                records.append(row)

        return pd.DataFrame(records) if records else pd.DataFrame()


class FailureRecoveryScenario(MultiWeekScenario):
    """Scenario for testing failure recovery.

    Days 1-3: Normal operation
    Days 4-5: Silver fails (Bronze succeeds)
    Day 6: Recovery run
    Days 7-10: Normal operation
    """

    def __init__(self, start_date: date = date(2025, 1, 6)):
        super().__init__(start_date, weeks=2)
        self.failed_days = {4, 5}  # Days where Silver "fails"
        self.recovery_day = 6

    def should_run_silver(self, day: int) -> bool:
        """Determine if Silver should run on this day (simulating failures)."""
        return day not in self.failed_days

    def is_recovery_run(self, day: int) -> bool:
        """Check if this day is a recovery run that processes backlog."""
        return day == self.recovery_day

    def get_backlog_days(self, day: int) -> List[int]:
        """Get list of days that need to be processed in recovery."""
        if day == self.recovery_day:
            return sorted(self.failed_days)
        return []
