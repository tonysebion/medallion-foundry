"""Data generators for irregular CDC pattern testing.

These generators create realistic but irregular data patterns for testing
CDC robustness over extended periods:
- Out-of-order batch arrivals
- Long gaps between delete and re-insert
- Duplicate batch processing
- Irregular schedules with random skips and backfills
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
import random

import pandas as pd


class CDCOperation(Enum):
    """CDC operation types."""
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


@dataclass
class CDCEvent:
    """A single CDC event."""
    id: int
    name: str
    value: int
    op: str
    ts: datetime
    day: int  # Logical day number


@dataclass
class EntityState:
    """Track the state of an entity across time."""
    id: int
    current_values: Dict
    is_deleted: bool = False
    deleted_on_day: Optional[int] = None
    created_on_day: int = 1
    version: int = 1


class OutOfOrderScenario:
    """Generate CDC data where batches arrive out of order.

    21-day scenario where data arrives in non-sequential order:
    - Days 1-5: Normal order
    - Days 6-10: Day 10 arrives first, then 6, 7, 8, 9
    - Days 11-15: Random order
    - Days 16-21: Day 18 data arrives on Day 16, actual Day 16 arrives on Day 18
    """

    def __init__(self, start_date: date = date(2025, 1, 6), seed: int = 42):
        self.start_date = start_date
        self.seed = seed
        random.seed(seed)

        self._entities: Dict[int, EntityState] = {}
        self._next_id = 1
        self._events_by_day: Dict[int, List[CDCEvent]] = {}

        # Generate all events for 21 days
        self._generate_all_events()

        # Define arrival order (which logical day's data arrives on each physical day)
        self.arrival_order = self._define_arrival_order()

    def _define_arrival_order(self) -> Dict[int, int]:
        """Define which logical day's data arrives on each physical day.

        Returns:
            Dict mapping physical_day -> logical_day (whose data arrives)
        """
        order = {}

        # Days 1-5: Normal order
        for d in range(1, 6):
            order[d] = d

        # Days 6-10: Day 10 arrives first, then 6, 7, 8, 9
        order[6] = 10  # Day 10's data arrives on physical day 6
        order[7] = 6
        order[8] = 7
        order[9] = 8
        order[10] = 9

        # Days 11-15: Random order
        logical_days = [11, 12, 13, 14, 15]
        random.shuffle(logical_days)
        for phys_day, log_day in enumerate(logical_days, start=11):
            order[phys_day] = log_day

        # Days 16-21: Swap Day 16 and 18
        order[16] = 18
        order[17] = 17
        order[18] = 16
        order[19] = 19
        order[20] = 20
        order[21] = 21

        return order

    def _generate_all_events(self):
        """Generate CDC events for all 21 logical days."""
        for day in range(1, 22):
            self._events_by_day[day] = self._generate_day_events(day)

    def _generate_day_events(self, day: int) -> List[CDCEvent]:
        """Generate CDC events for a single logical day."""
        events = []
        ts_base = datetime.combine(
            self.start_date + timedelta(days=day - 1),
            datetime.min.time().replace(hour=10)
        )

        if day == 1:
            # Day 1: Initial inserts (5 records)
            for _ in range(5):
                entity = self._create_entity(day)
                events.append(CDCEvent(
                    id=entity.id,
                    name=entity.current_values["name"],
                    value=entity.current_values["value"],
                    op="I",
                    ts=ts_base + timedelta(minutes=entity.id),
                    day=day,
                ))
        else:
            # Generate updates for some existing entities
            active_ids = [e.id for e in self._entities.values() if not e.is_deleted]
            if active_ids:
                update_count = min(2, len(active_ids))
                update_ids = random.sample(active_ids, update_count)
                for eid in update_ids:
                    entity = self._entities[eid]
                    entity.version += 1
                    entity.current_values["value"] += 10
                    entity.current_values["name"] = f"Entity_{eid}_v{entity.version}"
                    events.append(CDCEvent(
                        id=eid,
                        name=entity.current_values["name"],
                        value=entity.current_values["value"],
                        op="U",
                        ts=ts_base + timedelta(minutes=eid),
                        day=day,
                    ))

            # New inserts on specific days
            if day in [3, 6, 9, 12, 15, 18, 21]:
                entity = self._create_entity(day)
                events.append(CDCEvent(
                    id=entity.id,
                    name=entity.current_values["name"],
                    value=entity.current_values["value"],
                    op="I",
                    ts=ts_base + timedelta(minutes=entity.id),
                    day=day,
                ))

            # Deletes on specific days
            if day in [7, 14, 20]:
                active_ids = [e.id for e in self._entities.values() if not e.is_deleted]
                if active_ids:
                    delete_id = random.choice(active_ids)
                    entity = self._entities[delete_id]
                    entity.is_deleted = True
                    entity.deleted_on_day = day
                    events.append(CDCEvent(
                        id=delete_id,
                        name=entity.current_values["name"],
                        value=entity.current_values["value"],
                        op="D",
                        ts=ts_base + timedelta(minutes=delete_id + 30),
                        day=day,
                    ))

        return events

    def _create_entity(self, day: int) -> EntityState:
        """Create a new entity."""
        eid = self._next_id
        self._next_id += 1
        entity = EntityState(
            id=eid,
            current_values={
                "name": f"Entity_{eid}_v1",
                "value": eid * 100,
            },
            created_on_day=day,
        )
        self._entities[eid] = entity
        return entity

    def get_data_for_physical_day(self, physical_day: int) -> pd.DataFrame:
        """Get CDC data that arrives on a physical day.

        This returns the logical day's data based on the arrival order.
        """
        logical_day = self.arrival_order.get(physical_day, physical_day)
        events = self._events_by_day.get(logical_day, [])

        if not events:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])

        return pd.DataFrame([
            {
                "id": e.id,
                "name": e.name,
                "value": e.value,
                "op": e.op,
                "ts": e.ts.isoformat(),
            }
            for e in events
        ])

    def get_run_date(self, physical_day: int) -> str:
        """Get the run date string for a physical day."""
        return (self.start_date + timedelta(days=physical_day - 1)).isoformat()

    def get_expected_final_state(self) -> Dict[int, Dict]:
        """Get expected final state after all 21 days processed.

        Returns dict of id -> {name, value, is_deleted}
        """
        # Reset and replay all events in logical order
        final_state = {}
        for day in range(1, 22):
            for event in self._events_by_day[day]:
                if event.op == "D":
                    if event.id in final_state:
                        final_state[event.id]["is_deleted"] = True
                else:
                    final_state[event.id] = {
                        "name": event.name,
                        "value": event.value,
                        "is_deleted": False,
                    }
        return final_state


class LongGapResurrectionScenario:
    """Generate CDC data with long gaps between delete and re-insert.

    21-day scenario:
    - Day 1: Insert IDs 1-5
    - Day 3: Delete ID 2
    - Day 5: Delete ID 4
    - Day 18: Re-insert ID 2 with new values
    - Day 20: Re-insert ID 4 with new values
    - Various updates throughout
    """

    def __init__(self, start_date: date = date(2025, 1, 6)):
        self.start_date = start_date
        self._events_by_day: Dict[int, List[Dict]] = {}
        self._generate_all_events()

    def _generate_all_events(self):
        """Generate all CDC events for the 21-day scenario."""
        for day in range(1, 22):
            self._events_by_day[day] = []
            ts_base = datetime.combine(
                self.start_date + timedelta(days=day - 1),
                datetime.min.time().replace(hour=10)
            )

            if day == 1:
                # Initial inserts
                for i in range(1, 6):
                    self._events_by_day[day].append({
                        "id": i,
                        "name": f"Entity_{i}_v1",
                        "value": i * 100,
                        "op": "I",
                        "ts": (ts_base + timedelta(minutes=i)).isoformat(),
                    })

            elif day == 3:
                # Delete ID 2
                self._events_by_day[day].append({
                    "id": 2,
                    "name": "Entity_2_v1",
                    "value": 200,
                    "op": "D",
                    "ts": ts_base.isoformat(),
                })

            elif day == 5:
                # Delete ID 4
                self._events_by_day[day].append({
                    "id": 4,
                    "name": "Entity_4_v1",
                    "value": 400,
                    "op": "D",
                    "ts": ts_base.isoformat(),
                })
                # Update ID 1
                self._events_by_day[day].append({
                    "id": 1,
                    "name": "Entity_1_v2",
                    "value": 150,
                    "op": "U",
                    "ts": (ts_base + timedelta(minutes=1)).isoformat(),
                })

            elif day == 10:
                # Update ID 3
                self._events_by_day[day].append({
                    "id": 3,
                    "name": "Entity_3_v2",
                    "value": 350,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

            elif day == 18:
                # Re-insert ID 2 after 15-day gap!
                self._events_by_day[day].append({
                    "id": 2,
                    "name": "Entity_2_RESURRECTED",
                    "value": 2000,  # Completely new value
                    "op": "I",
                    "ts": ts_base.isoformat(),
                })

            elif day == 20:
                # Re-insert ID 4 after 15-day gap!
                self._events_by_day[day].append({
                    "id": 4,
                    "name": "Entity_4_RESURRECTED",
                    "value": 4000,
                    "op": "I",
                    "ts": ts_base.isoformat(),
                })

            elif day == 21:
                # Final updates
                self._events_by_day[day].append({
                    "id": 1,
                    "name": "Entity_1_v3",
                    "value": 175,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })
                self._events_by_day[day].append({
                    "id": 2,
                    "name": "Entity_2_RESURRECTED_v2",
                    "value": 2100,
                    "op": "U",
                    "ts": (ts_base + timedelta(minutes=1)).isoformat(),
                })

    def get_data_for_day(self, day: int) -> pd.DataFrame:
        """Get CDC data for a specific day."""
        events = self._events_by_day.get(day, [])
        if not events:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])
        return pd.DataFrame(events)

    def get_run_date(self, day: int) -> str:
        """Get the run date string for a day."""
        return (self.start_date + timedelta(days=day - 1)).isoformat()

    def get_expected_final_state(self, delete_mode: str = "ignore") -> Dict[int, Dict]:
        """Get expected final state based on delete mode."""
        if delete_mode == "ignore":
            # All 5 IDs active with their final values
            return {
                1: {"name": "Entity_1_v3", "value": 175, "is_deleted": False},
                2: {"name": "Entity_2_RESURRECTED_v2", "value": 2100, "is_deleted": False},
                3: {"name": "Entity_3_v2", "value": 350, "is_deleted": False},
                4: {"name": "Entity_4_RESURRECTED", "value": 4000, "is_deleted": False},
                5: {"name": "Entity_5_v1", "value": 500, "is_deleted": False},
            }
        elif delete_mode == "tombstone":
            # Same as ignore but with _deleted flag available
            return {
                1: {"name": "Entity_1_v3", "value": 175, "is_deleted": False},
                2: {"name": "Entity_2_RESURRECTED_v2", "value": 2100, "is_deleted": False},
                3: {"name": "Entity_3_v2", "value": 350, "is_deleted": False},
                4: {"name": "Entity_4_RESURRECTED", "value": 4000, "is_deleted": False},
                5: {"name": "Entity_5_v1", "value": 500, "is_deleted": False},
            }
        else:  # hard_delete
            return {
                1: {"name": "Entity_1_v3", "value": 175, "is_deleted": False},
                2: {"name": "Entity_2_RESURRECTED_v2", "value": 2100, "is_deleted": False},
                3: {"name": "Entity_3_v2", "value": 350, "is_deleted": False},
                4: {"name": "Entity_4_RESURRECTED", "value": 4000, "is_deleted": False},
                5: {"name": "Entity_5_v1", "value": 500, "is_deleted": False},
            }


class BatchRetryScenario:
    """Generate CDC data for testing duplicate batch processing.

    Simulates scenarios where the same batch is processed multiple times:
    - Day 5 batch processed twice
    - Day 10 batch processed 3 times
    - Day 15 batch arrives, then Day 14 re-arrives
    """

    def __init__(self, start_date: date = date(2025, 1, 6)):
        self.start_date = start_date
        self._events_by_day: Dict[int, List[Dict]] = {}
        self._generate_all_events()

        # Track which days get duplicated and how many times
        self.duplicate_schedule = {
            5: 2,   # Day 5 processed twice
            10: 3,  # Day 10 processed 3 times
            14: 2,  # Day 14 processed twice (once out of order after Day 15)
        }

    def _generate_all_events(self):
        """Generate CDC events for 21 days."""
        for day in range(1, 22):
            self._events_by_day[day] = []
            ts_base = datetime.combine(
                self.start_date + timedelta(days=day - 1),
                datetime.min.time().replace(hour=10)
            )

            if day == 1:
                for i in range(1, 6):
                    self._events_by_day[day].append({
                        "id": i,
                        "name": f"Entity_{i}_v1",
                        "value": i * 100,
                        "op": "I",
                        "ts": (ts_base + timedelta(minutes=i)).isoformat(),
                    })

            elif day == 5:
                # Updates that will be duplicated
                self._events_by_day[day].append({
                    "id": 1,
                    "name": "Entity_1_v2",
                    "value": 150,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })
                self._events_by_day[day].append({
                    "id": 6,
                    "name": "Entity_6_v1",
                    "value": 600,
                    "op": "I",
                    "ts": (ts_base + timedelta(minutes=1)).isoformat(),
                })

            elif day == 10:
                # Batch that gets processed 3 times
                self._events_by_day[day].append({
                    "id": 2,
                    "name": "Entity_2_v2",
                    "value": 250,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })
                self._events_by_day[day].append({
                    "id": 3,
                    "name": "Entity_3_v1",
                    "value": 300,
                    "op": "D",
                    "ts": (ts_base + timedelta(minutes=1)).isoformat(),
                })

            elif day == 14:
                # Batch that arrives again after Day 15
                self._events_by_day[day].append({
                    "id": 4,
                    "name": "Entity_4_v2",
                    "value": 450,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

            elif day == 15:
                self._events_by_day[day].append({
                    "id": 5,
                    "name": "Entity_5_v2",
                    "value": 550,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

            elif day == 20:
                self._events_by_day[day].append({
                    "id": 7,
                    "name": "Entity_7_v1",
                    "value": 700,
                    "op": "I",
                    "ts": ts_base.isoformat(),
                })

    def get_data_for_day(self, day: int) -> pd.DataFrame:
        """Get CDC data for a specific day."""
        events = self._events_by_day.get(day, [])
        if not events:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])
        return pd.DataFrame(events)

    def get_run_date(self, day: int) -> str:
        """Get the run date string for a day."""
        return (self.start_date + timedelta(days=day - 1)).isoformat()

    def get_processing_sequence(self) -> List[int]:
        """Get the sequence of days to process including duplicates.

        Returns list of day numbers in processing order.
        """
        sequence = []
        for day in range(1, 22):
            if day == 5:
                sequence.extend([5, 5])  # Day 5 twice
            elif day == 10:
                sequence.extend([10, 10, 10])  # Day 10 three times
            elif day == 15:
                sequence.extend([15, 14])  # Day 15, then Day 14 again
            elif day == 14:
                sequence.append(14)  # First time for Day 14
            else:
                sequence.append(day)
        return sequence


class IrregularScheduleScenario:
    """Generate CDC data with irregular schedule: random skips and backfills.

    21-day scenario with:
    - Days 4-6 skipped initially
    - Day 12 skipped
    - Backfill of Days 4-6 happens on Day 8
    - Backfill of Day 12 happens on Day 16
    - Some days have no data at all
    """

    def __init__(self, start_date: date = date(2025, 1, 6)):
        self.start_date = start_date
        self._events_by_day: Dict[int, List[Dict]] = {}
        self._generate_all_events()

        # Define schedule: physical_day -> list of logical days to process
        self.schedule = self._define_schedule()

    def _define_schedule(self) -> Dict[int, List[int]]:
        """Define which logical days are processed on each physical day."""
        schedule = {}

        # Days 1-3: Normal
        for d in range(1, 4):
            schedule[d] = [d]

        # Days 4-6: Skipped (no processing)
        for d in range(4, 7):
            schedule[d] = []

        # Day 7: Normal
        schedule[7] = [7]

        # Day 8: Backfill of days 4, 5, 6 plus normal day 8
        schedule[8] = [4, 5, 6, 8]

        # Days 9-11: Normal
        for d in range(9, 12):
            schedule[d] = [d]

        # Day 12: Skipped
        schedule[12] = []

        # Days 13-15: Normal
        for d in range(13, 16):
            schedule[d] = [d]

        # Day 16: Normal plus backfill of day 12
        schedule[16] = [12, 16]

        # Days 17-21: Normal
        for d in range(17, 22):
            schedule[d] = [d]

        return schedule

    def _generate_all_events(self):
        """Generate CDC events for all logical days."""
        for day in range(1, 22):
            self._events_by_day[day] = []
            ts_base = datetime.combine(
                self.start_date + timedelta(days=day - 1),
                datetime.min.time().replace(hour=10)
            )

            if day == 1:
                for i in range(1, 6):
                    self._events_by_day[day].append({
                        "id": i,
                        "name": f"Entity_{i}_v1",
                        "value": i * 100,
                        "op": "I",
                        "ts": (ts_base + timedelta(minutes=i)).isoformat(),
                    })

            elif day == 4:
                # Skipped day - has important update
                self._events_by_day[day].append({
                    "id": 1,
                    "name": "Entity_1_v2_skipped",
                    "value": 150,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

            elif day == 5:
                # Skipped day - has delete
                self._events_by_day[day].append({
                    "id": 2,
                    "name": "Entity_2_v1",
                    "value": 200,
                    "op": "D",
                    "ts": ts_base.isoformat(),
                })

            elif day == 6:
                # Skipped day - has new insert
                self._events_by_day[day].append({
                    "id": 6,
                    "name": "Entity_6_v1_skipped",
                    "value": 600,
                    "op": "I",
                    "ts": ts_base.isoformat(),
                })

            elif day == 12:
                # Skipped day - has critical update
                self._events_by_day[day].append({
                    "id": 3,
                    "name": "Entity_3_v2_skipped",
                    "value": 350,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })
                self._events_by_day[day].append({
                    "id": 7,
                    "name": "Entity_7_v1_skipped",
                    "value": 700,
                    "op": "I",
                    "ts": (ts_base + timedelta(minutes=1)).isoformat(),
                })

            elif day == 15:
                self._events_by_day[day].append({
                    "id": 4,
                    "name": "Entity_4_v2",
                    "value": 450,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

            elif day == 21:
                # Final day updates
                self._events_by_day[day].append({
                    "id": 5,
                    "name": "Entity_5_v2",
                    "value": 550,
                    "op": "U",
                    "ts": ts_base.isoformat(),
                })

    def get_data_for_logical_day(self, logical_day: int) -> pd.DataFrame:
        """Get CDC data for a specific logical day."""
        events = self._events_by_day.get(logical_day, [])
        if not events:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])
        return pd.DataFrame(events)

    def get_data_for_physical_day(self, physical_day: int) -> pd.DataFrame:
        """Get all CDC data that should be processed on a physical day.

        This combines data from multiple logical days if there's a backfill.
        """
        logical_days = self.schedule.get(physical_day, [])
        if not logical_days:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])

        all_events = []
        for log_day in logical_days:
            all_events.extend(self._events_by_day.get(log_day, []))

        if not all_events:
            return pd.DataFrame(columns=["id", "name", "value", "op", "ts"])

        return pd.DataFrame(all_events)

    def get_run_date(self, physical_day: int) -> str:
        """Get the run date string for a physical day."""
        return (self.start_date + timedelta(days=physical_day - 1)).isoformat()

    def get_expected_final_state(self) -> Dict[int, Dict]:
        """Get expected final state after all days processed (including backfills)."""
        return {
            1: {"name": "Entity_1_v2_skipped", "value": 150, "is_deleted": False},
            # ID 2 deleted on day 5
            3: {"name": "Entity_3_v2_skipped", "value": 350, "is_deleted": False},
            4: {"name": "Entity_4_v2", "value": 450, "is_deleted": False},
            5: {"name": "Entity_5_v2", "value": 550, "is_deleted": False},
            6: {"name": "Entity_6_v1_skipped", "value": 600, "is_deleted": False},
            7: {"name": "Entity_7_v1_skipped", "value": 700, "is_deleted": False},
        }
