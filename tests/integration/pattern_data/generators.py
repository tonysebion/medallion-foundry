"""Multi-batch test data generators for pattern verification.

This module generates realistic multi-batch test data for verifying
load pattern business logic across all patterns:
- SNAPSHOT: Full replacement each run
- INCREMENTAL_APPEND: Append new records only
- INCREMENTAL_MERGE: Upsert by primary key
- CURRENT_HISTORY: SCD Type 2 with current/history split

Data generation features:
- Configurable batch sizes (500-1000 rows default)
- Multiple time points (T0 → T1 → T2 → T3)
- Predictable change patterns (updates, inserts, unchanged)
- Deterministic seeding for reproducibility
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import pandas as pd


@dataclass
class PatternScenario:
    """Result container for generated pattern test data.

    Attributes:
        pattern: The load pattern this data is designed to test
        batches: Dictionary of batch name to DataFrame (e.g., t0, t1, t2, t3)
        metadata: Additional information about the generated data
    """

    pattern: str
    batches: Dict[str, pd.DataFrame]
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def t0(self) -> pd.DataFrame:
        """Get initial batch."""
        return self.batches.get("t0", pd.DataFrame())

    @property
    def t1(self) -> Optional[pd.DataFrame]:
        """Get first incremental batch."""
        return self.batches.get("t1")

    @property
    def t2(self) -> Optional[pd.DataFrame]:
        """Get second incremental batch."""
        return self.batches.get("t2")

    @property
    def t3(self) -> Optional[pd.DataFrame]:
        """Get third incremental batch."""
        return self.batches.get("t3")

    @property
    def batch_count(self) -> int:
        """Number of batches in scenario."""
        return len(self.batches)


class PatternTestDataGenerator:
    """Generate multi-batch test data for pattern verification.

    This generator creates realistic test data with configurable sizes,
    change rates, and batch counts. Data is deterministically generated
    from a seed for reproducibility.

    Example:
        generator = PatternTestDataGenerator(seed=42, base_rows=1000)

        # SNAPSHOT: single batch
        scenario = generator.generate_snapshot_scenario()

        # INCREMENTAL: multiple batches with changes
        scenario = generator.generate_incremental_scenario(
            batches=4,
            update_rate=0.2,
            insert_rate=0.1
        )

        # SCD2: entity state changes over time
        scenario = generator.generate_scd2_scenario(
            entities=500,
            changes_per_entity=3
        )
    """

    # Domain-specific values for realistic data
    STATUSES = ["pending", "active", "processing", "completed", "archived"]
    CATEGORIES = ["alpha", "beta", "gamma", "delta", "epsilon"]
    REGIONS = ["north", "south", "east", "west", "central"]
    PRIORITIES = ["low", "medium", "high", "critical"]

    def __init__(
        self,
        seed: int = 42,
        base_rows: int = 1000,
        base_date: Optional[date] = None,
    ):
        """Initialize the generator.

        Args:
            seed: Random seed for reproducibility
            base_rows: Default number of rows for T0 batch
            base_date: Starting date for data generation (default: 2024-01-01)
        """
        self.seed = seed
        self.base_rows = base_rows
        self.base_date = base_date or date(2024, 1, 1)
        self.rng = random.Random(seed)

    def reset(self) -> None:
        """Reset random state for reproducibility."""
        self.rng = random.Random(self.seed)

    def _generate_id(self, prefix: str, index: int, width: int = 8) -> str:
        """Generate a formatted ID string."""
        return f"{prefix}{index:0{width}d}"

    def _random_choice(self, options: List[Any]) -> Any:
        """Pick a random item from options."""
        return self.rng.choice(options)

    def _random_amount(self, min_val: float, max_val: float, decimals: int = 2) -> float:
        """Generate a random monetary amount."""
        return round(self.rng.uniform(min_val, max_val), decimals)

    def _random_date(self, start: date, end: date) -> date:
        """Generate a random date between start and end."""
        delta = (end - start).days
        return start + timedelta(days=self.rng.randint(0, max(0, delta)))

    def _random_datetime(self, base: datetime, range_hours: int = 24) -> datetime:
        """Generate a random datetime within range of base."""
        return base + timedelta(hours=self.rng.uniform(0, range_hours))

    # =========================================================================
    # SNAPSHOT Pattern
    # =========================================================================

    def generate_snapshot_scenario(
        self,
        rows: Optional[int] = None,
        include_replacement: bool = False,
    ) -> PatternScenario:
        """Generate data for SNAPSHOT pattern verification.

        SNAPSHOT pattern: Complete replacement each run. Each batch represents
        a full snapshot of the data at that point in time.

        Args:
            rows: Number of rows (default: base_rows)
            include_replacement: If True, generate a second snapshot (t1) with
                                 different data to test full replacement

        Returns:
            PatternScenario with t0 batch (and optionally t1 for replacement test)
        """
        self.reset()
        rows = rows or self.base_rows

        t0_df = self._generate_base_records(rows, batch_date=self.base_date)

        batches = {"t0": t0_df}
        metadata = {
            "pattern": "snapshot",
            "t0_rows": len(t0_df),
            "seed": self.seed,
        }

        if include_replacement:
            # Generate completely different snapshot for replacement test
            t1_date = self.base_date + timedelta(days=1)
            # Advance RNG to get different data
            for _ in range(rows * 2):
                self.rng.random()

            t1_df = self._generate_base_records(rows, batch_date=t1_date)
            # Use different IDs to make it clearly a replacement
            t1_df["record_id"] = [
                self._generate_id("REC", i + rows + 1) for i in range(len(t1_df))
            ]
            batches["t1"] = t1_df
            metadata["t1_rows"] = len(t1_df)
            metadata["replaces_t0"] = True

        return PatternScenario(pattern="snapshot", batches=batches, metadata=metadata)

    # =========================================================================
    # INCREMENTAL_APPEND Pattern
    # =========================================================================

    def generate_incremental_append_scenario(
        self,
        rows: Optional[int] = None,
        batches: int = 4,
        insert_rate: float = 0.1,
    ) -> PatternScenario:
        """Generate data for INCREMENTAL_APPEND pattern verification.

        INCREMENTAL_APPEND pattern: Append new records only (insert-only CDC).
        Each batch after T0 contains only NEW records to be appended.

        Args:
            rows: Number of rows in T0 (default: base_rows)
            batches: Number of batches to generate (1-4)
            insert_rate: Percentage of T0 size to insert per batch (0.0-1.0)

        Returns:
            PatternScenario with t0, t1, t2, t3 batches (new records only)
        """
        self.reset()
        rows = rows or self.base_rows
        batches = min(max(batches, 1), 4)
        new_per_batch = max(1, int(rows * insert_rate))

        # T0: Initial full load
        t0_df = self._generate_base_records(rows, batch_date=self.base_date)
        max_id = rows

        result_batches = {"t0": t0_df}
        metadata = {
            "pattern": "incremental_append",
            "t0_rows": rows,
            "insert_rate": insert_rate,
            "new_per_batch": new_per_batch,
            "seed": self.seed,
            "total_inserts": {},
        }

        # Generate incremental batches (new records only)
        for batch_num in range(1, batches):
            batch_name = f"t{batch_num}"
            batch_date = self.base_date + timedelta(days=batch_num)

            # Generate only NEW records
            new_records = []
            for i in range(new_per_batch):
                max_id += 1
                record = self._generate_single_record(max_id, batch_date)
                record["batch_source"] = batch_name
                new_records.append(record)

            batch_df = pd.DataFrame(new_records)
            result_batches[batch_name] = batch_df
            metadata["total_inserts"][batch_name] = len(batch_df)
            metadata[f"{batch_name}_rows"] = len(batch_df)

        return PatternScenario(
            pattern="incremental_append", batches=result_batches, metadata=metadata
        )

    # =========================================================================
    # INCREMENTAL_MERGE Pattern
    # =========================================================================

    def generate_incremental_merge_scenario(
        self,
        rows: Optional[int] = None,
        batches: int = 4,
        update_rate: float = 0.2,
        insert_rate: float = 0.1,
    ) -> PatternScenario:
        """Generate data for INCREMENTAL_MERGE pattern verification.

        INCREMENTAL_MERGE pattern: Upsert by primary key. Each batch contains
        a mix of updates to existing records AND new inserts.

        Args:
            rows: Number of rows in T0 (default: base_rows)
            batches: Number of batches to generate (1-4)
            update_rate: Percentage of existing records to update (0.0-1.0)
            insert_rate: Percentage of T0 size to insert per batch (0.0-1.0)

        Returns:
            PatternScenario with t0, t1, t2, t3 batches containing changes
        """
        self.reset()
        rows = rows or self.base_rows
        batches = min(max(batches, 1), 4)
        updates_per_batch = max(1, int(rows * update_rate))
        inserts_per_batch = max(1, int(rows * insert_rate))

        # T0: Initial full load
        t0_df = self._generate_base_records(rows, batch_date=self.base_date)
        max_id = rows

        # Track which records exist (for updates)
        existing_ids: Set[str] = set(t0_df["record_id"].tolist())
        current_state = t0_df.copy()

        result_batches = {"t0": t0_df}
        metadata = {
            "pattern": "incremental_merge",
            "t0_rows": rows,
            "update_rate": update_rate,
            "insert_rate": insert_rate,
            "updates_per_batch": updates_per_batch,
            "inserts_per_batch": inserts_per_batch,
            "seed": self.seed,
            "changes": {},
        }

        # Generate incremental batches (updates + inserts)
        for batch_num in range(1, batches):
            batch_name = f"t{batch_num}"
            batch_date = self.base_date + timedelta(days=batch_num)
            batch_records = []
            updated_ids = []
            inserted_ids = []

            # Select records to update
            available_ids = list(existing_ids)
            update_count = min(updates_per_batch, len(available_ids))
            ids_to_update = self.rng.sample(available_ids, update_count)

            # Generate updates
            for record_id in ids_to_update:
                # Find original record
                original = current_state[current_state["record_id"] == record_id].iloc[
                    0
                ]
                updated = self._apply_update(original.to_dict(), batch_date)
                updated["batch_source"] = batch_name
                batch_records.append(updated)
                updated_ids.append(record_id)

            # Generate new inserts
            for i in range(inserts_per_batch):
                max_id += 1
                record = self._generate_single_record(max_id, batch_date)
                record["batch_source"] = batch_name
                batch_records.append(record)
                existing_ids.add(record["record_id"])
                inserted_ids.append(record["record_id"])

            batch_df = pd.DataFrame(batch_records)
            result_batches[batch_name] = batch_df

            # Update current state for next batch
            for record in batch_records:
                mask = current_state["record_id"] == record["record_id"]
                if mask.any():
                    for col, val in record.items():
                        current_state.loc[mask, col] = val
                else:
                    current_state = pd.concat(
                        [current_state, pd.DataFrame([record])], ignore_index=True
                    )

            metadata["changes"][batch_name] = {
                "updated_ids": updated_ids,
                "inserted_ids": inserted_ids,
                "update_count": len(updated_ids),
                "insert_count": len(inserted_ids),
                "total_rows": len(batch_df),
            }
            metadata[f"{batch_name}_rows"] = len(batch_df)

        # Store final expected state
        metadata["expected_final_row_count"] = len(current_state)

        return PatternScenario(
            pattern="incremental_merge", batches=result_batches, metadata=metadata
        )

    # =========================================================================
    # CURRENT_HISTORY (SCD Type 2) Pattern
    # =========================================================================

    def generate_scd2_scenario(
        self,
        entities: int = 500,
        changes_per_entity: int = 3,
        include_deletes: bool = False,
    ) -> PatternScenario:
        """Generate data for CURRENT_HISTORY (SCD Type 2) pattern verification.

        CURRENT_HISTORY pattern: Maintains both current and historical views.
        Each entity has multiple versions over time.

        Args:
            entities: Number of unique entities
            changes_per_entity: Average number of versions per entity
            include_deletes: Include logical deletes in some entities

        Returns:
            PatternScenario with batches representing state changes over time
        """
        self.reset()

        # Generate all entity versions as a stream of changes
        all_records = []
        entity_versions: Dict[int, int] = {}  # entity_id -> current version
        batch_dates = [
            self.base_date + timedelta(days=i * 7) for i in range(changes_per_entity + 1)
        ]

        # T0: Initial state for all entities
        t0_records = []
        for entity_id in range(1, entities + 1):
            record = self._generate_entity_record(
                entity_id=entity_id,
                version=1,
                effective_date=batch_dates[0],
                is_current=True,
            )
            t0_records.append(record)
            entity_versions[entity_id] = 1

        t0_df = pd.DataFrame(t0_records)

        result_batches = {"t0": t0_df}
        metadata = {
            "pattern": "current_history",
            "entity_count": entities,
            "changes_per_entity": changes_per_entity,
            "seed": self.seed,
            "version_counts": {"t0": {eid: 1 for eid in range(1, entities + 1)}},
            "changes": {},
        }

        # Generate change batches
        for batch_num in range(1, changes_per_entity):
            batch_name = f"t{batch_num}"
            batch_date = batch_dates[batch_num]
            batch_records = []
            changed_entities = []

            # Randomly select entities to change
            change_count = max(1, entities // changes_per_entity)
            entities_to_change = self.rng.sample(range(1, entities + 1), change_count)

            for entity_id in entities_to_change:
                new_version = entity_versions[entity_id] + 1
                entity_versions[entity_id] = new_version

                # Determine if this is a delete
                is_deleted = include_deletes and self.rng.random() < 0.1

                record = self._generate_entity_record(
                    entity_id=entity_id,
                    version=new_version,
                    effective_date=batch_date,
                    is_current=True,
                    is_deleted=is_deleted,
                )
                batch_records.append(record)
                changed_entities.append(entity_id)

            batch_df = pd.DataFrame(batch_records)
            result_batches[batch_name] = batch_df

            metadata["changes"][batch_name] = {
                "changed_entities": changed_entities,
                "change_count": len(changed_entities),
            }
            metadata[f"{batch_name}_rows"] = len(batch_df)

        # Calculate expected final state
        total_versions = sum(entity_versions.values())
        metadata["expected_current_rows"] = entities
        metadata["expected_history_rows"] = total_versions
        metadata["entity_version_counts"] = dict(entity_versions)

        return PatternScenario(
            pattern="current_history", batches=result_batches, metadata=metadata
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_base_records(
        self, count: int, batch_date: date
    ) -> pd.DataFrame:
        """Generate base records for initial batch."""
        records = []
        for i in range(1, count + 1):
            records.append(self._generate_single_record(i, batch_date))
        return pd.DataFrame(records)

    def _generate_single_record(self, record_num: int, record_date: date) -> Dict[str, Any]:
        """Generate a single test record."""
        created_at = datetime.combine(record_date, datetime.min.time())
        amount = self._random_amount(100, 10000)

        return {
            "record_id": self._generate_id("REC", record_num),
            "entity_key": self._generate_id("ENT", (record_num % 100) + 1, 5),
            "category": self._random_choice(self.CATEGORIES),
            "region": self._random_choice(self.REGIONS),
            "status": self._random_choice(self.STATUSES[:3]),  # Initial states
            "priority": self._random_choice(self.PRIORITIES),
            "amount": amount,
            "quantity": self.rng.randint(1, 100),
            "score": self._random_amount(0, 100),
            "is_active": True,
            "created_at": created_at,
            "updated_at": created_at,
            "record_date": record_date,
        }

    def _apply_update(self, record: Dict[str, Any], update_date: date) -> Dict[str, Any]:
        """Apply updates to an existing record."""
        updated = record.copy()

        # Progress status
        current_status_idx = self.STATUSES.index(record["status"])
        if current_status_idx < len(self.STATUSES) - 1:
            updated["status"] = self.STATUSES[current_status_idx + 1]

        # Modify numeric values
        updated["amount"] = self._random_amount(
            record["amount"] * 0.8, record["amount"] * 1.2
        )
        updated["quantity"] = max(1, record["quantity"] + self.rng.randint(-10, 20))
        updated["score"] = min(100, max(0, record["score"] + self._random_amount(-10, 10)))

        # Update timestamp
        updated["updated_at"] = datetime.combine(update_date, datetime.min.time())
        updated["record_date"] = update_date

        return updated

    def _generate_entity_record(
        self,
        entity_id: int,
        version: int,
        effective_date: date,
        is_current: bool = True,
        is_deleted: bool = False,
    ) -> Dict[str, Any]:
        """Generate a record for SCD2 entity tracking."""
        effective_ts = datetime.combine(effective_date, datetime.min.time())

        return {
            "entity_id": self._generate_id("ENT", entity_id, 6),
            "entity_name": f"Entity {entity_id}" if version == 1 else f"Entity {entity_id} (v{version})",
            "status": "deleted" if is_deleted else self._random_choice(self.STATUSES),
            "category": self._random_choice(self.CATEGORIES),
            "amount": self._random_amount(100, 5000),
            "version": version,
            "is_current": is_current and not is_deleted,
            "is_deleted": is_deleted,
            "effective_from": effective_ts,
            "effective_to": None if is_current else effective_ts + timedelta(days=30),
            "created_at": effective_ts,
            "updated_at": effective_ts,
        }


# Convenience functions for quick scenario generation
def generate_all_pattern_scenarios(
    seed: int = 42,
    base_rows: int = 1000,
) -> Dict[str, PatternScenario]:
    """Generate test data for all load patterns.

    This is useful for running comprehensive pattern verification tests.

    Args:
        seed: Random seed for reproducibility
        base_rows: Number of rows for initial batches

    Returns:
        Dictionary mapping pattern name to PatternScenario
    """
    generator = PatternTestDataGenerator(seed=seed, base_rows=base_rows)

    return {
        "snapshot": generator.generate_snapshot_scenario(include_replacement=True),
        "incremental_append": generator.generate_incremental_append_scenario(
            batches=4, insert_rate=0.1
        ),
        "incremental_merge": generator.generate_incremental_merge_scenario(
            batches=4, update_rate=0.2, insert_rate=0.1
        ),
        "current_history": generator.generate_scd2_scenario(
            entities=500, changes_per_entity=3
        ),
    }
