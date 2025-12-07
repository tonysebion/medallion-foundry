"""Synthetic data generators for testing.

Per spec Section 10: Generates domain-aware test data for:
- Healthcare claims domain
- Retail/E-commerce domain
- Financial transactions domain

Supports T0/T1/T2 time series scenarios:
- T0: Initial full load
- T1: Incremental changes (inserts, updates)
- T2: Late data and backfills
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import pandas as pd


@dataclass
class SyntheticConfig:
    """Configuration for synthetic data generation."""

    row_count: int = 100
    seed: int = 42
    start_date: date = field(default_factory=lambda: date(2024, 1, 1))
    domains: List[str] = field(default_factory=lambda: ["claims", "orders", "transactions"])


class BaseSyntheticGenerator:
    """Base class for synthetic data generators."""

    def __init__(self, seed: int = 42, row_count: int = 100):
        self.seed = seed
        self.row_count = row_count
        self.rng = random.Random(seed)

    def reset(self) -> None:
        """Reset random state for reproducibility."""
        self.rng = random.Random(self.seed)

    def _generate_id(self, prefix: str, index: int, width: int = 8) -> str:
        """Generate a formatted ID string."""
        return f"{prefix}{index:0{width}d}"

    def _random_date(self, start: date, end: date) -> date:
        """Generate a random date between start and end."""
        delta = (end - start).days
        return start + timedelta(days=self.rng.randint(0, max(0, delta)))

    def _random_datetime(self, start: datetime, end: datetime) -> datetime:
        """Generate a random datetime between start and end."""
        delta = (end - start).total_seconds()
        return start + timedelta(seconds=self.rng.uniform(0, max(0, delta)))

    def _random_choice(self, options: List[Any]) -> Any:
        """Pick a random item from options."""
        return self.rng.choice(options)

    def _random_amount(self, min_val: float, max_val: float, decimals: int = 2) -> float:
        """Generate a random monetary amount."""
        return round(self.rng.uniform(min_val, max_val), decimals)

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate the initial (T0) dataset for tests."""

        raise NotImplementedError("Subclasses must implement generate_t0")


class ClaimsGenerator(BaseSyntheticGenerator):
    """Generate healthcare claims data."""

    CLAIM_STATUSES = ["submitted", "processing", "approved", "denied", "paid"]
    CLAIM_TYPES = ["medical", "dental", "vision", "pharmacy", "mental_health"]
    DIAGNOSIS_CODES = [f"ICD{100 + i}" for i in range(50)]
    PROCEDURE_CODES = [f"CPT{10000 + i}" for i in range(100)]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 (initial load) claims data."""
        self.reset()
        records = []
        base_date = run_date - timedelta(days=30)

        for i in range(1, self.row_count + 1):
            service_date = self._random_date(base_date, run_date)
            billed = self._random_amount(100, 5000)
            records.append({
                "claim_id": self._generate_id("CLM", i),
                "patient_id": self._generate_id("PAT", (i % 50) + 1, 5),
                "provider_id": self._generate_id("PRV", (i % 20) + 1, 4),
                "claim_type": self._random_choice(self.CLAIM_TYPES),
                "service_date": service_date,
                "billed_amount": billed,
                "paid_amount": round(billed * self.rng.uniform(0.6, 0.95), 2),
                "diagnosis_code": self._random_choice(self.DIAGNOSIS_CODES),
                "procedure_code": self._random_choice(self.PROCEDURE_CODES),
                "status": self._random_choice(self.CLAIM_STATUSES[:3]),  # Initial states
                "created_at": datetime.combine(service_date, datetime.min.time()),
                "updated_at": datetime.combine(service_date, datetime.min.time()),
            })

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 (incremental) claims data - new claims and updates."""
        self.reset()
        # Advance seed to get different data
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        update_count = min(20, len(t0_df) // 5)
        new_count = self.row_count // 5

        # Updates to existing claims
        for idx in self.rng.sample(range(len(t0_df)), update_count):
            row = t0_df.iloc[idx].to_dict()
            row["status"] = self._random_choice(self.CLAIM_STATUSES[2:])  # Progress status
            row["paid_amount"] = round(row["billed_amount"] * self.rng.uniform(0.7, 0.98), 2)
            row["updated_at"] = datetime.combine(run_date, datetime.min.time())
            records.append(row)

        # New claims
        max_id = int(t0_df["claim_id"].str.extract(r"(\d+)")[0].max())
        for i in range(1, new_count + 1):
            billed = self._random_amount(100, 5000)
            records.append({
                "claim_id": self._generate_id("CLM", max_id + i),
                "patient_id": self._generate_id("PAT", self.rng.randint(1, 50), 5),
                "provider_id": self._generate_id("PRV", self.rng.randint(1, 20), 4),
                "claim_type": self._random_choice(self.CLAIM_TYPES),
                "service_date": run_date - timedelta(days=self.rng.randint(0, 7)),
                "billed_amount": billed,
                "paid_amount": 0.0,  # New claims not yet paid
                "diagnosis_code": self._random_choice(self.DIAGNOSIS_CODES),
                "procedure_code": self._random_choice(self.PROCEDURE_CODES),
                "status": "submitted",
                "created_at": datetime.combine(run_date, datetime.min.time()),
                "updated_at": datetime.combine(run_date, datetime.min.time()),
            })

        return pd.DataFrame(records)

    def generate_t2_late(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T2 (late data) - backdated claims that arrive late."""
        self.reset()
        records = []
        late_count = self.row_count // 10
        max_id = int(t0_df["claim_id"].str.extract(r"(\d+)")[0].max()) + 100

        # Late claims from before T0
        base_date = run_date - timedelta(days=60)
        for i in range(1, late_count + 1):
            service_date = self._random_date(base_date, run_date - timedelta(days=30))
            billed = self._random_amount(100, 3000)
            records.append({
                "claim_id": self._generate_id("CLM", max_id + i),
                "patient_id": self._generate_id("PAT", self.rng.randint(1, 50), 5),
                "provider_id": self._generate_id("PRV", self.rng.randint(1, 20), 4),
                "claim_type": self._random_choice(self.CLAIM_TYPES),
                "service_date": service_date,
                "billed_amount": billed,
                "paid_amount": round(billed * self.rng.uniform(0.7, 0.95), 2),
                "diagnosis_code": self._random_choice(self.DIAGNOSIS_CODES),
                "procedure_code": self._random_choice(self.PROCEDURE_CODES),
                "status": "paid",  # Late claims often already processed
                "created_at": datetime.combine(service_date, datetime.min.time()),
                "updated_at": datetime.combine(run_date, datetime.min.time()),
            })

        return pd.DataFrame(records)


class OrdersGenerator(BaseSyntheticGenerator):
    """Generate retail/e-commerce orders data."""

    ORDER_STATUSES = ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled"]
    PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
    CATEGORIES = ["electronics", "clothing", "home", "books", "sports", "food"]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 (initial load) orders data."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())
        end_time = datetime.combine(run_date, datetime.min.time())

        for i in range(1, self.row_count + 1):
            order_time = self._random_datetime(base_time, end_time)
            quantity = self.rng.randint(1, 5)
            unit_price = self._random_amount(10, 500)
            records.append({
                "order_id": self._generate_id("ORD", i),
                "customer_id": self._generate_id("CUST", (i % 100) + 1, 5),
                "product_id": self._generate_id("PROD", self.rng.randint(1, 200), 5),
                "category": self._random_choice(self.CATEGORIES),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
                "discount": self._random_amount(0, 50) if self.rng.random() > 0.7 else 0,
                "payment_method": self._random_choice(self.PAYMENT_METHODS),
                "status": self._random_choice(self.ORDER_STATUSES[:4]),
                "order_ts": order_time,
                "updated_at": order_time,
            })

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 (incremental) - status updates and new orders."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        update_count = min(30, len(t0_df) // 4)
        new_count = self.row_count // 4

        # Status updates
        for idx in self.rng.sample(range(len(t0_df)), update_count):
            row = t0_df.iloc[idx].to_dict()
            current_status_idx = self.ORDER_STATUSES.index(row["status"])
            if current_status_idx < len(self.ORDER_STATUSES) - 2:
                row["status"] = self.ORDER_STATUSES[current_status_idx + 1]
            row["updated_at"] = datetime.combine(run_date, datetime.min.time())
            records.append(row)

        # New orders
        max_id = int(t0_df["order_id"].str.extract(r"(\d+)")[0].max())
        order_time = datetime.combine(run_date, datetime.min.time())
        for i in range(1, new_count + 1):
            quantity = self.rng.randint(1, 5)
            unit_price = self._random_amount(10, 500)
            records.append({
                "order_id": self._generate_id("ORD", max_id + i),
                "customer_id": self._generate_id("CUST", self.rng.randint(1, 100), 5),
                "product_id": self._generate_id("PROD", self.rng.randint(1, 200), 5),
                "category": self._random_choice(self.CATEGORIES),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
                "discount": self._random_amount(0, 50) if self.rng.random() > 0.7 else 0,
                "payment_method": self._random_choice(self.PAYMENT_METHODS),
                "status": "pending",
                "order_ts": order_time + timedelta(minutes=i),
                "updated_at": order_time + timedelta(minutes=i),
            })

        return pd.DataFrame(records)


class TransactionsGenerator(BaseSyntheticGenerator):
    """Generate financial transactions data."""

    TRANSACTION_TYPES = ["credit", "debit", "transfer", "payment", "refund"]
    TRANSACTION_STATUSES = ["pending", "completed", "failed", "reversed"]
    CURRENCIES = ["USD", "EUR", "GBP", "CAD"]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 (initial load) transactions data."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=7), datetime.min.time())
        end_time = datetime.combine(run_date, datetime.min.time())

        for i in range(1, self.row_count + 1):
            txn_time = self._random_datetime(base_time, end_time)
            txn_type = self._random_choice(self.TRANSACTION_TYPES)
            amount = self._random_amount(1, 10000)
            if txn_type in ("debit", "payment"):
                amount = -amount

            records.append({
                "transaction_id": self._generate_id("TXN", i, 10),
                "account_id": self._generate_id("ACC", (i % 50) + 1, 6),
                "transaction_type": txn_type,
                "amount": amount,
                "currency": self._random_choice(self.CURRENCIES),
                "status": self._random_choice(self.TRANSACTION_STATUSES[:2]),
                "description": f"{txn_type.title()} transaction",
                "reference_id": hashlib.md5(f"{i}{txn_time}".encode()).hexdigest()[:12],
                "transaction_ts": txn_time,
                "created_at": txn_time,
            })

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 (incremental) - new transactions and status updates."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        update_count = min(15, len(t0_df) // 6)
        new_count = self.row_count // 3

        # Status updates (pending -> completed/failed)
        pending_mask = t0_df["status"] == "pending"
        pending_indices = t0_df[pending_mask].index.tolist()
        for idx in self.rng.sample(pending_indices, min(update_count, len(pending_indices))):
            row = t0_df.iloc[idx].to_dict()
            row["status"] = self._random_choice(["completed", "failed"])
            records.append(row)

        # New transactions
        max_id = int(t0_df["transaction_id"].str.extract(r"(\d+)")[0].max())
        txn_time = datetime.combine(run_date, datetime.min.time())
        for i in range(1, new_count + 1):
            txn_type = self._random_choice(self.TRANSACTION_TYPES)
            amount = self._random_amount(1, 10000)
            if txn_type in ("debit", "payment"):
                amount = -amount

            records.append({
                "transaction_id": self._generate_id("TXN", max_id + i, 10),
                "account_id": self._generate_id("ACC", self.rng.randint(1, 50), 6),
                "transaction_type": txn_type,
                "amount": amount,
                "currency": self._random_choice(self.CURRENCIES),
                "status": "pending",
                "description": f"{txn_type.title()} transaction",
                "reference_id": hashlib.md5(f"{max_id + i}{txn_time}".encode()).hexdigest()[:12],
                "transaction_ts": txn_time + timedelta(seconds=i),
                "created_at": txn_time + timedelta(seconds=i),
            })

        return pd.DataFrame(records)


class StateChangeGenerator(BaseSyntheticGenerator):
    """Generate SCD-style state change data for testing history patterns."""

    def generate_scd_history(
        self,
        entity_count: int = 20,
        changes_per_entity: int = 3,
        run_date: date = date(2024, 1, 15),
    ) -> pd.DataFrame:
        """Generate SCD history data with multiple versions per entity."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=90), datetime.min.time())

        for entity_id in range(1, entity_count + 1):
            name = f"Entity {entity_id}"
            status_options = ["active", "inactive", "pending", "suspended"]
            current_time = base_time + timedelta(days=self.rng.randint(0, 30))

            for version in range(1, changes_per_entity + 1):
                records.append({
                    "entity_id": entity_id,
                    "name": name if version == 1 else f"{name} (v{version})",
                    "status": self._random_choice(status_options),
                    "amount": self._random_amount(100, 5000),
                    "effective_from": current_time,
                    "change_ts": current_time,
                    "version": version,
                })
                current_time = current_time + timedelta(days=self.rng.randint(7, 30))

        return pd.DataFrame(records)


# Factory function for creating generators
def create_generator(domain: str, seed: int = 42, row_count: int = 100) -> BaseSyntheticGenerator:
    """Create a generator for the specified domain."""
    generators = {
        "claims": ClaimsGenerator,
        "orders": OrdersGenerator,
        "transactions": TransactionsGenerator,
        "state": StateChangeGenerator,
    }
    generator_class = generators.get(domain, BaseSyntheticGenerator)
    return generator_class(seed=seed, row_count=row_count)


def generate_time_series_data(
    domain: str,
    t0_date: date,
    seed: int = 42,
    row_count: int = 100,
) -> Dict[str, pd.DataFrame]:
    """Generate a complete T0/T1/T2 time series for testing.

    Returns:
        Dictionary with keys 't0', 't1', 't2_late' containing DataFrames
    """
    generator = create_generator(domain, seed=seed, row_count=row_count)

    t0_df = generator.generate_t0(t0_date)
    t1_date = t0_date + timedelta(days=1)
    t2_date = t0_date + timedelta(days=2)

    result = {"t0": t0_df}

    if hasattr(generator, "generate_t1"):
        result["t1"] = generator.generate_t1(t1_date, t0_df)

    if hasattr(generator, "generate_t2_late"):
        result["t2_late"] = generator.generate_t2_late(t2_date, t0_df)

    return result
