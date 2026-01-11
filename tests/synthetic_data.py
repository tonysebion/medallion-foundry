"""Synthetic data generators for testing.

Per spec Section 10: Generates domain-aware test data for:
- Healthcare claims domain
- Retail/E-commerce domain
- Financial transactions domain
- Nested/JSON data structures (Story 1.5)
- Wide schemas with sparsity (Story 1.5)
- Late-arriving timezone-shifted data (Story 1.5)
- Duplicate injection for deduplication testing (Story 1.4)

Supports T0/T1/T2 time series scenarios:
- T0: Initial full load
- T1: Incremental changes (inserts, updates)
- T2: Late data and backfills
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Type, TypedDict
from zoneinfo import ZoneInfo

import pandas as pd


@dataclass
class SyntheticConfig:
    """Configuration for synthetic data generation."""

    row_count: int = 100
    seed: int = 42
    start_date: date = field(default_factory=lambda: date(2024, 1, 1))
    domains: List[str] = field(
        default_factory=lambda: ["claims", "orders", "transactions"]
    )


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

    def _random_amount(
        self, min_val: float, max_val: float, decimals: int = 2
    ) -> float:
        """Generate a random monetary amount."""
        return round(self.rng.uniform(min_val, max_val), decimals)

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate the initial (T0) dataset for tests."""

        raise NotImplementedError("Subclasses must implement generate_t0")


class LineItem(TypedDict):
    """Typed line item entry for nested JSON generators."""

    product_id: str
    name: str
    quantity: int
    price: float
    discount_pct: float


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
            records.append(
                {
                    "claim_id": self._generate_id("CLM", i),
                    "patient_id": self._generate_id("PAT", (i % 50) + 1, 5),
                    "provider_id": self._generate_id("PRV", (i % 20) + 1, 4),
                    "claim_type": self._random_choice(self.CLAIM_TYPES),
                    "service_date": service_date,
                    "billed_amount": billed,
                    "paid_amount": round(billed * self.rng.uniform(0.6, 0.95), 2),
                    "diagnosis_code": self._random_choice(self.DIAGNOSIS_CODES),
                    "procedure_code": self._random_choice(self.PROCEDURE_CODES),
                    "status": self._random_choice(
                        self.CLAIM_STATUSES[:3]
                    ),  # Initial states
                    "created_at": datetime.combine(service_date, datetime.min.time()),
                    "updated_at": datetime.combine(service_date, datetime.min.time()),
                }
            )

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
            row["status"] = self._random_choice(
                self.CLAIM_STATUSES[2:]
            )  # Progress status
            row["paid_amount"] = round(
                row["billed_amount"] * self.rng.uniform(0.7, 0.98), 2
            )
            row["updated_at"] = datetime.combine(run_date, datetime.min.time())
            records.append(row)

        # New claims
        max_id = int(t0_df["claim_id"].str.extract(r"(\d+)")[0].max())
        for i in range(1, new_count + 1):
            billed = self._random_amount(100, 5000)
            records.append(
                {
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
                }
            )

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
            records.append(
                {
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
                }
            )

        return pd.DataFrame(records)


class OrdersGenerator(BaseSyntheticGenerator):
    """Generate retail/e-commerce orders data."""

    ORDER_STATUSES = [
        "pending",
        "confirmed",
        "processing",
        "shipped",
        "delivered",
        "cancelled",
    ]
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
            records.append(
                {
                    "order_id": self._generate_id("ORD", i),
                    "customer_id": self._generate_id("CUST", (i % 100) + 1, 5),
                    "product_id": self._generate_id(
                        "PROD", self.rng.randint(1, 200), 5
                    ),
                    "category": self._random_choice(self.CATEGORIES),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": round(quantity * unit_price, 2),
                    "discount": self._random_amount(0, 50)
                    if self.rng.random() > 0.7
                    else 0,
                    "payment_method": self._random_choice(self.PAYMENT_METHODS),
                    "status": self._random_choice(self.ORDER_STATUSES[:4]),
                    "order_ts": order_time,
                    "updated_at": order_time,
                }
            )

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
            records.append(
                {
                    "order_id": self._generate_id("ORD", max_id + i),
                    "customer_id": self._generate_id(
                        "CUST", self.rng.randint(1, 100), 5
                    ),
                    "product_id": self._generate_id(
                        "PROD", self.rng.randint(1, 200), 5
                    ),
                    "category": self._random_choice(self.CATEGORIES),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": round(quantity * unit_price, 2),
                    "discount": self._random_amount(0, 50)
                    if self.rng.random() > 0.7
                    else 0,
                    "payment_method": self._random_choice(self.PAYMENT_METHODS),
                    "status": "pending",
                    "order_ts": order_time + timedelta(minutes=i),
                    "updated_at": order_time + timedelta(minutes=i),
                }
            )

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

            records.append(
                {
                    "transaction_id": self._generate_id("TXN", i, 10),
                    "account_id": self._generate_id("ACC", (i % 50) + 1, 6),
                    "transaction_type": txn_type,
                    "amount": amount,
                    "currency": self._random_choice(self.CURRENCIES),
                    "status": self._random_choice(self.TRANSACTION_STATUSES[:2]),
                    "description": f"{txn_type.title()} transaction",
                    "reference_id": hashlib.md5(f"{i}{txn_time}".encode()).hexdigest()[
                        :12
                    ],
                    "transaction_ts": txn_time,
                    "created_at": txn_time,
                }
            )

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
        for idx in self.rng.sample(
            pending_indices, min(update_count, len(pending_indices))
        ):
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

            records.append(
                {
                    "transaction_id": self._generate_id("TXN", max_id + i, 10),
                    "account_id": self._generate_id("ACC", self.rng.randint(1, 50), 6),
                    "transaction_type": txn_type,
                    "amount": amount,
                    "currency": self._random_choice(self.CURRENCIES),
                    "status": "pending",
                    "description": f"{txn_type.title()} transaction",
                    "reference_id": hashlib.md5(
                        f"{max_id + i}{txn_time}".encode()
                    ).hexdigest()[:12],
                    "transaction_ts": txn_time + timedelta(seconds=i),
                    "created_at": txn_time + timedelta(seconds=i),
                }
            )

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
                records.append(
                    {
                        "entity_id": entity_id,
                        "name": name if version == 1 else f"{name} (v{version})",
                        "status": self._random_choice(status_options),
                        "amount": self._random_amount(100, 5000),
                        "effective_from": current_time,
                        "change_ts": current_time,
                        "version": version,
                    }
                )
                current_time = current_time + timedelta(days=self.rng.randint(7, 30))

        return pd.DataFrame(records)


# =============================================================================
# Edge Case Generators (Story 1.5)
# =============================================================================


class NestedJsonGenerator(BaseSyntheticGenerator):
    """Generate nested/JSON data with arrays, objects, and mixed types.

    Useful for testing:
    - JSON column handling in Bronze extraction
    - Schema inference with nested structures
    - Array/object flattening in Silver transformations
    """

    CATEGORIES = ["electronics", "clothing", "home", "books", "sports"]
    TAGS = ["sale", "new", "featured", "clearance", "premium", "limited", "bestseller"]
    REGIONS = ["north", "south", "east", "west", "central"]
    SHIPPING_METHODS = ["standard", "express", "overnight", "pickup"]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 dataset with nested JSON structures."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            created_at = self._random_datetime(
                base_time, datetime.combine(run_date, datetime.min.time())
            )

            # Nested object: address
            address = {
                "street": f"{self.rng.randint(100, 9999)} {self._random_choice(['Main', 'Oak', 'Pine', 'Maple'])} St",
                "city": self._random_choice(
                    ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
                ),
                "state": self._random_choice(["NY", "CA", "IL", "TX", "AZ"]),
                "zip": f"{self.rng.randint(10000, 99999)}",
                "country": "USA",
            }

            # Nested array: tags (variable length)
            num_tags = self.rng.randint(0, 4)
            tags: List[str] = (
                self.rng.sample(self.TAGS, num_tags) if num_tags > 0 else []
            )

            # Nested array of objects: line_items
            num_items = self.rng.randint(1, 5)
            line_items: List[LineItem] = []
            for j in range(num_items):
                line_items.append(
                    {
                        "product_id": self._generate_id(
                            "PROD", self.rng.randint(1, 500), 5
                        ),
                        "name": f"Product {self.rng.randint(1, 100)}",
                        "quantity": self.rng.randint(1, 10),
                        "price": self._random_amount(5, 500),
                        "discount_pct": self._random_amount(0, 30)
                        if self.rng.random() > 0.7
                        else 0.0,
                    }
                )

            # Nested object: metadata with mixed types
            metadata = {
                "source": self._random_choice(["web", "mobile", "api", "pos"]),
                "session_id": hashlib.md5(f"{i}{created_at}".encode()).hexdigest()[:16],
                "is_guest": self.rng.random() > 0.7,
                "referral_code": f"REF{self.rng.randint(1000, 9999)}"
                if self.rng.random() > 0.5
                else None,
                "utm_params": {
                    "source": self._random_choice(
                        ["google", "facebook", "email", "direct"]
                    ),
                    "medium": self._random_choice(
                        ["cpc", "organic", "social", "referral"]
                    ),
                    "campaign": f"campaign_{self.rng.randint(1, 20)}"
                    if self.rng.random() > 0.3
                    else None,
                },
            }

            # Shipping preferences (nested object with optional fields)
            shipping = {
                "method": self._random_choice(self.SHIPPING_METHODS),
                "instructions": "Leave at door" if self.rng.random() > 0.7 else None,
                "signature_required": self.rng.random() > 0.8,
            }

            records.append(
                {
                    "order_id": self._generate_id("ORD", i),
                    "customer_id": self._generate_id("CUST", (i % 100) + 1, 5),
                    "category": self._random_choice(self.CATEGORIES),
                    "region": self._random_choice(self.REGIONS),
                    "total_amount": sum(
                        item["price"] * item["quantity"] for item in line_items
                    ),
                    "address": json.dumps(address),  # JSON string column
                    "tags": tags,  # Native array
                    "line_items": line_items,  # Native array of objects
                    "metadata": metadata,  # Native nested object
                    "shipping": shipping,  # Native nested object
                    "created_at": created_at,
                    "updated_at": created_at,
                }
            )

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 with updates to nested structures."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        update_count = min(20, len(t0_df) // 5)

        # Updates: modify nested fields
        for idx in self.rng.sample(range(len(t0_df)), update_count):
            row = t0_df.iloc[idx].to_dict()

            # Update tags
            if isinstance(row["tags"], list):
                if self.rng.random() > 0.5 and len(row["tags"]) < 4:
                    row["tags"] = row["tags"] + [self._random_choice(self.TAGS)]

            # Update metadata
            if isinstance(row["metadata"], dict):
                row["metadata"] = dict(row["metadata"])  # Copy
                row["metadata"]["last_modified"] = run_date.isoformat()

            row["updated_at"] = datetime.combine(run_date, datetime.min.time())
            records.append(row)

        return pd.DataFrame(records)


class WideSchemaGenerator(BaseSyntheticGenerator):
    """Generate wide tables with 50+ columns, sparsity, and null patterns.

    Useful for testing:
    - Schema handling with many columns
    - Null value propagation
    - Sparse data patterns
    - Type diversity in single tables
    """

    def __init__(
        self,
        seed: int = 42,
        row_count: int = 100,
        column_count: int = 60,
        null_rate: float = 0.2,
        sparse_rate: float = 0.3,
    ):
        super().__init__(seed=seed, row_count=row_count)
        self.column_count = column_count
        self.null_rate = null_rate  # Probability of null for nullable columns
        self.sparse_rate = sparse_rate  # Probability of sparse columns being null

    def _maybe_null(self, value: Any, rate: Optional[float] = None) -> Optional[Any]:
        """Return value or None based on null rate."""
        rate = rate if rate is not None else self.null_rate
        return None if self.rng.random() < rate else value

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 dataset with wide schema and controlled sparsity."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            created_at = self._random_datetime(
                base_time, datetime.combine(run_date, datetime.min.time())
            )

            record: Dict[str, Any] = {
                # Primary key columns (never null)
                "record_id": self._generate_id("REC", i, 10),
                "entity_key": self._generate_id("ENT", (i % 100) + 1, 6),
                # Core columns (low null rate)
                "name": f"Entity {i}",
                "category": self._random_choice(["A", "B", "C", "D", "E"]),
                "status": self._random_choice(["active", "inactive", "pending"]),
                "created_at": created_at,
                "updated_at": created_at,
            }

            # Integer columns (10 columns)
            for j in range(1, 11):
                col_name = f"int_col_{j:02d}"
                int_value = self.rng.randint(0, 10000)
                record[col_name] = self._maybe_null(int_value)

            # Float columns (10 columns)
            for j in range(1, 11):
                col_name = f"float_col_{j:02d}"
                float_value = self._random_amount(0, 100000, decimals=4)
                record[col_name] = self._maybe_null(float_value)

            # String columns (10 columns)
            for j in range(1, 11):
                col_name = f"str_col_{j:02d}"
                str_value = f"value_{self.rng.randint(1, 1000)}"
                record[col_name] = self._maybe_null(str_value)

            # Boolean columns (5 columns)
            for j in range(1, 6):
                col_name = f"bool_col_{j:02d}"
                bool_value = self.rng.random() > 0.5
                record[col_name] = self._maybe_null(bool_value)

            # Date columns (5 columns)
            for j in range(1, 6):
                col_name = f"date_col_{j:02d}"
                date_value = self._random_date(run_date - timedelta(days=365), run_date)
                record[col_name] = self._maybe_null(date_value)

            # Timestamp columns (5 columns)
            for j in range(1, 6):
                col_name = f"ts_col_{j:02d}"
                ts_value = self._random_datetime(
                    datetime.combine(
                        run_date - timedelta(days=30), datetime.min.time()
                    ),
                    datetime.combine(run_date, datetime.min.time()),
                )
                record[col_name] = self._maybe_null(ts_value)

            # Sparse columns (high null rate) - 10 columns
            for j in range(1, 11):
                col_name = f"sparse_col_{j:02d}"
                sparse_value = f"sparse_value_{self.rng.randint(1, 100)}"
                record[col_name] = self._maybe_null(sparse_value, rate=self.sparse_rate)

            # Extra columns to reach column_count (if needed)
            current_cols = len(record)
            for j in range(current_cols, self.column_count):
                col_name = f"extra_col_{j:02d}"
                extra_value = f"extra_{self.rng.randint(1, 100)}"
                record[col_name] = self._maybe_null(extra_value)

            records.append(record)

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 with updates to various columns."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        update_count = min(20, len(t0_df) // 5)

        for idx in self.rng.sample(range(len(t0_df)), update_count):
            row = t0_df.iloc[idx].to_dict()

            # Update some columns randomly
            row["status"] = self._random_choice(
                ["active", "inactive", "pending", "archived"]
            )
            row["updated_at"] = datetime.combine(run_date, datetime.min.time())

            # Update a few numeric columns
            for j in self.rng.sample(range(1, 11), 3):
                row[f"int_col_{j:02d}"] = self.rng.randint(0, 10000)
                row[f"float_col_{j:02d}"] = self._random_amount(0, 100000)

            records.append(row)

        return pd.DataFrame(records)


class LateDataGenerator(BaseSyntheticGenerator):
    """Generate late-arriving and timezone-shifted time series data.

    Useful for testing:
    - Late data handling modes (allow, reject, quarantine)
    - Timezone conversion and normalization
    - Out-of-order event processing
    - Backfill scenarios
    """

    TIMEZONES = [
        "UTC",
        "America/New_York",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Paris",
        "Asia/Tokyo",
        "Asia/Shanghai",
        "Australia/Sydney",
    ]

    EVENT_TYPES = ["click", "view", "purchase", "signup", "logout"]

    def __init__(
        self,
        seed: int = 42,
        row_count: int = 100,
        late_rate: float = 0.15,
        timezone_diversity: bool = True,
    ):
        super().__init__(seed=seed, row_count=row_count)
        self.late_rate = late_rate
        self.timezone_diversity = timezone_diversity

    def _get_timezone(self) -> ZoneInfo:
        """Get a random timezone if diversity enabled, else UTC."""
        if self.timezone_diversity:
            tz_name = self._random_choice(self.TIMEZONES)
            return ZoneInfo(tz_name)
        return ZoneInfo("UTC")

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate T0 dataset with mixed timezones."""
        self.reset()
        records = []
        base_time = datetime.combine(
            run_date - timedelta(days=7), datetime.min.time(), tzinfo=ZoneInfo("UTC")
        )
        end_time = datetime.combine(
            run_date, datetime.min.time(), tzinfo=ZoneInfo("UTC")
        )

        for i in range(1, self.row_count + 1):
            # Generate event in a random timezone
            event_tz = self._get_timezone()
            event_time_utc = self._random_datetime(
                base_time.replace(tzinfo=None), end_time.replace(tzinfo=None)
            )
            event_time_utc = event_time_utc.replace(tzinfo=ZoneInfo("UTC"))
            event_time_local = event_time_utc.astimezone(event_tz)

            records.append(
                {
                    "event_id": self._generate_id("EVT", i, 10),
                    "user_id": self._generate_id("USR", (i % 100) + 1, 6),
                    "event_type": self._random_choice(self.EVENT_TYPES),
                    "event_ts_utc": event_time_utc,
                    "event_ts_local": event_time_local,
                    "timezone": str(event_tz),
                    "value": self._random_amount(0, 1000),
                    "session_id": hashlib.md5(
                        f"{i}{event_time_utc}".encode()
                    ).hexdigest()[:12],
                    "is_late": False,
                    "arrival_delay_hours": 0,
                    "created_at": event_time_utc,
                }
            )

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 with new events and some late arrivals."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        new_count = self.row_count // 4
        max_id = int(t0_df["event_id"].str.extract(r"(\d+)")[0].max())

        event_time_utc = datetime.combine(
            run_date, datetime.min.time(), tzinfo=ZoneInfo("UTC")
        )

        for i in range(1, new_count + 1):
            event_tz = self._get_timezone()
            current_time = event_time_utc + timedelta(minutes=i)
            event_time_local = current_time.astimezone(event_tz)

            records.append(
                {
                    "event_id": self._generate_id("EVT", max_id + i, 10),
                    "user_id": self._generate_id("USR", self.rng.randint(1, 100), 6),
                    "event_type": self._random_choice(self.EVENT_TYPES),
                    "event_ts_utc": current_time,
                    "event_ts_local": event_time_local,
                    "timezone": str(event_tz),
                    "value": self._random_amount(0, 1000),
                    "session_id": hashlib.md5(
                        f"{max_id + i}{current_time}".encode()
                    ).hexdigest()[:12],
                    "is_late": False,
                    "arrival_delay_hours": 0,
                    "created_at": current_time,
                }
            )

        return pd.DataFrame(records)

    def generate_late_data(
        self,
        run_date: date,
        t0_df: pd.DataFrame,
        min_delay_hours: int = 24,
        max_delay_hours: int = 168,  # Up to 7 days late
    ) -> pd.DataFrame:
        """Generate late-arriving events (backdated events that arrive after their event time).

        These events have:
        - event_ts_utc: The actual event time (in the past)
        - created_at: The arrival/processing time (run_date)
        - is_late: True
        - arrival_delay_hours: How late the event arrived
        """
        self.reset()
        records = []
        late_count = max(1, int(self.row_count * self.late_rate))
        max_id = int(t0_df["event_id"].str.extract(r"(\d+)")[0].max()) + 1000

        arrival_time = datetime.combine(
            run_date, datetime.min.time(), tzinfo=ZoneInfo("UTC")
        )

        for i in range(1, late_count + 1):
            # Event occurred in the past
            delay_hours = self.rng.randint(min_delay_hours, max_delay_hours)
            event_time_utc = arrival_time - timedelta(hours=delay_hours)

            event_tz = self._get_timezone()
            event_time_local = event_time_utc.astimezone(event_tz)

            records.append(
                {
                    "event_id": self._generate_id("EVT", max_id + i, 10),
                    "user_id": self._generate_id("USR", self.rng.randint(1, 100), 6),
                    "event_type": self._random_choice(self.EVENT_TYPES),
                    "event_ts_utc": event_time_utc,
                    "event_ts_local": event_time_local,
                    "timezone": str(event_tz),
                    "value": self._random_amount(0, 1000),
                    "session_id": hashlib.md5(
                        f"{max_id + i}{event_time_utc}".encode()
                    ).hexdigest()[:12],
                    "is_late": True,
                    "arrival_delay_hours": delay_hours,
                    "created_at": arrival_time,  # Arrived now, but event was in the past
                }
            )

        return pd.DataFrame(records)

    def generate_out_of_order_batch(
        self,
        run_date: date,
        batch_size: int = 50,
    ) -> pd.DataFrame:
        """Generate a batch of events that arrive out of order.

        Some events are backdated (late), some are on-time, creating
        a realistic out-of-order event stream.
        """
        self.reset()
        records = []
        arrival_time = datetime.combine(
            run_date, datetime.min.time(), tzinfo=ZoneInfo("UTC")
        )

        for i in range(1, batch_size + 1):
            # Decide if this event is late or on-time
            is_late = self.rng.random() < self.late_rate

            if is_late:
                # Event from the past arriving now
                delay_hours = self.rng.randint(1, 72)
                event_time_utc = arrival_time - timedelta(hours=delay_hours)
            else:
                # Event from recent past (within expected window)
                event_time_utc = arrival_time - timedelta(
                    minutes=self.rng.randint(0, 60)
                )
                delay_hours = 0

            event_tz = self._get_timezone()
            event_time_local = event_time_utc.astimezone(event_tz)

            records.append(
                {
                    "event_id": self._generate_id("EVT", 10000 + i, 10),
                    "user_id": self._generate_id("USR", self.rng.randint(1, 100), 6),
                    "event_type": self._random_choice(self.EVENT_TYPES),
                    "event_ts_utc": event_time_utc,
                    "event_ts_local": event_time_local,
                    "timezone": str(event_tz),
                    "value": self._random_amount(0, 1000),
                    "session_id": hashlib.md5(
                        f"{10000 + i}{event_time_utc}".encode()
                    ).hexdigest()[:12],
                    "is_late": is_late,
                    "arrival_delay_hours": delay_hours,
                    "created_at": arrival_time,
                }
            )

        # Shuffle to simulate out-of-order arrival
        self.rng.shuffle(records)
        return pd.DataFrame(records)


class SchemaEvolutionGenerator(BaseSyntheticGenerator):
    """Generate datasets that simulate schema evolution scenarios.

    Useful for testing:
    - Column additions (new nullable columns)
    - Type widening (int -> bigint, float -> double)
    - Column removals (deprecated columns)
    - Schema drift detection
    """

    def generate_v1_schema(self, run_date: date) -> pd.DataFrame:
        """Generate data with V1 schema (original columns)."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            records.append(
                {
                    "id": i,
                    "name": f"Entity {i}",
                    "value": self.rng.randint(0, 1000),  # int type
                    "score": round(self.rng.uniform(0, 100), 2),  # float type
                    "status": self._random_choice(["active", "inactive"]),
                    "created_at": self._random_datetime(
                        base_time, datetime.combine(run_date, datetime.min.time())
                    ),
                }
            )

        return pd.DataFrame(records)

    def generate_v2_schema_new_columns(self, run_date: date) -> pd.DataFrame:
        """Generate data with V2 schema (adds new nullable columns)."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            records.append(
                {
                    "id": i,
                    "name": f"Entity {i}",
                    "value": self.rng.randint(0, 1000),
                    "score": round(self.rng.uniform(0, 100), 2),
                    "status": self._random_choice(["active", "inactive"]),
                    "created_at": self._random_datetime(
                        base_time, datetime.combine(run_date, datetime.min.time())
                    ),
                    # New columns in V2
                    "category": self._random_choice(["A", "B", "C"])
                    if self.rng.random() > 0.3
                    else None,
                    "priority": self.rng.randint(1, 5)
                    if self.rng.random() > 0.2
                    else None,
                    "tags": ",".join(
                        self.rng.sample(
                            ["tag1", "tag2", "tag3", "tag4"], self.rng.randint(0, 3)
                        )
                    ),
                }
            )

        return pd.DataFrame(records)

    def generate_v3_schema_type_widening(self, run_date: date) -> pd.DataFrame:
        """Generate data with V3 schema (type widening: int -> bigint range)."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            records.append(
                {
                    "id": i,
                    "name": f"Entity {i}",
                    "value": self.rng.randint(
                        0, 10_000_000_000
                    ),  # Larger range (bigint)
                    "score": round(self.rng.uniform(0, 1000000), 6),  # More precision
                    "status": self._random_choice(
                        ["active", "inactive", "pending", "archived"]
                    ),
                    "created_at": self._random_datetime(
                        base_time, datetime.combine(run_date, datetime.min.time())
                    ),
                    "category": self._random_choice(["A", "B", "C"])
                    if self.rng.random() > 0.3
                    else None,
                    "priority": self.rng.randint(1, 5)
                    if self.rng.random() > 0.2
                    else None,
                    "tags": ",".join(
                        self.rng.sample(
                            ["tag1", "tag2", "tag3", "tag4"], self.rng.randint(0, 3)
                        )
                    ),
                }
            )

        return pd.DataFrame(records)

    def generate_v4_schema_column_removal(self, run_date: date) -> pd.DataFrame:
        """Generate data with V4 schema (removes deprecated columns).

        Simulates removing the 'tags' column that was added in V2.
        This tests backward-incompatible schema changes.
        """
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            records.append(
                {
                    "id": i,
                    "name": f"Entity {i}",
                    "value": self.rng.randint(0, 10_000_000_000),
                    "score": round(self.rng.uniform(0, 1000000), 6),
                    "status": self._random_choice(
                        ["active", "inactive", "pending", "archived"]
                    ),
                    "created_at": self._random_datetime(
                        base_time, datetime.combine(run_date, datetime.min.time())
                    ),
                    "category": self._random_choice(["A", "B", "C"])
                    if self.rng.random() > 0.3
                    else None,
                    "priority": self.rng.randint(1, 5)
                    if self.rng.random() > 0.2
                    else None,
                    # NOTE: 'tags' column removed in V4
                    # New column to replace it
                    "metadata_version": "v4",
                }
            )

        return pd.DataFrame(records)

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Default T0 generates V1 schema."""
        return self.generate_v1_schema(run_date)


# =============================================================================
# Dimension Table Generators (for Join Testing - Story 1.6)
# =============================================================================


class CustomerDimensionGenerator(BaseSyntheticGenerator):
    """Generate customer dimension data for join testing."""

    TIERS = ["bronze", "silver", "gold", "platinum"]
    REGIONS = ["north", "south", "east", "west", "central"]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate customer dimension table."""
        self.reset()
        records = []

        for i in range(1, self.row_count + 1):
            signup_date = self._random_date(
                run_date - timedelta(days=365), run_date - timedelta(days=30)
            )
            records.append(
                {
                    "customer_id": self._generate_id("CUST", i, 5),
                    "customer_name": f"Customer {i}",
                    "email": f"customer{i}@example.com",
                    "tier": self._random_choice(self.TIERS),
                    "region": self._random_choice(self.REGIONS),
                    "signup_date": signup_date,
                    "lifetime_value": self._random_amount(0, 50000),
                    "is_active": self.rng.random() > 0.1,
                }
            )

        return pd.DataFrame(records)


class ProductDimensionGenerator(BaseSyntheticGenerator):
    """Generate product dimension data for join testing."""

    CATEGORIES = ["electronics", "clothing", "home", "books", "sports", "food"]
    BRANDS = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate product dimension table."""
        self.reset()
        records = []

        for i in range(1, self.row_count + 1):
            records.append(
                {
                    "product_id": self._generate_id("PROD", i, 5),
                    "product_name": f"Product {i}",
                    "category": self._random_choice(self.CATEGORIES),
                    "brand": self._random_choice(self.BRANDS),
                    "unit_cost": self._random_amount(1, 200),
                    "unit_price": self._random_amount(5, 500),
                    "is_active": self.rng.random() > 0.05,
                    "created_date": self._random_date(
                        run_date - timedelta(days=365), run_date
                    ),
                }
            )

        return pd.DataFrame(records)


class SalesFactGenerator(BaseSyntheticGenerator):
    """Generate sales fact data with foreign keys for join testing."""

    def __init__(
        self,
        seed: int = 42,
        row_count: int = 100,
        customer_count: int = 50,
        product_count: int = 100,
    ):
        super().__init__(seed=seed, row_count=row_count)
        self.customer_count = customer_count
        self.product_count = product_count

    def generate_t0(self, run_date: date) -> pd.DataFrame:
        """Generate sales fact table with FK references."""
        self.reset()
        records = []
        base_time = datetime.combine(run_date - timedelta(days=30), datetime.min.time())

        for i in range(1, self.row_count + 1):
            sale_time = self._random_datetime(
                base_time, datetime.combine(run_date, datetime.min.time())
            )
            quantity = self.rng.randint(1, 10)
            unit_price = self._random_amount(10, 500)

            # Include some orphan keys for testing (customer/product that doesn't exist)
            customer_id = self._generate_id(
                "CUST", self.rng.randint(1, self.customer_count + 5), 5
            )
            product_id = self._generate_id(
                "PROD", self.rng.randint(1, self.product_count + 5), 5
            )

            records.append(
                {
                    "sale_id": self._generate_id("SALE", i, 8),
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": round(quantity * unit_price, 2),
                    "discount_amount": self._random_amount(0, 50)
                    if self.rng.random() > 0.7
                    else 0,
                    "sale_ts": sale_time,
                    "created_at": sale_time,
                }
            )

        return pd.DataFrame(records)

    def generate_t1(self, run_date: date, t0_df: pd.DataFrame) -> pd.DataFrame:
        """Generate T1 sales data (new sales only, facts are typically append-only)."""
        self.reset()
        for _ in range(self.row_count):
            self.rng.random()

        records = []
        new_count = self.row_count // 4
        max_id = int(t0_df["sale_id"].str.extract(r"(\d+)")[0].max())

        sale_time = datetime.combine(run_date, datetime.min.time())

        for i in range(1, new_count + 1):
            quantity = self.rng.randint(1, 10)
            unit_price = self._random_amount(10, 500)

            records.append(
                {
                    "sale_id": self._generate_id("SALE", max_id + i, 8),
                    "customer_id": self._generate_id(
                        "CUST", self.rng.randint(1, self.customer_count), 5
                    ),
                    "product_id": self._generate_id(
                        "PROD", self.rng.randint(1, self.product_count), 5
                    ),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": round(quantity * unit_price, 2),
                    "discount_amount": self._random_amount(0, 50)
                    if self.rng.random() > 0.7
                    else 0,
                    "sale_ts": sale_time + timedelta(minutes=i),
                    "created_at": sale_time + timedelta(minutes=i),
                }
            )

        return pd.DataFrame(records)


# =============================================================================
# Duplicate Injection Utilities (Story 1.4 - Deduplication Testing)
# =============================================================================


@dataclass
class DuplicateConfig:
    """Configuration for duplicate injection behavior."""

    exact_duplicate_rate: float = 0.05  # 5% exact duplicates
    near_duplicate_rate: float = (
        0.03  # 3% near duplicates (same key, minor differences)
    )
    out_of_order_rate: float = 0.02  # 2% out-of-order duplicate arrivals
    seed: int = 42


class DuplicateInjector:
    """Inject duplicates into DataFrames for deduplication testing.

    Useful for testing:
    - INCREMENTAL_MERGE deduplication logic
    - Silver layer duplicate detection and removal
    - Idempotency of pipeline runs
    - Out-of-order event handling

    Example usage:
        >>> injector = DuplicateInjector(seed=42)
        >>> df_with_dupes = injector.inject_exact_duplicates(df, rate=0.1)
        >>> df_with_near_dupes = injector.inject_near_duplicates(
        ...     df, key_columns=["order_id"], mutable_columns=["updated_at", "status"]
        ... )
    """

    def __init__(self, seed: int = 42, config: Optional[DuplicateConfig] = None):
        self.seed = seed
        self.config = config or DuplicateConfig(seed=seed)
        self.rng = random.Random(seed)

    def reset(self) -> None:
        """Reset random state for reproducibility."""
        self.rng = random.Random(self.seed)

    def inject_exact_duplicates(
        self,
        df: pd.DataFrame,
        rate: Optional[float] = None,
        count: Optional[int] = None,
    ) -> pd.DataFrame:
        """Inject exact duplicates (identical rows) into a DataFrame.

        Args:
            df: Source DataFrame
            rate: Fraction of rows to duplicate (default from config)
            count: Exact number of duplicates to add (overrides rate)

        Returns:
            DataFrame with duplicates appended
        """
        self.reset()
        rate = rate if rate is not None else self.config.exact_duplicate_rate

        if count is None:
            count = max(1, int(len(df) * rate))

        if count == 0 or len(df) == 0:
            return df.copy()

        # Select random rows to duplicate
        indices = self.rng.choices(range(len(df)), k=count)
        duplicates = df.iloc[indices].copy()

        # Append duplicates
        result = pd.concat([df, duplicates], ignore_index=True)
        return result

    def inject_near_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        mutable_columns: Optional[List[str]] = None,
        rate: Optional[float] = None,
        count: Optional[int] = None,
        timestamp_column: Optional[str] = None,
        timestamp_offset_seconds: int = 1,
    ) -> pd.DataFrame:
        """Inject near duplicates (same key, minor field differences).

        Near duplicates have the same primary key but different values in
        mutable columns (e.g., updated_at, status). This simulates:
        - Multiple updates to the same record arriving in the same batch
        - Late-arriving updates that should be deduplicated

        Args:
            df: Source DataFrame
            key_columns: Columns that identify a unique record (primary key)
            mutable_columns: Columns to modify in duplicates (auto-detected if None)
            rate: Fraction of rows to create near duplicates for
            count: Exact number of near duplicates (overrides rate)
            timestamp_column: Column to increment for newer versions
            timestamp_offset_seconds: Seconds to add to timestamp for each duplicate

        Returns:
            DataFrame with near duplicates appended
        """
        self.reset()
        rate = rate if rate is not None else self.config.near_duplicate_rate

        if count is None:
            count = max(1, int(len(df) * rate))

        if count == 0 or len(df) == 0:
            return df.copy()

        # Auto-detect mutable columns if not provided
        if mutable_columns is None:
            mutable_columns = self._detect_mutable_columns(df, key_columns)

        # Select random rows to create near duplicates from
        indices = self.rng.choices(range(len(df)), k=count)
        near_dupes = []

        for idx in indices:
            row = df.iloc[idx].to_dict()

            # Modify mutable columns
            for col in mutable_columns:
                if col not in row:
                    continue
                row[col] = self._mutate_value(row[col], col)

            # Increment timestamp if specified
            if timestamp_column and timestamp_column in row:
                ts_value = row[timestamp_column]
                if isinstance(ts_value, datetime):
                    row[timestamp_column] = ts_value + timedelta(
                        seconds=timestamp_offset_seconds
                    )
                elif isinstance(ts_value, date):
                    row[timestamp_column] = ts_value + timedelta(days=1)

            near_dupes.append(row)

        near_dupes_df = pd.DataFrame(near_dupes)
        result = pd.concat([df, near_dupes_df], ignore_index=True)
        return result

    def inject_out_of_order_duplicates(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        timestamp_column: str,
        rate: Optional[float] = None,
        count: Optional[int] = None,
    ) -> pd.DataFrame:
        """Inject duplicates that arrive out of order (older version after newer).

        This simulates scenarios where:
        - An older update arrives after a newer one
        - Reprocessing historical data creates duplicate arrivals
        - Network delays cause out-of-order message delivery

        Args:
            df: Source DataFrame (assumed to be in chronological order)
            key_columns: Columns that identify a unique record
            timestamp_column: Column containing the event/update timestamp
            rate: Fraction of rows to create out-of-order duplicates for
            count: Exact number of out-of-order duplicates

        Returns:
            DataFrame with out-of-order duplicates inserted at random positions
        """
        self.reset()
        rate = rate if rate is not None else self.config.out_of_order_rate

        if count is None:
            count = max(1, int(len(df) * rate))

        if count == 0 or len(df) == 0:
            return df.copy()

        result = df.copy()

        # Select rows from the first half to duplicate (older records)
        first_half_size = len(df) // 2
        if first_half_size == 0:
            return result

        indices = self.rng.choices(range(first_half_size), k=count)

        for idx in indices:
            row = df.iloc[idx].to_dict()

            # Make the timestamp slightly older to simulate late arrival of old data
            if timestamp_column in row:
                ts_value = row[timestamp_column]
                if isinstance(ts_value, datetime):
                    row[timestamp_column] = ts_value - timedelta(
                        seconds=self.rng.randint(1, 3600)
                    )

            # Insert at a random position in the second half (simulating late arrival)
            insert_pos = self.rng.randint(first_half_size, len(result))
            row_df = pd.DataFrame([row])
            result = pd.concat(
                [result.iloc[:insert_pos], row_df, result.iloc[insert_pos:]],
                ignore_index=True,
            )

        return result

    def inject_all_duplicate_types(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        timestamp_column: Optional[str] = None,
        mutable_columns: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Inject all types of duplicates using configured rates.

        Convenience method that applies exact, near, and out-of-order duplicates
        in sequence using the rates from DuplicateConfig.

        Args:
            df: Source DataFrame
            key_columns: Columns that identify a unique record
            timestamp_column: Column for timestamp-based operations
            mutable_columns: Columns to modify for near duplicates

        Returns:
            DataFrame with all duplicate types injected
        """
        result = df.copy()

        # Apply exact duplicates
        result = self.inject_exact_duplicates(result)

        # Apply near duplicates
        result = self.inject_near_duplicates(
            result,
            key_columns=key_columns,
            mutable_columns=mutable_columns,
            timestamp_column=timestamp_column,
        )

        # Apply out-of-order duplicates if timestamp column provided
        if timestamp_column:
            result = self.inject_out_of_order_duplicates(
                result,
                key_columns=key_columns,
                timestamp_column=timestamp_column,
            )

        return result

    def _detect_mutable_columns(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
    ) -> List[str]:
        """Auto-detect columns that are likely mutable (can be modified in updates)."""
        mutable_patterns = [
            "updated",
            "modified",
            "status",
            "state",
            "amount",
            "value",
            "count",
        ]
        mutable_cols = []

        for col in df.columns:
            if col in key_columns:
                continue
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in mutable_patterns):
                mutable_cols.append(col)

        # If no pattern matches, use any non-key numeric or string columns
        if not mutable_cols:
            for col in df.columns:
                if col in key_columns:
                    continue
                if df[col].dtype in ["int64", "float64", "object"]:
                    mutable_cols.append(col)
                    if len(mutable_cols) >= 3:
                        break

        return mutable_cols

    def _mutate_value(self, value: Any, column_name: str) -> Any:
        """Mutate a value slightly for near-duplicate generation."""
        if value is None:
            return value

        col_lower = column_name.lower()

        # Status-like columns: pick a different status
        if "status" in col_lower or "state" in col_lower:
            if isinstance(value, str):
                statuses = ["updated", "modified", "changed", "processed"]
                return self.rng.choice(statuses)

        # Amount/value columns: slight adjustment
        if "amount" in col_lower or "value" in col_lower or "price" in col_lower:
            if isinstance(value, (int, float)):
                adjustment = self.rng.uniform(-0.01, 0.01)  # +/- 1%
                return round(value * (1 + adjustment), 2)

        # Count columns: increment by 1
        if "count" in col_lower or "quantity" in col_lower:
            if isinstance(value, int):
                return value + self.rng.randint(0, 2)

        # String columns: append marker
        if isinstance(value, str):
            return f"{value}_v{self.rng.randint(2, 9)}"

        # Numeric columns: slight adjustment
        if isinstance(value, float):
            return round(value * (1 + self.rng.uniform(-0.05, 0.05)), 4)
        if isinstance(value, int):
            return value + self.rng.randint(-1, 1)

        return value

    def get_duplicate_stats(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
    ) -> Dict[str, Any]:
        """Analyze a DataFrame for duplicate statistics.

        Returns:
            Dictionary with duplicate counts and percentages
        """
        total_rows = len(df)
        unique_keys = df[key_columns].drop_duplicates()
        unique_count = len(unique_keys)
        duplicate_count = total_rows - unique_count

        # Find rows with duplicates
        key_counts = df.groupby(key_columns).size()
        keys_with_dupes = key_counts[key_counts > 1]

        return {
            "total_rows": total_rows,
            "unique_keys": unique_count,
            "duplicate_rows": duplicate_count,
            "duplicate_rate": duplicate_count / total_rows if total_rows > 0 else 0,
            "keys_with_duplicates": len(keys_with_dupes),
            "max_duplicates_per_key": int(key_counts.max())
            if len(key_counts) > 0
            else 0,
        }


def create_duplicate_injector(
    seed: int = 42,
    exact_rate: float = 0.05,
    near_rate: float = 0.03,
    out_of_order_rate: float = 0.02,
) -> DuplicateInjector:
    """Create a DuplicateInjector with specified configuration.

    Args:
        seed: Random seed for reproducibility
        exact_rate: Rate of exact duplicates (default 5%)
        near_rate: Rate of near duplicates (default 3%)
        out_of_order_rate: Rate of out-of-order duplicates (default 2%)

    Returns:
        Configured DuplicateInjector instance

    Example:
        >>> injector = create_duplicate_injector(seed=42, exact_rate=0.1)
        >>> df_with_dupes = injector.inject_exact_duplicates(df)
    """
    config = DuplicateConfig(
        exact_duplicate_rate=exact_rate,
        near_duplicate_rate=near_rate,
        out_of_order_rate=out_of_order_rate,
        seed=seed,
    )
    return DuplicateInjector(seed=seed, config=config)


# Factory function for creating generators
def create_generator(
    domain: str, seed: int = 42, row_count: int = 100
) -> BaseSyntheticGenerator:
    """Create a generator for the specified domain.

    Available domains:
    - claims: Healthcare claims data
    - orders: E-commerce orders
    - transactions: Financial transactions
    - state: SCD-style state changes
    - nested_json: Nested/JSON structures (Story 1.5)
    - wide_schema: Wide tables with 60+ columns (Story 1.5)
    - late_data: Late-arriving timezone-shifted events (Story 1.5)
    - schema_evolution: Schema evolution scenarios (Story 1.5)
    - customer_dim: Customer dimension table (Story 1.6)
    - product_dim: Product dimension table (Story 1.6)
    - sales_fact: Sales fact table (Story 1.6)

    For duplicate injection, use create_duplicate_injector() instead.
    """
    generators: Dict[str, Type[BaseSyntheticGenerator]] = {
        "claims": ClaimsGenerator,
        "orders": OrdersGenerator,
        "transactions": TransactionsGenerator,
        "state": StateChangeGenerator,
        # Edge case generators (Story 1.5)
        "nested_json": NestedJsonGenerator,
        "wide_schema": WideSchemaGenerator,
        "late_data": LateDataGenerator,
        "schema_evolution": SchemaEvolutionGenerator,
        # Dimension/fact generators (Story 1.6)
        "customer_dim": CustomerDimensionGenerator,
        "product_dim": ProductDimensionGenerator,
        "sales_fact": SalesFactGenerator,
    }
    generator_class: Type[BaseSyntheticGenerator] = generators.get(
        domain, BaseSyntheticGenerator
    )
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
