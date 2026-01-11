"""Shared fixtures for pattern combination tests."""

from __future__ import annotations

from datetime import datetime
from typing import List, Dict, Any

import ibis
import pandas as pd
import pytest


@pytest.fixture
def con():
    """Create an Ibis DuckDB connection."""
    return ibis.duckdb.connect()


@pytest.fixture
def sample_state_data() -> List[Dict[str, Any]]:
    """Sample data for STATE entity tests (customer records)."""
    return [
        # T0 batch - initial load
        {
            "customer_id": "C001",
            "name": "Alice",
            "status": "active",
            "updated_at": datetime(2025, 1, 10),
        },
        {
            "customer_id": "C002",
            "name": "Bob",
            "status": "active",
            "updated_at": datetime(2025, 1, 10),
        },
        {
            "customer_id": "C003",
            "name": "Charlie",
            "status": "pending",
            "updated_at": datetime(2025, 1, 10),
        },
    ]


@pytest.fixture
def sample_state_data_update() -> List[Dict[str, Any]]:
    """Sample data for STATE entity update batch."""
    return [
        # T1 batch - updates
        {
            "customer_id": "C001",
            "name": "Alice Updated",
            "status": "inactive",
            "updated_at": datetime(2025, 1, 15),
        },
        {
            "customer_id": "C002",
            "name": "Bob",
            "status": "active",
            "updated_at": datetime(2025, 1, 10),
        },  # No change
        {
            "customer_id": "C004",
            "name": "Diana",
            "status": "active",
            "updated_at": datetime(2025, 1, 15),
        },  # New
    ]


@pytest.fixture
def sample_event_data() -> List[Dict[str, Any]]:
    """Sample data for EVENT entity tests (click events)."""
    return [
        {
            "event_id": "E001",
            "user_id": "U1",
            "event_type": "click",
            "ts": datetime(2025, 1, 10, 10, 0),
        },
        {
            "event_id": "E002",
            "user_id": "U1",
            "event_type": "view",
            "ts": datetime(2025, 1, 10, 10, 5),
        },
        {
            "event_id": "E003",
            "user_id": "U2",
            "event_type": "click",
            "ts": datetime(2025, 1, 10, 11, 0),
        },
    ]


@pytest.fixture
def sample_event_data_append() -> List[Dict[str, Any]]:
    """Sample data for EVENT entity append batch."""
    return [
        {
            "event_id": "E004",
            "user_id": "U1",
            "event_type": "purchase",
            "ts": datetime(2025, 1, 10, 12, 0),
        },
        {
            "event_id": "E005",
            "user_id": "U3",
            "event_type": "click",
            "ts": datetime(2025, 1, 10, 12, 30),
        },
    ]


@pytest.fixture
def sample_cdc_data() -> List[Dict[str, Any]]:
    """Sample CDC data with operation types."""
    return [
        {
            "op": "I",
            "product_id": "P001",
            "name": "Widget",
            "price": 10.00,
            "updated_at": datetime(2025, 1, 10),
        },
        {
            "op": "I",
            "product_id": "P002",
            "name": "Gadget",
            "price": 20.00,
            "updated_at": datetime(2025, 1, 10),
        },
        {
            "op": "U",
            "product_id": "P001",
            "name": "Widget Pro",
            "price": 15.00,
            "updated_at": datetime(2025, 1, 15),
        },
        {
            "op": "D",
            "product_id": "P002",
            "name": "Gadget",
            "price": 20.00,
            "updated_at": datetime(2025, 1, 15),
        },
    ]


def create_memtable(data: List[Dict[str, Any]]) -> ibis.Table:
    """Create an Ibis memtable from dict data."""
    return ibis.memtable(pd.DataFrame(data))
