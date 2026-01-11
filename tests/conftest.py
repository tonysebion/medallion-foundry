"""Test fixtures for bronze-foundry test suite.

Provides common fixtures for:
- Synthetic data generation
- Temporary directories
- Test configurations
- Mock services
"""

import os
import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List

import pandas as pd
import pytest


# Test scale factor for controlling data sizes
TEST_SCALE_FACTOR = float(os.environ.get("TEST_SCALE_FACTOR", "1.0"))


def scale_rows(base_count: int) -> int:
    """Scale row count by TEST_SCALE_FACTOR."""
    return max(1, int(base_count * TEST_SCALE_FACTOR))


@pytest.fixture(scope="session", autouse=True)
def clean_checkpoint_state() -> None:
    """Ensure local checkpoint store starts empty for each test session."""
    checkpoint_dir = Path(".state/checkpoints")
    if checkpoint_dir.exists():
        for child in checkpoint_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    else:
        checkpoint_dir.mkdir(parents=True, exist_ok=True)


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def bronze_output_dir(temp_dir: Path) -> Path:
    """Create Bronze output directory."""
    bronze_dir = temp_dir / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir


@pytest.fixture
def silver_output_dir(temp_dir: Path) -> Path:
    """Create Silver output directory."""
    silver_dir = temp_dir / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)
    return silver_dir


@pytest.fixture
def run_date() -> date:
    """Standard run date for tests."""
    return date(2024, 1, 15)


@pytest.fixture
def base_config(temp_dir: Path) -> Dict[str, Any]:
    """Base configuration for tests."""
    return {
        "config_version": 1,
        "pipeline_id": "test_pipeline",
        "layer": "bronze",
        "domain": "test_domain",
        "environment": "test",
        "data_classification": "internal",
        "owners": {
            "semantic_owner": "test-semantic",
            "technical_owner": "test-technical",
        },
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(temp_dir / "bronze"),
            }
        },
        "source": {
            "system": "test_system",
            "table": "test_table",
            "type": "file",
            "file": {
                "path": str(temp_dir / "input"),
                "format": "csv",
            },
            "run": {
                "load_pattern": "snapshot",
                "local_output_dir": str(temp_dir / "output"),
                "write_csv": True,
                "write_parquet": True,
            },
        },
    }


@pytest.fixture
def db_config(temp_dir: Path) -> Dict[str, Any]:
    """Database source configuration."""
    return {
        "config_version": 1,
        "pipeline_id": "test_db_pipeline",
        "layer": "bronze",
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(temp_dir / "bronze"),
            }
        },
        "source": {
            "system": "test_db",
            "table": "test_table",
            "type": "db_table",
            "db": {
                "conn_str_env": "TEST_DB_CONN",
                "base_query": "SELECT * FROM test_table",
            },
            "run": {
                "load_pattern": "snapshot",
                "local_output_dir": str(temp_dir / "output"),
            },
        },
    }


@pytest.fixture
def api_config(temp_dir: Path) -> Dict[str, Any]:
    """API source configuration."""
    return {
        "config_version": 1,
        "pipeline_id": "test_api_pipeline",
        "layer": "bronze",
        "platform": {
            "bronze": {
                "storage_backend": "local",
                "local_path": str(temp_dir / "bronze"),
            }
        },
        "source": {
            "system": "test_api",
            "table": "test_endpoint",
            "type": "api",
            "api": {
                "base_url": "https://api.example.com",
                "endpoint": "/data",
            },
            "run": {
                "load_pattern": "incremental_append",
                "local_output_dir": str(temp_dir / "output"),
            },
        },
    }


@pytest.fixture
def silver_config() -> Dict[str, Any]:
    """Silver transformation configuration."""
    return {
        "enabled": True,
        "domain": "test_domain",
        "entity": "test_entity",
        "version": 1,
        "primary_keys": ["id"],
        "order_column": "updated_at",
        "write_parquet": True,
        "write_csv": False,
    }


@pytest.fixture
def schema_config() -> Dict[str, Any]:
    """Schema configuration for validation tests."""
    return {
        "expected_columns": [
            {"name": "id", "type": "integer", "nullable": False, "primary_key": True},
            {"name": "name", "type": "string", "nullable": False},
            {
                "name": "amount",
                "type": "decimal",
                "nullable": True,
                "precision": 18,
                "scale": 2,
            },
            {"name": "created_at", "type": "timestamp", "nullable": True},
            {"name": "is_active", "type": "boolean", "nullable": True},
        ],
        "primary_keys": ["id"],
    }


@pytest.fixture
def quality_rules() -> List[Dict[str, Any]]:
    """Quality rules for validation tests."""
    return [
        {"id": "rule_not_null_id", "expression": "id IS NOT NULL", "level": "error"},
        {"id": "rule_positive_amount", "expression": "amount >= 0", "level": "error"},
        {"id": "rule_name_not_empty", "expression": "LEN(name) > 0", "level": "warn"},
    ]


# Time series test dates
@pytest.fixture
def t0_date() -> date:
    """T0: Initial load date."""
    return date(2024, 1, 1)


@pytest.fixture
def t1_date() -> date:
    """T1: First incremental load date."""
    return date(2024, 1, 2)


@pytest.fixture
def t2_date() -> date:
    """T2: Second incremental / late data date."""
    return date(2024, 1, 3)


# Sample DataFrames for testing
@pytest.fixture
def sample_events_df() -> pd.DataFrame:
    """Sample event data for testing."""
    rows = scale_rows(100)
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    return pd.DataFrame(
        {
            "event_id": [f"EVT{i:05d}" for i in range(1, rows + 1)],
            "user_id": [f"USR{(i % 10) + 1:03d}" for i in range(rows)],
            "event_type": [["click", "view", "purchase"][i % 3] for i in range(rows)],
            "event_ts": [base_time + timedelta(minutes=i) for i in range(rows)],
            "amount": [
                round(100 + (i * 1.5), 2) if i % 3 == 2 else None for i in range(rows)
            ],
        }
    )


@pytest.fixture
def sample_state_df() -> pd.DataFrame:
    """Sample state/entity data for testing."""
    rows = scale_rows(50)
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    return pd.DataFrame(
        {
            "id": list(range(1, rows + 1)),
            "name": [f"Entity {i}" for i in range(1, rows + 1)],
            "status": [["active", "inactive", "pending"][i % 3] for i in range(rows)],
            "amount": [round(100.0 + (i * 10.5), 2) for i in range(rows)],
            "updated_at": [base_time + timedelta(hours=i) for i in range(rows)],
            "is_active": [i % 2 == 0 for i in range(rows)],
        }
    )


@pytest.fixture
def sample_claims_df() -> pd.DataFrame:
    """Sample healthcare claims data for domain-specific testing."""
    rows = scale_rows(100)
    base_date = date(2024, 1, 1)
    return pd.DataFrame(
        {
            "claim_id": [f"CLM{i:08d}" for i in range(1, rows + 1)],
            "patient_id": [f"PAT{(i % 20) + 1:05d}" for i in range(rows)],
            "provider_id": [f"PRV{(i % 10) + 1:04d}" for i in range(rows)],
            "service_date": [base_date + timedelta(days=i % 30) for i in range(rows)],
            "billed_amount": [round(100 + (i * 25.5), 2) for i in range(rows)],
            "paid_amount": [round(80 + (i * 20.0), 2) for i in range(rows)],
            "diagnosis_code": [f"ICD{100 + (i % 50)}" for i in range(rows)],
            "procedure_code": [f"CPT{10000 + (i % 100)}" for i in range(rows)],
            "claim_status": [
                ["submitted", "processing", "paid", "denied"][i % 4]
                for i in range(rows)
            ],
            "created_at": [
                datetime.combine(
                    base_date + timedelta(days=i % 30), datetime.min.time()
                )
                for i in range(rows)
            ],
        }
    )


@pytest.fixture
def sample_orders_df() -> pd.DataFrame:
    """Sample retail orders data for domain-specific testing."""
    rows = scale_rows(100)
    base_time = datetime(2024, 1, 1, 9, 0, 0)
    return pd.DataFrame(
        {
            "order_id": [f"ORD{i:08d}" for i in range(1, rows + 1)],
            "customer_id": [f"CUST{(i % 50) + 1:05d}" for i in range(rows)],
            "product_id": [f"PROD{(i % 25) + 1:04d}" for i in range(rows)],
            "quantity": [(i % 5) + 1 for i in range(rows)],
            "unit_price": [round(10 + (i % 100) * 1.5, 2) for i in range(rows)],
            "total_amount": [
                round(((i % 5) + 1) * (10 + (i % 100) * 1.5), 2) for i in range(rows)
            ],
            "order_status": [
                ["pending", "confirmed", "shipped", "delivered"][i % 4]
                for i in range(rows)
            ],
            "order_ts": [base_time + timedelta(hours=i) for i in range(rows)],
        }
    )


# Mock fixtures
@pytest.fixture
def mock_db_connection(mocker):
    """Mock database connection."""
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def mock_api_response():
    """Sample API response data."""
    return {
        "data": [
            {"id": 1, "name": "Item 1", "value": 100},
            {"id": 2, "name": "Item 2", "value": 200},
            {"id": 3, "name": "Item 3", "value": 300},
        ],
        "pagination": {
            "page": 1,
            "per_page": 100,
            "total": 3,
            "has_more": False,
        },
    }
