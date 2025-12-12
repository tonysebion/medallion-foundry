"""Mock Database Extractor End-to-End Tests.

Story 7: Tests that verify database extractor with mocked database responses including:
- Single table extraction with basic query
- Custom SQL query extraction
- Watermark-based incremental queries
- Connection failure and recovery
- Large result set chunking behavior

These tests use unittest.mock to mock database connections without
requiring real database endpoints.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union
from unittest.mock import patch, MagicMock

import pytest

from core.domain.adapters.extractors.db_extractor import DbExtractor


# =============================================================================
# Test Data Generators
# =============================================================================


def generate_db_records(
    count: int,
    start_id: int = 1,
    cursor_column: Optional[str] = None,
) -> List[Union[Tuple[str, str, str, float], Tuple[str, str, str, float, str]]]:
    """Generate mock database row tuples.

    Args:
        count: Number of records to generate
        start_id: Starting ID for records
        cursor_column: If provided, include a cursor value

    Returns:
        List of row tuples as returned by pyodbc cursor
    """
    rows: List[Union[Tuple[str, str, str, float], Tuple[str, str, str, float, str]]] = []
    for i in range(count):
        row_id = start_id + i
        if cursor_column:
            # Include cursor value (timestamp-like string)
            cursor_val = f"2024-01-{15 + (i % 10):02d}T{(i % 24):02d}:00:00Z"
            rows.append((
                f"REC{row_id:06d}",
                f"Record {row_id}",
                "active",
                row_id * 100.0,
                cursor_val,
            ))
        else:
            rows.append((
                f"REC{row_id:06d}",
                f"Record {row_id}",
                "active",
                row_id * 100.0,
            ))
    return rows


def get_column_names(with_cursor: bool = False) -> List[str]:
    """Get column names for test data."""
    cols = ["id", "name", "status", "amount"]
    if with_cursor:
        cols.append("updated_at")
    return cols


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def db_extractor() -> DbExtractor:
    """Provide fresh DbExtractor instance."""
    return DbExtractor()


@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Provide base database configuration."""
    return {
        "source": {
            "system": "test_system",
            "table": "test_table",
            "db": {
                "driver": "pyodbc",
                "conn_str_env": "TEST_DB_CONN",
                "base_query": "SELECT id, name, status, amount FROM test_table",
            },
            "run": {
                "load_pattern": "snapshot",
                "timeout_seconds": 30,
            },
        },
    }


@pytest.fixture
def run_date() -> date:
    """Provide consistent run date."""
    return date(2024, 1, 15)


# =============================================================================
# Mock Database Connection Helper
# =============================================================================


class MockCursor:
    """Mock pyodbc cursor for testing."""

    def __init__(
        self,
        rows: List[Tuple[Any, ...]],
        columns: List[str],
        batch_size: int = 10000,
    ):
        self.rows = rows
        self.columns = columns
        self.batch_size = batch_size
        self.position = 0
        self.description = [(col,) for col in columns]

    def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Mock execute - just store for inspection."""
        self.executed_query = query
        self.executed_params = params

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """Return next batch of rows."""
        fetch_size = size or self.batch_size
        if self.position >= len(self.rows):
            return []
        end = min(self.position + fetch_size, len(self.rows))
        batch = self.rows[self.position:end]
        self.position = end
        return batch

    def close(self) -> None:
        """Mock close."""
        pass


class MockConnection:
    """Mock pyodbc connection for testing."""

    def __init__(self, cursor: MockCursor):
        self._cursor = cursor

    def cursor(self) -> MockCursor:
        """Return mock cursor."""
        return self._cursor

    def close(self) -> None:
        """Mock close."""
        pass


# =============================================================================
# Single Table Extraction Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorSingleTable:
    """Test database extractor with single table extraction."""

    def test_single_table_extraction_succeeds(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify successful extraction from single table."""
        rows = generate_db_records(10)
        columns = get_column_names()

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 10
        assert result[0]["id"] == "REC000001"
        assert result[0]["name"] == "Record 1"
        assert result[0]["status"] == "active"

    def test_empty_table_returns_empty_list(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify empty result set handling."""
        columns = get_column_names()
        mock_cursor = MockCursor([], columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 0
        assert cursor is None

    def test_column_mapping_preserved(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify column names are correctly mapped to record keys."""
        rows = [("ID001", "Test Name", "pending", 999.99)]
        columns = ["id", "name", "status", "amount"]

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 1
        assert result[0] == {
            "id": "ID001",
            "name": "Test Name",
            "status": "pending",
            "amount": 999.99,
        }


# =============================================================================
# Incremental Query Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorIncremental:
    """Test database extractor incremental/watermark handling."""

    def test_cursor_computed_from_records(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify cursor is computed from cursor_column in records."""
        rows = generate_db_records(5, cursor_column="updated_at")
        columns = get_column_names(with_cursor=True)

        # Enable incremental
        base_config["source"]["db"]["incremental"] = {
            "enabled": True,
            "cursor_column": "updated_at",
        }
        base_config["source"]["db"]["base_query"] = (
            "SELECT id, name, status, amount, updated_at FROM test_table"
        )

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 5
        assert cursor is not None
        # Cursor should be max updated_at value
        assert "2024-01" in cursor

    def test_no_cursor_without_incremental_config(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify no cursor returned when incremental not configured."""
        rows = generate_db_records(5)
        columns = get_column_names()

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert cursor is None

    def test_get_watermark_config_with_incremental(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
    ):
        """Verify watermark config returned when incremental configured."""
        base_config["source"]["db"]["incremental"] = {
            "enabled": True,
            "cursor_column": "updated_at",
            "cursor_type": "timestamp",
        }

        config = db_extractor.get_watermark_config(base_config)

        assert config is not None
        assert config["enabled"] is True
        assert config["column"] == "updated_at"
        assert config["type"] == "timestamp"

    def test_get_watermark_config_without_incremental(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
    ):
        """Verify no watermark config when incremental not configured."""
        config = db_extractor.get_watermark_config(base_config)
        assert config is None


# =============================================================================
# Error Handling Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorErrors:
    """Test database extractor error handling."""

    def test_missing_conn_str_env_raises_error(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify error when connection string env var not set."""
        from core.foundation.primitives.exceptions import ExtractionError

        # Remove env var
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ExtractionError) as exc_info:
                db_extractor.fetch_records(base_config, run_date)

        assert "TEST_DB_CONN" in str(exc_info.value)

    def test_missing_conn_str_env_config_raises_error(
        self,
        db_extractor: DbExtractor,
        run_date: date,
    ):
        """Verify error when conn_str_env not in config."""
        from core.foundation.primitives.exceptions import ExtractionError

        config = {
            "source": {
                "system": "test_system",
                "table": "test_table",
                "db": {
                    "driver": "pyodbc",
                    # Missing: conn_str_env
                    "base_query": "SELECT * FROM test",
                },
                "run": {},
            },
        }

        with pytest.raises(ExtractionError) as exc_info:
            db_extractor.fetch_records(config, run_date)

        assert "conn_str_env" in str(exc_info.value)

    def test_connection_error_raises(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify connection errors are raised after retries exhausted."""
        from core.foundation.primitives.exceptions import RetryExhaustedError

        with patch.dict("os.environ", {"TEST_DB_CONN": "invalid://connection"}):
            with patch("pyodbc.connect") as mock_connect:
                mock_connect.side_effect = Exception("Connection refused")

                with pytest.raises((Exception, RetryExhaustedError)):
                    db_extractor.fetch_records(base_config, run_date)

    def test_query_error_raises(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify query errors are raised (wrapped in retry error)."""
        from core.foundation.primitives.exceptions import ExtractionError, RetryExhaustedError

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Invalid SQL syntax")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                with pytest.raises((ExtractionError, RetryExhaustedError)):
                    db_extractor.fetch_records(base_config, run_date)


# =============================================================================
# Result Set Chunking Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorChunking:
    """Test database extractor large result set handling."""

    def test_large_result_set_chunked(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify large result sets are fetched in batches."""
        # Create 25 records, batch size is 10
        rows = generate_db_records(25)
        columns = get_column_names()

        # Use smaller batch size
        base_config["source"]["db"]["fetch_batch_size"] = 10

        mock_cursor = MockCursor(rows, columns, batch_size=10)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        # All records should be collected despite batching
        assert len(result) == 25
        assert result[0]["id"] == "REC000001"
        assert result[24]["id"] == "REC000025"

    def test_exact_batch_size_works(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify exact batch size boundary works correctly."""
        # Create exactly 10 records with batch size 10
        rows = generate_db_records(10)
        columns = get_column_names()

        base_config["source"]["db"]["fetch_batch_size"] = 10

        mock_cursor = MockCursor(rows, columns, batch_size=10)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 10


# =============================================================================
# Driver Configuration Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorDriverConfig:
    """Test database extractor driver configuration."""

    def test_default_driver_is_pyodbc(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify default driver is pyodbc."""
        rows = generate_db_records(3)
        columns = get_column_names()

        # Remove explicit driver config
        del base_config["source"]["db"]["driver"]

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn) as mock_connect:
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        # Verify pyodbc.connect was called
        mock_connect.assert_called_once_with("mock://connection")
        assert len(result) == 3

    def test_unsupported_driver_raises_error(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify unsupported driver raises error (wrapped in retry error)."""
        from core.foundation.primitives.exceptions import ExtractionError, RetryExhaustedError

        base_config["source"]["db"]["driver"] = "unsupported_driver"

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with pytest.raises((ExtractionError, RetryExhaustedError)) as exc_info:
                db_extractor.fetch_records(base_config, run_date)

        assert "unsupported_driver" in str(exc_info.value)


# =============================================================================
# Query Customization Tests
# =============================================================================


@pytest.mark.integration
class TestDbExtractorQueryCustomization:
    """Test database extractor custom query handling."""

    def test_custom_query_executed(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify custom base_query is executed."""
        rows = [("ID1", "Custom Result")]
        columns = ["custom_id", "custom_name"]

        base_config["source"]["db"]["base_query"] = (
            "SELECT custom_id, custom_name FROM custom_table WHERE active = 1"
        )

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 1
        assert result[0]["custom_id"] == "ID1"
        assert result[0]["custom_name"] == "Custom Result"

    def test_query_with_complex_columns(
        self,
        db_extractor: DbExtractor,
        base_config: Dict[str, Any],
        run_date: date,
    ):
        """Verify queries with computed/complex columns work."""
        rows = [
            ("ID1", 100, 200, 300),
            ("ID2", 150, 250, 400),
        ]
        columns = ["id", "amount", "tax", "total"]

        base_config["source"]["db"]["base_query"] = (
            "SELECT id, amount, tax, (amount + tax) as total FROM orders"
        )

        mock_cursor = MockCursor(rows, columns)
        mock_conn = MockConnection(mock_cursor)

        with patch.dict("os.environ", {"TEST_DB_CONN": "mock://connection"}):
            with patch("pyodbc.connect", return_value=mock_conn):
                result, cursor = db_extractor.fetch_records(base_config, run_date)

        assert len(result) == 2
        assert result[0]["total"] == 300
        assert result[1]["total"] == 400
