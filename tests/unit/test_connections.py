"""Tests for pipelines.lib.connections module."""

from unittest.mock import MagicMock, patch

import pytest

from pipelines.lib.connections import (
    close_all_connections,
    close_connection,
    get_connection,
    get_connection_count,
    list_connections,
    _connections,
)


@pytest.fixture(autouse=True)
def clean_connections():
    """Ensure connection registry is clean before and after each test."""
    close_all_connections()
    yield
    close_all_connections()


# ============================================
# Connection registry tests
# ============================================


class TestConnectionRegistry:
    """Tests for connection registry management."""

    def test_initial_state_empty(self):
        """Registry is empty initially."""
        assert get_connection_count() == 0
        assert list_connections() == []

    def test_list_connections_returns_names(self):
        """list_connections returns connection names."""
        # Add a mock connection to registry
        mock_conn = MagicMock()
        _connections["test_db"] = mock_conn

        assert "test_db" in list_connections()

    def test_get_connection_count(self):
        """get_connection_count returns correct count."""
        mock_conn = MagicMock()
        _connections["db1"] = mock_conn
        _connections["db2"] = mock_conn

        assert get_connection_count() == 2


class TestCloseConnection:
    """Tests for close_connection function."""

    def test_close_connection_removes_from_registry(self):
        """Closing connection removes it from registry."""
        mock_conn = MagicMock()
        _connections["test_db"] = mock_conn

        close_connection("test_db")

        assert "test_db" not in _connections

    def test_close_connection_calls_disconnect(self):
        """Closing connection calls disconnect method."""
        mock_conn = MagicMock()
        mock_conn.disconnect = MagicMock()
        _connections["test_db"] = mock_conn

        close_connection("test_db")

        mock_conn.disconnect.assert_called_once()

    def test_close_connection_calls_close_if_no_disconnect(self):
        """Falls back to close() if disconnect() not available."""
        mock_conn = MagicMock(spec=["close"])
        _connections["test_db"] = mock_conn

        close_connection("test_db")

        mock_conn.close.assert_called_once()

    def test_close_nonexistent_connection_no_error(self):
        """Closing non-existent connection doesn't raise error."""
        close_connection("nonexistent")  # Should not raise

    def test_close_connection_handles_exception(self):
        """Handles exception during close gracefully."""
        mock_conn = MagicMock()
        mock_conn.disconnect.side_effect = Exception("Close failed")
        _connections["test_db"] = mock_conn

        # Should not raise
        close_connection("test_db")
        assert "test_db" not in _connections


class TestCloseAllConnections:
    """Tests for close_all_connections function."""

    def test_closes_all_connections(self):
        """Closes all connections in registry."""
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        _connections["db1"] = mock_conn1
        _connections["db2"] = mock_conn2

        close_all_connections()

        assert get_connection_count() == 0
        mock_conn1.disconnect.assert_called_once()
        mock_conn2.disconnect.assert_called_once()

    def test_closes_empty_registry_no_error(self):
        """Closing empty registry doesn't raise error."""
        close_all_connections()  # Should not raise


# ============================================
# get_connection tests
# ============================================


class TestGetConnection:
    """Tests for get_connection function."""

    def test_reuses_existing_connection(self):
        """Returns existing connection if name already registered."""
        mock_conn = MagicMock()
        _connections["test_db"] = mock_conn

        result = get_connection("test_db", "database_mssql", {})

        assert result is mock_conn

    def test_unsupported_source_type_raises_error(self):
        """Raises ValueError for unsupported source type."""
        with pytest.raises(ValueError, match="Unsupported database source type"):
            get_connection("test_db", "database_unknown", {})

    def test_source_type_enum_value(self):
        """Handles SourceType enum correctly."""
        from pipelines.lib.bronze import SourceType

        with patch("pipelines.lib.connections._create_ibis_connection") as mock_create:
            mock_create.return_value = MagicMock()
            get_connection("test_mssql", SourceType.DATABASE_MSSQL, {})
            mock_create.assert_called_once_with("mssql", {})


class TestCreateMssqlConnection:
    """Tests for MSSQL connection creation."""

    def test_creates_mssql_connection(self):
        """Creates MSSQL connection with ibis."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_conn = MagicMock()
            mock_ibis.mssql.connect.return_value = mock_conn

            result = get_connection(
                "mssql_test",
                "database_mssql",
                {
                    "host": "localhost",
                    "database": "testdb",
                    "user": "sa",
                    "password": "password",
                    "port": 1433,
                },
            )

            mock_ibis.mssql.connect.assert_called_once()
            assert result is mock_conn

    def test_mssql_expands_env_vars(self, monkeypatch):
        """Expands environment variables in credentials."""
        monkeypatch.setenv("DB_HOST", "prod-server")
        monkeypatch.setenv("DB_PASSWORD", "secret123")

        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_ibis.mssql.connect.return_value = MagicMock()

            get_connection(
                "mssql_env",
                "database_mssql",
                {
                    "host": "${DB_HOST}",
                    "database": "testdb",
                    "password": "${DB_PASSWORD}",
                },
            )

            call_kwargs = mock_ibis.mssql.connect.call_args
            assert call_kwargs.kwargs["host"] == "prod-server"
            assert call_kwargs.kwargs["password"] == "secret123"

    def test_mssql_backend_not_available_raises_import_error(self):
        """Raises ImportError when MSSQL backend not installed."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_ibis.mssql.connect.side_effect = AttributeError("mssql not available")

            with pytest.raises(ImportError, match="ibis-framework\\[mssql\\]"):
                get_connection("mssql_missing", "database_mssql", {"host": "localhost"})


class TestCreatePostgresConnection:
    """Tests for PostgreSQL connection creation."""

    def test_creates_postgres_connection(self):
        """Creates PostgreSQL connection with ibis."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_conn = MagicMock()
            mock_ibis.postgres.connect.return_value = mock_conn

            result = get_connection(
                "pg_test",
                "database_postgres",
                {
                    "host": "localhost",
                    "database": "testdb",
                    "user": "postgres",
                    "password": "password",
                    "port": 5432,
                },
            )

            mock_ibis.postgres.connect.assert_called_once()
            assert result is mock_conn

    def test_postgres_backend_not_available_raises_import_error(self):
        """Raises ImportError when PostgreSQL backend not installed."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_ibis.postgres.connect.side_effect = AttributeError(
                "postgres not available"
            )

            with pytest.raises(ImportError, match="ibis-framework\\[postgres\\]"):
                get_connection("pg_missing", "database_postgres", {"host": "localhost"})


class TestCreateMysqlConnection:
    """Tests for MySQL connection creation."""

    def test_creates_mysql_connection(self):
        """Creates MySQL connection with ibis."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_conn = MagicMock()
            mock_ibis.mysql.connect.return_value = mock_conn

            result = get_connection(
                "mysql_test",
                "database_mysql",
                {
                    "host": "localhost",
                    "database": "testdb",
                    "user": "root",
                    "password": "password",
                    "port": 3306,
                },
            )

            mock_ibis.mysql.connect.assert_called_once()
            assert result is mock_conn

    def test_mysql_backend_not_available_raises_import_error(self):
        """Raises ImportError when MySQL backend not installed."""
        with patch("pipelines.lib.connections.ibis") as mock_ibis:
            mock_ibis.mysql.connect.side_effect = AttributeError("mysql not available")

            with pytest.raises(ImportError, match="ibis-framework\\[mysql\\]"):
                get_connection("mysql_missing", "database_mysql", {"host": "localhost"})


class TestCreateDb2Connection:
    """Tests for DB2 connection creation."""

    def test_db2_requires_pyodbc(self):
        """Raises ImportError when pyodbc not installed."""
        import pipelines.lib.connections as conn_module

        def mock_create_db2(options):
            raise ImportError("pyodbc not installed")

        with patch.object(conn_module, "_create_db2_connection", mock_create_db2):
            with pytest.raises(ImportError):
                get_connection("db2_test", "database_db2", {"host": "localhost"})

    def test_creates_db2_connection_wrapper(self):
        """Creates DB2Connection wrapper with pyodbc."""
        mock_pyodbc = MagicMock()
        mock_duckdb_conn = MagicMock()

        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            with patch("pipelines.lib.connections.ibis") as mock_ibis:
                mock_ibis.duckdb.connect.return_value = mock_duckdb_conn

                result = get_connection(
                    "db2_test",
                    "database_db2",
                    {
                        "host": "db2server",
                        "database": "SAMPLE",
                        "user": "db2user",
                        "password": "db2pass",
                        "port": 50000,
                    },
                )

                # Result should be DB2Connection wrapper
                assert hasattr(result, "sql")
                assert hasattr(result, "table")
                assert hasattr(result, "list_tables")
                assert hasattr(result, "disconnect")


class TestDb2ConnectionWrapper:
    """Tests for DB2Connection wrapper class."""

    def test_sql_method_executes_query(self):
        """sql() method executes query via ODBC and returns Ibis table."""
        mock_pyodbc = MagicMock()
        mock_odbc_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_odbc_conn

        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            with patch("pipelines.lib.connections.ibis") as mock_ibis:
                mock_duckdb = MagicMock()
                mock_ibis.duckdb.connect.return_value = mock_duckdb

                result = get_connection(
                    "db2_sql_test",
                    "database_db2",
                    {"host": "server", "database": "DB", "user": "u", "password": "p"},
                )

                # The wrapper should have sql method
                assert callable(result.sql)

    def test_table_method_fetches_table(self):
        """table() method fetches entire table."""
        mock_pyodbc = MagicMock()

        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            with patch("pipelines.lib.connections.ibis") as mock_ibis:
                mock_duckdb = MagicMock()
                mock_ibis.duckdb.connect.return_value = mock_duckdb

                result = get_connection(
                    "db2_table_test",
                    "database_db2",
                    {"host": "server", "database": "DB", "user": "u", "password": "p"},
                )

                # The wrapper should have table method
                assert callable(result.table)

    def test_disconnect_closes_connections(self):
        """disconnect() closes both ODBC and DuckDB connections."""
        mock_pyodbc = MagicMock()

        with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
            with patch("pipelines.lib.connections.ibis") as mock_ibis:
                mock_duckdb = MagicMock()
                mock_ibis.duckdb.connect.return_value = mock_duckdb

                result = get_connection(
                    "db2_disconnect_test",
                    "database_db2",
                    {"host": "server", "database": "DB", "user": "u", "password": "p"},
                )

                # disconnect should be callable
                assert callable(result.disconnect)
                result.disconnect()  # Should not raise
