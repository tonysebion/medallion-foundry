"""Connection pooling for database sources.

Provides a simple connection registry that reuses connections across
multiple BronzeSource instances pointing to the same database.

This avoids creating 50 connections when extracting 50 entities from
the same database.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, TYPE_CHECKING, Union

import ibis

from pipelines.lib.env import expand_env_vars

if TYPE_CHECKING:
    from pipelines.lib.bronze import SourceType

logger = logging.getLogger(__name__)

__all__ = ["close_all_connections", "get_connection"]

# Connection registry - keyed by connection_name
_connections: Dict[str, ibis.BaseBackend] = {}


def get_connection(
    connection_name: str,
    source_type: "Union[SourceType, str]",
    options: Dict[str, Any],
) -> ibis.BaseBackend:
    """Get or create a connection by name.

    Reuses existing connections to avoid connection overhead when
    extracting multiple entities from the same database.

    Args:
        connection_name: Unique name for this connection
        source_type: Type of database source (SourceType enum or string)
        options: Connection options (host, database, user, password, etc.)

    Returns:
        Ibis backend connection

    Example:
        >>> con = get_connection(
        ...     "claims_db",
        ...     SourceType.DATABASE_MSSQL,
        ...     {"host": "server.com", "database": "ClaimsDB"}
        ... )
    """
    if connection_name in _connections:
        logger.debug("Reusing existing connection: %s", connection_name)
        return _connections[connection_name]

    logger.info("Creating new connection: %s", connection_name)

    # Convert SourceType enum to string value for comparison
    source_type_str = (
        source_type.value if hasattr(source_type, "value") else str(source_type)
    )

    if source_type_str == "database_mssql":
        con = _create_mssql_connection(options)
    elif source_type_str == "database_postgres":
        con = _create_postgres_connection(options)
    elif source_type_str == "database_mysql":
        con = _create_mysql_connection(options)
    elif source_type_str == "database_db2":
        con = _create_db2_connection(options)
    else:
        raise ValueError(f"Unsupported database source type: {source_type}")

    _connections[connection_name] = con
    return con


def _create_mssql_connection(options: Dict[str, Any]) -> ibis.BaseBackend:
    """Create an MSSQL connection.

    Supports environment variable substitution for credentials.
    """
    host = expand_env_vars(options.get("host", ""))
    database = expand_env_vars(options.get("database", ""))
    user = expand_env_vars(options.get("user", ""))
    password = expand_env_vars(options.get("password", ""))
    port = options.get("port", 1433)

    # Build connection string for pyodbc
    driver = options.get("driver", "ODBC Driver 17 for SQL Server")

    # Check if Ibis MSSQL backend is available
    try:
        return ibis.mssql.connect(
            host=host,
            port=port,
            database=database,
            user=user if user else None,
            password=password if password else None,
            driver=driver,
        )
    except AttributeError:
        # Ibis MSSQL backend may not be installed
        logger.warning(
            "Ibis MSSQL backend not available. "
            "Consider installing ibis-framework[mssql]"
        )
        raise ImportError(
            "MSSQL support requires ibis-framework[mssql]. "
            "Install with: pip install ibis-framework[mssql]"
        )


def _create_postgres_connection(options: Dict[str, Any]) -> ibis.BaseBackend:
    """Create a PostgreSQL connection.

    Supports environment variable substitution for credentials.
    """
    host = expand_env_vars(options.get("host", "localhost"))
    database = expand_env_vars(options.get("database", ""))
    user = expand_env_vars(options.get("user", ""))
    password = expand_env_vars(options.get("password", ""))
    port = options.get("port", 5432)

    try:
        return ibis.postgres.connect(
            host=host,
            port=port,
            database=database,
            user=user if user else None,
            password=password if password else None,
        )
    except AttributeError:
        logger.warning(
            "Ibis PostgreSQL backend not available. "
            "Consider installing ibis-framework[postgres]"
        )
        raise ImportError(
            "PostgreSQL support requires ibis-framework[postgres]. "
            "Install with: pip install ibis-framework[postgres]"
        )


def _create_mysql_connection(options: Dict[str, Any]) -> ibis.BaseBackend:
    """Create a MySQL/MariaDB connection.

    Supports environment variable substitution for credentials.

    Options:
        host: Database host (default: localhost)
        port: Database port (default: 3306)
        database: Database name
        user: Username
        password: Password
    """
    host = expand_env_vars(options.get("host", "localhost"))
    database = expand_env_vars(options.get("database", ""))
    user = expand_env_vars(options.get("user", ""))
    password = expand_env_vars(options.get("password", ""))
    port = options.get("port", 3306)

    try:
        return ibis.mysql.connect(
            host=host,
            port=port,
            database=database,
            user=user if user else None,
            password=password if password else None,
        )
    except AttributeError:
        logger.warning(
            "Ibis MySQL backend not available. "
            "Consider installing ibis-framework[mysql]"
        )
        raise ImportError(
            "MySQL support requires ibis-framework[mysql]. "
            "Install with: pip install ibis-framework[mysql]"
        )


def _create_db2_connection(options: Dict[str, Any]) -> ibis.BaseBackend:
    """Create a DB2 connection via ODBC.

    DB2 is not natively supported by Ibis, so we use pyodbc with a DuckDB
    bridge for Ibis compatibility. The data is fetched via ODBC and then
    loaded into DuckDB for Ibis operations.

    Options:
        host: Database host
        port: Database port (default: 50000)
        database: Database name
        user: Username
        password: Password
        driver: ODBC driver name (default: "IBM DB2 ODBC DRIVER")
        query: SQL query to execute (required for DB2)

    Note: DB2 requires the IBM DB2 ODBC driver to be installed on the system.
    """
    try:
        import pyodbc
    except ImportError:
        raise ImportError(
            "DB2 support requires pyodbc. "
            "Install with: pip install pyodbc"
        )

    host = expand_env_vars(options.get("host", ""))
    database = expand_env_vars(options.get("database", ""))
    user = expand_env_vars(options.get("user", ""))
    password = expand_env_vars(options.get("password", ""))
    port = options.get("port", 50000)
    driver = options.get("driver", "IBM DB2 ODBC DRIVER")

    # Build DB2 connection string
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"DATABASE={database};"
        f"HOSTNAME={host};"
        f"PORT={port};"
        f"PROTOCOL=TCPIP;"
        f"UID={user};"
        f"PWD={password};"
    )

    # Store the pyodbc connection in options for later use by _read_database
    # We return a DuckDB connection that will be used for Ibis operations
    # The actual DB2 query execution happens in bronze.py._read_database
    logger.info("Creating DB2 connection to %s:%s/%s", host, port, database)

    # Create a wrapper that holds both the ODBC connection and DuckDB
    class DB2Connection:
        """Wrapper for DB2 ODBC connection with DuckDB bridge."""

        def __init__(self, conn_str: str):
            self._conn_str = conn_str
            self._odbc_conn = None
            self._duckdb = ibis.duckdb.connect()

        def _get_odbc(self):
            if self._odbc_conn is None:
                self._odbc_conn = pyodbc.connect(self._conn_str)
            return self._odbc_conn

        def sql(self, query: str):
            """Execute SQL and return as Ibis table via DuckDB."""
            import pandas as pd

            odbc_conn = self._get_odbc()
            df = pd.read_sql(query, odbc_conn)
            return self._duckdb.create_table(f"_db2_query_{id(df)}", df, overwrite=True)

        def table(self, name: str):
            """Fetch entire table from DB2."""
            return self.sql(f"SELECT * FROM {name}")

        def list_tables(self):
            """List tables in the DB2 database."""
            odbc_conn = self._get_odbc()
            cursor = odbc_conn.cursor()
            tables = [row.table_name for row in cursor.tables(tableType="TABLE")]
            cursor.close()
            return tables

        def disconnect(self):
            """Close connections."""
            if self._odbc_conn:
                self._odbc_conn.close()
                self._odbc_conn = None

        def close(self):
            """Alias for disconnect."""
            self.disconnect()

    return DB2Connection(conn_str)


def close_connection(connection_name: str) -> None:
    """Close a specific connection.

    Args:
        connection_name: Name of the connection to close
    """
    if connection_name in _connections:
        try:
            con = _connections.pop(connection_name)
            if hasattr(con, "disconnect"):
                con.disconnect()
            elif hasattr(con, "close"):
                con.close()
            logger.info("Closed connection: %s", connection_name)
        except Exception as e:
            logger.warning("Error closing connection %s: %s", connection_name, e)


def close_all_connections() -> None:
    """Clean up all connections.

    Call at end of batch run to release database resources.
    """
    for name in list(_connections.keys()):
        close_connection(name)
    logger.info("All connections closed")


def list_connections() -> list[str]:
    """List all active connection names.

    Returns:
        List of connection names currently in the pool
    """
    return list(_connections.keys())


def get_connection_count() -> int:
    """Get the number of active connections.

    Returns:
        Number of connections in the pool
    """
    return len(_connections)
