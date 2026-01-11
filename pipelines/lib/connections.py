"""Connection pooling for database sources.

Provides a simple connection registry that reuses connections across
multiple BronzeSource instances pointing to the same database.

This avoids creating 50 connections when extracting 50 entities from
the same database.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, TYPE_CHECKING, Union

import ibis  # type: ignore[import-untyped]

from pipelines.lib.env import expand_env_vars

if TYPE_CHECKING:
    from pipelines.lib.bronze import SourceType

logger = logging.getLogger(__name__)

__all__ = [
    "close_all_connections",
    "close_connection",
    "get_connection",
    "get_connection_count",
    "list_connections",
]


def _expand_credentials(options: Dict[str, Any]) -> Dict[str, str]:
    """Expand environment variables in database credentials.

    Args:
        options: Connection options dict

    Returns:
        Dict with expanded host, database, user, password values
    """
    return {
        "host": expand_env_vars(options.get("host", "")),
        "database": expand_env_vars(options.get("database", "")),
        "user": expand_env_vars(options.get("user", "")),
        "password": expand_env_vars(options.get("password", "")),
    }


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

    # Map source type to database type
    db_type_map = {
        "database_mssql": "mssql",
        "database_postgres": "postgres",
        "database_mysql": "mysql",
    }

    if source_type_str in db_type_map:
        con = _create_ibis_connection(db_type_map[source_type_str], options)
    elif source_type_str == "database_db2":
        con = _create_db2_connection(options)
    else:
        raise ValueError(f"Unsupported database source type: {source_type}")

    _connections[connection_name] = con
    return con


# Database configuration for unified connection factory
_DB_CONFIGS: Dict[str, Dict[str, Any]] = {
    "mssql": {
        "default_port": 1433,
        "backend": "mssql",
        "package": "ibis-framework[mssql]",
        "extra": lambda opts: {
            "driver": opts.get("driver", "ODBC Driver 17 for SQL Server")
        },
    },
    "postgres": {
        "default_port": 5432,
        "backend": "postgres",
        "package": "ibis-framework[postgres]",
        "default_host": "localhost",
    },
    "mysql": {
        "default_port": 3306,
        "backend": "mysql",
        "package": "ibis-framework[mysql]",
        "default_host": "localhost",
    },
}


def _create_ibis_connection(db_type: str, options: Dict[str, Any]) -> ibis.BaseBackend:
    """Create database connection using Ibis backend.

    Unified factory for MSSQL, PostgreSQL, and MySQL connections.
    Supports environment variable substitution for credentials.
    """
    config = _DB_CONFIGS[db_type]
    creds = _expand_credentials(options)

    if not creds["host"] and config.get("default_host"):
        creds["host"] = config["default_host"]

    connect_args: Dict[str, Any] = {
        "host": creds["host"],
        "port": options.get("port", config["default_port"]),
        "database": creds["database"],
        "user": creds["user"] or None,
        "password": creds["password"] or None,
    }

    if "extra" in config:
        connect_args.update(config["extra"](options))

    try:
        backend = getattr(ibis, config["backend"])
        return backend.connect(**connect_args)
    except AttributeError:
        logger.warning(
            "Ibis %s backend not available. Consider installing %s",
            config["backend"].upper(),
            config["package"],
        )
        raise ImportError(
            f"{config['backend'].upper()} support requires {config['package']}. "
            f"Install with: pip install {config['package']}"
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
            "DB2 support requires pyodbc. Install with: pip install pyodbc"
        )

    creds = _expand_credentials(options)
    port = options.get("port", 50000)
    driver = options.get("driver", "IBM DB2 ODBC DRIVER")

    # Build DB2 connection string
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"DATABASE={creds['database']};"
        f"HOSTNAME={creds['host']};"
        f"PORT={port};"
        f"PROTOCOL=TCPIP;"
        f"UID={creds['user']};"
        f"PWD={creds['password']};"
    )

    # Store the pyodbc connection in options for later use by _read_database
    # We return a DuckDB connection that will be used for Ibis operations
    # The actual DB2 query execution happens in bronze.py._read_database
    logger.info(
        "Creating DB2 connection to %s:%s/%s", creds["host"], port, creds["database"]
    )

    # Create a wrapper that holds both the ODBC connection and DuckDB
    class DB2Connection:
        """Wrapper for DB2 ODBC connection with DuckDB bridge."""

        def __init__(self, conn_str: str):
            self._conn_str = conn_str
            self._odbc_conn: Optional[Any] = None
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

        def __enter__(self):
            """Context manager entry."""
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Context manager exit - ensures cleanup."""
            self.close()
            return False

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
