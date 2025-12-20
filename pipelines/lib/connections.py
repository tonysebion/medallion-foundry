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
