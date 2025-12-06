"""Shared database query helper for extractors."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyodbc

from core.primitives.foundations.exceptions import ExtractionError


def fetch_records_from_query(
    driver: str,
    conn_str: str,
    query: str,
    params: Optional[Iterable[Any]] = None,
    batch_size: int = 10000,
    cursor_column: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Execute a query and return records plus the max cursor value."""

    if driver not in {"pyodbc", "pymssql"}:
        raise ValueError(
            f"Unsupported db.driver '{driver}'. Use 'pyodbc' (default) or 'pymssql'."
        )

    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()

    try:
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        columns = [col[0] for col in cur.description]
        records: List[Dict[str, Any]] = []
        max_cursor: Optional[str] = None

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            for row in rows:
                record = {col: val for col, val in zip(columns, row)}
                records.append(record)

                if cursor_column and cursor_column in record:
                    cursor_val = record[cursor_column]
                    if cursor_val is not None:
                        cursor_str = str(cursor_val)
                        if max_cursor is None or cursor_str > max_cursor:
                            max_cursor = cursor_str

        return records, max_cursor
    except Exception as exc:
        raise ExtractionError("Database query failed", extractor_type="db") from exc
    finally:
        cur.close()
        conn.close()
