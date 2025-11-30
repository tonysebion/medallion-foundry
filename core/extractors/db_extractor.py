"""Database extraction with incremental cursor support."""

import logging
import os
import json
from typing import Dict, Any, List, Optional, Tuple
from datetime import date
from pathlib import Path

import pyodbc
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from core.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class DbExtractor(BaseExtractor):
    """Extractor for database sources with incremental loading support."""

    def _get_state_file_path(self, cfg: Dict[str, Any]) -> Path:
        """Get the path to the state file for incremental loads."""
        source_cfg = cfg["source"]
        system = source_cfg["system"]
        table = source_cfg["table"]

        # Store state files in a .state directory
        state_dir = Path(".state")
        state_dir.mkdir(exist_ok=True)

        return state_dir / f"{system}_{table}_cursor.json"

    def _load_cursor(self, state_file: Path) -> Optional[str]:
        """Load the last cursor value from state file."""
        if not state_file.exists():
            logger.info("No previous cursor state found")
            return None

        try:
            with open(state_file, "r") as f:
                state = json.load(f)
            cursor: Optional[str] = state.get("cursor")
            if cursor is not None:
                cursor = str(cursor)
            last_run = state.get("last_run")
            logger.info(f"Loaded cursor state: cursor={cursor}, last_run={last_run}")
            return cursor
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load cursor state from {state_file}: {e}")
            return None

    def _save_cursor(self, state_file: Path, cursor: str, run_date: date) -> None:
        """Save the new cursor value to state file."""
        state = {
            "cursor": cursor,
            "last_run": run_date.isoformat(),
        }

        try:
            with open(state_file, "w") as f:
                json.dump(state, f, indent=2)
            logger.info(f"Saved cursor state: {cursor}")
        except OSError as e:
            logger.error(f"Failed to save cursor state to {state_file}: {e}")
            raise

    def _build_incremental_query(
        self, base_query: str, cursor_column: Optional[str], last_cursor: Optional[str]
    ) -> str:
        """Build query with incremental WHERE clause if applicable."""
        if not cursor_column or not last_cursor:
            return base_query

        # Check if query already has WHERE clause
        query_upper = base_query.upper()

        if "WHERE" in query_upper:
            # Append to existing WHERE
            connector = "AND"
        else:
            # Add new WHERE clause
            connector = "WHERE"

        # Build incremental filter
        incremental_clause = f"\n{connector} {cursor_column} > ?"

        return base_query + incremental_clause

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _execute_query(
        self, driver: str, conn_str: str, query: str, params: Optional[Tuple] = None
    ) -> Any:
        """Execute database query with retry logic for the selected driver."""
        logger.debug("Executing query with driver=%s params=%s", driver, params)

        if driver == "pyodbc":
            conn = pyodbc.connect(conn_str)
            cur = conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur
        elif driver == "pymssql":
            raise NotImplementedError(
                "Driver 'pymssql' selected but not implemented in runtime. "
                "Use 'pyodbc' or open an issue if you need first-class pymssql support."
            )
        else:
            raise ValueError(
                f"Unsupported db.driver '{driver}'. Use 'pyodbc' (default) or 'pymssql'."
            )

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from database with incremental cursor support."""
        source_cfg = cfg["source"]
        db_cfg = source_cfg["db"]

        # Driver selection (default: pyodbc)
        driver = (db_cfg.get("driver") or "pyodbc").lower()

        # Get connection string from environment
        conn_env = db_cfg.get("conn_str_env")
        if not conn_env:
            raise ValueError("db.conn_str_env is required in config for type=db")

        conn_str = os.environ.get(conn_env)
        if not conn_str:
            raise ValueError(
                f"Environment variable '{conn_env}' not set for DB connection string"
            )

        base_query = db_cfg["base_query"]

        # Incremental configuration
        incremental_cfg = db_cfg.get("incremental", {})
        cursor_column = incremental_cfg.get("cursor_column")
        use_incremental = incremental_cfg.get("enabled", False) and cursor_column

        # Load previous cursor if incremental is enabled
        last_cursor = None
        state_file = None

        if use_incremental:
            state_file = self._get_state_file_path(cfg)
            last_cursor = self._load_cursor(state_file)

        # Build query with incremental filter
        query = self._build_incremental_query(
            base_query, cursor_column if use_incremental else None, last_cursor
        )

        logger.info(
            f"Executing database query (incremental={'yes' if use_incremental else 'no'})"
        )

        # Execute query
        records: List[Dict[str, Any]] = []
        max_cursor: Optional[str] = None
        conn = None

        try:
            if last_cursor:
                cur = self._execute_query(driver, conn_str, query, (last_cursor,))
            else:
                cur = self._execute_query(driver, conn_str, query)

            conn = cur.connection
            columns = [col[0] for col in cur.description]

            # Fetch in batches
            batch_size = db_cfg.get("fetch_batch_size", 10000)
            total_rows = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break

                for row in rows:
                    rec = {col: val for col, val in zip(columns, row)}
                    records.append(rec)

                    # Track max cursor value
                    if use_incremental and cursor_column in rec:
                        cursor_val = rec[cursor_column]
                        if cursor_val is not None:
                            cursor_str = str(cursor_val)
                            if max_cursor is None or cursor_str > max_cursor:
                                max_cursor = cursor_str

                total_rows += len(rows)
                logger.info(f"Fetched {len(rows)} rows (total: {total_rows})")

            logger.info(f"Successfully extracted {len(records)} records from database")

        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise
        finally:
            if conn:
                conn.close()

        # Save new cursor if we have one
        new_cursor = max_cursor

        if use_incremental and new_cursor and state_file:
            self._save_cursor(state_file, new_cursor, run_date)

        return records, new_cursor
