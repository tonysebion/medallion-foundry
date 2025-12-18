"""Database extraction with incremental cursor support per spec Section 3.

Supports db_table and db_query source types with:
- Incremental loading via watermark columns
- Parameterized queries with cursor substitution
- Connection string from environment variables
- Retry logic with circuit breaker and exponential backoff
"""

import logging
import os
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from core.domain.adapters.extractors.cursor_state import (
    CursorStateManager,
    build_incremental_query,
)
from core.domain.adapters.extractors.db_runner import fetch_records_from_query
from core.domain.adapters.extractors.db_utils import is_retryable_db_error
from core.domain.adapters.extractors.resilience import ResilientExtractorMixin
from core.foundation.primitives.exceptions import ExtractionError
from core.infrastructure.io.extractors.base import BaseExtractor, register_extractor
from core.platform.resilience import RateLimiter

logger = logging.getLogger(__name__)


@register_extractor("db")
class DbExtractor(BaseExtractor, ResilientExtractorMixin):
    """Extractor for database sources with incremental loading support.

    Supports both db_table and db_query source types. Watermark handling
    allows for incremental extraction based on a timestamp or sequence column.
    """

    def __init__(self) -> None:
        self._state_manager = CursorStateManager()
        self._init_resilience()

    def get_watermark_config(self, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get watermark configuration for database extraction.

        Returns watermark config based on the incremental settings in source.db.
        """
        source = cfg.get("source", {})
        db_cfg = source.get("db", {})
        incremental = db_cfg.get("incremental", {})

        if not incremental.get("enabled", False):
            return None

        cursor_column = incremental.get("cursor_column")
        if not cursor_column:
            return None

        return {
            "enabled": True,
            "column": cursor_column,
            "type": incremental.get("cursor_type", "timestamp"),
        }

    def _execute_query(
        self,
        driver: str,
        conn_str: str,
        query: str,
        params: Optional[Tuple] = None,
        batch_size: int = 10000,
        cursor_column: Optional[str] = None,
        limiter: Optional[RateLimiter] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Execute database query with circuit breaker and retry logic."""

        def _do_query() -> Tuple[List[Dict[str, Any]], Optional[str]]:
            logger.debug("Executing query with driver=%s params=%s", driver, params)
            if limiter:
                limiter.acquire()
            return fetch_records_from_query(
                driver=driver,
                conn_str=conn_str,
                query=query,
                params=params,
                batch_size=batch_size,
                cursor_column=cursor_column,
            )

        return self._execute_with_resilience(
            _do_query,
            "db_extractor_query",
            retry_if=self._should_retry,
        )

    def _should_retry(self, exc: BaseException) -> bool:
        """Determine if a database error is retryable."""
        return is_retryable_db_error(exc)

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from database with incremental cursor support."""
        source_cfg = cfg["source"]
        db_cfg = source_cfg["db"]
        run_cfg = source_cfg.get("run", {})

        # Driver selection (default: pyodbc)
        driver = (db_cfg.get("driver") or "pyodbc").lower()

        # Get connection string from environment
        conn_env = db_cfg.get("conn_str_env")
        if not conn_env:
            raise ExtractionError(
                "db.conn_str_env is required in config for type=db",
                extractor_type="db",
            )

        conn_str = os.environ.get(conn_env)
        if not conn_str:
            raise ExtractionError(
                f"Environment variable '{conn_env}' not set for DB connection string",
                extractor_type="db",
            )

        base_query = db_cfg["base_query"]

        # Incremental configuration
        incremental_cfg = db_cfg.get("incremental", {})
        cursor_column = incremental_cfg.get("cursor_column")
        use_incremental = incremental_cfg.get("enabled", False) and cursor_column

        # Load previous cursor if incremental is enabled
        state_key = f"{source_cfg['system']}_{source_cfg['table']}"
        last_cursor = (
            self._state_manager.load_cursor(state_key) if use_incremental else None
        )

        # Build query with incremental filter
        query = build_incremental_query(
            base_query, cursor_column if use_incremental else None, last_cursor
        )

        logger.info(
            f"Executing database query (incremental={'yes' if use_incremental else 'no'})"
        )

        limiter = RateLimiter.from_config(
            db_cfg,
            run_cfg,
            component="db_extractor",
            env_var="BRONZE_DB_RPS",
        )

        batch_size = db_cfg.get("fetch_batch_size", 10000)
        records, max_cursor = self._execute_query(
            driver,
            conn_str,
            query,
            (last_cursor,) if last_cursor else None,
            batch_size=batch_size,
            cursor_column=cursor_column if use_incremental else None,
            limiter=limiter,
        )
        logger.info("Successfully extracted %d records from database", len(records))
        new_cursor = max_cursor

        if use_incremental and new_cursor:
            self._state_manager.save_cursor(state_key, new_cursor, run_date)

        return records, new_cursor
