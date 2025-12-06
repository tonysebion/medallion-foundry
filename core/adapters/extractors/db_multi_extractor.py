"""Multi-entity database extraction with parallel support per spec Section 3.5.

Supports extracting multiple database entities in parallel from a single connection.

Example config:
```yaml
source:
  type: "db_multi"
  connection_ref: "facets_oltp"
  entities:
    - name: "claim_header"
      database: "FACETS"
      schema: "dbo"
      table: "CLAIM_HDR"
      load:
        mode: incremental_append
        watermark:
          column: LAST_UPDATE_TS
          type: timestamp
    - name: "claim_line"
      database: "FACETS"
      schema: "dbo"
      table: "CLAIM_LINE"
      load:
        mode: incremental_append
        watermark:
          column: LAST_UPDATE_TS
          type: timestamp
```
"""

from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.adapters.extractors.base import BaseExtractor, register_extractor

logger = logging.getLogger(__name__)


@dataclass
class EntityConfig:
    """Configuration for a single entity in db_multi extraction."""

    name: str
    database: str
    schema: str
    table: str
    query: Optional[str] = None  # Custom query override
    load_mode: str = "snapshot"  # snapshot | incremental_append
    watermark_column: Optional[str] = None
    watermark_type: str = "timestamp"  # timestamp | integer
    fetch_batch_size: int = 10000

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EntityConfig":
        """Create EntityConfig from config dictionary."""
        load_cfg = data.get("load", {})
        watermark_cfg = load_cfg.get("watermark", {})

        return cls(
            name=data["name"],
            database=data.get("database", ""),
            schema=data.get("schema", "dbo"),
            table=data.get("table", data["name"]),
            query=data.get("query"),
            load_mode=load_cfg.get("mode", "snapshot"),
            watermark_column=watermark_cfg.get("column"),
            watermark_type=watermark_cfg.get("type", "timestamp"),
            fetch_batch_size=data.get("fetch_batch_size", 10000),
        )

    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        parts = []
        if self.database:
            parts.append(self.database)
        if self.schema:
            parts.append(self.schema)
        parts.append(self.table)
        return ".".join(parts)


@dataclass
class EntityResult:
    """Result of extracting a single entity."""

    entity_name: str
    records: List[Dict[str, Any]]
    cursor: Optional[str] = None
    error: Optional[str] = None
    row_count: int = 0


@dataclass
class MultiEntityResult:
    """Combined result of multi-entity extraction."""

    results: Dict[str, EntityResult] = field(default_factory=dict)
    total_records: int = 0
    failed_entities: List[str] = field(default_factory=list)

    def add_result(self, result: EntityResult) -> None:
        """Add an entity result."""
        self.results[result.entity_name] = result
        if result.error:
            self.failed_entities.append(result.entity_name)
        else:
            self.total_records += result.row_count


@register_extractor("db_multi")
class DbMultiExtractor(BaseExtractor):
    """Extractor for multiple database entities with parallel support.

    Per spec Section 3.5, supports:
    - Multiple entities from a single connection
    - Parallel extraction
    - Per-entity watermark tracking
    - Both snapshot and incremental_append modes
    """

    def __init__(self, max_workers: int = 4):
        """Initialize with configurable parallelism.

        Args:
            max_workers: Maximum number of parallel extraction threads
        """
        self.max_workers = max_workers

    def _get_state_dir(self) -> Path:
        """Get the state directory for cursor persistence."""
        state_dir = Path(".state")
        state_dir.mkdir(exist_ok=True)
        return state_dir

    def _get_entity_state_file(self, system: str, entity_name: str) -> Path:
        """Get state file path for a specific entity."""
        return self._get_state_dir() / f"{system}_{entity_name}_cursor.json"

    def _load_cursor(self, state_file: Path) -> Optional[str]:
        """Load cursor from state file."""
        if not state_file.exists():
            return None

        try:
            with open(state_file, "r") as f:
                state = json.load(f)
            cursor: Optional[str] = state.get("cursor")
            if cursor is not None:
                cursor = str(cursor)
            logger.debug(f"Loaded cursor {cursor} from {state_file}")
            return cursor
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not load cursor from {state_file}: {e}")
            return None

    def _save_cursor(self, state_file: Path, cursor: str, run_date: date) -> None:
        """Save cursor to state file."""
        state = {
            "cursor": cursor,
            "last_run": run_date.isoformat(),
        }
        try:
            with open(state_file, "w") as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved cursor {cursor} to {state_file}")
        except OSError as e:
            logger.error(f"Failed to save cursor to {state_file}: {e}")

    def _build_entity_query(
        self,
        entity: EntityConfig,
        last_cursor: Optional[str],
    ) -> Tuple[str, Optional[Tuple]]:
        """Build query for entity with optional watermark filter.

        Returns:
            Tuple of (query_string, parameters)
        """
        if entity.query:
            # Custom query provided
            base_query = entity.query
        else:
            # Build SELECT * FROM table
            base_query = f"SELECT * FROM {entity.full_table_name}"

        # Add watermark filter for incremental loads
        if (
            entity.load_mode == "incremental_append"
            and entity.watermark_column
            and last_cursor
        ):
            if "WHERE" in base_query.upper():
                connector = "AND"
            else:
                connector = "WHERE"

            query = f"{base_query} {connector} {entity.watermark_column} > ?"
            return query, (last_cursor,)

        return base_query, None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    def _extract_entity(
        self,
        entity: EntityConfig,
        conn_str: str,
        system: str,
        run_date: date,
    ) -> EntityResult:
        """Extract records for a single entity.

        This method is thread-safe and can be called in parallel.
        """
        logger.info(f"Starting extraction for entity: {entity.name}")

        # Load cursor for incremental loads
        last_cursor = None
        state_file = None
        if entity.load_mode == "incremental_append" and entity.watermark_column:
            state_file = self._get_entity_state_file(system, entity.name)
            last_cursor = self._load_cursor(state_file)
            if last_cursor:
                logger.info(f"Entity {entity.name}: resuming from cursor {last_cursor}")

        # Build query
        query, params = self._build_entity_query(entity, last_cursor)
        logger.debug(f"Entity {entity.name} query: {query}")

        records: List[Dict[str, Any]] = []
        max_cursor: Optional[str] = None
        conn = None

        try:
            # Each thread gets its own connection
            conn = pyodbc.connect(conn_str)
            cur = conn.cursor()

            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            columns = [col[0] for col in cur.description]

            # Fetch in batches
            total_rows = 0
            while True:
                rows = cur.fetchmany(entity.fetch_batch_size)
                if not rows:
                    break

                for row in rows:
                    rec = {col: val for col, val in zip(columns, row)}
                    records.append(rec)

                    # Track max watermark value
                    if entity.watermark_column and entity.watermark_column in rec:
                        cursor_val = rec[entity.watermark_column]
                        if cursor_val is not None:
                            cursor_str = str(cursor_val)
                            if max_cursor is None or cursor_str > max_cursor:
                                max_cursor = cursor_str

                total_rows += len(rows)

            logger.info(f"Entity {entity.name}: extracted {total_rows} records")

            # Save new cursor
            if max_cursor and state_file:
                self._save_cursor(state_file, max_cursor, run_date)

            return EntityResult(
                entity_name=entity.name,
                records=records,
                cursor=max_cursor,
                row_count=len(records),
            )

        except Exception as e:
            logger.error(f"Entity {entity.name} extraction failed: {e}")
            return EntityResult(
                entity_name=entity.name,
                records=[],
                error=str(e),
            )
        finally:
            if conn:
                conn.close()

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from multiple database entities in parallel.

        Returns a flat list of all records from all entities, with an
        entity_name field added to each record.

        For more control over per-entity results, use fetch_multi() instead.
        """
        result = self.fetch_multi(cfg, run_date)

        # Flatten all records with entity_name tag
        all_records: List[Dict[str, Any]] = []
        for entity_name, entity_result in result.results.items():
            for rec in entity_result.records:
                rec["_entity_name"] = entity_name
                all_records.append(rec)

        # Return combined cursor summary
        cursors = {
            name: r.cursor
            for name, r in result.results.items()
            if r.cursor
        }
        cursor_json = json.dumps(cursors) if cursors else None

        return all_records, cursor_json

    def fetch_multi(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> MultiEntityResult:
        """Fetch records from multiple entities with structured results.

        This is the preferred method when you need per-entity control.
        """
        source_cfg = cfg["source"]
        system = source_cfg.get("system", "unknown")

        # Get connection string
        conn_ref = source_cfg.get("connection_ref")
        db_cfg = source_cfg.get("db", {})
        conn_env = conn_ref or db_cfg.get("conn_str_env")

        if not conn_env:
            raise ValueError(
                "db_multi source requires connection_ref or db.conn_str_env"
            )

        conn_str = os.environ.get(conn_env)
        if not conn_str:
            raise ValueError(
                f"Environment variable '{conn_env}' not set for DB connection"
            )

        # Parse entity configs
        entities_raw = source_cfg.get("entities", [])
        if not entities_raw:
            raise ValueError("db_multi source requires at least one entity in 'entities' list")

        entities = [EntityConfig.from_dict(e) for e in entities_raw]
        logger.info(f"Extracting {len(entities)} entities with max_workers={self.max_workers}")

        # Extract entities in parallel
        result = MultiEntityResult()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._extract_entity,
                    entity,
                    conn_str,
                    system,
                    run_date,
                ): entity.name
                for entity in entities
            }

            for future in as_completed(futures):
                entity_name = futures[future]
                try:
                    entity_result = future.result()
                    result.add_result(entity_result)
                except Exception as e:
                    logger.error(f"Entity {entity_name} failed with exception: {e}")
                    result.add_result(EntityResult(
                        entity_name=entity_name,
                        records=[],
                        error=str(e),
                    ))

        # Log summary
        logger.info(
            f"Multi-entity extraction complete: "
            f"{result.total_records} total records, "
            f"{len(result.failed_entities)} failures"
        )

        if result.failed_entities:
            logger.warning(f"Failed entities: {result.failed_entities}")

        return result
