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
from typing import Any, Dict, List, Optional, Tuple

from core.domain.adapters.extractors.cursor_state import (
    CursorStateManager,
    build_incremental_query,
)
from core.domain.adapters.extractors.db_runner import fetch_records_from_query
from core.domain.adapters.extractors.mixins import default_retry
from core.infrastructure.io.extractors.base import BaseExtractor, register_extractor

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
        self._state_manager = CursorStateManager()
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

        cursor_column = (
            entity.watermark_column
            if entity.load_mode == "incremental_append"
            else None
        )
        if cursor_column and last_cursor:
            query = build_incremental_query(base_query, cursor_column, last_cursor)
            return query, (last_cursor,)

        return base_query, None

    @default_retry
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
        logger.info("Starting extraction for entity: %s", entity.name)

        state_key = f"{system}_{entity.name}"
        last_cursor = None
        if entity.load_mode == "incremental_append" and entity.watermark_column:
            last_cursor = self._state_manager.load_cursor(state_key)
            if last_cursor:
                logger.info("Entity %s: resuming from cursor %s", entity.name, last_cursor)

        # Build query
        query, params = self._build_entity_query(entity, last_cursor)
        logger.debug("Entity %s query: %s", entity.name, query)

        try:
            records, max_cursor = fetch_records_from_query(
                driver="pyodbc",
                conn_str=conn_str,
                query=query,
                params=params,
                batch_size=entity.fetch_batch_size,
                cursor_column=entity.watermark_column
                if entity.load_mode == "incremental_append"
                else None,
            )
        except Exception as exc:
            logger.error("Entity %s extraction failed: %s", entity.name, exc)
            return EntityResult(
                entity_name=entity.name,
                records=[],
                error=str(exc),
            )

        logger.info("Entity %s: extracted %d records", entity.name, len(records))
        if max_cursor:
            self._state_manager.save_cursor(state_key, max_cursor, run_date)

        return EntityResult(
            entity_name=entity.name,
            records=records,
            cursor=max_cursor,
            row_count=len(records),
        )

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
        logger.info("Extracting %d entities with max_workers=%d", len(entities), self.max_workers)

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
                    logger.error("Entity %s failed with exception: %s", entity_name, e)
                    result.add_result(EntityResult(
                        entity_name=entity_name,
                        records=[],
                        error=str(e),
                    ))

        # Log summary
        logger.info(
            "Multi-entity extraction complete: %d total records, %d failures",
            result.total_records,
            len(result.failed_entities),
        )

        if result.failed_entities:
            logger.warning("Failed entities: %s", result.failed_entities)

        return result
