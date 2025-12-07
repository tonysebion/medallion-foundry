"""Multi-source join configuration for Silver transformation per spec Section 5.

Implements the multi_source_join pattern that joins multiple Bronze sources
into a single Silver output.

Example config:
```yaml
silver:
  transformation_pattern: multi_source_join
  join_config:
    sources:
      - name: orders
        path: "bronze/orders/current"
        role: primary
      - name: order_items
        path: "bronze/order_items/current"
        role: secondary
    join_keys:
      - left: order_id
        right: order_id
    join_type: inner
    output:
      primary_keys: [order_id, item_id]
      order_column: updated_at
```
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.infrastructure.runtime.file_io import DataFrameLoader

logger = logging.getLogger(__name__)


@dataclass
class JoinSource:
    """Configuration for a join source."""

    name: str
    path: str
    role: str = "secondary"  # primary, secondary
    select_columns: Optional[List[str]] = None
    filter_expression: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JoinSource":
        """Create from dictionary."""
        name = data.get("name")
        if not name:
            raise ValueError("Join source must have a 'name'")

        path = data.get("path")
        if not path:
            raise ValueError(f"Join source '{name}' must have a 'path'")

        return cls(
            name=name,
            path=path,
            role=data.get("role", "secondary"),
            select_columns=data.get("select_columns"),
            filter_expression=data.get("filter"),
        )


@dataclass
class JoinKeyPair:
    """Key mapping for join."""

    left: str
    right: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JoinKeyPair":
        """Create from dictionary."""
        if isinstance(data, str):
            return cls(left=data, right=data)
        return cls(
            left=data.get("left") or data.get("source", ""),
            right=data.get("right") or data.get("target", ""),
        )


@dataclass
class JoinConfig:
    """Configuration for multi-source join."""

    sources: List[JoinSource] = field(default_factory=list)
    join_keys: List[JoinKeyPair] = field(default_factory=list)
    join_type: str = "inner"
    output_primary_keys: List[str] = field(default_factory=list)
    output_order_column: Optional[str] = None
    chunk_size: int = 0  # 0 = no chunking

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JoinConfig":
        """Create from dictionary."""
        sources_raw = data.get("sources", [])
        sources = [JoinSource.from_dict(s) for s in sources_raw]

        if len(sources) < 2:
            raise ValueError("multi_source_join requires at least 2 sources")

        join_keys_raw = data.get("join_keys", [])
        join_keys = []
        for key_data in join_keys_raw:
            if isinstance(key_data, str):
                join_keys.append(JoinKeyPair(left=key_data, right=key_data))
            else:
                join_keys.append(JoinKeyPair.from_dict(key_data))

        if not join_keys:
            raise ValueError("multi_source_join requires at least one join_key")

        output_cfg = data.get("output", {})

        return cls(
            sources=sources,
            join_keys=join_keys,
            join_type=data.get("join_type", "inner"),
            output_primary_keys=output_cfg.get("primary_keys", []),
            output_order_column=output_cfg.get("order_column"),
            chunk_size=data.get("chunk_size", 0),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sources": [
                {
                    "name": s.name,
                    "path": s.path,
                    "role": s.role,
                    "select_columns": s.select_columns,
                    "filter": s.filter_expression,
                }
                for s in self.sources
            ],
            "join_keys": [{"left": k.left, "right": k.right} for k in self.join_keys],
            "join_type": self.join_type,
            "output": {
                "primary_keys": self.output_primary_keys,
                "order_column": self.output_order_column,
            },
            "chunk_size": self.chunk_size,
        }


@dataclass
class JoinResult:
    """Result of multi-source join."""

    joined_df: pd.DataFrame
    stats: Dict[str, Any] = field(default_factory=dict)
    column_lineage: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for metadata."""
        return {
            "row_count": len(self.joined_df),
            "column_count": len(self.joined_df.columns),
            "stats": self.stats,
            "column_lineage": self.column_lineage,
        }


class MultiSourceJoiner:
    """Handles multi-source join operations."""

    VALID_JOIN_TYPES = {"inner", "left", "right", "outer"}

    def __init__(self, base_path: Optional[Path] = None):
        """Initialize joiner.

        Args:
            base_path: Base path for resolving relative source paths
        """
        self.base_path = base_path or Path(".")

    def load_source(self, source: JoinSource) -> pd.DataFrame:
        """Load a join source.

        Args:
            source: Source configuration

        Returns:
            DataFrame with source data
        """
        source_path = Path(source.path)
        if not source_path.is_absolute():
            source_path = self.base_path / source_path

        df = self._read_source_data(source_path)

        # Apply column selection
        if source.select_columns:
            missing = [c for c in source.select_columns if c not in df.columns]
            if missing:
                raise ValueError(f"Columns missing in source '{source.name}': {missing}")
            df = df[source.select_columns]

        # Apply filter
        if source.filter_expression:
            try:
                df = df.query(source.filter_expression)
            except Exception as e:
                logger.warning("Could not apply filter to '%s': %s", source.name, e)

        logger.info("Loaded source '%s' with %d rows", source.name, len(df))
        return df

    def _read_source_data(self, path: Path) -> pd.DataFrame:
        """Read source data from path."""
        return DataFrameLoader.from_directory(path, recursive=True)

    def join(self, config: JoinConfig) -> JoinResult:
        """Execute multi-source join.

        Args:
            config: Join configuration

        Returns:
            JoinResult with joined DataFrame and statistics
        """
        if config.join_type not in self.VALID_JOIN_TYPES:
            raise ValueError(f"join_type must be one of {self.VALID_JOIN_TYPES}")

        # Separate primary and secondary sources
        primary_sources = [s for s in config.sources if s.role == "primary"]
        secondary_sources = [s for s in config.sources if s.role != "primary"]

        if not primary_sources:
            # If no primary specified, use first source as primary
            primary_sources = [config.sources[0]]
            secondary_sources = config.sources[1:]

        # Load and join
        result_df = self.load_source(primary_sources[0])
        column_lineage: Dict[str, str] = {
            col: primary_sources[0].name for col in result_df.columns
        }

        stats = {
            "sources": {},
            "join_type": config.join_type,
            "total_joins": len(secondary_sources),
        }
        stats["sources"][primary_sources[0].name] = {
            "role": "primary",
            "rows": len(result_df),
            "columns": list(result_df.columns),
        }

        # Join each secondary source
        for secondary in secondary_sources:
            result_df, join_stats = self._join_source(
                result_df, secondary, config
            )
            stats["sources"][secondary.name] = join_stats

            # Track column lineage for new columns
            for col in result_df.columns:
                if col not in column_lineage:
                    column_lineage[col] = secondary.name

        stats["final_rows"] = len(result_df)
        stats["final_columns"] = list(result_df.columns)

        return JoinResult(
            joined_df=result_df,
            stats=stats,
            column_lineage=column_lineage,
        )

    def _join_source(
        self,
        left_df: pd.DataFrame,
        source: JoinSource,
        config: JoinConfig,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Join a single source to the result."""
        right_df = self.load_source(source)

        # Extract join keys
        left_keys = [k.left for k in config.join_keys]
        right_keys = [k.right for k in config.join_keys]

        # Validate keys
        missing_left = [k for k in left_keys if k not in left_df.columns]
        missing_right = [k for k in right_keys if k not in right_df.columns]

        if missing_left:
            raise ValueError(f"Left join keys missing: {missing_left}")
        if missing_right:
            raise ValueError(f"Right join keys missing for '{source.name}': {missing_right}")

        # Rename right keys to match left
        right_key_rename = {r: l for l, r in zip(left_keys, right_keys) if l != r}
        if right_key_rename:
            right_df = right_df.rename(columns=right_key_rename)

        # Perform join
        original_left_len = len(left_df)
        result = pd.merge(
            left_df,
            right_df,
            how=config.join_type,
            on=left_keys,
            suffixes=("", f"_{source.name}"),
        )

        stats = {
            "role": source.role,
            "rows": len(right_df),
            "columns": list(right_df.columns),
            "rows_after_join": len(result),
            "expansion_factor": len(result) / max(1, original_left_len),
        }

        logger.info(
            "Joined '%s': %d -> %d rows", source.name, original_left_len, len(result)
        )

        return result, stats


def parse_join_config(config: Dict[str, Any]) -> Optional[JoinConfig]:
    """Parse join configuration from silver config.

    Args:
        config: Full pipeline config or silver config section

    Returns:
        JoinConfig if configured, None otherwise
    """
    silver_cfg = config.get("silver", config)
    join_cfg = silver_cfg.get("join_config")

    if not join_cfg:
        return None

    try:
        return JoinConfig.from_dict(join_cfg)
    except Exception as e:
        logger.warning("Could not parse join config: %s", e)
        return None


def execute_multi_source_join(
    config: Dict[str, Any],
    base_path: Optional[Path] = None,
) -> Optional[JoinResult]:
    """Convenience function to execute multi-source join from config.

    Args:
        config: Pipeline config with silver.join_config section
        base_path: Base path for resolving source paths

    Returns:
        JoinResult if join configured, None otherwise
    """
    join_config = parse_join_config(config)

    if not join_config:
        return None

    joiner = MultiSourceJoiner(base_path)
    return joiner.join(join_config)
