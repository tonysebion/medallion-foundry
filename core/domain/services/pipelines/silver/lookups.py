"""Lookup enrichment for Silver transformation per spec Section 5.

Implements the single_source_with_lookups pattern that enriches primary
data with reference/lookup tables.

Example config:
```yaml
silver:
  transformation_pattern: single_source_with_lookups
  lookups:
    - name: product_dim
      path: "silver/dimensions/product/current"
      join_keys:
        - source: product_id
          lookup: id
      select_columns:
        - product_name
        - category
    - name: customer_dim
      path: "silver/dimensions/customer/current"
      join_keys:
        - source: customer_id
          lookup: customer_id
      select_columns:
        - customer_name
        - segment
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
class LookupJoinKey:
    """Key mapping for lookup join."""

    source: str  # Column in primary data
    lookup: str  # Column in lookup table

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LookupJoinKey":
        """Create from dictionary."""
        return cls(
            source=data.get("source") or data.get("left", ""),
            lookup=data.get("lookup") or data.get("right", ""),
        )


@dataclass
class LookupConfig:
    """Configuration for a single lookup table."""

    name: str
    path: str
    join_keys: List[LookupJoinKey] = field(default_factory=list)
    select_columns: List[str] = field(default_factory=list)
    join_type: str = "left"  # left, inner
    prefix: Optional[str] = None  # Prefix for joined columns
    cache: bool = True  # Whether to cache the lookup table

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LookupConfig":
        """Create from dictionary."""
        name = data.get("name")
        if not name:
            raise ValueError("Lookup must have a 'name'")

        path = data.get("path")
        if not path:
            raise ValueError(f"Lookup '{name}' must have a 'path'")

        join_keys_raw = data.get("join_keys", [])
        join_keys = []
        for key_data in join_keys_raw:
            if isinstance(key_data, str):
                join_keys.append(LookupJoinKey(source=key_data, lookup=key_data))
            elif isinstance(key_data, dict):
                join_keys.append(LookupJoinKey.from_dict(key_data))
            else:
                raise ValueError(f"Invalid join_key format in lookup '{name}'")

        if not join_keys:
            raise ValueError(f"Lookup '{name}' must have at least one join_key")

        return cls(
            name=name,
            path=path,
            join_keys=join_keys,
            select_columns=data.get("select_columns", []),
            join_type=data.get("join_type", "left"),
            prefix=data.get("prefix"),
            cache=data.get("cache", True),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "path": self.path,
            "join_keys": [
                {"source": k.source, "lookup": k.lookup} for k in self.join_keys
            ],
            "select_columns": self.select_columns,
            "join_type": self.join_type,
            "prefix": self.prefix,
            "cache": self.cache,
        }


@dataclass
class LookupResult:
    """Result of lookup enrichment."""

    enriched_df: pd.DataFrame
    lookup_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    columns_added: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for metadata."""
        return {
            "columns_added": self.columns_added,
            "lookup_stats": self.lookup_stats,
            "total_lookups": len(self.lookup_stats),
        }


class LookupEnricher:
    """Enriches data with lookup tables.

    Handles loading, caching, and joining lookup tables to primary data.
    """

    def __init__(self, base_path: Optional[Path] = None):
        """Initialize enricher.

        Args:
            base_path: Base path for resolving relative lookup paths
        """
        self.base_path = base_path or Path(".")
        self._cache: Dict[str, pd.DataFrame] = {}

    def load_lookup(self, config: LookupConfig) -> pd.DataFrame:
        """Load a lookup table.

        Args:
            config: Lookup configuration

        Returns:
            DataFrame with lookup data
        """
        cache_key = config.path

        if config.cache and cache_key in self._cache:
            logger.debug("Using cached lookup '%s'", config.name)
            return self._cache[cache_key]

        lookup_path = Path(config.path)
        if not lookup_path.is_absolute():
            lookup_path = self.base_path / lookup_path

        df = self._read_lookup_data(lookup_path)

        if config.cache:
            self._cache[cache_key] = df

        logger.info("Loaded lookup '%s' with %d rows", config.name, len(df))
        return df

    def _read_lookup_data(self, path: Path) -> pd.DataFrame:
        """Read lookup data from path."""
        return DataFrameLoader.from_directory(path, recursive=True)

    def enrich(
        self,
        df: pd.DataFrame,
        configs: List[LookupConfig],
    ) -> LookupResult:
        """Enrich data with multiple lookup tables.

        Args:
            df: Primary DataFrame to enrich
            configs: List of lookup configurations

        Returns:
            LookupResult with enriched DataFrame and statistics
        """
        result_df = df.copy()
        lookup_stats: Dict[str, Dict[str, Any]] = {}
        all_columns_added: List[str] = []

        for config in configs:
            try:
                result_df, stats, columns_added = self._apply_lookup(
                    result_df, config
                )
                lookup_stats[config.name] = stats
                all_columns_added.extend(columns_added)
            except Exception as e:
                logger.error("Failed to apply lookup '%s': %s", config.name, e)
                lookup_stats[config.name] = {
                    "status": "error",
                    "error": str(e),
                    "rows_matched": 0,
                }

        return LookupResult(
            enriched_df=result_df,
            lookup_stats=lookup_stats,
            columns_added=all_columns_added,
        )

    def _apply_lookup(
        self,
        df: pd.DataFrame,
        config: LookupConfig,
    ) -> Tuple[pd.DataFrame, Dict[str, Any], List[str]]:
        """Apply a single lookup to the DataFrame."""
        lookup_df = self.load_lookup(config)

        # Build join keys
        left_keys = [k.source for k in config.join_keys]
        right_keys = [k.lookup for k in config.join_keys]

        # Validate keys exist
        missing_left = [k for k in left_keys if k not in df.columns]
        missing_right = [k for k in right_keys if k not in lookup_df.columns]

        if missing_left:
            raise ValueError(f"Source columns missing for lookup '{config.name}': {missing_left}")
        if missing_right:
            raise ValueError(f"Lookup columns missing for '{config.name}': {missing_right}")

        # Select columns from lookup
        if config.select_columns:
            select_cols = right_keys + [
                c for c in config.select_columns if c not in right_keys
            ]
            missing_select = [c for c in select_cols if c not in lookup_df.columns]
            if missing_select:
                raise ValueError(
                    f"Select columns missing in lookup '{config.name}': {missing_select}"
                )
            lookup_subset = lookup_df[select_cols].copy()
        else:
            lookup_subset = lookup_df.copy()

        # Rename lookup columns to avoid conflicts
        rename_map: Dict[str, str] = {}
        columns_added: List[str] = []

        for col in lookup_subset.columns:
            if col in right_keys:
                continue  # Join keys will be handled separately
            new_name = col
            if config.prefix:
                new_name = f"{config.prefix}{col}"
            elif col in df.columns:
                new_name = f"{config.name}_{col}"
            rename_map[col] = new_name
            columns_added.append(new_name)

        lookup_subset = lookup_subset.rename(columns=rename_map)

        # Create join condition (rename right keys to match left)
        right_key_rename = {
            r: left_key
            for left_key, r in zip(left_keys, right_keys)
        }
        lookup_subset = lookup_subset.rename(columns=right_key_rename)

        # Perform join
        original_len = len(df)
        result = pd.merge(
            df,
            lookup_subset,
            how=config.join_type,
            on=left_keys,
            suffixes=("", f"_{config.name}"),
        )

        # Calculate statistics
        matched = len(result[result[columns_added[0]].notna()]) if columns_added else len(result)
        unmatched = original_len - matched

        stats = {
            "status": "success",
            "rows_in": original_len,
            "rows_out": len(result),
            "rows_matched": matched,
            "rows_unmatched": unmatched,
            "columns_added": columns_added,
            "join_type": config.join_type,
        }

        logger.info(
            "Applied lookup '%s': %d/%d rows matched", config.name, matched, original_len
        )

        return result, stats, columns_added

    def clear_cache(self) -> None:
        """Clear the lookup cache."""
        self._cache.clear()
        logger.debug("Cleared lookup cache")


def parse_lookup_configs(config: Dict[str, Any]) -> List[LookupConfig]:
    """Parse lookup configurations from silver config.

    Args:
        config: Full pipeline config or silver config section

    Returns:
        List of LookupConfig objects
    """
    silver_cfg = config.get("silver", config)
    lookups_raw = silver_cfg.get("lookups", [])

    if not lookups_raw:
        return []

    configs = []
    for lookup_data in lookups_raw:
        try:
            configs.append(LookupConfig.from_dict(lookup_data))
        except Exception as e:
            logger.warning("Could not parse lookup config: %s", e)

    logger.info("Parsed %d lookup configurations", len(configs))
    return configs


def enrich_with_lookups(
    df: pd.DataFrame,
    config: Dict[str, Any],
    base_path: Optional[Path] = None,
) -> LookupResult:
    """Convenience function to enrich data with lookups from config.

    Args:
        df: Primary DataFrame to enrich
        config: Pipeline config with silver.lookups section
        base_path: Base path for resolving lookup paths

    Returns:
        LookupResult with enriched DataFrame
    """
    lookup_configs = parse_lookup_configs(config)

    if not lookup_configs:
        return LookupResult(enriched_df=df)

    enricher = LookupEnricher(base_path)
    return enricher.enrich(df, lookup_configs)
