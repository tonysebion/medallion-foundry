"""Centralized path & partition abstractions for Bronze and Silver layers.

This module provides:
- BronzePartition / SilverPartition: Dataclasses for partition metadata
- build_bronze_partition / build_silver_partition: Partition builders from config
- build_bronze_relative_path / build_silver_partition_path: Path construction helpers
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

from core.foundation.primitives.patterns import LoadPattern


def _bronze_config(cfg: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    source = cfg["source"]
    platform = cfg["platform"]
    bronze = platform.get("bronze", {})
    run_cfg = source.get("run", {})
    bronze_options = bronze.get("options", {})
    path_structure = cfg.get("path_structure", {})
    bronze_keys = (
        path_structure.get("bronze", {}) if isinstance(path_structure, dict) else {}
    )
    return bronze, run_cfg, bronze_options, bronze_keys


def _bronze_key_names(bronze_keys: Dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        bronze_keys.get("system_key", "system"),
        bronze_keys.get("entity_key", "table"),
        bronze_keys.get("pattern_key", "pattern"),
        bronze_keys.get("date_key", "dt"),
    )


# =============================================================================
# Partition Dataclasses
# =============================================================================


@dataclass(frozen=True)
class BronzePartition:
    """Partition metadata for Bronze layer paths."""

    system: str
    table: str
    pattern: str
    run_date: date
    system_key: str = "system"
    entity_key: str = "table"
    pattern_key: str = "pattern"
    date_key: str = "dt"

    def relative_path(self) -> Path:
        return (
            Path(f"{self.system_key}={self.system}")
            / f"{self.entity_key}={self.table}"
            / f"{self.pattern_key}={self.pattern}"
            / f"{self.date_key}={self.run_date.isoformat()}"
        )


@dataclass(frozen=True)
class SilverPartition:
    """Partition metadata for Silver layer paths."""

    domain: str
    entity: str
    version: int
    load_partition_name: str
    run_date: date
    include_pattern_folder: bool
    pattern: str | None = None
    domain_key: str = "domain"
    entity_key: str = "entity"
    version_key: str = "v"
    pattern_key: str = "pattern"
    load_date_key: str = "load_date"

    def base_path(self) -> Path:
        parts = [
            f"{self.domain_key}={self.domain}",
            f"{self.entity_key}={self.entity}",
            f"{self.version_key}{self.version}",
        ]
        if self.include_pattern_folder and self.pattern:
            parts.append(f"{self.pattern_key}={self.pattern}")
        parts.append(f"{self.load_date_key}={self.run_date.isoformat()}")
        return Path("/").joinpath(*parts)  # normalized assembly


# =============================================================================
# Partition Builders
# =============================================================================


def build_bronze_partition(cfg: Dict[str, Any], run_date: date) -> BronzePartition:
    """Build a BronzePartition from configuration."""
    source = cfg["source"]
    bronze, run_cfg, bronze_options, bronze_keys = _bronze_config(cfg)
    pattern = (
        bronze_options.get("pattern_folder")
        or run_cfg.get("pattern_folder")
        or run_cfg.get("load_pattern", "full")
    )
    system_key, entity_key, pattern_key, date_key = _bronze_key_names(bronze_keys)

    return BronzePartition(
        system=source["system"],
        table=source["table"],
        pattern=pattern,
        run_date=run_date,
        system_key=system_key,
        entity_key=entity_key,
        pattern_key=pattern_key,
        date_key=date_key,
    )


def build_silver_partition(cfg: Dict[str, Any], run_date: date) -> SilverPartition:
    """Build a SilverPartition from configuration."""
    silver = cfg.get("silver", {})
    source = cfg["source"]
    pattern = source.get("run", {}).get("load_pattern", "full")

    # Get path structure keys
    path_structure = cfg.get("path_structure", {})
    silver_keys = (
        path_structure.get("silver", {}) if isinstance(path_structure, dict) else {}
    )

    domain_key = silver_keys.get("domain_key", "domain")
    entity_key = silver_keys.get("entity_key", "entity")
    version_key = silver_keys.get("version_key", "v")
    pattern_key = silver_keys.get("pattern_key", "pattern")
    load_date_key = silver_keys.get("load_date_key", "load_date")

    return SilverPartition(
        domain=silver.get("domain", "default"),
        entity=silver.get("entity", "dataset"),
        version=silver.get("version", 1),
        load_partition_name=silver.get("load_partition_name", "load_date"),
        run_date=run_date,
        include_pattern_folder=silver.get("include_pattern_folder", False),
        pattern=pattern,
        domain_key=domain_key,
        entity_key=entity_key,
        version_key=version_key,
        pattern_key=pattern_key,
        load_date_key=load_date_key,
    )


# =============================================================================
# Path Construction Helpers
# =============================================================================


def build_bronze_relative_path(cfg: dict, run_date: date) -> str:
    """Build relative path for Bronze output directory."""
    bronze, run_cfg, bronze_options, bronze_keys = _bronze_config(cfg)
    partitioning = bronze.get("partitioning", {})
    use_dt = partitioning.get("use_dt_partition", True)

    partition_strategy = partitioning.get("partition_strategy", "date")
    system_key, entity_key, pattern_key, date_key = _bronze_key_names(bronze_keys)

    source_cfg = cfg["source"]
    system = source_cfg["system"]
    table = source_cfg["table"]

    base_path = f"{system_key}={system}/{entity_key}={table}/"

    load_pattern = run_cfg.get("load_pattern", LoadPattern.SNAPSHOT.value)
    pattern_folder = (
        bronze_options.get("pattern_folder")
        or run_cfg.get("pattern_folder")
        or load_pattern
    )
    if pattern_folder:
        base_path += f"{pattern_key}={pattern_folder}/"

    if not use_dt:
        return base_path

    if partition_strategy == "date":
        partition = build_bronze_partition(cfg, run_date)
        return partition.relative_path().as_posix() + "/"
    elif partition_strategy == "hourly":
        current_hour = datetime.now().strftime("%H")
        return f"{base_path}{date_key}={run_date.isoformat()}/hour={current_hour}/"
    elif partition_strategy == "timestamp":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        return f"{base_path}{date_key}={run_date.isoformat()}/batch={timestamp}/"
    elif partition_strategy == "batch_id":
        from datetime import datetime as dt
        import uuid

        batch_id = run_cfg.get("batch_id") or f"{dt.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        return f"{base_path}{date_key}={run_date.isoformat()}/batch_id={batch_id}/"
    else:
        return f"{base_path}{date_key}={run_date.isoformat()}/"


def build_silver_partition_path(
    silver_base: Path,
    domain: str,
    entity: str,
    version: int,
    load_partition_name: str,
    include_pattern_folder: bool,
    load_pattern: LoadPattern,
    run_date: date,
    path_structure: dict | None = None,
    pattern_folder: str | None = None,
) -> Path:
    """Build path for Silver output directory."""
    # Extract path structure keys for silver layer
    if path_structure and isinstance(path_structure, dict):
        silver_keys = path_structure.get("silver", {})
    else:
        silver_keys = {}

    domain_key = silver_keys.get("domain_key", "domain")
    entity_key = silver_keys.get("entity_key", "entity")
    version_key = silver_keys.get("version_key", "v")
    pattern_key = silver_keys.get("pattern_key", "pattern")
    load_date_key = silver_keys.get("load_date_key", "load_date")

    # Build path: domain/entity/v{version}/[pattern/]load_date
    path = (
        silver_base
        / f"{domain_key}={domain}"
        / f"{entity_key}={entity}"
        / f"{version_key}{version}"
    )
    if include_pattern_folder:
        # Use pattern_folder if provided, otherwise fall back to load_pattern enum value
        pattern_value = pattern_folder if pattern_folder else load_pattern.value
        path = path / f"{pattern_key}={pattern_value}"
    path = path / f"{load_date_key}={run_date.isoformat()}"
    return path
