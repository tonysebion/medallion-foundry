"""Centralized path & partition abstractions for Bronze and Silver layers."""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from datetime import date
from typing import Dict, Any


@dataclass(frozen=True)
class BronzePartition:
    system: str
    table: str
    pattern: str
    run_date: date

    def relative_path(self) -> Path:
        return (
            Path(f"system={self.system}")
            / f"table={self.table}"
            / f"pattern={self.pattern}"
            / f"dt={self.run_date.isoformat()}"
        )


@dataclass(frozen=True)
class SilverPartition:
    domain: str
    entity: str
    version: int
    load_partition_name: str
    run_date: date
    include_pattern_folder: bool
    pattern: str | None = None

    def base_path(self) -> Path:
        parts = [
            f"domain={self.domain}",
            f"entity={self.entity}",
            f"version={self.version}",
        ]
        if self.include_pattern_folder and self.pattern:
            parts.append(f"pattern={self.pattern}")
        parts.append(f"{self.load_partition_name}={self.run_date.isoformat()}")
        return Path("/").joinpath(*parts)  # normalized assembly


def build_bronze_partition(cfg: Dict[str, Any], run_date: date) -> BronzePartition:
    source = cfg["source"]
    pattern = source.get("run", {}).get("load_pattern", "full")
    return BronzePartition(
        system=source["system"],
        table=source["table"],
        pattern=pattern,
        run_date=run_date,
    )


def build_silver_partition(cfg: Dict[str, Any], run_date: date) -> SilverPartition:
    silver = cfg.get("silver", {})
    source = cfg["source"]
    pattern = source.get("run", {}).get("load_pattern", "full")
    return SilverPartition(
        domain=silver.get("domain", "default"),
        entity=silver.get("entity", "dataset"),
        version=silver.get("version", 1),
        load_partition_name=silver.get("load_partition_name", "load_date"),
        run_date=run_date,
        include_pattern_folder=silver.get("include_pattern_folder", False),
        pattern=pattern,
    )
