"""Dataset configuration model.

This module contains the DatasetConfig class that combines Bronze and Silver intent
configurations into a unified dataset configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from core.config.models.intent import BronzeIntent, SilverIntent
from core.config.models.polybase import PolybaseSetup
from core.config.models.helpers import (
    require_optional_str,
    ensure_bucket_reference,
)


DEFAULT_BRONZE_BASE = Path("sampledata") / "bronze_samples"
DEFAULT_SILVER_BASE = Path("sampledata") / "silver_samples"


@dataclass
class PathStructure:
    """Configuration for how Bronze and Silver paths are structured.

    Allows independent control of path ordering for Bronze vs Silver layers.
    """
    bronze: Dict[str, str] = field(default_factory=dict)  # e.g., system_key, entity_key, pattern_key, date_key
    silver: Dict[str, str] = field(default_factory=dict)  # e.g., sample_key, silver_model_key, domain_key, load_date_key

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PathStructure":
        """Parse path_structure from config dict.

        Supports both nested format (bronze/silver subsections) and legacy flat format.
        """
        if not data:
            return cls()

        if not isinstance(data, dict):
            raise ValueError("path_structure must be a dictionary")

        # Check if using new nested format
        if "bronze" in data and isinstance(data["bronze"], dict):
            bronze_keys = data["bronze"]
        else:
            # Legacy flat format - extract bronze keys
            bronze_keys = {
                k: data.get(k, v)
                for k, v in {
                    "system_key": "system",
                    "entity_key": "table",
                    "pattern_key": "pattern",
                    "date_key": "dt",
                }.items()
            }

        # Check if using new nested format for silver
        if "silver" in data and isinstance(data["silver"], dict):
            silver_keys = data["silver"]
        else:
            # Legacy flat format - extract silver keys
            silver_keys = {
                k: data.get(k, v)
                for k, v in {
                    "domain_key": "domain",
                    "entity_key": "entity",
                    "version_key": "v",
                    "pattern_key": "pattern",
                    "load_date_key": "load_date",
                }.items()
            }

        return cls(bronze=bronze_keys, silver=silver_keys)


@dataclass
class DatasetConfig:
    """Complete dataset configuration combining Bronze and Silver intents."""

    system: str
    entity: str
    environment: Optional[str]
    domain: Optional[str]
    bronze: BronzeIntent
    silver: SilverIntent
    path_structure: PathStructure
    polybase_setup: Optional[PolybaseSetup] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetConfig":
        """Parse DatasetConfig from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("Dataset configuration must be a dictionary")
        system = data.get("system")
        entity = data.get("entity")
        if not system:
            raise ValueError("system is required")
        if not entity:
            raise ValueError("entity is required")
        environment = require_optional_str(data.get("environment"), "environment")
        domain = require_optional_str(data.get("domain"), "domain")

        # Handle top-level storage section (new format) or inline storage config (backward compat)
        bronze_data = dict(data.get("bronze") or {})
        silver_data = dict(data.get("silver") or {})

        storage_config = data.get("storage")
        if storage_config:
            # New format: Apply storage section to bronze and silver configs
            source_storage = storage_config.get("source", {})
            bronze_storage = storage_config.get("bronze", {})
            silver_storage = storage_config.get("silver", {})
            ensure_bucket_reference(source_storage, "source_data")
            ensure_bucket_reference(bronze_storage, "bronze_data")
            ensure_bucket_reference(silver_storage, "silver_data")

            # Map storage.source to bronze.source_storage and construct full path
            if "backend" in source_storage:
                backend = source_storage["backend"]
                bronze_data.setdefault("source_storage", backend)

                # Get path pattern from bronze section (relative path)
                path_pattern = bronze_data.get("path_pattern", "")

                # Construct full path from storage config + bronze.path_pattern
                if (
                    path_pattern
                    and not path_pattern.startswith("s3://")
                    and not path_pattern.startswith("./")
                ):
                    # Path is relative, combine with storage prefix
                    bucket = source_storage.get("bucket", "")
                    prefix = source_storage.get("prefix", "")

                    if backend == "s3":
                        # Build S3 URI: s3://bucket/prefix/path_pattern
                        full_path = f"s3://{bucket}/{prefix}{path_pattern}"
                        bronze_data["path_pattern"] = full_path
                    elif backend == "local" and prefix:
                        # Build local path: prefix/path_pattern
                        full_path = f"{prefix}{path_pattern}"
                        bronze_data["path_pattern"] = full_path

            # Map storage.bronze to bronze.output_storage/bucket/prefix
            if "backend" in bronze_storage:
                bronze_data.setdefault("output_storage", bronze_storage["backend"])
            if "bucket" in bronze_storage:
                bronze_data.setdefault("output_bucket", bronze_storage["bucket"])
            if "prefix" in bronze_storage:
                bronze_data.setdefault("output_prefix", bronze_storage["prefix"])

            # Map storage.silver to silver.input_storage/output_storage/bucket/prefix
            if "backend" in silver_storage:
                silver_data.setdefault("input_storage", silver_storage["backend"])
                silver_data.setdefault("output_storage", silver_storage["backend"])
            if "bucket" in silver_storage:
                silver_data.setdefault("output_bucket", silver_storage["bucket"])
            if "prefix" in silver_storage:
                silver_data.setdefault("output_prefix", silver_storage["prefix"])

        bronze_cfg = BronzeIntent.from_dict(bronze_data)
        silver_cfg = SilverIntent.from_dict(silver_data)
        polybase_cfg = PolybaseSetup.from_dict(data.get("polybase_setup"))
        path_struct = PathStructure.from_dict(data.get("path_structure", {}))
        return cls(
            system=system,
            entity=entity,
            environment=environment,
            domain=domain,
            bronze=bronze_cfg,
            silver=silver_cfg,
            path_structure=path_struct,
            polybase_setup=polybase_cfg,
        )

    @property
    def dataset_id(self) -> str:
        """Return the unique dataset identifier."""
        return f"{self.system}.{self.entity}"

    @property
    def bronze_base_path(self) -> Path:
        """Return the base path for Bronze data."""
        if self.environment:
            return DEFAULT_BRONZE_BASE / f"env={self.environment}"
        return DEFAULT_BRONZE_BASE

    @property
    def silver_base_path(self) -> Path:
        """Return the base path for Silver data."""
        if self.environment:
            return DEFAULT_SILVER_BASE / f"env={self.environment}"
        return DEFAULT_SILVER_BASE

    def bronze_relative_prefix(self) -> str:
        """Return the relative prefix for Bronze paths."""
        parts = [f"system={self.system}", f"entity={self.entity}"]
        return "/".join(parts)


def is_new_intent_config(raw: Dict[str, Any]) -> bool:
    """Check if a raw config dict uses the new intent-based format."""
    return (
        isinstance(raw, dict)
        and "system" in raw
        and "entity" in raw
        and "bronze" in raw
        and "silver" in raw
    )


__all__ = [
    "DEFAULT_BRONZE_BASE",
    "DEFAULT_SILVER_BASE",
    "PathStructure",
    "DatasetConfig",
    "is_new_intent_config",
]
