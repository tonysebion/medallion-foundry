"""Builder functions for run metadata per spec Section 8.

This module contains functions for creating, loading, and persisting run metadata:
- generate_run_id: Generate a unique run ID
- compute_config_hash: Compute hash of configuration for reproducibility
- get_code_version: Get current code version from package or git
- build_run_metadata: Build RunMetadata from config dictionary
- get_metadata_path: Get path for run metadata
- write_run_metadata: Write metadata to file
- load_run_metadata: Load metadata from file
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .metadata_models import (
    DataClassification,
    Layer,
    OwnerInfo,
    QualityRuleResult,
    RunMetadata,
    RunStatus,
)

logger = logging.getLogger(__name__)


def generate_run_id() -> str:
    """Generate a unique run ID."""
    return str(uuid.uuid4())


def compute_config_hash(config: Dict[str, Any]) -> str:
    """Compute a SHA-256 hash of the configuration for reproducibility tracking."""
    # Sort keys for deterministic hashing
    config_str = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(config_str.encode("utf-8")).hexdigest()[:16]


def get_code_version() -> str:
    """Get the current code version from package metadata or git."""
    try:
        # Try to get version from package metadata
        from importlib.metadata import version

        return version("medallion-foundry")
    except Exception:
        pass

    try:
        # Fall back to git describe
        import subprocess

        result = subprocess.run(
            ["git", "describe", "--tags", "--always", "--dirty"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass

    return "unknown"


def build_run_metadata(
    config: Dict[str, Any],
    layer: Layer,
    run_id: Optional[str] = None,
) -> RunMetadata:
    """Build RunMetadata from a config dictionary.

    Args:
        config: The pipeline configuration dictionary
        layer: The layer (bronze or silver)
        run_id: Optional run ID (generated if not provided)

    Returns:
        Initialized RunMetadata object
    """
    source = config.get("source", {})
    silver = config.get("silver", {})
    config.get("platform", {})

    # Extract pipeline_id from config (pattern_id or generate from system.table)
    pipeline_id = source.get("pattern_id") or config.get("pipeline_id")
    if not pipeline_id:
        system = source.get("system", "unknown")
        table = source.get("table", "unknown")
        pipeline_id = f"{layer.value}_{system}_{table}_ingest"

    # Extract environment
    environment = config.get("environment", "dev")

    # Extract domain (from silver config or source system)
    domain = silver.get("domain") or source.get("system", "default")

    # Extract data classification
    classification_str = config.get("data_classification", "internal")
    try:
        data_classification = DataClassification(classification_str.lower())
    except ValueError:
        data_classification = DataClassification.INTERNAL

    # Extract schema evolution mode (supports root-level or source.run overrides)
    schema_cfg = config.get("schema_evolution")
    if not isinstance(schema_cfg, dict):
        schema_cfg = {}

    if not schema_cfg:
        run_cfg = config.get("source", {}).get("run", {})
        run_schema_cfg = run_cfg.get("schema_evolution")
        if isinstance(run_schema_cfg, dict):
            schema_cfg = run_schema_cfg

    schema_evolution_mode = schema_cfg.get("mode", "strict") if isinstance(schema_cfg, dict) else "strict"

    # Extract owners
    owners = OwnerInfo.from_dict(config.get("owners"))

    return RunMetadata(
        pipeline_id=pipeline_id,
        run_id=run_id or generate_run_id(),
        layer=layer,
        environment=environment,
        source_system=source.get("system", "unknown"),
        domain=domain,
        table=source.get("table", "unknown"),
        data_classification=data_classification,
        schema_evolution_mode=schema_evolution_mode,
        config_hash=compute_config_hash(config),
        code_version=get_code_version(),
        owners=owners,
    )


def get_metadata_path(
    base_path: Path,
    environment: str,
    layer: Layer,
    domain: str,
    table: str,
    run_id: str,
) -> Path:
    """Get the path for run metadata per spec Section 8.

    Path format: /data/<env>/<layer>/<domain>/<table>/_metadata/runs/run=<run_id>.json
    """
    layer_segment = str(layer.value)
    return (
        base_path
        / "data"
        / environment
        / layer_segment
        / domain
        / table
        / "_metadata"
        / "runs"
        / f"run={run_id}.json"
    )


def write_run_metadata(
    metadata: RunMetadata,
    base_path: Path,
    use_spec_path: bool = True,
) -> Path:
    """Write run metadata to the appropriate path.

    Args:
        metadata: The RunMetadata to write
        base_path: Base path for data storage
        use_spec_path: If True, use spec-compliant path structure

    Returns:
        Path to the written metadata file
    """
    if use_spec_path:
        metadata_path = get_metadata_path(
            base_path,
            metadata.environment,
            metadata.layer,
            metadata.domain,
            metadata.table,
            metadata.run_id,
        )
    else:
        # Legacy path: write to _metadata.json in output directory
        metadata_path = base_path / "_metadata.json"

    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(metadata.to_dict(), f, indent=2)

    logger.info("Wrote run metadata to %s", metadata_path)
    return metadata_path


def load_run_metadata(path: Path) -> RunMetadata:
    """Load run metadata from a JSON file."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Parse datetime fields
    start_time = None
    end_time = None
    if data.get("start_time"):
        start_time = datetime.fromisoformat(data["start_time"].rstrip("Z"))
    if data.get("end_time"):
        end_time = datetime.fromisoformat(data["end_time"].rstrip("Z"))

    # Parse rule results
    rule_results = [
        QualityRuleResult(
            rule_id=r["rule_id"],
            level=r["level"],
            expression=r["expression"],
            passed=r["passed"],
            failed_count=r.get("failed_count", 0),
            total_count=r.get("total_count", 0),
        )
        for r in data.get("rule_results", [])
    ]

    return RunMetadata(
        pipeline_id=data["pipeline_id"],
        run_id=data["run_id"],
        layer=Layer(data["layer"]),
        environment=data["environment"],
        source_system=data["source_system"],
        domain=data["domain"],
        table=data.get("table", ""),
        data_classification=DataClassification(data.get("data_classification", "internal")),
        schema_evolution_mode=data.get("schema_evolution_mode", "strict"),
        start_time=start_time,
        end_time=end_time,
        status=RunStatus(data.get("status", "pending")),
        row_count_in=data.get("row_count_in", 0),
        row_count_out=data.get("row_count_out", 0),
        config_hash=data.get("config_hash", ""),
        code_version=data.get("code_version", ""),
        rule_results=rule_results,
        upstream_runs=data.get("upstream_runs", []),
        owners=OwnerInfo.from_dict(data.get("owners")),
    )


__all__ = [
    "generate_run_id",
    "compute_config_hash",
    "get_code_version",
    "build_run_metadata",
    "get_metadata_path",
    "write_run_metadata",
    "load_run_metadata",
]
