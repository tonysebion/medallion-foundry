"""Centralized run metadata builder per spec Section 8.

Every run must produce OM-ready metadata with the following fields:
- pipeline_id
- run_id
- layer
- environment
- source_system
- domain
- data_classification
- schema_evolution_mode
- start_time
- end_time
- status
- row_count_in
- row_count_out
- config_hash
- code_version
- rule_results
- upstream_runs (Silver only)

Metadata is written to:
/data/<env>/<layer>/<domain>/<table>/_metadata/runs/run=<run_id>.json
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class Layer(str, Enum):
    """Data layer in the medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"


class RunStatus(str, Enum):
    """Status of a pipeline run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some records failed quality rules


class DataClassification(str, Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass
class QualityRuleResult:
    """Result of a single quality rule evaluation."""

    rule_id: str
    level: str  # "error" or "warn"
    expression: str
    passed: bool
    failed_count: int = 0
    total_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "level": self.level,
            "expression": self.expression,
            "passed": self.passed,
            "failed_count": self.failed_count,
            "total_count": self.total_count,
        }


@dataclass
class OwnerInfo:
    """Owner information for a pipeline."""

    semantic_owner: Optional[str] = None
    technical_owner: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic_owner": self.semantic_owner,
            "technical_owner": self.technical_owner,
        }

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "OwnerInfo":
        if not data:
            return cls()
        return cls(
            semantic_owner=data.get("semantic_owner"),
            technical_owner=data.get("technical_owner"),
        )


@dataclass
class RunMetadata:
    """Complete metadata for a pipeline run per spec Section 8."""

    # Required fields
    pipeline_id: str
    run_id: str
    layer: Layer
    environment: str
    source_system: str
    domain: str

    # Optional fields with defaults
    data_classification: DataClassification = DataClassification.INTERNAL
    schema_evolution_mode: str = "strict"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: RunStatus = RunStatus.PENDING
    row_count_in: int = 0
    row_count_out: int = 0
    config_hash: str = ""
    code_version: str = ""
    rule_results: List[QualityRuleResult] = field(default_factory=list)
    upstream_runs: List[str] = field(default_factory=list)  # Silver only
    owners: OwnerInfo = field(default_factory=OwnerInfo)
    table: str = ""  # Source table name

    # Additional context
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.utcnow()

    def start(self) -> "RunMetadata":
        """Mark the run as started."""
        self.start_time = datetime.utcnow()
        self.status = RunStatus.RUNNING
        return self

    def complete(
        self,
        row_count_in: int,
        row_count_out: int,
        status: Optional[RunStatus] = None,
    ) -> "RunMetadata":
        """Mark the run as completed."""
        self.end_time = datetime.utcnow()
        self.row_count_in = row_count_in
        self.row_count_out = row_count_out
        if status is not None:
            self.status = status
        elif self.status == RunStatus.RUNNING:
            self.status = RunStatus.SUCCESS
        return self

    def fail(self, error: Optional[str] = None) -> "RunMetadata":
        """Mark the run as failed."""
        self.end_time = datetime.utcnow()
        self.status = RunStatus.FAILED
        if error:
            self.extra["error"] = error
        return self

    def add_rule_result(self, result: QualityRuleResult) -> "RunMetadata":
        """Add a quality rule result."""
        self.rule_results.append(result)
        return self

    def add_upstream_run(self, run_id: str) -> "RunMetadata":
        """Add an upstream run ID (for Silver lineage)."""
        if run_id not in self.upstream_runs:
            self.upstream_runs.append(run_id)
        return self

    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate run duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "layer": self.layer.value,
            "environment": self.environment,
            "source_system": self.source_system,
            "domain": self.domain,
            "table": self.table,
            "data_classification": self.data_classification.value,
            "schema_evolution_mode": self.schema_evolution_mode,
            "start_time": self.start_time.isoformat() + "Z" if self.start_time else None,
            "end_time": self.end_time.isoformat() + "Z" if self.end_time else None,
            "status": self.status.value,
            "row_count_in": self.row_count_in,
            "row_count_out": self.row_count_out,
            "config_hash": self.config_hash,
            "code_version": self.code_version,
            "rule_results": [r.to_dict() for r in self.rule_results],
            "owners": self.owners.to_dict(),
        }

        # Only include upstream_runs for Silver layer
        if self.layer == Layer.SILVER:
            result["upstream_runs"] = self.upstream_runs

        # Include duration if available
        if self.duration_seconds is not None:
            result["duration_seconds"] = self.duration_seconds

        # Merge extra fields
        result.update(self.extra)

        return result


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
    platform = config.get("platform", {})

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

    # Extract schema evolution mode
    schema_cfg = config.get("schema_evolution", {})
    if isinstance(schema_cfg, dict):
        schema_evolution_mode = schema_cfg.get("mode", "strict")
    else:
        schema_evolution_mode = "strict"

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
    return (
        base_path
        / "data"
        / environment
        / layer.value
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

    logger.info(f"Wrote run metadata to {metadata_path}")
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
