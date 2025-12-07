"""Data models for run metadata per spec Section 8.

This module contains the core data structures for tracking pipeline runs:
- Layer: Enum for medallion architecture layers (bronze, silver)
- RunStatus: Enum for run statuses (pending, running, success, failed, partial)
- DataClassification: Enum for data classification levels
- QualityRuleResult: Result of a single quality rule evaluation
- OwnerInfo: Owner information for a pipeline
- RunMetadata: Complete metadata for a pipeline run
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from core.foundation.primitives.base import RichEnumMixin
from core.foundation.time_utils import utc_now


# Module-level constants for Layer
_LAYER_DESCRIPTIONS: Dict[str, str] = {
    "bronze": "Raw data layer - landed data with minimal transformation",
    "silver": "Refined data layer - cleansed and transformed data",
}


class Layer(RichEnumMixin, str, Enum):
    """Data layer in the medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "Layer":
        """Normalize a layer value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            raise ValueError("Layer value must be provided")

        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member

        raise ValueError(
            f"Invalid Layer '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        value_str = str(self.value)
        return _LAYER_DESCRIPTIONS.get(value_str, value_str)


# Module-level constants for RunStatus
_RUN_STATUS_DESCRIPTIONS: Dict[str, str] = {
    "pending": "Run is queued but not yet started",
    "running": "Run is currently in progress",
    "success": "Run completed successfully",
    "failed": "Run failed with errors",
    "partial": "Run completed but some records failed quality rules",
}


class RunStatus(RichEnumMixin, str, Enum):
    """Status of a pipeline run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some records failed quality rules

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "RunStatus":
        """Normalize a run status value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.PENDING

        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member

        raise ValueError(
            f"Invalid RunStatus '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        value_str = str(self.value)
        return _RUN_STATUS_DESCRIPTIONS.get(value_str, value_str)

    @property
    def is_terminal(self) -> bool:
        """Check if this status is terminal (no more transitions expected)."""
        return self in {self.SUCCESS, self.FAILED, self.PARTIAL}

    @property
    def is_success(self) -> bool:
        """Check if this status indicates successful completion."""
        return self in {self.SUCCESS, self.PARTIAL}


# Module-level constants for DataClassification
_DATA_CLASSIFICATION_DESCRIPTIONS: Dict[str, str] = {
    "public": "Public data with no access restrictions",
    "internal": "Internal data accessible within the organization",
    "confidential": "Confidential data with restricted access",
    "restricted": "Highly restricted data with strict access controls",
}


class DataClassification(RichEnumMixin, str, Enum):
    """Data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "DataClassification":
        """Normalize a classification value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.INTERNAL

        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member

        raise ValueError(
            f"Invalid DataClassification '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        value_str = str(self.value)
        return _DATA_CLASSIFICATION_DESCRIPTIONS.get(value_str, value_str)


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
            self.start_time = utc_now()

    def start(self) -> "RunMetadata":
        """Mark the run as started."""
        self.start_time = utc_now()
        self.status = RunStatus.RUNNING
        return self

    def complete(
        self,
        row_count_in: int,
        row_count_out: int,
        status: Optional[RunStatus] = None,
    ) -> "RunMetadata":
        """Mark the run as completed."""
        self.end_time = utc_now()
        self.row_count_in = row_count_in
        self.row_count_out = row_count_out
        if status is not None:
            self.status = status
        elif self.status == RunStatus.RUNNING:
            self.status = RunStatus.SUCCESS
        return self

    def fail(self, error: Optional[str] = None) -> "RunMetadata":
        """Mark the run as failed."""
        self.end_time = utc_now()
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


__all__ = [
    "Layer",
    "RunStatus",
    "DataClassification",
    "QualityRuleResult",
    "OwnerInfo",
    "RunMetadata",
]
