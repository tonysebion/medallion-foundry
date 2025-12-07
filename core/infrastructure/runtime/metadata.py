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

This module re-exports from metadata_models and metadata_builder for backwards compatibility.
See those modules for implementation details.
"""

from __future__ import annotations

# Re-export models
from .metadata_models import (
    DataClassification,
    Layer,
    OwnerInfo,
    QualityRuleResult,
    RunMetadata,
    RunStatus,
)

# Re-export builder functions
from .metadata_builder import (
    build_run_metadata,
    compute_config_hash,
    generate_run_id,
    get_code_version,
    get_metadata_path,
    load_run_metadata,
    write_run_metadata,
)

__all__ = [
    # Models
    "Layer",
    "RunStatus",
    "DataClassification",
    "QualityRuleResult",
    "OwnerInfo",
    "RunMetadata",
    # Builder functions
    "generate_run_id",
    "compute_config_hash",
    "get_code_version",
    "build_run_metadata",
    "get_metadata_path",
    "write_run_metadata",
    "load_run_metadata",
]
