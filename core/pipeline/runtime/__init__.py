"""Runtime context and configuration for bronze-foundry.

This package contains execution context and run configuration:
- context: RunContext dataclass and builders
- options: RunOptions for load patterns, output formats, webhooks
- paths: BronzePartition, SilverPartition, path builders
- metadata: RunMetadata, Layer, RunStatus for observability
"""

from .context import RunContext, build_run_context, run_context_to_dict, load_run_context
from .options import RunOptions

# Expose default_artifacts as a module-level function for backward compatibility
def default_artifacts():
    """Return default artifact names dictionary."""
    return RunOptions.default_artifacts()
from .paths import (
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_silver_partition,
    build_bronze_relative_path,
    build_silver_partition_path,
)
from .metadata import (
    Layer,
    RunStatus,
    QualityRuleResult,
    RunMetadata,
    generate_run_id,
    build_run_metadata,
)

__all__ = [
    # Context
    "RunContext",
    "build_run_context",
    "run_context_to_dict",
    "load_run_context",
    # Options
    "RunOptions",
    "default_artifacts",
    # Paths
    "BronzePartition",
    "SilverPartition",
    "build_bronze_partition",
    "build_silver_partition",
    "build_bronze_relative_path",
    "build_silver_partition_path",
    # Metadata
    "Layer",
    "RunStatus",
    "QualityRuleResult",
    "RunMetadata",
    "generate_run_id",
    "build_run_metadata",
]
