"""Bronze -> Silver data pipeline for bronze-foundry.

This package contains the medallion pipeline layers (siblings):
- runtime/: RunContext, RunOptions, paths, metadata
- bronze/: Bronze extraction I/O and chunking
- silver/: Silver transformation models and artifacts
"""

from .runtime import (
    RunContext,
    build_run_context,
    run_context_to_dict,
    load_run_context,
    RunOptions,
    default_artifacts,
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_silver_partition,
    build_bronze_relative_path,
    build_silver_partition_path,
    Layer,
    RunStatus,
    QualityRuleResult,
    RunMetadata,
    generate_run_id,
    build_run_metadata,
)
from .silver import (
    apply_schema_settings,
    build_current_view,
    normalize_dataframe,
    MODEL_PROFILES,
    SilverModel,
    resolve_profile,
)

# Bronze exports are minimal to avoid circular imports
# Import from core.pipeline.bronze.io or core.pipeline.bronze.base directly

__all__ = [
    # Runtime
    "RunContext",
    "build_run_context",
    "run_context_to_dict",
    "load_run_context",
    "RunOptions",
    "default_artifacts",
    "BronzePartition",
    "SilverPartition",
    "build_bronze_partition",
    "build_silver_partition",
    "build_bronze_relative_path",
    "build_silver_partition_path",
    "Layer",
    "RunStatus",
    "QualityRuleResult",
    "RunMetadata",
    "generate_run_id",
    "build_run_metadata",
    # Silver
    "apply_schema_settings",
    "build_current_view",
    "normalize_dataframe",
    "MODEL_PROFILES",
    "SilverModel",
    "resolve_profile",
]
