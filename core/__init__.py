"""Core modules for Bronze extraction framework.

This package contains the core functionality organized into:
- foundations/: Shared primitives (patterns, exceptions, logging)
- runtime/: Execution context & configuration
- state/: State management for incremental patterns
- resilience/: Failure handling & recovery patterns
- catalog/: External catalog integrations
- bronze/: Bronze layer utilities
- silver/: Silver layer transformation engine
- config/: Configuration loading & validation
- extractors/: Data extraction implementations
- storage/: Storage backends (S3, Azure, Local)
- quality/: Data quality rules & reporting
- schema/: Schema validation & evolution
- runner/: Job execution & chunking
- polybase/: PolyBase DDL generation
"""

__version__ = "1.0.0"

# =============================================================================
# Backward Compatibility Re-exports
# =============================================================================

# Foundations
from core.primitives.foundations.patterns import LoadPattern
from core.primitives.foundations.exceptions import (
    BronzeFoundryError,
    ConfigValidationError,
    ExtractionError,
    StorageError,
    AuthenticationError,
    PaginationError,
    StateManagementError,
    DataQualityError,
    RetryExhaustedError,
    BronzeFoundryDeprecationWarning,
    BronzeFoundryCompatibilityWarning,
    DeprecationSpec,
    emit_deprecation,
    emit_compat,
)
from core.primitives.foundations.logging import setup_logging

# Runtime
from core.runtime.context import RunContext, build_run_context, run_context_to_dict, load_run_context
from core.runtime.options import RunOptions
from core.runtime import default_artifacts
from core.runtime.paths import (
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_silver_partition,
    build_bronze_relative_path,
    build_silver_partition_path,
)
from core.runtime.metadata import (
    Layer,
    RunStatus,
    QualityRuleResult,
    RunMetadata,
    generate_run_id,
    build_run_metadata,
)

# State
from core.primitives.state.watermark import Watermark, WatermarkStore, WatermarkType
from core.primitives.state.manifest import FileEntry, FileManifest, ManifestTracker

# Resilience
from core.infrastructure.resilience.retry import (
    RetryPolicy,
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    execute_with_retry,
    execute_with_retry_async,
)
from core.infrastructure.resilience.late_data import LateDataMode, LateDataConfig, LateDataResult

# Catalog
from core.primitives.catalog.hooks import (
    notify_catalog,
    report_schema_snapshot,
    report_quality_snapshot,
    report_run_metadata,
    report_lineage,
)
from core.primitives.catalog.webhooks import fire_webhooks
from core.primitives.catalog.tracing import trace_span

__all__ = [
    # Version
    "__version__",
    # Foundations
    "LoadPattern",
    "BronzeFoundryError",
    "ConfigValidationError",
    "ExtractionError",
    "StorageError",
    "AuthenticationError",
    "PaginationError",
    "StateManagementError",
    "DataQualityError",
    "RetryExhaustedError",
    "BronzeFoundryDeprecationWarning",
    "BronzeFoundryCompatibilityWarning",
    "DeprecationSpec",
    "emit_deprecation",
    "emit_compat",
    "setup_logging",
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
    # State
    "Watermark",
    "WatermarkStore",
    "WatermarkType",
    "FileEntry",
    "FileManifest",
    "ManifestTracker",
    # Resilience
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "RateLimiter",
    "execute_with_retry",
    "execute_with_retry_async",
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    # Catalog
    "notify_catalog",
    "report_schema_snapshot",
    "report_quality_snapshot",
    "report_run_metadata",
    "report_lineage",
    "fire_webhooks",
    "trace_span",
]
