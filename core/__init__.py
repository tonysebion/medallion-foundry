"""Core modules for Bronze extraction framework.

.. deprecated::
    The ``core/`` module is deprecated and will be removed in a future release.
    Use ``pipelines/`` instead for all new development.

    See ``core/DEPRECATED.md`` for migration guide and ``pipelines/QUICKREF.md``
    for the new API documentation.

Layer Structure:
    core.foundation      - L0: Zero-dependency building blocks
    core.platform        - L1: Cross-cutting platform services
    core.infrastructure  - L2: Config, I/O, runtime
    core.domain          - L3: Adapters, services, pipelines
    core.orchestration   - L4: Job execution
"""
import warnings

warnings.warn(
    "The 'core' module is deprecated. Use 'pipelines' instead. "
    "See core/DEPRECATED.md for migration guide.",
    DeprecationWarning,
    stacklevel=2,
)

__version__ = "1.0.0"

# =============================================================================
# Public API Re-exports (from canonical layer locations)
# =============================================================================

# Foundations (L0)
from core.foundation.primitives.patterns import LoadPattern
from core.foundation.primitives.exceptions import (
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
from core.foundation.primitives.logging import setup_logging

# State (L0)
from core.foundation.state.watermark import Watermark, WatermarkStore, WatermarkType
from core.foundation.state.manifest import FileEntry, FileManifest, ManifestTracker

# Catalog (L0)
from core.foundation.catalog.hooks import (
    notify_catalog,
    report_schema_snapshot,
    report_quality_snapshot,
    report_run_metadata,
    report_lineage,
)
from core.foundation.catalog.webhooks import fire_webhooks
from core.foundation.catalog.tracing import trace_span

# Resilience (L1)
from core.platform.resilience import (
    RetryPolicy,
    CircuitBreaker,
    CircuitState,
    RateLimiter,
    execute_with_retry,
    execute_with_retry_async,
    LateDataMode,
    LateDataConfig,
    LateDataResult,
)

# Runtime (L2)
from core.infrastructure.runtime.context import RunContext, build_run_context, load_run_context
from core.infrastructure.runtime.options import RunOptions
from core.infrastructure.runtime import default_artifacts
from core.infrastructure.runtime.paths import (
    BronzePartition,
    SilverPartition,
    build_bronze_partition,
    build_silver_partition,
    build_bronze_relative_path,
    build_silver_partition_path,
)
from core.infrastructure.runtime import (
    Layer,
    RunStatus,
    QualityRuleResult,
    RunMetadata,
    generate_run_id,
    build_run_metadata,
)

__all__ = [
    # Version
    "__version__",
    # Foundations (L0)
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
    # State (L0)
    "Watermark",
    "WatermarkStore",
    "WatermarkType",
    "FileEntry",
    "FileManifest",
    "ManifestTracker",
    # Catalog (L0)
    "notify_catalog",
    "report_schema_snapshot",
    "report_quality_snapshot",
    "report_run_metadata",
    "report_lineage",
    "fire_webhooks",
    "trace_span",
    # Resilience (L1)
    "RetryPolicy",
    "CircuitBreaker",
    "CircuitState",
    "RateLimiter",
    "execute_with_retry",
    "execute_with_retry_async",
    "LateDataMode",
    "LateDataConfig",
    "LateDataResult",
    # Runtime (L2)
    "RunContext",
    "build_run_context",
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
]
