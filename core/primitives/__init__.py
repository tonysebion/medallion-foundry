"""Zero-dependency building blocks for bronze-foundry.

This package contains foundational primitives with no internal dependencies:
- foundations/: LoadPattern, exceptions, logging
- state/: Watermark tracking, file manifests
- catalog/: External catalog hooks, webhooks, tracing
"""

from .foundations import (
    LoadPattern,
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
    setup_logging,
)
from .state import (
    Watermark,
    WatermarkStore,
    WatermarkType,
    build_watermark_store,
    compute_max_watermark,
    FileEntry,
    FileManifest,
    ManifestTracker,
)
from .catalog import (
    notify_catalog,
    report_schema_snapshot,
    report_quality_snapshot,
    report_run_metadata,
    report_lineage,
    report_quality_rule_results,
    report_dataset_registered,
    fire_webhooks,
    trace_span,
)

__all__ = [
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
    # State
    "Watermark",
    "WatermarkStore",
    "WatermarkType",
    "build_watermark_store",
    "compute_max_watermark",
    "FileEntry",
    "FileManifest",
    "ManifestTracker",
    # Catalog
    "notify_catalog",
    "report_schema_snapshot",
    "report_quality_snapshot",
    "report_run_metadata",
    "report_lineage",
    "report_quality_rule_results",
    "report_dataset_registered",
    "fire_webhooks",
    "trace_span",
]
