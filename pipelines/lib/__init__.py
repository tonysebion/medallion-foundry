"""Pipeline library modules.

This package contains the core abstractions and utilities for building
Ibis-based medallion architecture pipelines.
"""

from pipelines.lib.bronze import BronzeSource, LoadPattern, SourceType
from pipelines.lib.connections import close_all_connections, get_connection
from pipelines.lib.env import expand_env_vars, expand_options
from pipelines.lib.late_data import (
    LateDataConfig,
    LateDataMode,
    LateDataResult,
    detect_late_data,
    filter_late_data,
)
from pipelines.lib.rate_limiter import RateLimiter, rate_limited
from pipelines.lib.curate import (
    build_history,
    coalesce_columns,
    dedupe_earliest,
    dedupe_exact,
    dedupe_latest,
    filter_incremental,
    rank_by_keys,
    union_dedupe,
)
from pipelines.lib.io import (
    ReadResult,
    WriteMetadata,
    get_latest_partition,
    list_partitions,
    read_bronze,
    write_partitioned,
    write_silver,
)
from pipelines.lib.quality import (
    QualityCheckFailed,
    QualityResult,
    QualityRule,
    Severity,
    check_quality,
    in_list,
    matches_pattern,
    non_negative,
    not_empty,
    not_null,
    positive,
    standard_dimension_rules,
    standard_fact_rules,
    unique_key,
    valid_timestamp,
)
from pipelines.lib.resilience import with_retry
from pipelines.lib.runner import pipeline
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.validate import (
    ValidationIssue,
    ValidationSeverity,
    format_validation_report,
    validate_and_raise,
    validate_bronze_source,
    validate_silver_entity,
)
from pipelines.lib.watermark import delete_watermark, get_watermark, save_watermark

__all__ = [
    # Bronze
    "BronzeSource",
    "LoadPattern",
    "SourceType",
    # Connections
    "close_all_connections",
    "get_connection",
    # Environment
    "expand_env_vars",
    "expand_options",
    # Late Data
    "LateDataConfig",
    "LateDataMode",
    "LateDataResult",
    "detect_late_data",
    "filter_late_data",
    # Rate Limiting
    "RateLimiter",
    "rate_limited",
    # Curate
    "build_history",
    "coalesce_columns",
    "dedupe_earliest",
    "dedupe_exact",
    "dedupe_latest",
    "filter_incremental",
    "rank_by_keys",
    "union_dedupe",
    # I/O
    "ReadResult",
    "WriteMetadata",
    "get_latest_partition",
    "list_partitions",
    "read_bronze",
    "write_partitioned",
    "write_silver",
    # Quality
    "QualityCheckFailed",
    "QualityResult",
    "QualityRule",
    "Severity",
    "check_quality",
    "in_list",
    "matches_pattern",
    "non_negative",
    "not_empty",
    "not_null",
    "positive",
    "standard_dimension_rules",
    "standard_fact_rules",
    "unique_key",
    "valid_timestamp",
    # Resilience
    "with_retry",
    # Runner
    "pipeline",
    # Silver
    "EntityKind",
    "HistoryMode",
    "SilverEntity",
    # Validate
    "ValidationIssue",
    "ValidationSeverity",
    "format_validation_report",
    "validate_and_raise",
    "validate_bronze_source",
    "validate_silver_entity",
    # Watermark
    "delete_watermark",
    "get_watermark",
    "save_watermark",
]
