"""Pipeline library modules.

This package contains the core abstractions and utilities for building
Ibis-based medallion architecture pipelines.
"""

from pipelines.lib.api import ApiOutputMetadata, ApiSource, create_api_source_from_options
from pipelines.lib.auth import AuthConfig, AuthType, build_auth_headers
from pipelines.lib.bronze import BronzeOutputMetadata, BronzeSource, LoadPattern, SourceType
from pipelines.lib.pipeline import Pipeline
from pipelines.lib.pagination import (
    CursorPaginationState,
    NoPaginationState,
    OffsetPaginationState,
    PagePaginationState,
    PaginationConfig,
    PaginationState,
    PaginationStrategy,
    build_pagination_config_from_dict,
    build_pagination_state,
)
from pipelines.lib.checksum import (
    ChecksumManifest,
    ChecksumValidationError,
    ChecksumVerificationResult,
    compute_file_sha256,
    validate_bronze_checksums,
    verify_checksum_manifest,
    write_checksum_manifest,
)
from pipelines.lib.connections import close_all_connections, get_connection
from pipelines.lib.env import expand_env_vars, expand_options, load_env_file
from pipelines.lib.polybase import (
    PolyBaseConfig,
    generate_external_table_ddl,
    generate_from_metadata,
    generate_polybase_setup,
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
    OutputMetadata,
    ReadResult,
    SilverOutputMetadata,
    WriteMetadata,
    get_latest_partition,
    infer_column_types,
    list_partitions,
    read_bronze,
    write_partitioned,
    write_silver,
    write_silver_with_artifacts,
)
from pipelines.lib.quality import (
    QualityCheckFailed,
    QualityResult,
    QualityRule,
    Severity,
    check_quality,
    check_quality_pandera,
    create_pandera_schema,
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
    BronzeConfig,
    PipelineSettings,
    SilverConfig,
    ValidationIssue,
    ValidationSeverity,
    format_validation_report,
    validate_and_raise,
    validate_bronze_source,
    validate_silver_entity,
)
from pipelines.lib.state import (
    LateDataConfig,
    LateDataMode,
    LateDataResult,
    clear_all_watermarks,
    delete_watermark,
    detect_late_data,
    filter_late_data,
    get_late_records,
    get_watermark,
    get_watermark_age,
    list_watermarks,
    save_watermark,
)

__all__ = [
    # API
    "ApiOutputMetadata",
    "ApiSource",
    "create_api_source_from_options",
    # Auth
    "AuthConfig",
    "AuthType",
    "build_auth_headers",
    # Bronze
    "BronzeOutputMetadata",
    "BronzeSource",
    "LoadPattern",
    "SourceType",
    # Pipeline
    "Pipeline",
    # Pagination
    "CursorPaginationState",
    "NoPaginationState",
    "OffsetPaginationState",
    "PagePaginationState",
    "PaginationConfig",
    "PaginationState",
    "PaginationStrategy",
    "build_pagination_config_from_dict",
    "build_pagination_state",
    # Checksum
    "ChecksumManifest",
    "ChecksumValidationError",
    "ChecksumVerificationResult",
    "compute_file_sha256",
    "validate_bronze_checksums",
    "verify_checksum_manifest",
    "write_checksum_manifest",
    # Connections
    "close_all_connections",
    "get_connection",
    # Environment
    "expand_env_vars",
    "expand_options",
    "load_env_file",
    # Late Data
    "LateDataConfig",
    "LateDataMode",
    "LateDataResult",
    "detect_late_data",
    "filter_late_data",
    "get_late_records",
    # PolyBase
    "PolyBaseConfig",
    "generate_external_table_ddl",
    "generate_from_metadata",
    "generate_polybase_setup",
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
    "OutputMetadata",
    "ReadResult",
    "SilverOutputMetadata",
    "WriteMetadata",
    "get_latest_partition",
    "infer_column_types",
    "list_partitions",
    "read_bronze",
    "write_partitioned",
    "write_silver",
    "write_silver_with_artifacts",
    # Quality
    "QualityCheckFailed",
    "QualityResult",
    "QualityRule",
    "Severity",
    "check_quality",
    "check_quality_pandera",
    "create_pandera_schema",
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
    "BronzeConfig",
    "PipelineSettings",
    "SilverConfig",
    "ValidationIssue",
    "ValidationSeverity",
    "format_validation_report",
    "validate_and_raise",
    "validate_bronze_source",
    "validate_silver_entity",
    # Watermark
    "clear_all_watermarks",
    "list_watermarks",
    "get_watermark_age",
    "delete_watermark",
    "get_watermark",
    "save_watermark",
]
