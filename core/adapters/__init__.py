"""External system adapters for bronze-foundry.

This package contains integrations with external systems:
- extractors/: Data source extractors (API, DB, file)
- polybase/: PolyBase DDL generation
- schema/: Schema validation (future feature)
- quality/: Quality rules engine (future feature)
"""

from .polybase import (
    generate_polybase_setup,
    generate_temporal_functions_sql,
    generate_joined_table_ddl,
    generate_lookup_table_ddl,
    generate_cte_view_ddl,
    generate_current_state_view_ddl,
    generate_history_summary_view_ddl,
    generate_event_aggregation_view_ddl,
)
from .schema import (
    SchemaValidator,
    validate_schema,
    SchemaEvolutionMode,
    apply_evolution_rules,
    ColumnSpec,
    SchemaSpec,
    parse_schema_config,
)
from .quality import (
    QualityRule,
    RuleLevel,
    RuleDefinition,
    QualityEngine,
    evaluate_rules,
    RuleResult,
    QualityReport,
    format_quality_report,
)

# Extractors are imported explicitly to avoid heavy dependencies
# Use: from core.adapters.extractors.api_extractor import ApiExtractor

__all__ = [
    # Polybase
    "generate_polybase_setup",
    "generate_temporal_functions_sql",
    "generate_joined_table_ddl",
    "generate_lookup_table_ddl",
    "generate_cte_view_ddl",
    "generate_current_state_view_ddl",
    "generate_history_summary_view_ddl",
    "generate_event_aggregation_view_ddl",
    # Schema
    "SchemaValidator",
    "validate_schema",
    "SchemaEvolutionMode",
    "apply_evolution_rules",
    "ColumnSpec",
    "SchemaSpec",
    "parse_schema_config",
    # Quality
    "QualityRule",
    "RuleLevel",
    "RuleDefinition",
    "QualityEngine",
    "evaluate_rules",
    "RuleResult",
    "QualityReport",
    "format_quality_report",
]
