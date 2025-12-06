"""Polybase external table configuration and DDL generation.

Enhanced per spec Section 8 with:
- Joined table DDL for multi_source_join pattern
- Lookup table DDL for single_source_with_lookups pattern
- CTE view DDL for complex query patterns
- Current state and history summary views for SCD entities
- Event aggregation views for event entities
"""

from core.polybase.polybase_generator import (
    generate_polybase_setup,
    generate_temporal_functions_sql,
    generate_joined_table_ddl,
    generate_lookup_table_ddl,
    generate_cte_view_ddl,
    generate_current_state_view_ddl,
    generate_history_summary_view_ddl,
    generate_event_aggregation_view_ddl,
)

__all__ = [
    "generate_polybase_setup",
    "generate_temporal_functions_sql",
    "generate_joined_table_ddl",
    "generate_lookup_table_ddl",
    "generate_cte_view_ddl",
    "generate_current_state_view_ddl",
    "generate_history_summary_view_ddl",
    "generate_event_aggregation_view_ddl",
]
