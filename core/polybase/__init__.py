"""Polybase external table configuration and DDL generation."""

from core.polybase.polybase_generator import (
    generate_polybase_setup,
    generate_temporal_functions_sql,
)

__all__ = ["generate_polybase_setup", "generate_temporal_functions_sql"]
