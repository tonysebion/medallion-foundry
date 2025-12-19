"""Ibis-based medallion architecture pipelines.

This package provides declarative Bronze and Silver layer abstractions
that use Ibis for portable, deferred execution across multiple backends.

Usage:
    python -m pipelines claims.header --date 2025-01-15
    python -m pipelines claims.header:bronze --date 2025-01-15
    python -m pipelines claims.header:silver --date 2025-01-15
"""

from pipelines.lib.bronze import BronzeSource, SourceType, LoadPattern
from pipelines.lib.silver import SilverEntity, EntityKind, HistoryMode

__all__ = [
    "BronzeSource",
    "SourceType",
    "LoadPattern",
    "SilverEntity",
    "EntityKind",
    "HistoryMode",
]
