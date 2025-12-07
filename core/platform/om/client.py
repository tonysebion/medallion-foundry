"""Proxy module for OpenMetadata client APIs."""

from __future__ import annotations

from core.foundation.catalog.client import ColumnSchema, LineageEdge, OpenMetadataClient, TableSchema

__all__ = ["ColumnSchema", "LineageEdge", "OpenMetadataClient", "TableSchema"]
