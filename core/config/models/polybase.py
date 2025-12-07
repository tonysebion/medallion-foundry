"""Polybase configuration models for SQL Server external tables.

These dataclasses define the configuration for PolyBase external data sources,
file formats, and external tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.config.models.helpers import (
    require_list_of_strings,
    require_optional_str,
    require_bool,
)


@dataclass
class PolybaseExternalDataSource:
    """Polybase external data source configuration for cloud storage."""

    name: str
    data_source_type: str  # e.g., "HADOOP", "BLOB_STORAGE"
    location: str  # e.g., "wasbs://container@account.blob.core.windows.net/path"
    credential_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolybaseExternalDataSource":
        """Parse PolybaseExternalDataSource from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_data_source must be a dictionary")
        name = require_optional_str(
            data.get("name"), "polybase_setup.external_data_source.name"
        )
        if not name:
            raise ValueError("polybase_setup.external_data_source.name is required")
        data_source_type = (
            require_optional_str(
                data.get("data_source_type"),
                "polybase_setup.external_data_source.data_source_type",
            )
            or "HADOOP"
        )
        location = require_optional_str(
            data.get("location"), "polybase_setup.external_data_source.location"
        )
        if not location:
            raise ValueError("polybase_setup.external_data_source.location is required")
        credential_name = require_optional_str(
            data.get("credential_name"),
            "polybase_setup.external_data_source.credential_name",
        )
        return cls(
            name=name,
            data_source_type=data_source_type,
            location=location,
            credential_name=credential_name,
        )


@dataclass
class PolybaseExternalFileFormat:
    """Polybase external file format configuration."""

    name: str
    format_type: str  # e.g., "PARQUET", "DELIMITEDTEXT"
    compression: Optional[str] = None  # e.g., "SNAPPY", "GZIP"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolybaseExternalFileFormat":
        """Parse PolybaseExternalFileFormat from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_file_format must be a dictionary")
        name = require_optional_str(
            data.get("name"), "polybase_setup.external_file_format.name"
        )
        if not name:
            raise ValueError("polybase_setup.external_file_format.name is required")
        format_type = (
            require_optional_str(
                data.get("format_type"),
                "polybase_setup.external_file_format.format_type",
            )
            or "PARQUET"
        )
        compression = require_optional_str(
            data.get("compression"), "polybase_setup.external_file_format.compression"
        )
        return cls(
            name=name,
            format_type=format_type,
            compression=compression,
        )


@dataclass
class PolybaseExternalTable:
    """Polybase external table configuration for a single artifact/model."""

    artifact_name: str  # e.g., "orders_events", "orders_state"
    schema_name: str  # e.g., "dbo", "silver"
    table_name: str  # e.g., "fact_orders", "dim_orders"
    partition_columns: List[str] = field(default_factory=list)  # e.g., ["event_date"]
    reject_type: str = "VALUE"  # ROWS, VALUE, PERCENTAGE
    reject_value: int = 0
    sample_queries: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PolybaseExternalTable":
        """Parse PolybaseExternalTable from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_tables item must be a dictionary")
        artifact_name = require_optional_str(
            data.get("artifact_name"), "polybase_setup.external_tables.artifact_name"
        )
        if not artifact_name:
            raise ValueError("polybase_setup.external_tables.artifact_name is required")
        schema_name = (
            require_optional_str(
                data.get("schema_name"), "polybase_setup.external_tables.schema_name"
            )
            or "dbo"
        )
        table_name = require_optional_str(
            data.get("table_name"), "polybase_setup.external_tables.table_name"
        )
        if not table_name:
            raise ValueError("polybase_setup.external_tables.table_name is required")
        partition_columns = require_list_of_strings(
            data.get("partition_columns"),
            "polybase_setup.external_tables.partition_columns",
        )
        reject_type = (
            require_optional_str(
                data.get("reject_type"), "polybase_setup.external_tables.reject_type"
            )
            or "VALUE"
        )
        reject_value = data.get("reject_value", 0)
        if not isinstance(reject_value, int):
            raise ValueError(
                "polybase_setup.external_tables.reject_value must be an integer"
            )
        sample_queries = require_list_of_strings(
            data.get("sample_queries"), "polybase_setup.external_tables.sample_queries"
        )
        return cls(
            artifact_name=artifact_name,
            schema_name=schema_name,
            table_name=table_name,
            partition_columns=partition_columns,
            reject_type=reject_type,
            reject_value=reject_value,
            sample_queries=sample_queries,
        )


@dataclass
class PolybaseSetup:
    """Complete Polybase setup configuration for SQL Server external tables."""

    enabled: bool = True
    external_data_source: Optional[PolybaseExternalDataSource] = None
    external_file_format: Optional[PolybaseExternalFileFormat] = None
    external_tables: List[PolybaseExternalTable] = field(default_factory=list)
    # Future: Trino and Iceberg configurations
    trino_enabled: bool = False
    iceberg_enabled: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "PolybaseSetup":
        """Parse PolybaseSetup from a dictionary."""
        if not data:
            return cls(enabled=False)
        if not isinstance(data, dict):
            raise ValueError("polybase_setup must be a dictionary")

        enabled = require_bool(data.get("enabled"), "polybase_setup.enabled", True)

        external_data_source = None
        eds_data = data.get("external_data_source")
        if eds_data:
            external_data_source = PolybaseExternalDataSource.from_dict(eds_data)

        external_file_format = None
        eff_data = data.get("external_file_format")
        if eff_data:
            external_file_format = PolybaseExternalFileFormat.from_dict(eff_data)

        external_tables = []
        et_list = data.get("external_tables") or []
        if et_list and not isinstance(et_list, list):
            raise ValueError("polybase_setup.external_tables must be a list")
        for et_data in et_list:
            external_tables.append(PolybaseExternalTable.from_dict(et_data))

        trino_enabled = require_bool(
            data.get("trino_enabled"), "polybase_setup.trino_enabled", False
        )
        iceberg_enabled = require_bool(
            data.get("iceberg_enabled"), "polybase_setup.iceberg_enabled", False
        )

        return cls(
            enabled=enabled,
            external_data_source=external_data_source,
            external_file_format=external_file_format,
            external_tables=external_tables,
            trino_enabled=trino_enabled,
            iceberg_enabled=iceberg_enabled,
        )


__all__ = [
    "PolybaseExternalDataSource",
    "PolybaseExternalFileFormat",
    "PolybaseExternalTable",
    "PolybaseSetup",
]
