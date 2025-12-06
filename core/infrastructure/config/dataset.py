"""Intent-driven dataset configuration models.

These dataclasses implement the unified Bronze+Silver schema described in
docs/framework/silver_patterns.md and docs/framework/pipeline_engine.md. They are lightweight and
focus on semantic intent so higher-level orchestration can remain simple.
"""

from __future__ import annotations

import logging
from core.primitives.foundations.base import RichEnumMixin
from core.primitives.foundations.exceptions import emit_compat
from core.primitives.foundations.patterns import LoadPattern
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


# Module-level descriptions for enums (can't be class attributes due to Enum metaclass)
_ENTITY_KIND_DESCRIPTIONS: Dict[str, str] = {
    "event": "Immutable event records (append-only)",
    "state": "Mutable state records (current snapshot)",
    "derived_state": "State derived from other entities",
    "derived_event": "Events derived from other entities",
}

_HISTORY_MODE_DESCRIPTIONS: Dict[str, str] = {
    "scd2": "Type 2 slowly changing dimension - full history with date ranges",
    "scd1": "Type 1 slowly changing dimension - overwrite with latest values",
    "latest_only": "Keep only the most recent version of each record",
}

_INPUT_MODE_DESCRIPTIONS: Dict[str, str] = {
    "append_log": "Append new records to existing data",
    "replace_daily": "Replace all data for each daily partition",
}

_DELETE_MODE_DESCRIPTIONS: Dict[str, str] = {
    "ignore": "Ignore delete markers in source data",
    "tombstone_state": "Apply tombstone logic for state entities",
    "tombstone_event": "Apply tombstone logic for event entities",
}

_SCHEMA_MODE_DESCRIPTIONS: Dict[str, str] = {
    "strict": "Reject any schema changes",
    "allow_new_columns": "Allow new columns to be added",
}


class EntityKind(RichEnumMixin, str, Enum):
    """Kind of entity for Silver layer processing."""

    EVENT = "event"
    STATE = "state"
    DERIVED_STATE = "derived_state"
    DERIVED_EVENT = "derived_event"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "EntityKind":
        """Normalize an entity kind value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            raise ValueError("EntityKind value must be provided")
        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member
        raise ValueError(
            f"Invalid EntityKind '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _ENTITY_KIND_DESCRIPTIONS.get(self.value, self.value)

    @property
    def is_event_like(self) -> bool:
        """Check if entity is event-like (append-only)."""
        return self in {self.EVENT, self.DERIVED_EVENT}

    @property
    def is_state_like(self) -> bool:
        """Check if entity is state-like (mutable)."""
        return self in {self.STATE, self.DERIVED_STATE}


class HistoryMode(RichEnumMixin, str, Enum):
    """History tracking mode for state entities."""

    SCD2 = "scd2"
    SCD1 = "scd1"
    LATEST_ONLY = "latest_only"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "HistoryMode":
        """Normalize a history mode value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.SCD2  # Default to SCD2
        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member
        raise ValueError(
            f"Invalid HistoryMode '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _HISTORY_MODE_DESCRIPTIONS.get(self.value, self.value)


class InputMode(RichEnumMixin, str, Enum):
    """Input processing mode for event entities."""

    APPEND_LOG = "append_log"
    REPLACE_DAILY = "replace_daily"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "InputMode":
        """Normalize an input mode value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.APPEND_LOG  # Default to append_log
        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member
        raise ValueError(
            f"Invalid InputMode '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _INPUT_MODE_DESCRIPTIONS.get(self.value, self.value)


class DeleteMode(RichEnumMixin, str, Enum):
    """Delete handling mode for entity processing."""

    IGNORE = "ignore"
    TOMBSTONE_STATE = "tombstone_state"
    TOMBSTONE_EVENT = "tombstone_event"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "DeleteMode":
        """Normalize a delete mode value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.IGNORE  # Default to ignore
        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member
        raise ValueError(
            f"Invalid DeleteMode '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _DELETE_MODE_DESCRIPTIONS.get(self.value, self.value)


class SchemaMode(RichEnumMixin, str, Enum):
    """Schema evolution mode for entity processing."""

    STRICT = "strict"
    ALLOW_NEW_COLUMNS = "allow_new_columns"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "SchemaMode":
        """Normalize a schema mode value."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.STRICT  # Default to strict
        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member
        raise ValueError(
            f"Invalid SchemaMode '{raw}'. Valid options: {', '.join(cls.choices())}"
        )

    def describe(self) -> str:
        """Return human-readable description."""
        return _SCHEMA_MODE_DESCRIPTIONS.get(self.value, self.value)


DEFAULT_BRONZE_BASE = Path("sampledata") / "bronze_samples"
DEFAULT_SILVER_BASE = Path("sampledata") / "silver_samples"


def _require_list_of_strings(values: Any, field_name: str) -> List[str]:
    if values is None:
        return []
    if not isinstance(values, list) or any(
        not isinstance(item, str) for item in values
    ):
        raise ValueError(f"{field_name} must be a list of strings")
    return [item.strip() for item in values if item and item.strip()]


def _require_optional_str(value: Any, field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    value = value.strip()
    return value or None


def _require_bool(value: Any, field_name: str, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _ensure_bucket_reference(storage: Any, default_ref: str) -> None:
    if not isinstance(storage, dict):
        return
    backend = storage.get("backend")
    if backend != "s3":
        return
    if "bucket" not in storage or not storage["bucket"]:
        storage["bucket"] = default_ref


@dataclass
class BronzeIntent:
    enabled: bool
    source_type: str
    connection_name: Optional[str]
    source_query: Optional[str]
    path_pattern: Optional[str]
    partition_column: Optional[str]
    owner_team: Optional[str] = None
    owner_contact: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    # Storage backend configuration
    source_storage: str = "local"  # "local" or "s3" - where to read source files from
    output_storage: str = "local"  # "local" or "s3" - where to write Bronze chunks
    output_bucket: Optional[str] = None  # Bucket name or reference (for S3)
    output_prefix: str = ""  # Prefix within bucket (for S3)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BronzeIntent":
        if not isinstance(data, dict):
            raise ValueError("bronze section must be a dictionary")
        enabled = bool(data.get("enabled", True))
        source_type = data.get("source_type", "file")
        if source_type not in {"db", "file", "api", "custom"}:
            raise ValueError(
                "bronze.source_type must be one of {'db', 'file', 'api', 'custom'}"
            )
        connection_name = _require_optional_str(
            data.get("connection_name"), "bronze.connection_name"
        )
        source_query = _require_optional_str(
            data.get("source_query"), "bronze.source_query"
        )
        path_pattern = _require_optional_str(
            data.get("path_pattern"), "bronze.path_pattern"
        )
        partition_column = _require_optional_str(
            data.get("partition_column"), "bronze.partition_column"
        )
        options = data.get("options") or {}
        if options and not isinstance(options, dict):
            raise ValueError("bronze.options must be a dictionary when provided")

        # Storage backend configuration
        source_storage = data.get("source_storage", "local")
        if source_storage not in {"local", "s3"}:
            raise ValueError("bronze.source_storage must be 'local' or 's3'")

        output_storage = data.get("output_storage", "local")
        if output_storage not in {"local", "s3"}:
            raise ValueError("bronze.output_storage must be 'local' or 's3'")

        output_bucket = _require_optional_str(
            data.get("output_bucket"), "bronze.output_bucket"
        )
        output_prefix = data.get("output_prefix", "")
        if output_prefix and not isinstance(output_prefix, str):
            raise ValueError("bronze.output_prefix must be a string")

        return cls(
            enabled=enabled,
            source_type=source_type,
            connection_name=connection_name,
            source_query=source_query,
            path_pattern=path_pattern,
            partition_column=partition_column,
            owner_team=_require_optional_str(
                data.get("owner_team"), "bronze.owner_team"
            ),
            owner_contact=_require_optional_str(
                data.get("owner_contact"), "bronze.owner_contact"
            ),
            options=options,
            source_storage=source_storage,
            output_storage=output_storage,
            output_bucket=output_bucket,
            output_prefix=output_prefix,
        )


@dataclass
class SilverIntent:
    enabled: bool
    entity_kind: EntityKind
    history_mode: Optional[HistoryMode]
    input_mode: Optional[InputMode]
    delete_mode: DeleteMode
    schema_mode: SchemaMode
    natural_keys: List[str]
    event_ts_column: Optional[str]
    change_ts_column: Optional[str]
    attributes: List[str]
    partition_by: List[str]
    output_dir: Optional[str] = None
    version: int = 1
    load_partition_name: str = "load_date"
    include_pattern_folder: bool = False
    write_parquet: bool = True
    write_csv: bool = False
    require_checksum: bool = False
    semantic_owner: Optional[str] = None
    semantic_contact: Optional[str] = None
    # Unified temporal configuration (V1: record_time only, no secondary partitions)
    record_time_column: Optional[str] = None  # Column with record/event/change time
    record_time_partition: Optional[str] = (
        None  # Partition key name (e.g., "event_date", "effective_from_date")
    )
    load_batch_id_column: str = "load_batch_id"  # Standard column for batch tracking
    # Storage backend configuration
    input_storage: str = "local"  # "local" or "s3" - where to read Bronze data from
    output_storage: str = "local"  # "local" or "s3" - where to write Silver artifacts
    output_bucket: Optional[str] = None  # Bucket name or reference (for S3)
    output_prefix: str = ""  # Prefix within bucket (for S3)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SilverIntent":
        if not isinstance(data, dict):
            raise ValueError("silver section must be a dictionary")
        enabled = bool(data.get("enabled", True))
        entity_kind_raw = data.get("entity_kind")
        if not entity_kind_raw:
            raise ValueError("silver.entity_kind is required")
        try:
            entity_kind = EntityKind(entity_kind_raw)
        except ValueError as exc:
            raise ValueError(
                "silver.entity_kind must be one of "
                f"{[kind.value for kind in EntityKind]}"
            ) from exc

        history_mode = data.get("history_mode")
        history_value = None
        if history_mode:
            try:
                history_value = HistoryMode(history_mode)
            except ValueError as exc:
                raise ValueError(
                    "silver.history_mode must be one of "
                    f"{[mode.value for mode in HistoryMode]}"
                ) from exc
        elif entity_kind.is_state_like:
            history_value = HistoryMode.SCD2

        input_mode = data.get("input_mode")
        input_value = None
        if input_mode:
            try:
                input_value = InputMode(input_mode)
            except ValueError as exc:
                raise ValueError(
                    "silver.input_mode must be one of "
                    f"{[mode.value for mode in InputMode]}"
                ) from exc
        elif entity_kind.is_event_like:
            input_value = InputMode.APPEND_LOG

        if entity_kind.is_event_like and history_mode:
            raise ValueError(
                "silver.history_mode is only valid for state-like and derived_state entity kinds"
            )
        if entity_kind.is_state_like and input_mode:
            raise ValueError(
                "silver.input_mode is not valid when entity_kind is state-like"
            )
        if entity_kind.is_state_like and history_value is None:
            history_value = HistoryMode.SCD2
        if entity_kind.is_event_like and input_value is None:
            input_value = InputMode.APPEND_LOG

        delete_mode_raw = data.get("delete_mode", DeleteMode.IGNORE.value)
        try:
            delete_mode = DeleteMode(delete_mode_raw)
        except ValueError as exc:
            raise ValueError(
                "silver.delete_mode must be one of "
                f"{[mode.value for mode in DeleteMode]}"
            ) from exc

        schema_mode_raw = data.get("schema_mode", SchemaMode.STRICT.value)
        try:
            schema_mode = SchemaMode(schema_mode_raw)
        except ValueError as exc:
            raise ValueError(
                "silver.schema_mode must be one of "
                f"{[mode.value for mode in SchemaMode]}"
            ) from exc

        natural_keys = _require_list_of_strings(
            data.get("natural_keys"), "silver.natural_keys"
        )
        if not natural_keys:
            raise ValueError("silver.natural_keys must include at least one column")

        event_ts_column = _require_optional_str(
            data.get("event_ts_column"), "silver.event_ts_column"
        )
        change_ts_column = _require_optional_str(
            data.get("change_ts_column"), "silver.change_ts_column"
        )

        if entity_kind.is_event_like and not event_ts_column:
            raise ValueError(
                "silver.event_ts_column is required for event or derived_event entities"
            )
        if entity_kind.is_state_like and not change_ts_column:
            raise ValueError(
                "silver.change_ts_column is required for state or derived_state entities"
            )

        attributes = _require_list_of_strings(
            data.get("attributes"), "silver.attributes"
        )
        partition_by = _require_list_of_strings(
            data.get("partition_by"), "silver.partition_by"
        )
        require_checksum = _require_bool(
            data.get("require_checksum"), "silver.require_checksum", False
        )
        semantic_owner = _require_optional_str(
            data.get("semantic_owner"), "silver.semantic_owner"
        )
        semantic_contact = _require_optional_str(
            data.get("semantic_contact"), "silver.semantic_contact"
        )
        output_dir = _require_optional_str(data.get("output_dir"), "silver.output_dir")
        version_raw = data.get("version", 1)
        if not isinstance(version_raw, int) or version_raw <= 0:
            raise ValueError("silver.version must be a positive integer")
        load_partition_name = data.get("load_partition_name", "load_date")
        if not isinstance(load_partition_name, str):
            raise ValueError("silver.load_partition_name must be a string")
        include_pattern_folder = bool(data.get("include_pattern_folder", False))
        write_parquet = _require_bool(
            data.get("write_parquet"), "silver.write_parquet", True
        )
        write_csv = _require_bool(data.get("write_csv"), "silver.write_csv", False)

        # Unified temporal configuration
        record_time_column = _require_optional_str(
            data.get("record_time_column"), "silver.record_time_column"
        )
        record_time_partition = _require_optional_str(
            data.get("record_time_partition"), "silver.record_time_partition"
        )
        load_batch_id_column = (
            _require_optional_str(
                data.get("load_batch_id_column"), "silver.load_batch_id_column"
            )
            or "load_batch_id"
        )

        # Storage backend configuration
        input_storage = data.get("input_storage", "local")
        if input_storage not in {"local", "s3"}:
            raise ValueError("silver.input_storage must be 'local' or 's3'")

        output_storage = data.get("output_storage", "local")
        if output_storage not in {"local", "s3"}:
            raise ValueError("silver.output_storage must be 'local' or 's3'")

        output_bucket = _require_optional_str(
            data.get("output_bucket"), "silver.output_bucket"
        )
        output_prefix = data.get("output_prefix", "")
        if output_prefix and not isinstance(output_prefix, str):
            raise ValueError("silver.output_prefix must be a string")

        return cls(
            enabled=enabled,
            entity_kind=entity_kind,
            history_mode=history_value,
            input_mode=input_value,
            delete_mode=delete_mode,
            schema_mode=schema_mode,
            natural_keys=natural_keys,
            event_ts_column=event_ts_column,
            change_ts_column=change_ts_column,
            attributes=attributes,
            partition_by=partition_by,
            output_dir=output_dir,
            version=version_raw,
            load_partition_name=load_partition_name,
            include_pattern_folder=include_pattern_folder,
            write_parquet=write_parquet,
            write_csv=write_csv,
            require_checksum=require_checksum,
            semantic_owner=semantic_owner,
            semantic_contact=semantic_contact,
            record_time_column=record_time_column,
            record_time_partition=record_time_partition,
            load_batch_id_column=load_batch_id_column,
            input_storage=input_storage,
            output_storage=output_storage,
            output_bucket=output_bucket,
            output_prefix=output_prefix,
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
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_data_source must be a dictionary")
        name = _require_optional_str(
            data.get("name"), "polybase_setup.external_data_source.name"
        )
        if not name:
            raise ValueError("polybase_setup.external_data_source.name is required")
        data_source_type = (
            _require_optional_str(
                data.get("data_source_type"),
                "polybase_setup.external_data_source.data_source_type",
            )
            or "HADOOP"
        )
        location = _require_optional_str(
            data.get("location"), "polybase_setup.external_data_source.location"
        )
        if not location:
            raise ValueError("polybase_setup.external_data_source.location is required")
        credential_name = _require_optional_str(
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
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_file_format must be a dictionary")
        name = _require_optional_str(
            data.get("name"), "polybase_setup.external_file_format.name"
        )
        if not name:
            raise ValueError("polybase_setup.external_file_format.name is required")
        format_type = (
            _require_optional_str(
                data.get("format_type"),
                "polybase_setup.external_file_format.format_type",
            )
            or "PARQUET"
        )
        compression = _require_optional_str(
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
        if not isinstance(data, dict):
            raise ValueError("polybase_setup.external_tables item must be a dictionary")
        artifact_name = _require_optional_str(
            data.get("artifact_name"), "polybase_setup.external_tables.artifact_name"
        )
        if not artifact_name:
            raise ValueError("polybase_setup.external_tables.artifact_name is required")
        schema_name = (
            _require_optional_str(
                data.get("schema_name"), "polybase_setup.external_tables.schema_name"
            )
            or "dbo"
        )
        table_name = _require_optional_str(
            data.get("table_name"), "polybase_setup.external_tables.table_name"
        )
        if not table_name:
            raise ValueError("polybase_setup.external_tables.table_name is required")
        partition_columns = _require_list_of_strings(
            data.get("partition_columns"),
            "polybase_setup.external_tables.partition_columns",
        )
        reject_type = (
            _require_optional_str(
                data.get("reject_type"), "polybase_setup.external_tables.reject_type"
            )
            or "VALUE"
        )
        reject_value = data.get("reject_value", 0)
        if not isinstance(reject_value, int):
            raise ValueError(
                "polybase_setup.external_tables.reject_value must be an integer"
            )
        sample_queries = _require_list_of_strings(
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
        if not data:
            return cls(enabled=False)
        if not isinstance(data, dict):
            raise ValueError("polybase_setup must be a dictionary")

        enabled = _require_bool(data.get("enabled"), "polybase_setup.enabled", True)

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

        trino_enabled = _require_bool(
            data.get("trino_enabled"), "polybase_setup.trino_enabled", False
        )
        iceberg_enabled = _require_bool(
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


@dataclass
class PathStructure:
    """Configuration for how Bronze and Silver paths are structured.
    
    Allows independent control of path ordering for Bronze vs Silver layers.
    """
    bronze: Dict[str, str] = field(default_factory=dict)  # e.g., system_key, entity_key, pattern_key, date_key
    silver: Dict[str, str] = field(default_factory=dict)  # e.g., sample_key, silver_model_key, domain_key, load_date_key
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PathStructure":
        """Parse path_structure from config dict.
        
        Supports both nested format (bronze/silver subsections) and legacy flat format.
        """
        if not data:
            return cls()
        
        if not isinstance(data, dict):
            raise ValueError("path_structure must be a dictionary")
        
        # Check if using new nested format
        if "bronze" in data and isinstance(data["bronze"], dict):
            bronze_keys = data["bronze"]
        else:
            # Legacy flat format - extract bronze keys
            bronze_keys = {
                k: data.get(k, v)
                for k, v in {
                    "system_key": "system",
                    "entity_key": "table",
                    "pattern_key": "pattern",
                    "date_key": "dt",
                }.items()
            }
        
        # Check if using new nested format for silver
        if "silver" in data and isinstance(data["silver"], dict):
            silver_keys = data["silver"]
        else:
            # Legacy flat format - extract silver keys
            silver_keys = {
                k: data.get(k, v)
                for k, v in {
                    "domain_key": "domain",
                    "entity_key": "entity",
                    "version_key": "v",
                    "pattern_key": "pattern",
                    "load_date_key": "load_date",
                }.items()
            }
        
        return cls(bronze=bronze_keys, silver=silver_keys)


@dataclass
class DatasetConfig:
    system: str
    entity: str
    environment: Optional[str]
    domain: Optional[str]
    bronze: BronzeIntent
    silver: SilverIntent
    path_structure: PathStructure
    polybase_setup: Optional[PolybaseSetup] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetConfig":
        if not isinstance(data, dict):
            raise ValueError("Dataset configuration must be a dictionary")
        system = data.get("system")
        entity = data.get("entity")
        if not system:
            raise ValueError("system is required")
        if not entity:
            raise ValueError("entity is required")
        environment = _require_optional_str(data.get("environment"), "environment")
        domain = _require_optional_str(data.get("domain"), "domain")

        # Handle top-level storage section (new format) or inline storage config (backward compat)
        bronze_data = dict(data.get("bronze") or {})
        silver_data = dict(data.get("silver") or {})

        storage_config = data.get("storage")
        if storage_config:
            # New format: Apply storage section to bronze and silver configs
            source_storage = storage_config.get("source", {})
            bronze_storage = storage_config.get("bronze", {})
            silver_storage = storage_config.get("silver", {})
            _ensure_bucket_reference(source_storage, "source_data")
            _ensure_bucket_reference(bronze_storage, "bronze_data")
            _ensure_bucket_reference(silver_storage, "silver_data")

            # Map storage.source to bronze.source_storage and construct full path
            if "backend" in source_storage:
                backend = source_storage["backend"]
                bronze_data.setdefault("source_storage", backend)

                # Get path pattern from bronze section (relative path)
                path_pattern = bronze_data.get("path_pattern", "")

                # Construct full path from storage config + bronze.path_pattern
                if (
                    path_pattern
                    and not path_pattern.startswith("s3://")
                    and not path_pattern.startswith("./")
                ):
                    # Path is relative, combine with storage prefix
                    bucket = source_storage.get("bucket", "")
                    prefix = source_storage.get("prefix", "")

                    if backend == "s3":
                        # Build S3 URI: s3://bucket/prefix/path_pattern
                        full_path = f"s3://{bucket}/{prefix}{path_pattern}"
                        bronze_data["path_pattern"] = full_path
                    elif backend == "local" and prefix:
                        # Build local path: prefix/path_pattern
                        full_path = f"{prefix}{path_pattern}"
                        bronze_data["path_pattern"] = full_path

            # Map storage.bronze to bronze.output_storage/bucket/prefix
            if "backend" in bronze_storage:
                bronze_data.setdefault("output_storage", bronze_storage["backend"])
            if "bucket" in bronze_storage:
                bronze_data.setdefault("output_bucket", bronze_storage["bucket"])
            if "prefix" in bronze_storage:
                bronze_data.setdefault("output_prefix", bronze_storage["prefix"])

            # Map storage.silver to silver.input_storage/output_storage/bucket/prefix
            if "backend" in silver_storage:
                silver_data.setdefault("input_storage", silver_storage["backend"])
                silver_data.setdefault("output_storage", silver_storage["backend"])
            if "bucket" in silver_storage:
                silver_data.setdefault("output_bucket", silver_storage["bucket"])
            if "prefix" in silver_storage:
                silver_data.setdefault("output_prefix", silver_storage["prefix"])

        bronze_cfg = BronzeIntent.from_dict(bronze_data)
        silver_cfg = SilverIntent.from_dict(silver_data)
        polybase_cfg = PolybaseSetup.from_dict(data.get("polybase_setup"))
        path_struct = PathStructure.from_dict(data.get("path_structure", {}))
        return cls(
            system=system,
            entity=entity,
            environment=environment,
            domain=domain,
            bronze=bronze_cfg,
            silver=silver_cfg,
            path_structure=path_struct,
            polybase_setup=polybase_cfg,
        )

    @property
    def dataset_id(self) -> str:
        return f"{self.system}.{self.entity}"

    @property
    def bronze_base_path(self) -> Path:
        if self.environment:
            return DEFAULT_BRONZE_BASE / f"env={self.environment}"
        return DEFAULT_BRONZE_BASE

    @property
    def silver_base_path(self) -> Path:
        if self.environment:
            return DEFAULT_SILVER_BASE / f"env={self.environment}"
        return DEFAULT_SILVER_BASE

    def bronze_relative_prefix(self) -> str:
        parts = [f"system={self.system}", f"entity={self.entity}"]
        return "/".join(parts)


def is_new_intent_config(raw: Dict[str, Any]) -> bool:
    return (
        isinstance(raw, dict)
        and "system" in raw
        and "entity" in raw
        and "bronze" in raw
        and "silver" in raw
    )


# Compatibility helpers

logger = logging.getLogger(__name__)


def dataset_to_runtime_config(dataset: DatasetConfig) -> Dict[str, Any]:
    """Build a legacy-style runtime config dict from a DatasetConfig."""
    bronze_base_path = dataset.bronze_base_path
    bronze_override = dataset.bronze.options.get("local_output_dir")
    if bronze_override:
        bronze_base_path = Path(bronze_override).resolve()
    bronze_base = str(bronze_base_path)
    pattern_folder = dataset.bronze.options.get("pattern_folder")

    if dataset.silver.output_dir:
        silver_base_path = Path(dataset.silver.output_dir).resolve()
    else:
        silver_base_path = dataset.silver_base_path
    silver_base = str(silver_base_path)
    pattern_override = dataset.bronze.options.get("load_pattern")
    if pattern_override:
        load_pattern_value = LoadPattern.normalize(pattern_override).value
    else:
        load_pattern_value = LoadPattern.SNAPSHOT.value

    bronze_backend = dataset.bronze.output_storage or "local"
    local_run = {
        "load_pattern": load_pattern_value,
        "local_output_dir": bronze_base,
        "write_parquet": dataset.silver.write_parquet,
        "write_csv": dataset.silver.write_csv,
        "storage_enabled": bronze_backend == "s3",
    }

    source_cfg: Dict[str, Any] = {
        "type": dataset.bronze.source_type,
        "system": dataset.system,
        "table": dataset.entity,
        "run": local_run,
    }
    if pattern_folder:
        source_cfg["run"]["pattern_folder"] = pattern_folder
    if dataset.bronze.owner_team:
        source_cfg["owner_team"] = dataset.bronze.owner_team
    if dataset.bronze.owner_contact:
        source_cfg["owner_contact"] = dataset.bronze.owner_contact

    if dataset.bronze.source_type == "file":
        if not dataset.bronze.path_pattern:
            raise ValueError(
                f"{dataset.dataset_id} bronze.path_pattern must be provided for file sources"
            )
        file_cfg: Dict[str, Any] = {
            "path": dataset.bronze.path_pattern,
            "format": dataset.bronze.options.get("format", "csv"),
        }
        file_cfg.update(dataset.bronze.options.get("file", {}))
        source_cfg["file"] = file_cfg
    elif dataset.bronze.source_type == "db":
        db_cfg = dataset.bronze.options.get("db", {})
        conn = dataset.bronze.connection_name or db_cfg.get("conn_str_env")
        if not conn:
            raise ValueError(
                f"{dataset.dataset_id} bronze.connection_name (or db.conn_str_env) is required for db sources"
            )
        if not dataset.bronze.source_query and not db_cfg.get("base_query"):
            raise ValueError(
                f"{dataset.dataset_id} bronze.source_query is required for db sources"
            )
        source_cfg["db"] = {
            "conn_str_env": conn,
            "base_query": dataset.bronze.source_query or db_cfg.get("base_query"),
        }
    elif dataset.bronze.source_type == "api":
        api_cfg = dataset.bronze.options.get("api", {}).copy()
        base_url = api_cfg.get("base_url") or dataset.bronze.connection_name
        if not base_url:
            raise ValueError(
                f"{dataset.dataset_id} bronze.connection_name (or api.base_url) is required for api sources"
            )
        endpoint = api_cfg.get("endpoint") or dataset.bronze.source_query or "/"
        api_cfg.setdefault("base_url", base_url)
        api_cfg.setdefault("endpoint", endpoint)
        source_cfg["api"] = api_cfg
    elif dataset.bronze.source_type == "custom":
        custom_cfg = dataset.bronze.options.get("custom_extractor", {})
        if not custom_cfg:
            raise ValueError(
                f"{dataset.dataset_id} requires bronze.options.custom_extractor for custom sources"
            )
        source_cfg["custom_extractor"] = custom_cfg

    partitioning = {"use_dt_partition": True, "partition_strategy": "date"}
    if dataset.bronze.partition_column:
        partitioning["column"] = dataset.bronze.partition_column

    bronze_cfg: Dict[str, Any] = {
        "storage_backend": bronze_backend,
        "local_path": bronze_base,
        "partitioning": partitioning,
        "output_defaults": dataset.bronze.options.get(
            "output_defaults",
            {
                "allow_csv": True,
                "allow_parquet": True,
                "parquet_compression": "snappy",
            },
        ),
    }
    if bronze_backend == "s3":
        bronze_cfg["s3_bucket"] = dataset.bronze.output_bucket
        bronze_cfg["s3_prefix"] = dataset.bronze.output_prefix
    bronze_options = bronze_cfg.setdefault("options", {})
    if pattern_folder:
        bronze_options["pattern_folder"] = pattern_folder

    platform_cfg = {
        "bronze": bronze_cfg,
        "s3_connection": {
            "endpoint_url_env": "BRONZE_S3_ENDPOINT",
            "access_key_env": "AWS_ACCESS_KEY_ID",
            "secret_key_env": "AWS_SECRET_ACCESS_KEY",
        },
    }

    order_column = (
        dataset.silver.change_ts_column
        if dataset.silver.entity_kind.is_state_like
        else dataset.silver.event_ts_column
    )

    from core.primitives.foundations.models import SilverModel

    if dataset.silver.entity_kind.is_state_like:
        if dataset.silver.history_mode == HistoryMode.SCD1:
            model = SilverModel.SCD_TYPE_1.value
        elif dataset.silver.history_mode == HistoryMode.LATEST_ONLY:
            model = SilverModel.FULL_MERGE_DEDUPE.value
        else:
            model = SilverModel.SCD_TYPE_2.value
    elif dataset.silver.entity_kind == EntityKind.DERIVED_EVENT:
        model = SilverModel.INCREMENTAL_MERGE.value
    else:
        model = SilverModel.PERIODIC_SNAPSHOT.value

    silver_cfg: Dict[str, Any] = {
        "output_dir": silver_base,
        "domain": dataset.domain or dataset.system,
        "entity": dataset.entity,
        "version": dataset.silver.version,
        "load_partition_name": dataset.silver.load_partition_name,
        "include_pattern_folder": dataset.silver.include_pattern_folder,
        "write_parquet": dataset.silver.write_parquet,
        "write_csv": dataset.silver.write_csv,
        "parquet_compression": "snappy",
        "primary_keys": list(dataset.silver.natural_keys),
        "order_column": order_column,
        "partitioning": {"columns": list(dataset.silver.partition_by)},
        "schema": {
            "rename_map": {},
            "column_order": list(dataset.silver.attributes) or None,
        },
        "normalization": {"trim_strings": False, "empty_strings_as_null": False},
        "error_handling": {
            "enabled": False,
            "max_bad_records": 0,
            "max_bad_percent": 0.0,
        },
        "model": model,
        "require_checksum": dataset.silver.require_checksum,
        "semantic_owner": dataset.silver.semantic_owner,
        "semantic_contact": dataset.silver.semantic_contact,
    }

    return {
        "config_version": 3,
        "platform": platform_cfg,
        "source": source_cfg,
        "silver": silver_cfg,
        "path_structure": {
            "bronze": dataset.path_structure.bronze,
            "silver": dataset.path_structure.silver,
        },
    }


def legacy_to_dataset(cfg: Dict[str, Any]) -> Optional[DatasetConfig]:
    """Best-effort translation from legacy config dictionaries to DatasetConfig."""
    try:
        source = cfg["source"]
        silver_raw = cfg.get("silver", {})
        system = source["system"]
        entity = source["table"]
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug(
            "Legacy config missing required keys for dataset conversion: %s", exc
        )
        return None

    source_type = source.get("type", "file")
    run_cfg = source.get("run", {})
    partition_column = run_cfg.get("partition_column")
    connection_name = None
    source_query = None
    path_pattern = None
    options: Dict[str, Any] = {}

    if source_type == "db":
        db_cfg = source.get("db", {})
        connection_name = db_cfg.get("conn_str_env")
        source_query = db_cfg.get("base_query")
        options["db"] = db_cfg
    elif source_type == "api":
        api_cfg = source.get("api", {})
        connection_name = api_cfg.get("base_url")
        source_query = api_cfg.get("endpoint")
        options["api"] = api_cfg
    elif source_type == "file":
        file_cfg = source.get("file", {})
        path_pattern = file_cfg.get("path")
        options["file"] = file_cfg

    bronze_intent = BronzeIntent(
        enabled=True,
        source_type=source_type,
        connection_name=connection_name,
        source_query=source_query,
        path_pattern=path_pattern,
        partition_column=partition_column,
        options=options,
    )

    primary_keys = silver_raw.get("primary_keys") or []
    order_column = silver_raw.get("order_column") or run_cfg.get("order_column")
    attributes = (silver_raw.get("schema") or {}).get("column_order") or []
    partition_by = (silver_raw.get("partitioning") or {}).get("columns") or []
    model = (silver_raw.get("model") or "").lower()

    if model in {"scd_type_1", "scd_type_2"}:
        entity_kind = EntityKind.STATE
    elif model == "full_merge_dedupe":
        entity_kind = EntityKind.STATE
    else:
        entity_kind = EntityKind.EVENT

    history_mode = None
    if entity_kind.is_state_like:
        if model == "scd_type_1":
            history_mode = HistoryMode.SCD1
        elif model == "full_merge_dedupe":
            history_mode = HistoryMode.LATEST_ONLY
        else:
            history_mode = HistoryMode.SCD2

    input_mode = None
    if entity_kind.is_event_like:
        load_pattern = run_cfg.get("load_pattern", LoadPattern.SNAPSHOT.value)
        input_mode = (
            InputMode.REPLACE_DAILY
            if load_pattern == LoadPattern.SNAPSHOT.value
            else InputMode.APPEND_LOG
        )

    if not order_column:
        if entity_kind.is_state_like:
            order_column = "_change_ts"
        else:
            order_column = "_event_ts"
        emit_compat(
            "Legacy config missing silver.order_column; defaulting to placeholder column",
            code="CFG_NEW_ORDER",
        )

    write_parquet = _require_bool(
        silver_raw.get("write_parquet"), "silver.write_parquet", True
    )
    write_csv = _require_bool(silver_raw.get("write_csv"), "silver.write_csv", False)
    silver_intent = SilverIntent(
        enabled=True,
        entity_kind=entity_kind,
        history_mode=history_mode,
        input_mode=input_mode,
        delete_mode=DeleteMode.IGNORE,
        schema_mode=SchemaMode.STRICT,
        natural_keys=primary_keys,
        event_ts_column=order_column if entity_kind.is_event_like else None,
        change_ts_column=order_column if entity_kind.is_state_like else None,
        attributes=attributes,
        partition_by=partition_by,
        write_parquet=write_parquet,
        write_csv=write_csv,
    )

    domain = silver_raw.get("domain")
    env = cfg.get("environment")

    return DatasetConfig(
        system=system,
        entity=entity,
        environment=env,
        domain=domain,
        bronze=bronze_intent,
        silver=silver_intent,
    )
