"""Bronze and Silver intent configuration models.

These dataclasses define the semantic intent for Bronze extraction and Silver transformation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.infrastructure.config.models.enums import (
    EntityKind,
    HistoryMode,
    InputMode,
    DeleteMode,
    SchemaMode,
)
from core.infrastructure.config.models.helpers import (
    require_list_of_strings,
    require_optional_str,
    require_bool,
)


@dataclass
class BronzeIntent:
    """Configuration intent for Bronze layer extraction."""

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
        """Parse BronzeIntent from a dictionary."""
        if not isinstance(data, dict):
            raise ValueError("bronze section must be a dictionary")
        enabled = bool(data.get("enabled", True))
        source_type = data.get("source_type", "file")
        if source_type not in {"db", "file", "api", "custom"}:
            raise ValueError(
                "bronze.source_type must be one of {'db', 'file', 'api', 'custom'}"
            )
        connection_name = require_optional_str(
            data.get("connection_name"), "bronze.connection_name"
        )
        source_query = require_optional_str(
            data.get("source_query"), "bronze.source_query"
        )
        path_pattern = require_optional_str(
            data.get("path_pattern"), "bronze.path_pattern"
        )
        partition_column = require_optional_str(
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

        output_bucket = require_optional_str(
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
            owner_team=require_optional_str(
                data.get("owner_team"), "bronze.owner_team"
            ),
            owner_contact=require_optional_str(
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
    """Configuration intent for Silver layer transformation."""

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
        """Parse SilverIntent from a dictionary."""
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

        natural_keys = require_list_of_strings(
            data.get("natural_keys"), "silver.natural_keys"
        )
        if not natural_keys:
            raise ValueError("silver.natural_keys must include at least one column")

        event_ts_column = require_optional_str(
            data.get("event_ts_column"), "silver.event_ts_column"
        )
        change_ts_column = require_optional_str(
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

        attributes = require_list_of_strings(
            data.get("attributes"), "silver.attributes"
        )
        partition_by = require_list_of_strings(
            data.get("partition_by"), "silver.partition_by"
        )
        require_checksum = require_bool(
            data.get("require_checksum"), "silver.require_checksum", False
        )
        semantic_owner = require_optional_str(
            data.get("semantic_owner"), "silver.semantic_owner"
        )
        semantic_contact = require_optional_str(
            data.get("semantic_contact"), "silver.semantic_contact"
        )
        output_dir = require_optional_str(data.get("output_dir"), "silver.output_dir")
        version_raw = data.get("version", 1)
        if not isinstance(version_raw, int) or version_raw <= 0:
            raise ValueError("silver.version must be a positive integer")
        load_partition_name = data.get("load_partition_name", "load_date")
        if not isinstance(load_partition_name, str):
            raise ValueError("silver.load_partition_name must be a string")
        include_pattern_folder = bool(data.get("include_pattern_folder", False))
        write_parquet = require_bool(
            data.get("write_parquet"), "silver.write_parquet", True
        )
        write_csv = require_bool(data.get("write_csv"), "silver.write_csv", False)

        # Unified temporal configuration
        record_time_column = require_optional_str(
            data.get("record_time_column"), "silver.record_time_column"
        )
        record_time_partition = require_optional_str(
            data.get("record_time_partition"), "silver.record_time_partition"
        )
        load_batch_id_column = (
            require_optional_str(
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

        output_bucket = require_optional_str(
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


__all__ = [
    "BronzeIntent",
    "SilverIntent",
]
