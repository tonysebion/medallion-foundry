"""
Typed configuration primitives for Bronze -> Silver pipelines.

These dataclasses centralize validation logic so core.config can remain
focused on wiring and IO concerns rather than hand-validating nested dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.patterns import LoadPattern
from core.silver_models import SilverModel


def _ensure_type(value: Any, expected: type, message: str) -> Any:
    if value is None:
        return value
    if not isinstance(value, expected):
        raise ValueError(message)
    return value


def _ensure_bool(value: Any, message: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(message)
    return value


def _ensure_str(value: Any, message: str) -> str:
    if not isinstance(value, str):
        raise ValueError(message)
    return value


def _ensure_positive_int(value: Any, message: str) -> int:
    if not isinstance(value, int) or value < 0:
        raise ValueError(message)
    return value


@dataclass
class SilverSchema:
    column_order: Optional[List[str]] = None
    rename_map: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverSchema":
        data = raw or {}
        column_order = data.get("column_order")
        if column_order is not None:
            if not isinstance(column_order, list) or any(not isinstance(col, str) for col in column_order):
                raise ValueError("silver.schema.column_order must be a list of strings")
        rename_map = data.get("rename_map") or {}
        if not isinstance(rename_map, dict) or any(
            not isinstance(k, str) or not isinstance(v, str) for k, v in rename_map.items()
        ):
            raise ValueError("silver.schema.rename_map must be a mapping of strings")
        return cls(column_order=column_order, rename_map=rename_map)

    def to_dict(self) -> Dict[str, Any]:
        return {"column_order": self.column_order, "rename_map": dict(self.rename_map)}


@dataclass
class SilverNormalization:
    trim_strings: bool = False
    empty_strings_as_null: bool = False

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverNormalization":
        data = raw or {}
        if not isinstance(data, dict):
            raise ValueError("silver.normalization must be a dictionary")
        trim_strings = data.get("trim_strings", False)
        if "trim_strings" in data:
            _ensure_bool(trim_strings, "silver.normalization.trim_strings must be a boolean")
        empty_strings = data.get("empty_strings_as_null", False)
        if "empty_strings_as_null" in data:
            _ensure_bool(empty_strings, "silver.normalization.empty_strings_as_null must be a boolean")
        return cls(trim_strings=trim_strings, empty_strings_as_null=empty_strings)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trim_strings": self.trim_strings,
            "empty_strings_as_null": self.empty_strings_as_null,
        }


@dataclass
class SilverErrorHandling:
    enabled: bool = False
    max_bad_records: int = 0
    max_bad_percent: float = 0.0

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverErrorHandling":
        data = raw or {}
        if not isinstance(data, dict):
            raise ValueError("silver.error_handling must be a dictionary")
        enabled = data.get("enabled", False)
        if "enabled" in data:
            _ensure_bool(enabled, "silver.error_handling.enabled must be a boolean")
        max_bad_records = data.get("max_bad_records", 0)
        if "max_bad_records" in data:
            if not isinstance(max_bad_records, int) or max_bad_records < 0:
                raise ValueError("silver.error_handling.max_bad_records must be a non-negative integer")
        max_bad_percent = data.get("max_bad_percent", 0.0)
        if "max_bad_percent" in data:
            if not isinstance(max_bad_percent, (int, float)) or max_bad_percent < 0:
                raise ValueError("silver.error_handling.max_bad_percent must be a non-negative number")
        return cls(
            enabled=enabled,
            max_bad_records=max_bad_records,
            max_bad_percent=float(max_bad_percent),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "max_bad_records": self.max_bad_records,
            "max_bad_percent": self.max_bad_percent,
        }


@dataclass
class SilverPartitioning:
    columns: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverPartitioning":
        data = raw or {}
        if not isinstance(data, dict):
            raise ValueError("silver.partitioning must be a dictionary")
        columns = data.get("columns")
        column = data.get("column")
        if columns is not None:
            if not isinstance(columns, list) or any(not isinstance(col, str) for col in columns):
                raise ValueError("silver.partitioning.columns must be a list of strings")
        elif column:
            if not isinstance(column, str):
                raise ValueError("silver.partitioning.column must be a string")
            columns = [column]
        else:
            columns = []
        return cls(columns=columns)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "columns": list(self.columns),
            "column": self.columns[0] if self.columns else None,
        }


@dataclass
class SilverConfig:
    output_dir: str
    write_parquet: bool
    write_csv: bool
    parquet_compression: str
    domain: str
    entity: str
    version: int
    load_partition_name: str
    include_pattern_folder: bool
    require_checksum: bool
    primary_keys: List[str]
    order_column: Optional[str]
    full_output_name: str
    current_output_name: str
    history_output_name: str
    cdc_output_name: str
    schema: SilverSchema
    normalization: SilverNormalization
    error_handling: SilverErrorHandling
    partitioning: SilverPartitioning
    model: SilverModel

    @classmethod
    def from_raw(
        cls,
        raw: Optional[Dict[str, Any]],
        source: Dict[str, Any],
        load_pattern: LoadPattern,
    ) -> "SilverConfig":
        data = raw.copy() if raw else {}
        output_dir = data.get("output_dir", "./silver_output")
        if "output_dir" in data:
            _ensure_str(output_dir, "silver.output_dir must be a string path")

        write_parquet = data.get("write_parquet", True)
        if "write_parquet" in data:
            _ensure_bool(write_parquet, "silver.write_parquet must be a boolean")
        write_csv = data.get("write_csv", False)
        if "write_csv" in data:
            _ensure_bool(write_csv, "silver.write_csv must be a boolean")

        parquet_compression = data.get("parquet_compression", "snappy")
        if "parquet_compression" in data:
            _ensure_str(parquet_compression, "silver.parquet_compression must be a string")

        domain = data.get("domain", source["system"])
        if data.get("domain") is not None:
            _ensure_str(domain, "silver.domain must be a string when provided")
        entity = data.get("entity", source["table"])
        if data.get("entity") is not None:
            _ensure_str(entity, "silver.entity must be a string when provided")

        version = data.get("version", 1)
        if "version" in data:
            if not isinstance(version, int):
                raise ValueError("silver.version must be an integer")
        load_partition_name = data.get("load_partition_name", "load_date")
        if "load_partition_name" in data:
            _ensure_str(load_partition_name, "silver.load_partition_name must be a string")
        include_pattern_folder = data.get("include_pattern_folder", False)
        if "include_pattern_folder" in data:
            _ensure_bool(include_pattern_folder, "silver.include_pattern_folder must be a boolean")
        require_checksum = data.get("require_checksum", False)
        if "require_checksum" in data:
            _ensure_bool(require_checksum, "silver.require_checksum must be a boolean")

        primary_keys = data.get("primary_keys", [])
        if primary_keys is None:
            primary_keys = []
        if not isinstance(primary_keys, list) or any(not isinstance(pk, str) for pk in primary_keys):
            raise ValueError("silver.primary_keys must be a list of strings")

        order_column = data.get("order_column")
        if order_column is not None and not isinstance(order_column, str):
            raise ValueError("silver.order_column must be a string when provided")

        name_fields = {
            "full_output_name": "full_snapshot",
            "current_output_name": "current",
            "history_output_name": "history",
            "cdc_output_name": "cdc_changes",
        }
        output_names: Dict[str, str] = {}
        for key, fallback in name_fields.items():
            value = data.get(key, fallback)
            if key in data and not isinstance(value, str):
                raise ValueError(f"silver.{key} must be a string when provided")
            output_names[key] = value

        schema = SilverSchema.from_dict(data.get("schema"))
        normalization = SilverNormalization.from_dict(data.get("normalization"))
        error_handling = SilverErrorHandling.from_dict(data.get("error_handling"))
        partitioning = SilverPartitioning.from_dict(data.get("partitioning"))
        model_value = data.get("model")
        if model_value is not None:
            _ensure_str(model_value, "silver.model must be a string")
            model = SilverModel.normalize(model_value)
        else:
            model = SilverModel.default_for_load_pattern(load_pattern)

        if load_pattern == LoadPattern.CURRENT_HISTORY:
            if not primary_keys:
                raise ValueError("silver.primary_keys must be provided when load_pattern='current_history'")
            if not order_column:
                raise ValueError("silver.order_column must be provided when load_pattern='current_history'")

        return cls(
            output_dir=output_dir,
            write_parquet=write_parquet,
            write_csv=write_csv,
            parquet_compression=parquet_compression,
            domain=domain,
            entity=entity,
            version=version,
            load_partition_name=load_partition_name,
            include_pattern_folder=include_pattern_folder,
            require_checksum=require_checksum,
            primary_keys=primary_keys,
            order_column=order_column,
            full_output_name=output_names["full_output_name"],
            current_output_name=output_names["current_output_name"],
            history_output_name=output_names["history_output_name"],
            cdc_output_name=output_names["cdc_output_name"],
            schema=schema,
            normalization=normalization,
            error_handling=error_handling,
            partitioning=partitioning,
            model=model,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "output_dir": self.output_dir,
            "write_parquet": self.write_parquet,
            "write_csv": self.write_csv,
            "parquet_compression": self.parquet_compression,
            "domain": self.domain,
            "entity": self.entity,
            "version": self.version,
            "load_partition_name": self.load_partition_name,
            "include_pattern_folder": self.include_pattern_folder,
            "require_checksum": self.require_checksum,
            "primary_keys": list(self.primary_keys),
            "order_column": self.order_column,
            "full_output_name": self.full_output_name,
            "current_output_name": self.current_output_name,
            "history_output_name": self.history_output_name,
            "cdc_output_name": self.cdc_output_name,
            "schema": self.schema.to_dict(),
            "normalization": self.normalization.to_dict(),
            "error_handling": self.error_handling.to_dict(),
            "partitioning": self.partitioning.to_dict(),
            "model": self.model.value,
        }
