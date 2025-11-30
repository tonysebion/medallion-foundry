"""Typed configuration models using Pydantic for validation.

Initial pass: mirrors existing dict-based structure while providing
field validation. We keep to_dict() helpers for compatibility with
legacy code paths still expecting dictionaries.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)
from enum import Enum

from core.patterns import LoadPattern
from core.silver.models import SilverModel, resolve_profile

from .dataset import DatasetConfig


class StorageBackend(str, Enum):
    s3 = "s3"
    azure = "azure"
    local = "local"


class SourceType(str, Enum):
    api = "api"
    db = "db"
    custom = "custom"
    file = "file"


class BronzeConfig(BaseModel):
    storage_backend: StorageBackend = StorageBackend.s3
    local_path: Optional[str] = None
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    azure_container: Optional[str] = None
    output_defaults: Dict[str, Any] = Field(default_factory=dict)
    partitioning: Dict[str, Any] = Field(default_factory=dict)


class APIConfig(BaseModel):
    base_url: str
    endpoint: str = "/"


class DBConfig(BaseModel):
    conn_str_env: str
    base_query: str


class FileSourceConfig(BaseModel):
    path: str
    format: Optional[str] = None


class CustomExtractorConfig(BaseModel):
    module: str
    class_name: str


class RunConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    load_pattern: LoadPattern = LoadPattern.FULL
    local_output_dir: str = "./output"
    write_csv: bool = True
    write_parquet: bool = False
    parallel_workers: int = 1

    @field_validator("parallel_workers")
    def _validate_parallel_workers(cls, value: int) -> int:
        if value < 1:
            raise ValueError("parallel_workers must be a positive integer")
        return value


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    pattern_id: Optional[str] = (
        None  # Configurable identifier for tracing across layers (e.g., pattern1_full_events, retail_pos_api)
    )
    config_name: Optional[str] = None
    type: SourceType = SourceType.api
    system: str
    table: str
    api: Optional[APIConfig] = None
    db: Optional[DBConfig] = None
    file: Optional[FileSourceConfig] = None
    custom_extractor: Optional[CustomExtractorConfig] = None
    run: RunConfig

    @model_validator(mode="after")
    def validate_required_configs(self):
        if self.type == SourceType.api and self.api is None:
            raise ValueError("source.api required for api type")
        if self.type == SourceType.db and self.db is None:
            raise ValueError("source.db required for db type")
        if self.type == SourceType.file and self.file is None:
            raise ValueError("source.file required for file type")
        return self


class SilverPartitioning(BaseModel):
    columns: List[str] = Field(default_factory=list)

    @field_validator("columns", mode="before")
    @classmethod
    def handle_column_field(cls, v):
        if isinstance(v, str):
            # If "column" was provided instead of "columns"
            return [v]
        elif isinstance(v, list):
            return v
        else:
            return []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "columns": list(self.columns),
            "column": self.columns[0] if self.columns else None,
        }


class SilverErrorHandling(BaseModel):
    enabled: bool = False
    max_bad_records: int = 0
    max_bad_percent: float = 0.0


class SilverNormalization(BaseModel):
    trim_strings: bool = False
    empty_strings_as_null: bool = False

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverNormalization":
        data = raw or {}
        if not isinstance(data, dict):
            raise ValueError("silver.normalization must be a dictionary")
        trim_strings = data.get("trim_strings", False)
        if "trim_strings" in data and not isinstance(trim_strings, bool):
            raise ValueError("silver.normalization.trim_strings must be a boolean")
        empty_strings = data.get("empty_strings_as_null", False)
        if "empty_strings_as_null" in data and not isinstance(empty_strings, bool):
            raise ValueError(
                "silver.normalization.empty_strings_as_null must be a boolean"
            )
        return cls(trim_strings=trim_strings, empty_strings_as_null=empty_strings)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trim_strings": self.trim_strings,
            "empty_strings_as_null": self.empty_strings_as_null,
        }


class SilverSchema(BaseModel):
    rename_map: Dict[str, str] = Field(default_factory=dict)
    column_order: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "SilverSchema":
        data = raw or {}
        column_order = data.get("column_order")
        if column_order is not None:
            if not isinstance(column_order, list) or any(
                not isinstance(col, str) for col in column_order
            ):
                raise ValueError("silver.schema.column_order must be a list of strings")
        rename_map = data.get("rename_map") or {}
        if not isinstance(rename_map, dict) or any(
            not isinstance(k, str) or not isinstance(v, str)
            for k, v in rename_map.items()
        ):
            raise ValueError("silver.schema.rename_map must be a mapping of strings")
        return cls(column_order=column_order, rename_map=rename_map)

    def to_dict(self) -> Dict[str, Any]:
        return {"column_order": self.column_order, "rename_map": dict(self.rename_map)}


class SilverConfig(BaseModel):
    output_dir: str = "./silver_output"
    domain: str = "default"
    entity: str = "dataset"
    version: int = 1
    load_partition_name: str = "load_date"
    include_pattern_folder: bool = False
    require_checksum: StrictBool = False
    write_parquet: bool = True
    write_csv: bool = False
    parquet_compression: str = "snappy"
    primary_keys: List[str] = Field(default_factory=list)
    order_column: Optional[str] = None
    current_output_name: str = "current"
    history_output_name: str = "history"
    cdc_output_name: str = "cdc_changes"
    full_output_name: str = "full_snapshot"
    schema_config: SilverSchema = Field(default_factory=SilverSchema)
    normalization: SilverNormalization = Field(default_factory=SilverNormalization)
    error_handling: SilverErrorHandling = Field(default_factory=SilverErrorHandling)
    partitioning: SilverPartitioning = Field(default_factory=SilverPartitioning)
    model_profile: Optional[str] = None
    model: SilverModel = Field(default_factory=lambda: SilverModel.PERIODIC_SNAPSHOT)

    @classmethod
    def from_raw(
        cls,
        raw: Optional[Dict[str, Any]],
        source: Dict[str, Any],
        load_pattern: LoadPattern,
    ) -> "SilverConfig":
        data = raw.copy() if raw else {}

        # Map legacy field names
        if "schema" in data and "schema_config" not in data:
            data["schema_config"] = data.pop("schema")

        # Set defaults based on source and load_pattern
        data.setdefault("domain", source["system"])
        data.setdefault("entity", source["table"])
        data.setdefault(
            "model", SilverModel.default_for_load_pattern(load_pattern).value
        )

        # Handle model_profile
        profile_value = data.get("model_profile")
        if profile_value:
            profile_model = resolve_profile(profile_value)
            if profile_model:
                data["model"] = profile_model.value

        # Validate primary_keys and order_column for current_history
        if load_pattern == LoadPattern.CURRENT_HISTORY:
            if not data.get("primary_keys"):
                raise ValueError(
                    "silver.primary_keys must be provided when load_pattern='current_history'"
                )
            if not data.get("order_column"):
                raise ValueError(
                    "silver.order_column must be provided when load_pattern='current_history'"
                )

        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()


class PlatformConfig(BaseModel):
    bronze: BronzeConfig
    s3_connection: Optional[Dict[str, Any]] = None
    azure_connection: Optional[Dict[str, Any]] = None


class RootConfig(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        json_encoders={DatasetConfig: lambda value: value},
    )
    config_version: int = 1
    platform: PlatformConfig
    source: SourceConfig
    silver: Optional[SilverConfig] = None

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        extra = self.__pydantic_extra__
        if extra is not None:
            dataset = extra.get("__dataset__")
            if dataset is not None:
                data["__dataset__"] = dataset
        return data


# Factory parsing entry point


def parse_root_config(data: Dict[str, Any]) -> RootConfig:
    # Accept missing version; default to 1 while emitting warning externally.
    return RootConfig(**data)
