"""Typed configuration models using Pydantic for validation.

Initial pass: mirrors existing dict-based structure while providing
field validation. We keep to_dict() helpers for compatibility with
legacy code paths still expecting dictionaries.
"""
from __future__ import annotations

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from enum import Enum

class StorageBackend(str, Enum):
    s3 = "s3"
    azure = "azure"
    local = "local"

class SourceType(str, Enum):
    api = "api"
    db = "db"
    custom = "custom"
    file = "file"

class LoadPattern(str, Enum):
    full = "full"
    cdc = "cdc"
    current_history = "current_history"

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
    load_pattern: LoadPattern = LoadPattern.full
    local_output_dir: str = "./output"
    write_csv: bool = True
    write_parquet: bool = False

class SourceConfig(BaseModel):
    type: SourceType = SourceType.api
    system: str
    table: str
    api: Optional[APIConfig] = None
    db: Optional[DBConfig] = None
    file: Optional[FileSourceConfig] = None
    custom_extractor: Optional[CustomExtractorConfig] = None
    run: RunConfig

    @validator("api", always=True)
    def validate_api(cls, v, values):
        if values.get("type") == SourceType.api and v is None:
            raise ValueError("source.api required for api type")
        return v

    @validator("db", always=True)
    def validate_db(cls, v, values):
        if values.get("type") == SourceType.db and v is None:
            raise ValueError("source.db required for db type")
        return v

    @validator("file", always=True)
    def validate_file(cls, v, values):
        if values.get("type") == SourceType.file and v is None:
            raise ValueError("source.file required for file type")
        return v

class SilverPartitioning(BaseModel):
    columns: List[str] = Field(default_factory=list)

class SilverErrorHandling(BaseModel):
    enabled: bool = False
    max_bad_records: int = 0
    max_bad_percent: float = 0.0

class SilverNormalization(BaseModel):
    trim_strings: bool = False
    empty_strings_as_null: bool = False

class SilverSchema(BaseModel):
    rename_map: Dict[str, str] = Field(default_factory=dict)
    column_order: Optional[List[str]] = None

class SilverConfig(BaseModel):
    output_dir: str = "./silver_output"
    domain: str = "default"
    entity: str = "dataset"
    version: int = 1
    load_partition_name: str = "load_date"
    include_pattern_folder: bool = False
    write_parquet: bool = True
    write_csv: bool = False
    parquet_compression: str = "snappy"
    primary_keys: List[str] = Field(default_factory=list)
    order_column: Optional[str] = None
    current_output_name: str = "current"
    history_output_name: str = "history"
    cdc_output_name: str = "cdc_changes"
    full_output_name: str = "full_snapshot"
    schema: SilverSchema = Field(default_factory=SilverSchema)
    normalization: SilverNormalization = Field(default_factory=SilverNormalization)
    error_handling: SilverErrorHandling = Field(default_factory=SilverErrorHandling)
    partitioning: SilverPartitioning = Field(default_factory=SilverPartitioning)

class PlatformConfig(BaseModel):
    bronze: BronzeConfig
    s3_connection: Optional[Dict[str, Any]] = None
    azure_connection: Optional[Dict[str, Any]] = None

class RootConfig(BaseModel):
    config_version: int = 1
    platform: PlatformConfig
    source: SourceConfig
    silver: Optional[SilverConfig] = None

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

# Factory parsing entry point

def parse_root_config(data: Dict[str, Any]) -> RootConfig:
    # Accept missing version; default to 1 while emitting warning externally.
    return RootConfig(**data)
