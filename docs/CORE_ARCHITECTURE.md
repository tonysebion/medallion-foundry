# Core Directory Architecture

This document describes the structure and purpose of each folder in the `core/` directory. The architecture follows a **5-layer design** where each layer can only import from layers below it.

---

## Layer Overview

```
Layer 0 (L0): foundation/     Zero-dependency building blocks
Layer 1 (L1): platform/       Cross-cutting platform services
Layer 2 (L2): infrastructure/ Config, I/O, runtime
Layer 3 (L3): domain/         Business logic, adapters, pipelines
Layer 4 (L4): orchestration/  Job execution
```

**Rule**: Each layer can only import from layers below it. This is enforced by `lint-imports`.

---

## L0: Foundation (`core/foundation/`)

The foundation layer contains the most fundamental components with **no internal dependencies** on other core layers. All other layers may depend on this layer.

### `core/foundation/primitives/`

Base building blocks used throughout the system.

| Module | Purpose |
|--------|---------|
| `base.py` | `RichEnumMixin`, `SerializableMixin` - base classes for enums and serialization |
| `patterns.py` | `LoadPattern` enum: `SNAPSHOT`, `INCREMENTAL_APPEND`, `INCREMENTAL_MERGE` |
| `models.py` | `SilverModel` enum: `SCD_TYPE_1`, `SCD_TYPE_2`, etc. |
| `entity_kinds.py` | `EntityKind`, `HistoryMode`, `InputMode`, `DeleteMode`, `SchemaMode` |
| `exceptions.py` | Domain exception hierarchy (`BronzeFoundryError` and subclasses) |
| `logging.py` | `JSONFormatter`, `setup_logging()` for structured logging |

### `core/foundation/state/`

State management for incremental extraction patterns.

| Module | Purpose |
|--------|---------|
| `storage.py` | `StateStorageBackend` base class for state persistence (local, S3) |
| `watermark.py` | `Watermark`, `WatermarkStore` - track extraction progress via timestamps/cursors |
| `manifest.py` | `FileEntry`, `FileManifest`, `ManifestTracker` - track processed files for file_batch sources |
| `checkpoint.py` | `Checkpoint`, `CheckpointStore` - extraction checkpointing for resumption and conflict detection |

### `core/foundation/catalog/`

Integration hooks for external catalog systems.

| Module | Purpose |
|--------|---------|
| `hooks.py` | Catalog notification hooks: `notify_catalog()`, `report_schema_snapshot()`, `report_lineage()` |
| `webhooks.py` | `fire_webhooks()` - send notifications for lifecycle events |
| `tracing.py` | `trace_span()` - OpenTelemetry tracing wrapper |

---

## L1: Platform (`core/platform/`)

Cross-cutting platform services that build on the foundation layer.

### `core/platform/resilience/`

Reliability and fault-tolerance patterns.

| Module | Purpose |
|--------|---------|
| `retry.py` | `RetryPolicy`, `execute_with_retry()` - exponential backoff with jitter |
| `circuit_breaker.py` | `CircuitBreaker`, `CircuitState` - circuit breaker pattern (open/half-open/closed) |
| `rate_limiter.py` | `RateLimiter` - token bucket rate limiting |
| `error_mapping.py` | Map third-party exceptions to domain exceptions |
| `late_data.py` | `LateDataHandler`, `LateDataConfig` - detect and process late-arriving data |
| `constants.py` | Default configuration values for resilience components |
| `config.py` | `parse_retry_config()`, `resolve_rate_limit_config()` |
| `mixins.py` | `ResilienceMixin` - add retry/circuit breaker to extractors |

### `core/platform/observability/`

Tracing and monitoring utilities.

| Module | Purpose |
|--------|---------|
| `tracing.py` | OpenTelemetry tracing helpers |

### `core/platform/om/`

OpenMetadata integration client.

| Module | Purpose |
|--------|---------|
| `client.py` | `OpenMetadataClient` - interact with OpenMetadata catalog |

---

## L2: Infrastructure (`core/infrastructure/`)

Core infrastructure services for configuration, I/O, and runtime context.

### `core/infrastructure/config/`

Configuration loading, validation, and typed models.

| Module | Purpose |
|--------|---------|
| `loaders.py` | `load_config()`, `load_configs()` - load YAML configurations |
| `validation.py` | `validate_config_dict()` - validate configuration structure |
| `placeholders.py` | `resolve_env_vars()`, `substitute_env_vars()` - environment variable substitution |
| `migration.py` | `legacy_to_dataset()` - migrate legacy config formats |

#### `core/infrastructure/config/models/`

Pydantic models for configuration.

| Module | Purpose |
|--------|---------|
| `root.py` | `RootConfig`, `PlatformConfig`, `SourceConfig`, `SilverConfig` |
| `dataset.py` | `DatasetConfig`, `PathStructure` - dataset-level configuration |
| `intent.py` | `BronzeIntent`, `SilverIntent` - extraction intent configuration |
| `environment.py` | `EnvironmentConfig`, `S3ConnectionConfig` |
| `polybase.py` | `PolybaseSetup`, `PolybaseExternalTable` - PolyBase DDL models |
| `helpers.py` | Validation helpers: `require_list_of_strings()`, `require_bool()` |

### `core/infrastructure/io/`

Storage backends, HTTP clients, and extractor base classes.

#### `core/infrastructure/io/storage/`

Cloud and local storage backends.

| Module | Purpose |
|--------|---------|
| `base.py` | `BaseCloudStorage` abstract class |
| `s3.py` | AWS S3 storage backend |
| `azure.py` | Azure Blob storage backend |
| `local.py` | Local filesystem storage backend |
| `factory.py` | `get_storage_backend()` - factory function |

#### `core/infrastructure/io/http/`

HTTP client utilities.

| Module | Purpose |
|--------|---------|
| `client.py` | `AsyncApiClient` - async HTTP client with retries |
| `auth.py` | Authentication handlers (OAuth, API key, etc.) |

#### `core/infrastructure/io/extractors/`

Base extractor classes and registry.

| Module | Purpose |
|--------|---------|
| `base.py` | `BaseExtractor`, `ExtractionResult` - extractor interface |
| `registry.py` | `register_extractor()`, `get_extractor_class()` - extractor registration |

### `core/infrastructure/runtime/`

Execution context, paths, and metadata.

| Module | Purpose |
|--------|---------|
| `context.py` | `RunContext`, `build_run_context()` - execution context dataclass |
| `options.py` | `RunOptions` - load patterns, output formats, webhooks |
| `metadata_models.py` | `RunMetadata`, `Layer`, `RunStatus`, `QualityRuleResult` |
| `metadata_builder.py` | `build_run_metadata()`, `generate_run_id()`, `write_run_metadata()` |
| `file_io.py` | `DataFrameLoader`, `DataFrameWriter`, `chunk_records()` |

#### `core/infrastructure/runtime/paths/`

Path building utilities for Bronze and Silver layers.

| Module | Purpose |
|--------|---------|
| `bronze.py` | `BronzePartition`, `build_bronze_partition()`, `build_bronze_relative_path()` |
| `silver.py` | `SilverPartition`, `build_silver_partition()`, `build_silver_partition_path()` |

---

## L3: Domain (`core/domain/`)

Business domain logic for data extraction and transformation.

### `core/domain/adapters/`

External system adapters.

#### `core/domain/adapters/extractors/`

Data source extractors (the main business logic for extraction).

| Module | Purpose |
|--------|---------|
| `api_extractor.py` | `ApiExtractor` - extract from REST APIs with pagination |
| `db_extractor.py` | `DbExtractor` - extract from databases via SQL queries |
| `db_multi_extractor.py` | `DbMultiExtractor` - extract from multiple database tables |
| `file_extractor.py` | `FileExtractor` - extract from files (CSV, JSON, Parquet, Excel) |
| `file_batch_extractor.py` | `FileBatchExtractor` - extract from file batches with manifest tracking |
| `factory.py` | `get_extractor()` - factory function to instantiate extractors |
| `resilience.py` | `ResilientExtractorMixin` - add retry/resilience to extractors |
| `db_utils.py` | `is_retryable_db_error()` - shared database retry logic |

#### `core/domain/adapters/polybase/`

PolyBase DDL generation for SQL Server external tables.

| Module | Purpose |
|--------|---------|
| `generator.py` | `generate_polybase_setup()` - generate PolyBase DDL scripts |

#### `core/domain/adapters/schema/`

Schema validation and inference.

| Module | Purpose |
|--------|---------|
| `validator.py` | `SchemaValidator` - validate data against schemas |
| `inference.py` | Schema inference utilities |

#### `core/domain/adapters/quality/`

Data quality rules engine.

| Module | Purpose |
|--------|---------|
| `engine.py` | `evaluate_rules()` - evaluate quality rules against data |
| `rules.py` | Rule definitions and parsing |

### `core/domain/services/`

Pipeline processing services.

#### `core/domain/services/pipelines/bronze/`

Bronze layer extraction and I/O.

| Module | Purpose |
|--------|---------|
| `io.py` | Bronze I/O utilities: read/write Parquet/CSV, merge records |

#### `core/domain/services/pipelines/silver/`

Silver layer transformation and promotion.

| Module | Purpose |
|--------|---------|
| `processor.py` | `SilverProcessor` - main silver transformation orchestrator |
| `io.py` | `WriteConfig`, `write_silver_outputs()` - Silver I/O utilities |
| `writer.py` | `SilverArtifactWriter`, `TransactionalSilverArtifactWriter` |
| `preparation.py` | Data preparation and column validation |

##### `core/domain/services/pipelines/silver/handlers/`

Silver model-specific handlers (SCD types, events, etc.).

| Module | Purpose |
|--------|---------|
| `base.py` | `SilverHandler` base class, column validation utilities |
| `state_handler.py` | `StateHandler` - handle SCD Type 1/2 state tables |
| `event_handler.py` | `EventHandler` - handle event/fact tables |
| `derived_event_handler.py` | `DerivedEventHandler` - handle derived event tables |

### `core/domain/catalog/`

Catalog utilities.

| Module | Purpose |
|--------|---------|
| `yaml_generator.py` | `generate_yaml_skeleton()` - generate config YAML from OpenMetadata |

---

## L4: Orchestration (`core/orchestration/`)

Job execution and coordination.

### `core/orchestration/runner/`

Job execution.

| Module | Purpose |
|--------|---------|
| `job.py` | `ExtractJob`, `run_extract()`, `build_extractor()` - main extraction job execution |

### `core/orchestration/parallel.py`

Parallel execution utilities.

| Module | Purpose |
|--------|---------|
| `parallel.py` | `run_parallel_extracts()` - execute multiple extractions in parallel |

---

## Import Examples

```python
# L0 Foundation
from core.foundation.primitives import LoadPattern, BronzeFoundryError
from core.foundation.state import Watermark, WatermarkStore
from core.foundation.catalog import notify_catalog, fire_webhooks

# L1 Platform
from core.platform.resilience import RetryPolicy, CircuitBreaker, RateLimiter

# L2 Infrastructure
from core.infrastructure.config import load_config, RootConfig
from core.infrastructure.io.storage import get_storage_backend
from core.infrastructure.runtime import RunContext, build_run_context

# L3 Domain
from core.domain.adapters.extractors import get_extractor
from core.domain.services.pipelines.bronze import read_bronze_records
from core.domain.services.pipelines.silver import SilverProcessor

# L4 Orchestration
from core.orchestration.runner import run_extract, ExtractJob
from core.orchestration.parallel import run_parallel_extracts

# Top-level convenience imports
from core import LoadPattern, RunContext, RetryPolicy, CircuitBreaker
```

---

## Visual Dependency Graph

```
                    ┌────────────────────┐
                    │   orchestration/   │  L4
                    │      runner/       │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │      domain/       │  L3
                    │  adapters/         │
                    │  services/         │
                    │  catalog/          │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │  infrastructure/   │  L2
                    │  config/           │
                    │  io/               │
                    │  runtime/          │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │     platform/      │  L1
                    │  resilience/       │
                    │  observability/    │
                    │  om/               │
                    └─────────┬──────────┘
                              │
                    ┌─────────▼──────────┐
                    │    foundation/     │  L0
                    │  primitives/       │
                    │  state/            │
                    │  catalog/          │
                    └────────────────────┘
```
