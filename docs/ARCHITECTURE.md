# Core Architecture

This document describes the layered architecture of the `core/` package and the rules for maintaining architectural integrity.

## Layer Hierarchy

The `core/` package is organized into five domain layers, each with specific responsibilities and import restrictions:

```
core/
├── primitives/              # Layer 0: Zero-dependency building blocks
│   ├── foundations/         # patterns, exceptions, logging, models
│   ├── state/               # watermark, manifest
│   └── catalog/             # hooks, webhooks, tracing
│
├── infrastructure/          # Layer 1: Cross-cutting concerns
│   ├── resilience/          # retry, circuit breaker, late_data
│   ├── storage/             # S3, Azure, local backends
│   └── config/              # configuration loading
│
├── pipeline/                # Layer 2: Bronze → Silver data flow
│   ├── bronze/              # extraction I/O, chunking, storage plan
│   ├── silver/              # transforms, models, artifacts
│   └── runtime/             # RunContext, RunOptions, paths
│
├── adapters/                # Layer 3: External system integrations
│   ├── extractors/          # API, DB, file extractors
│   ├── polybase/            # DDL generation
│   ├── schema/              # schema validation
│   └── quality/             # quality rules
│
└── orchestration/           # Layer 4: Execution coordination
    ├── runner/              # job execution
    └── parallel.py          # parallel execution
```

## Import Rules

Each layer can only import from layers with lower numbers:

| Layer | Can Import From |
|-------|-----------------|
| Layer 0: primitives | Nothing (no core/ dependencies) |
| Layer 1: infrastructure | primitives |
| Layer 2: pipeline | primitives, infrastructure |
| Layer 3: adapters | primitives, infrastructure, pipeline |
| Layer 4: orchestration | primitives, infrastructure, pipeline, adapters |

**Key Rule**: Lower layers MUST NOT import from higher layers.

## Enforcement

Layer import rules are enforced by automated tests in `tests/test_layer_imports.py`. These tests run with every `pytest` execution and will fail if any file violates the import hierarchy.

### What the Tests Check

1. **No layer violations**: Every Python file in `core/` is scanned for imports that violate the hierarchy
2. **Hierarchy completeness**: All five layers are defined
3. **Hierarchy consistency**: The allowed import rules are consistent with layer levels
4. **Core directory exists**: Sanity check that the test can find files

### Handling Cross-Layer Dependencies

When you need functionality from a higher layer, use one of these patterns:

#### 1. Lazy Import with importlib

For runtime-only dependencies that don't need static type checking:

```python
def get_schema_spec():
    import importlib
    schema_types = importlib.import_module("core.adapters.schema.types")
    return schema_types.SchemaSpec
```

#### 2. TYPE_CHECKING Guard

For type annotations that don't need runtime import:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.higher_layer.module import SomeType

def my_function(arg: "SomeType") -> None:
    # Runtime code doesn't import SomeType
    pass
```

#### 3. Move Common Types Down

If a type is needed by multiple layers, consider moving it to a lower layer:

- `SilverModel` enum lives in `primitives/foundations/models.py` (not pipeline)
- `LoadPattern` enum lives in `primitives/foundations/patterns.py`
- Dataclasses like `StoragePlan` live in `pipeline/bronze/plan.py` (not orchestration)

## Layer Responsibilities

### Layer 0: primitives/

Zero-dependency building blocks that define core abstractions:

- **foundations/**: Base enums (`LoadPattern`, `SilverModel`), exceptions, logging setup
- **state/**: Watermark tracking, file manifests
- **catalog/**: Hooks for catalog integration, webhooks, tracing spans

### Layer 1: infrastructure/

Cross-cutting concerns that provide services to higher layers:

- **resilience/**: Retry policies, circuit breakers, late data handling
- **storage/**: Abstract storage backends (S3, Azure, local)
- **config/**: Configuration loading and validation

### Layer 2: pipeline/

The core data transformation logic for Bronze → Silver:

- **bronze/**: I/O utilities, chunking, storage planning
- **silver/**: Transform models, artifact generation
- **runtime/**: Run context, options, path builders

### Layer 3: adapters/

Integrations with external systems:

- **extractors/**: API, database, file extractors
- **polybase/**: SQL Server external table DDL
- **schema/**: Schema validation
- **quality/**: Data quality rules

### Layer 4: orchestration/

Job execution and coordination:

- **runner/**: ExtractJob, chunk processing
- **parallel.py**: Parallel execution utilities

## Backward Compatibility

The root `core/__init__.py` re-exports common symbols for backward compatibility. Code outside `core/` can import directly from `core`:

```python
from core import LoadPattern, SilverModel, RunContext
```

These re-exports maintain API stability while the internal structure evolves.
