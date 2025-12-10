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
- Dataclasses like `StoragePlan` now live in `core.infrastructure.io.storage.plan` so storage policies stay in L2

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

---

## Structural Consistency Patterns

The codebase enforces consistent patterns across all layers. These patterns are tested automatically in `tests/test_pattern_consistency.py`.

### Pattern 1: Rich Enums

All enums should follow the `RichEnumMixin` pattern with these methods:

```python
from enum import Enum
from core.foundation.primitives.base import RichEnumMixin

# Module-level constants (not class attributes due to Enum metaclass)
_MY_ENUM_DESCRIPTIONS = {
    "value_a": "Description for value A",
    "value_b": "Description for value B",
}

class MyEnum(RichEnumMixin, str, Enum):
    VALUE_A = "value_a"
    VALUE_B = "value_b"

    @classmethod
    def choices(cls) -> List[str]:
        """Return list of valid enum values."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, raw: str | None) -> "MyEnum":
        """Parse string to enum with case-insensitivity and alias support."""
        if isinstance(raw, cls):
            return raw
        if raw is None:
            return cls.VALUE_A  # or raise ValueError

        candidate = raw.strip().lower()
        for member in cls:
            if member.value == candidate:
                return member

        raise ValueError(f"Invalid MyEnum '{raw}'")

    def describe(self) -> str:
        """Return human-readable description."""
        return _MY_ENUM_DESCRIPTIONS.get(self.value, self.value)
```

**Enums following this pattern:**
- `LoadPattern`, `SilverModel` (primitives)
- `WatermarkType` (state)
- `StorageBackend`, `SourceType`, `DataClassification` (infrastructure)
- `Layer`, `RunStatus` (pipeline)

### Pattern 2: Serializable Dataclasses

All key dataclasses should have `to_dict()` and `from_dict()` methods:

```python
from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class MyConfig:
    name: str
    count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "count": self.count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MyConfig":
        """Create instance from dictionary."""
        return cls(
            name=data["name"],
            count=data.get("count", 0),
        )
```

**Dataclasses following this pattern:**
- `Watermark`, `FileEntry`, `FileManifest` (state)
- `RunContext` (pipeline)
- `RetryPolicy`, `CircuitBreaker` (infrastructure)

### Pattern 3: Extractor Registry

New extractors should register themselves using the `@register_extractor` decorator:

```python
from core.io.extractors.base import BaseExtractor, register_extractor

@register_extractor("my_type")
class MyExtractor(BaseExtractor):
    def fetch_records(self, cfg, run_date):
        # Implementation
        pass
```

The registry enables dynamic extractor lookup:

```python
from core.io.extractors.base import (
    get_extractor_class,
    list_extractor_types,
    EXTRACTOR_REGISTRY,
)

# Get extractor class
cls = get_extractor_class("api")  # Returns ApiExtractor class

# List registered types
types = list_extractor_types()  # ["api", "db", "db_multi", "file"]
```

**Registered extractors:**
- `api` → `ApiExtractor`
- `db` → `DbExtractor`
- `db_multi` → `DbMultiExtractor`
- `file` → `FileExtractor`

### Pattern 4: Explicit `__init__.py` Exports

All `__init__.py` files should have explicit `__all__` lists:

```python
# Good
from .module import Class1, Class2, function1

__all__ = [
    "Class1",
    "Class2",
    "function1",
]

# Bad - using __getattr__ for lazy loading
def __getattr__(name):
    if name == "module":
        from . import module
        return module
    raise AttributeError(name)
```

Use lazy imports inside functions when needed, not in `__init__.py`.

---

## Testing Pattern Enforcement

Pattern consistency is enforced by tests in `tests/test_pattern_consistency.py`:

- **TestRichEnumPattern**: Ensures key enums have `choices()`, `normalize()`, `describe()`
- **TestSerializableDataclassPattern**: Ensures key dataclasses have `to_dict()`, `from_dict()`
- **TestExtractorRegistryPattern**: Ensures extractors are registered and subclass `BaseExtractor`
- **TestInitExportPattern**: Ensures `__init__.py` files have explicit `__all__` and no `__getattr__`

Run these tests with:

```bash
python -m pytest tests/test_pattern_consistency.py -v
```
