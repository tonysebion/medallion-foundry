# Core Architecture

The `core` package implements a **5-layer architecture** with explicit folder structure for each layer.

## 5-Layer Structure

```
core/
├── foundation/        # L0: Zero-dependency building blocks
│   ├── primitives/    # patterns, exceptions, logging, base models
│   ├── state/         # watermark, manifest
│   └── catalog/       # hooks, webhooks, tracing
│
├── platform/          # L1: Cross-cutting platform services
│   ├── resilience/    # retry, circuit breaker, rate limiter
│   ├── observability/ # errors, logging, tracing helpers
│   └── om/            # OpenMetadata client
│
├── infrastructure/    # L2: Core infrastructure services
│   ├── config/        # configuration loading, validation, models
│   ├── io/            # storage backends, HTTP, extractor base
│   └── runtime/       # execution context, paths, metadata
│
├── domain/            # L3: Business domain logic
│   ├── adapters/      # extractors (API, DB, file), quality, schema
│   ├── services/      # pipeline processing (bronze, silver)
│   └── catalog/       # yaml_generator
│
├── orchestration/     # L4: Job execution and coordination
│   ├── runner/        # job execution
│   └── parallel.py    # parallel execution
│
└── _compat/           # Backward compatibility shims (deprecated paths)
```

## Layer Rules

1. **Each layer can ONLY import from layers below it**
   - L0 (foundation) has NO internal dependencies
   - L1 (platform) can import from L0
   - L2 (infrastructure) can import from L0, L1
   - L3 (domain) can import from L0, L1, L2
   - L4 (orchestration) can import from L0, L1, L2, L3

2. **Layer boundaries are enforced by `import-linter`**
   - Run `lint-imports` to check for violations
   - Configured in `pyproject.toml`

## Migration Guide

Old imports continue to work but emit deprecation warnings:

| Old Import | New Canonical Import |
|-----------|---------------------|
| `from core.primitives import X` | `from core.foundation.primitives import X` |
| `from core.primitives.state import X` | `from core.foundation.state import X` |
| `from core.primitives.catalog import X` | `from core.foundation.catalog import X` |
| `from core.resilience import X` | `from core.platform.resilience import X` |
| `from core.observability import X` | `from core.platform.observability import X` |
| `from core.om import X` | `from core.platform.om import X` |
| `from core.config import X` | `from core.infrastructure.config import X` |
| `from core.io import X` | `from core.infrastructure.io import X` |
| `from core.runtime import X` | `from core.infrastructure.runtime import X` |
| `from core.storage import X` | `from core.infrastructure.io.storage import X` |
| `from core.adapters import X` | `from core.domain.adapters import X` |
| `from core.services import X` | `from core.domain.services import X` |

**Note:** The root `from core import X` API remains unchanged and does not emit warnings.

## Recommended Import Patterns

```python
# Convenience imports from root (stable API)
from core import LoadPattern, RunContext, RetryPolicy

# Direct imports from canonical locations
from core.foundation.primitives import LoadPattern, BronzeFoundryError
from core.platform.resilience import CircuitBreaker, RateLimiter
from core.infrastructure.config import load_config, RootConfig
from core.infrastructure.io.storage import get_storage_backend
from core.domain.adapters.extractors import get_extractor
```

## Bronze ↔ Silver Symmetry

The Bronze and Silver pipeline packages follow similar idioms:

- Bronze: `core/domain/services/pipelines/bronze/` - schema inference, metadata emission, chunk writing
- Silver: `core/domain/services/pipelines/silver/` - transformation, model application, promotion

When adding new helpers, consider whether a symmetric counterpart is needed in the other layer.
