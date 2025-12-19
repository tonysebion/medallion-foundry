# DEPRECATED: Use `pipelines/` Instead

**This directory (`core/`) is deprecated and will be removed in a future release.**

All new development should use the `pipelines/` module instead, which provides:
- Simpler, declarative API
- Better documentation
- Full feature parity
- Active maintenance

## Migration Guide

### Old (core/)
```python
from core.domain.adapters.extractors import DatabaseExtractor
from core.foundation.state.watermark import WatermarkManager
```

### New (pipelines/)
```python
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.watermark import get_watermark, save_watermark
```

## Feature Mapping

| core/ module | pipelines/ equivalent |
|--------------|----------------------|
| `core.foundation.state.watermark` | `pipelines.lib.watermark` |
| `core.platform.resilience.retry` | `pipelines.lib.resilience` |
| `core.infrastructure.io.storage` | `pipelines.lib.storage` |
| `core.domain.adapters.quality` | `pipelines.lib.quality` |
| `core.domain.adapters.polybase` | `pipelines.lib.polybase` |
| Database extractors | `BronzeSource` with `SourceType.DATABASE_MSSQL` |
| File extractors | `BronzeSource` with `SourceType.FILE_*` |

## Timeline

- **Now**: `pipelines/` is the recommended approach for all new work
- **Future**: `core/` will be removed once all dependent code is migrated

See [pipelines/QUICKREF.md](../pipelines/QUICKREF.md) for the new API documentation.
