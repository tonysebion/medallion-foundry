"""Bronze -> Silver data pipeline for bronze-foundry.

This package contains the medallion pipeline layers (siblings):
- runtime/: RunContext, RunOptions, paths, metadata
- bronze/: Bronze extraction I/O and chunking
- silver/: Silver transformation models and artifacts

Import from child packages directly:
    from core.pipeline.runtime import RunContext, build_run_context
    from core.pipeline.bronze.io import write_csv_chunk
    from core.pipeline.silver import SilverModel
"""

# Expose child packages for attribute access
from . import runtime
from . import bronze
from . import silver

__all__ = [
    "runtime",
    "bronze",
    "silver",
]
