"""Bronze -> Silver data pipeline for bronze-foundry.

This package contains the medallion pipeline layers (siblings):
- runtime/: Legacy shim (now implemented in core.runtime)
- bronze/: Bronze extraction I/O and chunking
- silver/: Silver transformation models and artifacts

Import from child packages directly:
    from core.runtime import RunContext, build_run_context
    from core.services.pipelines.bronze.io import write_csv_chunk
    from core.services.pipelines.silver import SilverModel
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
