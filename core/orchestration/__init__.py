"""Execution coordination for bronze-foundry.

This package contains job execution and orchestration:
- runner/: Extract job execution, chunking
- parallel: Parallel extraction support

Import from child packages directly:
    from core.orchestration.runner import run_extract, build_extractor
    from core.orchestration.parallel import run_parallel_extracts
"""

# Expose child packages for attribute access
from . import runner
from . import parallel

__all__ = [
    "runner",
    "parallel",
]
