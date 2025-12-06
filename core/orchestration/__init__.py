"""Execution coordination for bronze-foundry.

This package contains job execution and orchestration:
- runner/: Extract job execution, chunking
- parallel: Parallel extraction support
"""

from .runner import build_extractor, ExtractJob, run_extract
from .parallel import run_parallel_extracts

__all__ = [
    # Runner
    "build_extractor",
    "ExtractJob",
    "run_extract",
    # Parallel
    "run_parallel_extracts",
]
