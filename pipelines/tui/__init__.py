"""prompt_toolkit TUI for pipeline configuration.

This package provides a Terminal User Interface for creating and editing
pipeline YAML configuration files with real-time validation and contextual help.

Usage:
    python -m pipelines.tui           # Create new pipeline
    python -m pipelines.tui config.yaml  # Edit existing pipeline
"""

from __future__ import annotations

__version__ = "0.2.0"

__all__ = [
    "PipelineConfigApp",
    "run_create_wizard",
    "run_editor",
]


def __getattr__(name: str):
    """Lazy import of TUI components."""
    if name == "PipelineConfigApp":
        from pipelines.tui.app import PipelineConfigApp
        return PipelineConfigApp
    if name == "run_create_wizard":
        from pipelines.tui.app import run_create_wizard
        return run_create_wizard
    if name == "run_editor":
        from pipelines.tui.app import run_editor
        return run_editor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
