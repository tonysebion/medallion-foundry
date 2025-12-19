"""Internal helpers for resolving targets and checking whether data exists.

These helpers are intentionally small so that classes across Bronze/Api/Silver
can share the same path-resolution and existence checks without duplicating
the S3-vs-local branching logic.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

__all__ = ["resolve_target_path", "path_has_data"]


def resolve_target_path(
    template: str,
    *,
    target_override: Optional[str] = None,
    env_var: Optional[str] = None,
    format_vars: Optional[Dict[str, Any]] = None,
) -> str:
    """Resolve a templated target path with overrides/env vars."""
    base = (
        target_override
        or (os.environ.get(env_var) if env_var else None)
        or template
    )
    format_vars = format_vars or {}
    try:
        return base.format(**format_vars)
    except KeyError as exc:
        raise ValueError(
            f"Target path '{base}' is missing formatting key: {exc}"
        ) from exc


def _s3_has_data(path: str) -> bool:
    try:
        import fsspec

        fs = fsspec.filesystem("s3")
        exists = fs.exists(path)
        if not exists:
            return False
        entries = fs.ls(path)
        return len(entries) > 0
    except ImportError:  # pragma: no cover - optional dependency
        logger.warning(
            "fsspec not installed; cannot check S3 path %s for existing data",
            path,
        )
        return False
    except Exception as exc:
        logger.warning("Failed to inspect S3 path %s: %s", path, exc)
        return False


def _local_has_data(path: str) -> bool:
    try:
        dir_path = Path(path)
        return dir_path.exists() and any(dir_path.iterdir())
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to inspect local path %s: %s", path, exc)
        return False


def path_has_data(path: str) -> bool:
    """Return True if the path contains any files, supporting S3 and local."""
    if path.startswith("s3://"):
        return _s3_has_data(path)
    return _local_has_data(path)
