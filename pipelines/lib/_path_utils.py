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

from pipelines.lib.storage import get_storage

logger = logging.getLogger(__name__)

__all__ = [
    "is_adls_path",
    "is_object_storage_path",
    "is_s3_path",
    "parse_s3_uri",
    "path_has_data",
    "resolve_target_path",
    "storage_path_exists",
]


def is_s3_path(path: str) -> bool:
    """Check if path is an S3 URI.

    Args:
        path: Path to check

    Returns:
        True if path starts with 's3://'
    """
    return path.startswith("s3://")


def is_adls_path(path: str) -> bool:
    """Check if path is an Azure Data Lake Storage URI.

    Supports all Azure Blob/ADLS protocols:
    - abfs:// (Azure Blob Filesystem)
    - abfss:// (Azure Blob Filesystem Secure)
    - wasbs:// (Windows Azure Storage Blob Secure)
    - az:// (Azure shorthand)

    Args:
        path: Path to check

    Returns:
        True if path uses any Azure storage protocol
    """
    return path.startswith(("abfs://", "abfss://", "wasbs://", "az://"))


def is_object_storage_path(path: str) -> bool:
    """Check if a path uses object storage protocols (S3-compatible or Azure Blob).

    This includes both cloud services (AWS S3, Azure Blob) and on-prem
    S3-compatible storage (MinIO, Nutanix Objects, Ceph, etc.).

    Args:
        path: Path to check

    Returns:
        True if path uses S3 or Azure Blob storage protocols
    """
    return is_s3_path(path) or is_adls_path(path)


def parse_s3_uri(path: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and prefix.

    Args:
        path: S3 URI like 's3://bucket/prefix/path'

    Returns:
        Tuple of (bucket, prefix) where prefix excludes leading slash

    Raises:
        ValueError: If path is not an S3 URI

    Example:
        >>> parse_s3_uri("s3://my-bucket/data/files/")
        ('my-bucket', 'data/files/')
        >>> parse_s3_uri("s3://my-bucket")
        ('my-bucket', '')
    """
    if not is_s3_path(path):
        raise ValueError(f"Not an S3 path: {path}")
    parts = path[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix

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
    """Return True if the path contains any files, supporting object storage and local."""
    if is_object_storage_path(path):
        return _s3_has_data(path)
    return _local_has_data(path)


def storage_path_exists(path: str) -> bool:
    """Return True if the storage path or glob matches any files."""
    try:
        if "*" in path or "?" in path:
            base_path = path.split("*")[0].split("?")[0].rstrip("/\\")
            if not base_path:
                base_path = "."
            storage = get_storage(base_path)
            pattern = Path(path).name if "/" in path or "\\" in path else path
            return storage.exists(pattern)

        storage = get_storage(
            str(Path(path).parent) if Path(path).suffix else path
        )
        relative = Path(path).name if Path(path).suffix else ""
        return storage.exists(relative) if relative else True
    except Exception:
        return False
