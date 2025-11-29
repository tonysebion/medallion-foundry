"""Filesystem factory for creating fsspec filesystems for different storage backends.

This module provides utilities for creating fsspec filesystem instances
that work with local, S3, and Azure storage backends.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

import fsspec

if TYPE_CHECKING:
    from core.config.environment import EnvironmentConfig
    from core.storage.uri import StorageURI

logger = logging.getLogger(__name__)


def create_filesystem(
    uri: "StorageURI",
    env_config: Optional["EnvironmentConfig"] = None
) -> fsspec.AbstractFileSystem:
    """Create an fsspec filesystem for the given URI.

    Args:
        uri: Parsed storage URI
        env_config: Optional environment configuration for credentials

    Returns:
        fsspec filesystem instance

    Raises:
        ValueError: If S3/Azure URI is used without environment config
        NotImplementedError: If Azure backend is requested (not yet implemented)

    Example:
        >>> from core.storage.uri import StorageURI
        >>> uri = StorageURI.parse("./local/file.csv")
        >>> fs = create_filesystem(uri)
        >>> isinstance(fs, fsspec.AbstractFileSystem)
        True

        >>> # For S3, requires environment config
        >>> uri = StorageURI.parse("s3://bucket/file.csv")
        >>> fs = create_filesystem(uri, env_config)
    """
    if uri.backend == "local":
        return fsspec.filesystem("file")

    elif uri.backend == "s3":
        if not env_config or not env_config.s3:
            raise ValueError(
                f"S3 URI '{uri.original}' requires environment config with S3 settings. "
                "Please provide an environment config file with S3 credentials."
            )

        s3_config = env_config.s3

        # Build S3 filesystem options
        fs_options: Dict[str, Any] = {
            "anon": False,  # Require authentication
        }

        # Client kwargs for boto3
        client_kwargs: Dict[str, Any] = {}

        if s3_config.endpoint_url:
            client_kwargs["endpoint_url"] = s3_config.endpoint_url

        if s3_config.region:
            client_kwargs["region_name"] = s3_config.region

        if client_kwargs:
            fs_options["client_kwargs"] = client_kwargs

        # Credentials
        if s3_config.access_key_id and s3_config.secret_access_key:
            fs_options["key"] = s3_config.access_key_id
            fs_options["secret"] = s3_config.secret_access_key
        else:
            logger.info(
                "No explicit S3 credentials found in environment config. "
                "Will attempt to use default AWS credential chain "
                "(environment variables, ~/.aws/credentials, IAM roles)."
            )

        logger.debug(f"Creating S3 filesystem with endpoint={s3_config.endpoint_url or 'default AWS'}")

        return fsspec.filesystem("s3", **fs_options)

    elif uri.backend == "azure":
        # Future implementation
        raise NotImplementedError(
            "Azure Blob Storage support is not yet implemented. "
            "Please use local filesystem or S3 storage."
        )

    else:
        raise ValueError(f"Unsupported storage backend: {uri.backend}")


def get_fs_for_path(
    path: str,
    env_config: Optional["EnvironmentConfig"] = None
) -> tuple[fsspec.AbstractFileSystem, str]:
    """Convenience function to get filesystem and resolved path from a path string.

    Args:
        path: Path string (local or URI)
        env_config: Optional environment configuration

    Returns:
        Tuple of (filesystem, resolved_path)

    Example:
        >>> fs, resolved = get_fs_for_path("./local/file.csv")
        >>> fs, resolved = get_fs_for_path("s3://bucket/file.csv", env_config)
    """
    from core.storage.uri import StorageURI

    uri = StorageURI.parse(path)
    fs = create_filesystem(uri, env_config)
    resolved_path = uri.to_fsspec_path(env_config)

    return fs, resolved_path
