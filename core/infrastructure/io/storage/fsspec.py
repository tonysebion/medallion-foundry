"""Filesystem factory for creating fsspec filesystems for different storage backends.

This module provides utilities for creating fsspec filesystem instances
that work with local, S3, and Azure storage backends.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Optional

import fsspec  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from core.infrastructure.config import EnvironmentConfig
    from core.infrastructure.io.storage.uri import StorageURI

logger = logging.getLogger(__name__)


def create_filesystem(
    uri: "StorageURI", env_config: Optional["EnvironmentConfig"] = None
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
        >>> from core.infrastructure.io.storage.uri import StorageURI
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

        logger.debug(
            f"Creating S3 filesystem with endpoint={s3_config.endpoint_url or 'default AWS'}"
        )

        return fsspec.filesystem("s3", **fs_options)

    elif uri.backend == "azure":
        if not env_config or not env_config.azure:
            raise ValueError(
                "Azure URI requires environment configuration with connection settings. "
                "Please provide platform.azure_connection or an environment config file with Azure credentials."
            )
        fs_options = _build_azure_fs_options(env_config.azure)
        logger.debug("Creating Azure filesystem with options: %s", list(fs_options.keys()))
        return fsspec.filesystem("az", **fs_options)

    else:
        raise ValueError(f"Unsupported storage backend: {uri.backend}")


def get_fs_for_path(
    path: str, env_config: Optional["EnvironmentConfig"] = None
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
    from core.infrastructure.io.storage.uri import StorageURI

    uri = StorageURI.parse(path)
    fs = create_filesystem(uri, env_config)
    resolved_path = uri.to_fsspec_path(env_config)

    return fs, resolved_path


def _build_azure_fs_options(azure_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Build fsspec filesystem options for Azure storage from config."""
    fs_options: Dict[str, Any] = {}
    connection_string_env = azure_cfg.get("connection_string_env")
    if connection_string_env:
        connection_string = os.environ.get(connection_string_env)
        if connection_string:
            fs_options["connection_string"] = connection_string
            return fs_options

    account_name_env = azure_cfg.get("account_name_env")
    account_key_env = azure_cfg.get("account_key_env")
    account_name = os.environ.get(account_name_env) if account_name_env else None
    account_key = os.environ.get(account_key_env) if account_key_env else None
    account_url = (
        str(azure_cfg.get("account_url")) if azure_cfg.get("account_url") else None
    )
    if not account_url and account_name:
        account_url = f"https://{account_name}.blob.core.windows.net"

    if account_name and account_key:
        fs_options["account_name"] = account_name
        fs_options["account_key"] = account_key
        fs_options["account_url"] = account_url or f"https://{account_name}.blob.core.windows.net"
        return fs_options

    tenant_id_env = azure_cfg.get("tenant_id_env")
    client_id_env = azure_cfg.get("client_id_env")
    client_secret_env = azure_cfg.get("client_secret_env")
    tenant_id = os.environ.get(tenant_id_env) if tenant_id_env else None
    client_id = os.environ.get(client_id_env) if client_id_env else None
    client_secret = os.environ.get(client_secret_env) if client_secret_env else None

    if tenant_id and client_id and client_secret:
        try:
            from azure.identity import ClientSecretCredential
        except ImportError as exc:
            raise ValueError(
                "Azure identity dependencies are required for service principal authentication. "
                "Install the azure extra (pip install 'medallion-foundry[azure]')"
            ) from exc

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        fs_options["credential"] = credential
        fs_options["account_url"] = account_url or (
            f"https://{account_name}.blob.core.windows.net" if account_name else None
        )
        if not fs_options["account_url"]:
            raise ValueError(
                "For Azure service principal auth you must provide platform.azure.account_url or account_name_env."
            )
        return fs_options

    if azure_cfg.get("use_managed_identity"):
        try:
            from azure.identity import ManagedIdentityCredential
        except ImportError as exc:
            raise ValueError(
                "Azure identity dependencies are required for managed identity authentication. "
                "Install the azure extra (pip install 'medallion-foundry[azure]')"
            ) from exc

        managed_account_url = account_url
        if not managed_account_url and account_name:
            managed_account_url = f"https://{account_name}.blob.core.windows.net"
        if not managed_account_url:
            raise ValueError(
                "Managed identity requires account_url or account_name_env to be defined for Azure storage."
            )
        fs_options["credential"] = ManagedIdentityCredential()
        fs_options["account_url"] = managed_account_url
        return fs_options

    if account_url:
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as exc:
            raise ValueError(
                "Default Azure credential support requires azure-identity. "
                "Install the azure extra (pip install 'medallion-foundry[azure]')"
            ) from exc
        fs_options["credential"] = DefaultAzureCredential()
        fs_options["account_url"] = account_url
        return fs_options

    raise ValueError(
        "Failed to build Azure filesystem options. Please provide one of: "
        "connection_string_env, account_name_env+account_key_env, "
        "tenant_id_env+client_id_env+client_secret_env, or use_managed_identity."
    )
