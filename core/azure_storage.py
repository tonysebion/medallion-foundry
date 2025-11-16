"""Azure Blob Storage / ADLS Gen2 backend for medallion-foundry."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from core.storage import StorageBackend
from core.storage_registry import register_backend

logger = logging.getLogger(__name__)


@register_backend("azure")
class AzureStorage(StorageBackend):
    """Azure Blob Storage / ADLS Gen2 backend."""

    def __init__(self, config: Dict[str, Any]) -> None:
        bronze_cfg = config.get("bronze", {})
        azure_cfg = config.get("azure_connection", {})

        self.container_name = bronze_cfg.get("azure_container")
        if not self.container_name:
            raise ValueError("bronze.azure_container is required for Azure storage")

        self.prefix = bronze_cfg.get("azure_prefix", "").strip("/")
        self._client = self._build_client(azure_cfg)
        self.container: ContainerClient = self._client.get_container_client(self.container_name)

        try:
            self.container.get_container_properties()
        except ResourceNotFoundError:
            logger.info("Creating Azure container %s", self.container_name)
            self.container.create_container()
        except AzureError as exc:
            logger.warning("Unable to verify Azure container %s: %s", self.container_name, exc)

    def _build_client(self, azure_cfg: Dict[str, Any]) -> BlobServiceClient:
        """Build the BlobServiceClient using the first available credential."""
        conn_env = azure_cfg.get("connection_string_env")
        if conn_env:
            conn = os.environ.get(conn_env)
            if conn:
                logger.debug("Azure storage using connection string from %s", conn_env)
                return BlobServiceClient.from_connection_string(conn)

        account_env = azure_cfg.get("account_name_env")
        key_env = azure_cfg.get("account_key_env")
        if account_env and key_env:
            account = os.environ.get(account_env)
            key = os.environ.get(key_env)
            if account and key:
                account_url = f"https://{account}.blob.core.windows.net"
                logger.debug("Azure storage using account key for %s", account_url)
                return BlobServiceClient(account_url=account_url, credential=key)

        logger.debug("Azure storage using DefaultAzureCredential")
        return BlobServiceClient(
            account_url=azure_cfg.get("account_url"),
            credential=DefaultAzureCredential(),
        )

    def _remote_path(self, remote_path: str) -> str:
        clean = remote_path.lstrip("/")
        return f"{self.prefix}/{clean}" if self.prefix else clean

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        blob_path = self._remote_path(remote_path)
        try:
            with open(local_path, "rb") as handle:
                self.container.upload_blob(blob_path, handle, overwrite=True)
            return True
        except AzureError as exc:
            logger.error("Azure upload failed [%s]: %s", blob_path, exc)
            raise

    def download_file(self, remote_path: str, local_path: str) -> bool:
        blob_path = self._remote_path(remote_path)
        try:
            blob = self.container.get_blob_client(blob_path)
            with open(local_path, "wb") as handle:
                stream = blob.download_blob()
                stream.readinto(handle)
            return True
        except AzureError as exc:
            logger.error("Azure download failed [%s]: %s", blob_path, exc)
            raise

    def list_files(self, prefix: str) -> List[str]:
        blob_prefix = self._remote_path(prefix)
        try:
            return [blob.name for blob in self.container.list_blobs(name_starts_with=blob_prefix)]
        except AzureError as exc:
            logger.error("Azure list failed [%s]: %s", blob_prefix, exc)
            raise

    def delete_file(self, remote_path: str) -> bool:
        blob_path = self._remote_path(remote_path)
        try:
            self.container.delete_blob(blob_path)
            return True
        except AzureError as exc:
            logger.error("Azure delete failed [%s]: %s", blob_path, exc)
            raise

    def get_backend_type(self) -> str:
        return "azure"
