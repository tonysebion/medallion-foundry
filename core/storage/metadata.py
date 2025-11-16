from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

VALID_BOUNDARIES = {"onprem", "cloud"}
VALID_PROVIDER_TYPES = {
    "s3_local",
    "s3_cloud",
    "azure_blob",
    "azure_adls",
    "gcs_cloud",
    "gcs_onprem",
    "local_generic",
}
VALID_CLOUD_PROVIDERS = {None, "", "azure", "aws", "gcp"}


@dataclass
class StorageMetadata:
    boundary: str
    provider_type: Optional[str] = None
    cloud_provider: Optional[str] = None
