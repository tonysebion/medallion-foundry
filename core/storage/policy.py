"""Storage policy helpers enforcing metadata + on-prem scope."""

from __future__ import annotations

from typing import Dict, Any

from .metadata import (
    VALID_BOUNDARIES,
    VALID_PROVIDER_TYPES,
    VALID_CLOUD_PROVIDERS,
)


def validate_storage_metadata(platform_cfg: Dict[str, Any]) -> None:
    bronze_cfg = platform_cfg.get("bronze", {})
    metadata = bronze_cfg.get("storage_metadata")
    if not metadata:
        return

    boundary = metadata.get("boundary")
    if boundary not in VALID_BOUNDARIES:
        raise ValueError(f"storage_metadata.boundary must be one of {VALID_BOUNDARIES}")

    provider_type = metadata.get("provider_type")
    if provider_type and provider_type not in VALID_PROVIDER_TYPES:
        raise ValueError(
            f"storage_metadata.provider_type must be one of {VALID_PROVIDER_TYPES}"
        )

    cloud_provider = metadata.get("cloud_provider")
    if cloud_provider not in VALID_CLOUD_PROVIDERS:
        raise ValueError(
            "storage_metadata.cloud_provider must be one of "
            f"{[p for p in VALID_CLOUD_PROVIDERS if p]}"
        )


def enforce_storage_scope(platform_cfg: Dict[str, Any], scope: str | None) -> None:
    if not scope or scope == "any":
        return

    if scope not in {"onprem", "any"}:
        raise ValueError("storage_scope must be one of 'any' or 'onprem'")

    bronze_cfg = platform_cfg.get("bronze", {})
    metadata = bronze_cfg.get("storage_metadata") or {}
    backend = bronze_cfg.get("storage_backend", "s3").lower()

    if backend == "azure":
        raise ValueError("Azure storage is not allowed when storage_scope='onprem'")

    boundary = metadata.get("boundary")
    if boundary != "onprem":
        raise ValueError(
            "storage_scope='onprem' requires storage_metadata.boundary='onprem'"
        )

    provider_type = metadata.get("provider_type", "")
    if "_cloud" in provider_type:
        raise ValueError("On-prem storage cannot use a cloud provider_type")

    cloud_provider = metadata.get("cloud_provider")
    if cloud_provider:
        raise ValueError(
            "storage_scope='onprem' requires storage_metadata.cloud_provider to be null"
        )
