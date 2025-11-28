"""Validate storage metadata & on-prem scope enforcement."""

from __future__ import annotations

import pytest

from core.storage.policy import validate_storage_metadata, enforce_storage_scope


def _platform(metadata: dict | None = None, backend: str = "s3") -> dict[str, object]:
    cfg = {"bronze": {"storage_backend": backend}}
    if metadata is not None:
        cfg["bronze"]["storage_metadata"] = metadata
    return cfg


def test_validate_storage_metadata_rejects_unknown_boundary():
    with pytest.raises(ValueError):
        validate_storage_metadata(_platform(metadata={"boundary": "unknown"}))


def test_enforce_storage_scope_onprem_requires_metadata():
    with pytest.raises(ValueError):
        enforce_storage_scope(_platform(), "onprem")


def test_enforce_storage_scope_onprem_blocks_cloud_provider():
    metadata = {
        "boundary": "onprem",
        "provider_type": "s3_cloud",
        "cloud_provider": None,
    }
    with pytest.raises(ValueError):
        enforce_storage_scope(_platform(metadata=metadata), "onprem")


def test_enforce_storage_scope_onprem_disallows_azure_backend():
    metadata = {"boundary": "onprem", "provider_type": "s3_local"}
    with pytest.raises(ValueError):
        enforce_storage_scope(_platform(metadata=metadata, backend="azure"), "onprem")


def test_enforce_storage_scope_onprem_success():
    metadata = {"boundary": "onprem", "provider_type": "s3_local"}
    enforce_storage_scope(_platform(metadata=metadata), "onprem")
