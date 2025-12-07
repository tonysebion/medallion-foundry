"""Tests for the storage filesystem factory covering Azure support."""

import os

import pytest

from core.runtime.config import EnvironmentConfig
from core.storage import create_filesystem, StorageURI


def test_create_filesystem_requires_azure_config():
    uri = StorageURI.parse("az://container/blob")
    with pytest.raises(ValueError, match="Azure URI requires environment"):
        create_filesystem(uri)


def test_create_filesystem_uses_connection_string(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key")
    env = EnvironmentConfig(name="dev", azure={"connection_string_env": "AZURE_STORAGE_CONNECTION_STRING"})

    called = {}

    def fake_fs(protocol, **options):
        called["protocol"] = protocol
        called["options"] = options
        return "azurefs"

    monkeypatch.setattr("core.io.storage.filesystem.fsspec.filesystem", fake_fs)

    uri = StorageURI.parse("az://container/blob")
    fs = create_filesystem(uri, env_config=env)

    assert fs == "azurefs"
    assert called["protocol"] == "az"
    assert called["options"]["connection_string"] == os.environ["AZURE_STORAGE_CONNECTION_STRING"]


def test_create_filesystem_uses_account_key(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "customaccount")
    monkeypatch.setenv("AZURE_STORAGE_KEY", "secret-key")
    env = EnvironmentConfig(
        name="prod",
        azure={
            "account_name_env": "AZURE_STORAGE_ACCOUNT",
            "account_key_env": "AZURE_STORAGE_KEY",
        },
    )

    called = {}

    def fake_fs(protocol, **options):
        called["protocol"] = protocol
        called["options"] = options
        return "azurefs-key"

    monkeypatch.setattr("core.io.storage.filesystem.fsspec.filesystem", fake_fs)

    fs = create_filesystem(StorageURI.parse("az://container/blob"), env_config=env)

    assert fs == "azurefs-key"
    assert called["protocol"] == "az"
    assert called["options"]["account_name"] == "customaccount"
    assert called["options"]["account_key"] == "secret-key"
    assert called["options"]["account_url"].startswith("https://customaccount")
