from __future__ import annotations

from typing import Dict, Any, Union
from core.config.typed_models import RootConfig
import logging

from core.storage.plugin_manager import get_backend_factory, resolve_backend_type

import core.storage.plugin_factories  # noqa: F401 register built-in backends

logger = logging.getLogger(__name__)


class StorageBackend:
    """Abstract base class for storage backends."""

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        raise NotImplementedError

    def download_file(self, remote_path: str, local_path: str) -> bool:
        raise NotImplementedError

    def list_files(self, prefix: str) -> list[str]:
        raise NotImplementedError

    def delete_file(self, remote_path: str) -> bool:
        raise NotImplementedError

    def get_backend_type(self) -> str:
        raise NotImplementedError


_STORAGE_BACKEND_CACHE: Dict[int, StorageBackend] = {}


def _cache_key(config: Dict[str, Any]) -> int:
    return id(config)


def get_storage_backend(
    config: Union[Dict[str, Any], RootConfig], use_cache: bool = True
) -> StorageBackend:
    # normalize typed config into dict for caching key + factory
    cfg_dict = config.model_dump() if hasattr(config, "model_dump") else config
    cache_key = _cache_key(cfg_dict)
    if use_cache and cache_key in _STORAGE_BACKEND_CACHE:
        return _STORAGE_BACKEND_CACHE[cache_key]

    backend_type = resolve_backend_type(cfg_dict)
    factory = get_backend_factory(backend_type)
    backend = factory(cfg_dict)

    if use_cache:
        _STORAGE_BACKEND_CACHE[cache_key] = backend
    return backend
