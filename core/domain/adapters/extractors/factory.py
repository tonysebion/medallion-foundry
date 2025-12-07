"""Helper for constructing extractor instances."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from pathlib import Path
from typing import Any, Dict, Optional, Type, cast

import core.domain.adapters.extractors as extractors_pkg
from core.infrastructure.io.extractors.base import (
    BaseExtractor,
    EXTRACTOR_REGISTRY,
    get_extractor_class,
)

logger = logging.getLogger(__name__)


def _load_extractors() -> None:
    pkg_path = Path(extractors_pkg.__file__).parent
    for finder, name, _ in pkgutil.iter_modules([str(pkg_path)]):
        if name.startswith("_"):
            continue
        importlib.import_module(f"{extractors_pkg.__name__}.{name}")


_loaded = False


def ensure_extractors_loaded() -> None:
    """Import all extractor modules so they register themselves."""
    global _loaded
    if _loaded:
        return
    _load_extractors()
    _loaded = True


def get_extractor(
    cfg: Dict[str, Any],
    env_config: Optional[Any] = None,
) -> BaseExtractor:
    """Construct an extractor for the provided configuration."""
    ensure_extractors_loaded()

    src = cfg["source"]
    src_type = src.get("type", "api")

    if src_type == "custom":
        custom_cfg = src.get("custom_extractor", {})
        module_name = custom_cfg.get("module")
        class_name = custom_cfg.get("class_name")
        if not module_name or not class_name:
            raise ValueError(
                "custom extractor requires both 'module' and 'class_name' "
                "in source.custom_extractor"
            )

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        cls_typed: Type[BaseExtractor] = cast(Type[BaseExtractor], cls)
        return cls_typed()

    extractor_cls = get_extractor_class(src_type)
    if extractor_cls is None:
        registered = ", ".join(EXTRACTOR_REGISTRY.keys())
        raise ValueError(
            f"Unknown source.type: '{src_type}'. Registered types: {registered or 'none'}"
        )

    if src_type == "db_multi":
        max_workers = src.get("run", {}).get("parallel_workers", 4)
        return extractor_cls(max_workers=max_workers)
    if src_type == "file":
        return extractor_cls(env_config=env_config)

    return extractor_cls()


__all__ = [
    "ensure_extractors_loaded",
    "get_extractor",
]
