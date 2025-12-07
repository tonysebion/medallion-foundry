"""Plugin helpers for extractor extensions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ExtractorPlugin:
    """Metadata descriptor for an extractor plugin."""

    name: str
    entry_point: str
    version: Optional[str] = None
    description: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)


PLUGIN_REGISTRY: Dict[str, ExtractorPlugin] = {}


def register_plugin(plugin: ExtractorPlugin) -> None:
    """Register an extractor plugin descriptor."""
    PLUGIN_REGISTRY[plugin.name] = plugin


def list_plugins() -> Dict[str, ExtractorPlugin]:
    """Return registered extractor plugins."""

    return dict(PLUGIN_REGISTRY)
