"""Silver pattern handlers for entity-specific processing.

This package contains handlers for each Silver entity pattern:
- EventHandler: Processes EVENT entity types
- StateHandler: Processes STATE and DERIVED_STATE entity types
- DerivedEventHandler: Processes DERIVED_EVENT entity types

Each handler implements the BasePatternHandler interface and registers itself
via the @register_handler decorator.

Usage:
    from core.domain.services.pipelines.silver.handlers import create_handler
    from core.infrastructure.config import EntityKind
    handler = create_handler(EntityKind.EVENT, dataset)
"""

from core.domain.services.pipelines.silver.handlers.base import BasePatternHandler
from core.domain.services.pipelines.silver.handlers.registry import (
    register_handler,
    create_handler,
    get_handler_class,
    list_handlers,
    ensure_handlers_loaded,
    HANDLER_REGISTRY,
)
from core.domain.services.pipelines.silver.handlers.event_handler import EventHandler
from core.domain.services.pipelines.silver.handlers.state_handler import StateHandler
from core.domain.services.pipelines.silver.handlers.derived_event_handler import (
    DerivedEventHandler,
)

__all__ = [
    # Base class
    "BasePatternHandler",
    # Handler classes
    "EventHandler",
    "StateHandler",
    "DerivedEventHandler",
    # Registry functions
    "register_handler",
    "create_handler",
    "get_handler_class",
    "list_handlers",
    "ensure_handlers_loaded",
    "HANDLER_REGISTRY",
]
