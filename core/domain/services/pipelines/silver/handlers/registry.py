"""Handler registry for Silver pattern handlers.

This module provides a registry pattern for Silver entity handlers, matching
the pattern used by extractors (@register_extractor) and storage backends
(@register_backend).

Usage:
    @register_handler(EntityKind.EVENT)
    class EventHandler(BasePatternHandler):
        ...

    # Create handler instance
    handler = create_handler(EntityKind.EVENT, dataset)
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from core.infrastructure.config import DatasetConfig
    from core.foundation.primitives.entity_kinds import EntityKind
    from core.domain.services.pipelines.silver.handlers.base import BasePatternHandler

logger = logging.getLogger(__name__)

# Type alias for handler factory function
HandlerFactory = Callable[["DatasetConfig"], "BasePatternHandler"]

# Registry mapping EntityKind to handler class and optional factory kwargs
# Format: EntityKind -> (HandlerClass, factory_kwargs)
HANDLER_REGISTRY: Dict["EntityKind", Tuple[Type["BasePatternHandler"], Dict[str, bool]]] = {}


def register_handler(
    entity_kind: "EntityKind",
    **factory_kwargs: bool,
) -> Callable[[Type["BasePatternHandler"]], Type["BasePatternHandler"]]:
    """Decorator to register a handler for an entity kind.

    Args:
        entity_kind: The entity kind this handler processes.
        **factory_kwargs: Additional kwargs to pass when creating the handler.
            For example, `derived=True` for StateHandler processing DERIVED_STATE.

    Usage:
        @register_handler(EntityKind.EVENT)
        class EventHandler(BasePatternHandler):
            ...

        @register_handler(EntityKind.DERIVED_STATE, derived=True)
        class StateHandler(BasePatternHandler):
            ...
    """
    def decorator(cls: Type["BasePatternHandler"]) -> Type["BasePatternHandler"]:
        HANDLER_REGISTRY[entity_kind] = (cls, dict(factory_kwargs))
        logger.debug(
            "Registered handler %s for entity kind %s with kwargs %s",
            cls.__name__,
            entity_kind.value,
            factory_kwargs,
        )
        return cls

    return decorator


def get_handler_class(
    entity_kind: "EntityKind",
) -> Tuple[Type["BasePatternHandler"], Dict[str, bool]]:
    """Get the handler class and factory kwargs for an entity kind.

    Args:
        entity_kind: The entity kind to get a handler for.

    Returns:
        Tuple of (handler_class, factory_kwargs).

    Raises:
        ValueError: If no handler is registered for the entity kind.
    """
    if entity_kind not in HANDLER_REGISTRY:
        available = [k.value for k in HANDLER_REGISTRY.keys()]
        raise ValueError(
            f"No handler registered for entity kind '{entity_kind.value}'. "
            f"Available: {available}"
        )
    return HANDLER_REGISTRY[entity_kind]


def create_handler(
    entity_kind: "EntityKind",
    dataset: "DatasetConfig",
    **extra_kwargs: bool,
) -> "BasePatternHandler":
    """Create a handler instance for an entity kind.

    Args:
        entity_kind: The entity kind to create a handler for.
        dataset: Dataset configuration with Silver settings.
        **extra_kwargs: Additional kwargs to override factory defaults.

    Returns:
        Configured handler instance.

    Raises:
        ValueError: If no handler is registered for the entity kind.
    """
    handler_cls, factory_kwargs = get_handler_class(entity_kind)

    # Merge factory kwargs with extra kwargs (extra takes precedence)
    kwargs = {**factory_kwargs, **extra_kwargs}

    return handler_cls(dataset, **kwargs)


def list_handlers() -> list[str]:
    """Return all registered handler entity kinds.

    Returns:
        List of entity kind values (strings).
    """
    return [k.value for k in HANDLER_REGISTRY.keys()]


def ensure_handlers_loaded() -> None:
    """Ensure all handler modules are loaded.

    This function imports the handler modules to trigger decorator execution.
    Called by SilverProcessor to ensure handlers are registered.
    """
    # Import handler modules to trigger @register_handler decorators
    from core.domain.services.pipelines.silver.handlers import (  # noqa: F401
        event_handler,
        state_handler,
        derived_event_handler,
    )
