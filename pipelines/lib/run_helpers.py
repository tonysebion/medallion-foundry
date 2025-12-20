"""Shared run helpers for pipeline layers."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Tuple

__all__ = ["maybe_skip_if_exists", "maybe_dry_run"]

ExistsFn = Callable[[], bool]


def maybe_skip_if_exists(
    *,
    skip_if_exists: bool,
    exists_fn: ExistsFn,
    target: str,
    logger: logging.Logger,
    context: Optional[str] = None,
    reason: str = "already_exists",
) -> Optional[Dict[str, Any]]:
    """Return skip metadata when data already exists."""

    if not skip_if_exists:
        return None

    if exists_fn():
        label = context or "pipeline"
        logger.info("Skipping %s - data already exists at %s", label, target)
        return {"skipped": True, "reason": reason, "target": target}

    return None


def maybe_dry_run(
    *,
    dry_run: bool,
    logger: logging.Logger,
    message: str,
    message_args: Tuple[Any, ...] = (),
    message_kwargs: Optional[Dict[str, Any]] = None,
    target: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Return dry-run metadata when dry_run flag is set."""

    if not dry_run:
        return None

    logger.info(message, *message_args, **(message_kwargs or {}))
    result: Dict[str, Any] = {"dry_run": True, "target": target}
    if extra:
        result.update(extra)
    return result
