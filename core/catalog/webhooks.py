"""
Utility helpers for sending lifecycle webhooks (success/failure notifications).
"""

from __future__ import annotations

import json
import logging
from typing import Iterable, Mapping, Any
from urllib import request, error

logger = logging.getLogger(__name__)


def fire_webhooks(
    urls: Iterable[str] | None,
    payload: Mapping[str, Any],
    timeout: float = 5.0,
) -> None:
    """
    Send the provided payload (JSON) to each URL. Failures are logged but do not raise.
    """
    if not urls:
        return

    body = json.dumps(payload).encode("utf-8")
    for url in urls:
        if not url:
            continue
        req = request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with request.urlopen(req, timeout=timeout):
                logger.debug("Webhook POST succeeded for %s", url)
        except error.URLError as exc:
            logger.warning("Webhook POST failed for %s: %s", url, exc)
