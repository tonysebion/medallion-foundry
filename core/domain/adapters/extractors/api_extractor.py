"""API-based extraction with authentication and pagination support per spec Section 3.

Supports api source type with:
- Multiple authentication methods (bearer, api_key, basic, none)
- Pagination (offset, page, cursor, none)
- Rate limiting and retry logic
- Async HTTP support with connection pooling
- Sync HTTP with configurable connection pool
- Watermark-based incremental extraction
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import date

import requests
from requests.adapters import HTTPAdapter

from core.platform.resilience import (
    RetryPolicy,
    execute_with_retry,
    CircuitBreaker,
    RateLimiter,
)
from core.primitives.catalog.tracing import trace_span
from core.infrastructure.io.http.auth import build_api_auth
from core.infrastructure.io.http.session import AsyncApiClient, HttpPoolConfig, is_async_enabled

from core.infrastructure.io.extractors.base import BaseExtractor, register_extractor
from core.domain.adapters.extractors.pagination import (
    PagePaginationState,
    build_pagination_state,
)


@dataclass
class SyncPoolConfig:
    """Configuration for synchronous HTTP connection pooling.

    These settings configure the requests.Session HTTPAdapter for
    connection reuse across API requests.

    Attributes:
        pool_connections: Number of urllib3 connection pools to cache
        pool_maxsize: Maximum connections per pool (per host)
        pool_block: Block when pool is full (vs raise error)
        max_retries: Maximum retries for connection-level errors (0 to disable)
    """

    pool_connections: int = 10
    pool_maxsize: int = 10
    pool_block: bool = False
    max_retries: int = 0

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "SyncPoolConfig":
        """Create from dictionary configuration."""
        if data is None:
            return cls()
        return cls(
            pool_connections=data.get("pool_connections", 10),
            pool_maxsize=data.get("pool_maxsize", 10),
            pool_block=data.get("pool_block", False),
            max_retries=data.get("max_retries", 0),
        )

logger = logging.getLogger(__name__)


def _create_pooled_session(pool_config: Optional[SyncPoolConfig] = None) -> requests.Session:
    """Create a requests.Session with explicit connection pooling configuration.

    Args:
        pool_config: Pool configuration. Uses defaults if None.

    Returns:
        Configured requests.Session with HTTPAdapter mounted
    """
    config = pool_config or SyncPoolConfig()
    session = requests.Session()

    adapter = HTTPAdapter(
        pool_connections=config.pool_connections,
        pool_maxsize=config.pool_maxsize,
        pool_block=config.pool_block,
        max_retries=config.max_retries,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    logger.debug(
        "Created pooled session with connections=%d, maxsize=%d",
        config.pool_connections,
        config.pool_maxsize,
    )
    return session


@register_extractor("api")
class ApiExtractor(BaseExtractor):
    """Extractor for REST API sources with authentication and pagination.

    Supports bearer token, API key, and basic authentication. Handles
    various pagination strategies and includes rate limiting support.
    Connection pooling is configurable for both sync and async paths.
    """

    def get_watermark_config(self, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get watermark configuration for API extraction.

        Returns watermark config based on cursor_field in source.api.
        """
        source = cfg.get("source", {})
        api_cfg = source.get("api", {})

        cursor_field = api_cfg.get("cursor_field")
        if not cursor_field:
            return None

        return {
            "enabled": True,
            "column": cursor_field,
            "type": api_cfg.get("cursor_type", "timestamp"),
        }

    _breaker = CircuitBreaker(
        failure_threshold=5, cooldown_seconds=30.0, half_open_max_calls=1
    )
    _limiter: Optional[RateLimiter] = None

    def _make_request(
        self,
        session: requests.Session,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        timeout: int,
        auth: Optional[Tuple[str, str]] = None,
    ) -> requests.Response:
        """Make HTTP request with retry logic (exponential backoff + jitter)."""
        logger.debug("Making request to %s with params %s", url, params)
        # Rate limiter is derived from config per call site; fallback to none
        # The limiter is injected via closure when called from paginate

        def _once() -> requests.Response:
            limiter = getattr(self, "_limiter", None)
            if limiter is not None:
                limiter.acquire()
            with trace_span("api.request"):
                resp = session.get(
                    url, headers=headers, params=params, timeout=timeout, auth=auth
                )
            # Only retry on 5xx; 4xx should raise immediately
            try:
                resp.raise_for_status()
            except (
                requests.exceptions.HTTPError
            ) as http_err:  # decide retryability via predicate
                raise http_err
            return resp

        def _retry_if(exc: BaseException) -> bool:
            # Retry for timeouts/connection errors
            if isinstance(
                exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
            ):
                return True
            # Retry 5xx HTTP errors
            if isinstance(exc, requests.exceptions.HTTPError):
                resp = getattr(exc, "response", None)
                status_code = getattr(resp, "status_code", None)
                if status_code is None:
                    return False
                status = int(status_code)
                return status == 429 or 500 <= status < 600
            return False

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> float | None:
            if isinstance(exc, requests.exceptions.HTTPError):
                resp = getattr(exc, "response", None)
                if resp is not None:
                    retry_after = (
                        resp.headers.get("Retry-After")
                        if hasattr(resp, "headers")
                        else None
                    )
                    if retry_after:
                        try:
                            return float(retry_after)
                        except Exception:
                            return None
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_on_exceptions=(),  # use custom predicate
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        # emit simple metrics on breaker transitions
        def _on_state_change(state: str) -> None:
            logger.info("metric=breaker_state component=api_extractor state=%s", state)

        ApiExtractor._breaker.on_state_change = _on_state_change

        return execute_with_retry(
            _once,
            policy=policy,
            breaker=ApiExtractor._breaker,
            operation_name="api_get",
        )

    def _extract_records(
        self, data: Any, api_cfg: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract records from API response data."""
        # Support custom data path
        data_path = api_cfg.get("data_path", "")

        if data_path:
            # Navigate nested structure (e.g., "data.items")
            for key in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                else:
                    logger.warning("Cannot navigate path '%s' in response", data_path)
                    break

        # Convert to list of records
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Check common patterns
            from typing import cast

            for key in ["items", "data", "results", "records"]:
                if key in data and isinstance(data[key], list):
                    return cast(List[Dict[str, Any]], data[key])
            # If no common pattern, wrap single record
            return [data]
        else:
            logger.warning("Unexpected data type: %s", type(data))
            return []

    def _paginate(
        self,
        session: requests.Session,
        base_url: str,
        endpoint: str,
        headers: Dict[str, str],
        api_cfg: Dict[str, Any],
        run_cfg: Dict[str, Any],
        auth: Optional[Tuple[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Handle pagination and collect all records."""
        url = base_url.rstrip("/") + endpoint

        timeout = run_cfg.get("timeout_seconds", 30)
        self._limiter = RateLimiter.from_config(
            api_cfg, run_cfg, component="api_extractor"
        )
        params = dict(api_cfg.get("params", {}))
        pagination_cfg = api_cfg.get("pagination", {}) or {}
        state = build_pagination_state(pagination_cfg, params)

        all_records: List[Dict[str, Any]] = []

        while state.should_fetch_more():
            current_params = state.build_params()
            resp = self._make_request(session, url, headers, current_params, timeout, auth)
            data = resp.json()
            records = self._extract_records(data, api_cfg)
            if not records:
                break

            all_records.extend(records)
            logger.info(
                "Fetched %d records %s (total: %d)",
                len(records),
                state.describe(),
                len(all_records),
            )

            if state.max_records > 0 and len(all_records) >= state.max_records:
                all_records = all_records[: state.max_records]
                logger.info("Reached max_records limit of %d", state.max_records)
                break

            if not state.on_records(records, data):
                break

        if (
            isinstance(state, PagePaginationState)
            and state.max_pages
            and state.max_pages_limit_hit
        ):
            logger.info("Reached max_pages limit of %d", state.max_pages)

        return all_records

    async def _paginate_async(
        self,
        base_url: str,
        endpoint: str,
        headers: Dict[str, str],
        api_cfg: Dict[str, Any],
        run_cfg: Dict[str, Any],
        auth: Optional[Tuple[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Async variant of pagination using httpx client with connection pooling.

        Uses a single pooled AsyncApiClient for all requests, significantly
        reducing connection overhead for paginated extractions.
        """
        timeout = run_cfg.get("timeout_seconds", 30)
        max_conc = api_cfg.get("max_concurrency", 5)

        # Build pool config from api_cfg if present
        pool_cfg_dict = api_cfg.get("http_pool", {})
        pool_config = HttpPoolConfig.from_dict(pool_cfg_dict) if pool_cfg_dict else None

        limiter = RateLimiter.from_config(
            api_cfg, run_cfg, component="api_extractor"
        )
        params = dict(api_cfg.get("params", {}))
        pagination_cfg = api_cfg.get("pagination", {}) or {}
        state = build_pagination_state(pagination_cfg, params)

        all_records: List[Dict[str, Any]] = []

        # Use context manager for proper cleanup
        async with AsyncApiClient(
            base_url, headers, auth=auth, timeout=timeout,
            max_concurrent=max_conc, pool_config=pool_config
        ) as client:
            async def _get(params_local: Dict[str, Any]) -> Dict[str, Any]:
                if limiter:
                    await limiter.async_acquire()
                with trace_span("api.request"):
                    return await client.get(endpoint, params=params_local)

            while state.should_fetch_more():
                data = await _get(state.build_params())
                records = self._extract_records(data, api_cfg)
                if not records:
                    break

                all_records.extend(records)
                logger.info(
                    "Fetched %d records %s (total: %d)",
                    len(records),
                    state.describe(),
                    len(all_records),
                )

                if state.max_records > 0 and len(all_records) >= state.max_records:
                    all_records = all_records[: state.max_records]
                    logger.info("Reached max_records limit of %d", state.max_records)
                    break

                if not state.on_records(records, data):
                    break

            if (
                isinstance(state, PagePaginationState)
                and state.max_pages
                and state.max_pages_limit_hit
            ):
                logger.info("Reached max_pages limit of %d", state.max_pages)

            # Log connection metrics
            logger.debug(
                "Async pagination complete. Connection reuse ratio: %.2f",
                client.metrics.reuse_ratio,
            )

        return all_records

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from API with authentication and pagination."""
        source_cfg = cfg["source"]
        api_cfg = source_cfg["api"]
        run_cfg = source_cfg["run"]

        # Build sync pool config from api_cfg if present
        sync_pool_dict = api_cfg.get("sync_pool", {})
        sync_pool_config = SyncPoolConfig.from_dict(sync_pool_dict) if sync_pool_dict else None
        session = _create_pooled_session(sync_pool_config)

        headers, auth = build_api_auth(api_cfg)

        base_url = api_cfg["base_url"]
        endpoint = api_cfg["endpoint"]

        logger.info("Starting API extraction from %s%s", base_url, endpoint)

        try:
            if is_async_enabled(api_cfg):
                records = asyncio.run(
                    self._paginate_async(
                        base_url, endpoint, headers, api_cfg, run_cfg, auth
                    )
                )
            else:
                records = self._paginate(
                    session, base_url, endpoint, headers, api_cfg, run_cfg, auth
                )
        except requests.exceptions.RequestException as e:
            logger.error("API request failed: %s", e)
            raise
        finally:
            session.close()

        # Compute cursor for incremental loading (if configured)
        new_cursor: Optional[str] = None
        cursor_field = api_cfg.get("cursor_field")

        if cursor_field and records:
            # Find the maximum cursor value from records
            try:
                cursor_values = [
                    r[cursor_field]
                    for r in records
                    if cursor_field in r and r[cursor_field] is not None
                ]
                if cursor_values:
                    cursor_values_str = [str(v) for v in cursor_values]
                    new_cursor = str(max(cursor_values_str))
                    logger.info("Computed new cursor: %s", new_cursor)
            except (TypeError, ValueError) as e:
                logger.warning(
                    "Could not compute cursor from field '%s': %s", cursor_field, e
                )

        logger.info("Successfully extracted %d records from API", len(records))
        return records, new_cursor
