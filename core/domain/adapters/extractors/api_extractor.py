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
from typing import Dict, Any, List, Optional, Tuple
from datetime import date

import requests
from requests.adapters import HTTPAdapter

from core.platform.resilience import (
    execute_with_retry,
    RateLimiter,
)
from core.foundation.catalog.tracing import trace_span
from core.infrastructure.io.http.auth import build_api_auth
from core.infrastructure.io.http.session import (
    AsyncApiClient,
    HttpPoolConfig,
    SyncPoolConfig,
    is_async_enabled,
)

from core.infrastructure.io.extractors.base import BaseExtractor, register_extractor
from core.domain.adapters.extractors.pagination import (
    PaginationState,
    PagePaginationState,
    build_pagination_state,
)
from core.domain.adapters.extractors.resilience import ResilientExtractorMixin

logger = logging.getLogger(__name__)


def _process_page_result(
    state: PaginationState,
    data: Any,
    extract_records: Any,
    api_cfg: Dict[str, Any],
    all_records: List[Dict[str, Any]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Process a single page result from pagination.

    This function contains all shared logic between sync and async pagination loops:
    extracting records, extending the collection, logging progress, and checking limits.

    Args:
        state: Pagination state machine
        data: Raw response data from fetch
        extract_records: Callable that extracts records from response data
        api_cfg: API configuration dict
        all_records: Accumulated records so far (will be extended)

    Returns:
        Tuple of (should_continue, updated_all_records)
    """
    records = extract_records(data, api_cfg)
    if not records:
        return False, all_records

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
        return False, all_records

    if not state.on_records(records, data):
        return False, all_records

    return True, all_records


def _log_max_pages_if_reached(state: PaginationState) -> None:
    """Log if max_pages limit was reached (for PagePaginationState)."""
    if (
        isinstance(state, PagePaginationState)
        and state.max_pages
        and state.max_pages_limit_hit
    ):
        logger.info("Reached max_pages limit of %d", state.max_pages)


def _run_pagination_loop(
    state: PaginationState,
    fetch_page: Any,
    extract_records: Any,
    api_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Sync pagination loop.

    Args:
        state: Pagination state machine
        fetch_page: Callable that fetches a page given params, returns response data
        extract_records: Callable that extracts records from response data
        api_cfg: API configuration dict

    Returns:
        All collected records
    """
    all_records: List[Dict[str, Any]] = []

    while state.should_fetch_more():
        data = fetch_page(state.build_params())
        should_continue, all_records = _process_page_result(
            state, data, extract_records, api_cfg, all_records
        )
        if not should_continue:
            break

    _log_max_pages_if_reached(state)
    return all_records


async def _run_pagination_loop_async(
    state: PaginationState,
    fetch_page: Any,
    extract_records: Any,
    api_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Async pagination loop.

    Args:
        state: Pagination state machine
        fetch_page: Async callable that fetches a page given params
        extract_records: Callable that extracts records from response data
        api_cfg: API configuration dict

    Returns:
        All collected records
    """
    all_records: List[Dict[str, Any]] = []

    while state.should_fetch_more():
        data = await fetch_page(state.build_params())
        should_continue, all_records = _process_page_result(
            state, data, extract_records, api_cfg, all_records
        )
        if not should_continue:
            break

    _log_max_pages_if_reached(state)
    return all_records


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
class ApiExtractor(BaseExtractor, ResilientExtractorMixin):
    """Extractor for REST API sources with authentication and pagination.

    Supports bearer token, API key, and basic authentication. Handles
    various pagination strategies and includes rate limiting support.
    Connection pooling is configurable for both sync and async paths.

    Uses ResilientExtractorMixin for circuit breaker and retry capabilities.
    """

    _limiter: Optional[RateLimiter] = None

    def __init__(self) -> None:
        """Initialize ApiExtractor with resilience patterns."""
        self._init_resilience(
            failure_threshold=5,
            cooldown_seconds=30.0,
            half_open_max_calls=1,
        )

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

    def _should_retry_api(self, exc: BaseException) -> bool:
        """Determine if an API exception should trigger a retry.

        Args:
            exc: The exception that was raised

        Returns:
            True if the operation should be retried
        """
        # Retry for timeouts/connection errors
        if isinstance(
            exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        ):
            return True
        # Retry 5xx HTTP errors and 429 rate limit
        if isinstance(exc, requests.exceptions.HTTPError):
            resp = getattr(exc, "response", None)
            status_code = getattr(resp, "status_code", None)
            if status_code is None:
                return False
            status = int(status_code)
            return status == 429 or 500 <= status < 600
        return False

    def _delay_from_api_exc(
        self, exc: BaseException, attempt: int, default_delay: float
    ) -> Optional[float]:
        """Extract retry delay from API exception (e.g., Retry-After header).

        Args:
            exc: The exception that was raised
            attempt: Current attempt number
            default_delay: Default delay to use if no header found

        Returns:
            Delay in seconds from Retry-After header, or None for default
        """
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
            except requests.exceptions.HTTPError as http_err:
                raise http_err
            return resp

        # Build retry policy with API-specific handlers
        policy = self._build_retry_policy(
            retry_if=self._should_retry_api,
            delay_from_exception=self._delay_from_api_exc,
            retry_config=api_cfg.get("retry"),
        )

        return execute_with_retry(
            _once,
            policy=policy,
            breaker=self._breaker,
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

        def fetch_page(current_params: Dict[str, Any]) -> Dict[str, Any]:
            resp = self._make_request(session, url, headers, current_params, timeout, auth)
            result: Dict[str, Any] = resp.json()
            return result

        return _run_pagination_loop(state, fetch_page, self._extract_records, api_cfg)

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

        # Use context manager for proper cleanup
        async with AsyncApiClient(
            base_url, headers, auth=auth, timeout=timeout,
            max_concurrent=max_conc, pool_config=pool_config
        ) as client:
            async def fetch_page(params_local: Dict[str, Any]) -> Dict[str, Any]:
                if limiter:
                    await limiter.async_acquire()
                with trace_span("api.request"):
                    return await client.get(endpoint, params=params_local)

            all_records = await _run_pagination_loop_async(
                state, fetch_page, self._extract_records, api_cfg
            )

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
