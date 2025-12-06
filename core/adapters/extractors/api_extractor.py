"""API-based extraction with authentication and pagination support per spec Section 3.

Supports api source type with:
- Multiple authentication methods (bearer, api_key, basic, none)
- Pagination (offset, page, cursor, none)
- Rate limiting and retry logic
- Async HTTP support
- Watermark-based incremental extraction
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import date

import requests

from core.infrastructure.resilience.retry import (
    RetryPolicy,
    execute_with_retry,
    CircuitBreaker,
    RateLimiter,
)
from core.primitives.catalog.tracing import trace_span
from .async_http import AsyncApiClient, is_async_enabled

from core.adapters.extractors.base import BaseExtractor, register_extractor

logger = logging.getLogger(__name__)


class _PaginationState:
    def __init__(self, pagination_cfg: Dict[str, Any], params: Dict[str, Any]):
        self.pagination_cfg = pagination_cfg or {}
        self.base_params = dict(params or {})
        self.max_records = self.pagination_cfg.get("max_records", 0)

    def should_fetch_more(self) -> bool:
        return True

    def build_params(self) -> Dict[str, Any]:
        return dict(self.base_params)

    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        return False

    def describe(self) -> str:
        return "(no pagination)"


class _NoPaginationState(_PaginationState):
    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        return False


class _OffsetPaginationState(_PaginationState):
    def __init__(self, pagination_cfg: Dict[str, Any], params: Dict[str, Any]):
        super().__init__(pagination_cfg, params)
        self.offset_param = pagination_cfg.get("offset_param", "offset")
        self.limit_param = pagination_cfg.get("limit_param", "limit")
        self.page_size = pagination_cfg.get("page_size", 100)
        self.offset = 0
        self._last_offset = 0

    def build_params(self) -> Dict[str, Any]:
        params = super().build_params()
        params[self.limit_param] = self.page_size
        params[self.offset_param] = self.offset
        self._last_offset = self.offset
        return params

    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records or len(records) < self.page_size:
            return False
        self.offset += self.page_size
        return True

    def describe(self) -> str:
        return f"at offset {self._last_offset}"


class _PagePaginationState(_PaginationState):
    def __init__(self, pagination_cfg: Dict[str, Any], params: Dict[str, Any]):
        super().__init__(pagination_cfg, params)
        self.page = 1
        self._last_page = 1
        self.page_param = pagination_cfg.get("page_param", "page")
        self.page_size_param = pagination_cfg.get("page_size_param", "page_size")
        self.page_size = pagination_cfg.get("page_size", 100)
        self.max_pages = pagination_cfg.get("max_pages", 0)
        self._max_pages_reached = False

    def should_fetch_more(self) -> bool:
        if self.max_pages and self.page > self.max_pages:
            self._max_pages_reached = True
            return False
        return True

    def build_params(self) -> Dict[str, Any]:
        params = super().build_params()
        params[self.page_param] = self.page
        params[self.page_size_param] = self.page_size
        self._last_page = self.page
        return params

    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records or len(records) < self.page_size:
            return False
        self.page += 1
        return True

    def describe(self) -> str:
        return f"from page {self._last_page}"

    @property
    def max_pages_limit_hit(self) -> bool:
        return self._max_pages_reached


class _CursorPaginationState(_PaginationState):
    def __init__(self, pagination_cfg: Dict[str, Any], params: Dict[str, Any]):
        super().__init__(pagination_cfg, params)
        self.cursor_param = pagination_cfg.get("cursor_param", "cursor")
        self.cursor_path = pagination_cfg.get("cursor_path", "next_cursor")
        self.cursor: Optional[str] = None

    def build_params(self) -> Dict[str, Any]:
        params = super().build_params()
        if self.cursor:
            params[self.cursor_param] = self.cursor
        return params

    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records:
            return False
        self.cursor = self._extract_cursor(data)
        return bool(self.cursor)

    def describe(self) -> str:
        return f"(cursor pagination, next_cursor={self.cursor})"

    def _extract_cursor(self, data: Any) -> Optional[str]:
        obj: Any = data
        if isinstance(obj, dict):
            for key in self.cursor_path.split("."):
                obj = obj.get(key) if isinstance(obj, dict) else None
                if obj is None:
                    break
            return obj
        return None


def _build_pagination_state(
    pagination_cfg: Dict[str, Any], params: Dict[str, Any]
) -> _PaginationState:
    pagination_type = (pagination_cfg or {}).get("type", "none")
    if pagination_type == "offset":
        return _OffsetPaginationState(pagination_cfg, params)
    if pagination_type == "page":
        return _PagePaginationState(pagination_cfg, params)
    if pagination_type == "cursor":
        return _CursorPaginationState(pagination_cfg, params)
    return _NoPaginationState(pagination_cfg, params)


@register_extractor("api")
class ApiExtractor(BaseExtractor):
    """Extractor for REST API sources with authentication and pagination.

    Supports bearer token, API key, and basic authentication. Handles
    various pagination strategies and includes rate limiting support.
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

    def _build_auth_headers(self, api_cfg: Dict[str, Any]) -> Dict[str, str]:
        """Build authentication headers based on config."""
        headers = {"Accept": "application/json"}

        auth_type = api_cfg.get("auth_type", "none")

        if auth_type == "bearer":
            token_env = api_cfg.get("auth_token_env")
            if not token_env:
                raise ValueError(
                    "auth_type='bearer' requires 'auth_token_env' in config"
                )

            token = os.environ.get(token_env)
            if not token:
                raise ValueError(
                    f"Environment variable '{token_env}' not set for bearer token"
                )

            headers["Authorization"] = f"Bearer {token}"
            logger.debug("Added bearer token authentication")

        elif auth_type == "api_key":
            key_env = api_cfg.get("auth_key_env")
            key_header = api_cfg.get("auth_key_header", "X-API-Key")

            if not key_env:
                raise ValueError(
                    "auth_type='api_key' requires 'auth_key_env' in config"
                )

            api_key = os.environ.get(key_env)
            if not api_key:
                raise ValueError(
                    f"Environment variable '{key_env}' not set for API key"
                )

            headers[key_header] = api_key
            logger.debug(f"Added API key authentication in header '{key_header}'")

        elif auth_type == "basic":
            username_env = api_cfg.get("auth_username_env")
            password_env = api_cfg.get("auth_password_env")

            if not username_env or not password_env:
                raise ValueError(
                    "auth_type='basic' requires 'auth_username_env' and 'auth_password_env'"
                )

            # Basic auth is handled separately via requests auth parameter
            logger.debug("Using basic authentication")

        elif auth_type != "none":
            raise ValueError(
                f"Unsupported auth_type: '{auth_type}'. Use 'bearer', 'api_key', 'basic', or 'none'"
            )

        # Add any custom headers from config
        custom_headers = api_cfg.get("headers", {})
        headers.update(custom_headers)

        return headers

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
        logger.debug(f"Making request to {url} with params {params}")
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
                    logger.warning(f"Cannot navigate path '{data_path}' in response")
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
            logger.warning(f"Unexpected data type: {type(data)}")
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
        self._limiter = RateLimiter.from_config(api_cfg, run_cfg)
        params = dict(api_cfg.get("params", {}))
        pagination_cfg = api_cfg.get("pagination", {}) or {}
        state = _build_pagination_state(pagination_cfg, params)

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
                f"Fetched {len(records)} records {state.describe()} (total: {len(all_records)})"
            )

            if state.max_records > 0 and len(all_records) >= state.max_records:
                all_records = all_records[: state.max_records]
                logger.info(f"Reached max_records limit of {state.max_records}")
                break

            if not state.on_records(records, data):
                break

        if (
            isinstance(state, _PagePaginationState)
            and state.max_pages
            and state.max_pages_limit_hit
        ):
            logger.info(f"Reached max_pages limit of {state.max_pages}")

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
        """Async variant of pagination using httpx client.

        Executes sequentially for correctness; still benefits from async I/O.
        """
        timeout = run_cfg.get("timeout_seconds", 30)
        max_conc = api_cfg.get("max_concurrency", 5)
        client = AsyncApiClient(
            base_url, headers, auth=auth, timeout=timeout, max_concurrent=max_conc
        )
        limiter = RateLimiter.from_config(api_cfg, run_cfg)
        params = dict(api_cfg.get("params", {}))
        pagination_cfg = api_cfg.get("pagination", {}) or {}
        state = _build_pagination_state(pagination_cfg, params)

        async def _get(params_local: Dict[str, Any]) -> Dict[str, Any]:
            if limiter:
                await limiter.async_acquire()
            with trace_span("api.request"):
                return await client.get(endpoint, params=params_local)

        all_records: List[Dict[str, Any]] = []

        while state.should_fetch_more():
            data = await _get(state.build_params())
            records = self._extract_records(data, api_cfg)
            if not records:
                break

            all_records.extend(records)
            logger.info(
                f"Fetched {len(records)} records {state.describe()} (total: {len(all_records)})"
            )

            if state.max_records > 0 and len(all_records) >= state.max_records:
                all_records = all_records[: state.max_records]
                logger.info(f"Reached max_records limit of {state.max_records}")
                break

            if not state.on_records(records, data):
                break

        if (
            isinstance(state, _PagePaginationState)
            and state.max_pages
            and state.max_pages_limit_hit
        ):
            logger.info(f"Reached max_pages limit of {state.max_pages}")

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

        session = requests.Session()
        headers = self._build_auth_headers(api_cfg)

        # Handle basic auth separately
        auth = None
        if api_cfg.get("auth_type") == "basic":
            username = os.environ.get(api_cfg["auth_username_env"])
            password = os.environ.get(api_cfg["auth_password_env"])
            if username and password:
                auth = (username, password)

        base_url = api_cfg["base_url"]
        endpoint = api_cfg["endpoint"]

        logger.info(f"Starting API extraction from {base_url}{endpoint}")

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
            logger.error(f"API request failed: {e}")
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
                    logger.info(f"Computed new cursor: {new_cursor}")
            except (TypeError, ValueError) as e:
                logger.warning(
                    f"Could not compute cursor from field '{cursor_field}': {e}"
                )

        logger.info(f"Successfully extracted {len(records)} records from API")
        return records, new_cursor
