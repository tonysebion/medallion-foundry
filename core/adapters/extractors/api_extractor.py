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

from core.infrastructure.resilience.retry import RetryPolicy, execute_with_retry, CircuitBreaker, RateLimiter
from core.primitives.catalog.tracing import trace_span
from .async_http import AsyncApiClient, is_async_enabled

from core.adapters.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


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
        all_records: List[Dict[str, Any]] = []
        url = base_url.rstrip("/") + endpoint

        timeout = run_cfg.get("timeout_seconds", 30)
        # set up per-extractor rate limiter, used by _make_request via attribute
        self._limiter = RateLimiter.from_config(api_cfg, run_cfg)
        pagination_cfg = api_cfg.get("pagination", {})
        pagination_type = pagination_cfg.get("type", "none")

        params = dict(api_cfg.get("params", {}))

        if pagination_type == "none":
            # Single request, no pagination
            resp = self._make_request(session, url, headers, params, timeout, auth)
            data = resp.json()
            all_records = self._extract_records(data, api_cfg)
            logger.info(f"Fetched {len(all_records)} records (no pagination)")

        elif pagination_type == "offset":
            # Offset-based pagination
            offset_param = pagination_cfg.get("offset_param", "offset")
            limit_param = pagination_cfg.get("limit_param", "limit")
            page_size = pagination_cfg.get("page_size", 100)
            max_records = pagination_cfg.get("max_records", 0)

            offset = 0
            params[limit_param] = page_size

            while True:
                params[offset_param] = offset
                resp = self._make_request(session, url, headers, params, timeout, auth)
                data = resp.json()

                records = self._extract_records(data, api_cfg)
                if not records:
                    break

                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records at offset {offset} (total: {len(all_records)})"
                )

                if len(records) < page_size:
                    break

                if max_records > 0 and len(all_records) >= max_records:
                    all_records = all_records[:max_records]
                    logger.info(f"Reached max_records limit of {max_records}")
                    break

                offset += page_size

        elif pagination_type == "page":
            # Page-based pagination
            page_param = pagination_cfg.get("page_param", "page")
            page_size_param = pagination_cfg.get("page_size_param", "page_size")
            page_size = pagination_cfg.get("page_size", 100)
            max_pages = pagination_cfg.get("max_pages", 0)

            page = 1
            params[page_size_param] = page_size

            while True:
                if max_pages > 0 and page > max_pages:
                    logger.info(f"Reached max_pages limit of {max_pages}")
                    break

                params[page_param] = page
                resp = self._make_request(session, url, headers, params, timeout, auth)
                data = resp.json()

                records = self._extract_records(data, api_cfg)
                if not records:
                    break

                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records from page {page} (total: {len(all_records)})"
                )

                if len(records) < page_size:
                    break

                page += 1

        elif pagination_type == "cursor":
            # Cursor-based pagination
            cursor_param = pagination_cfg.get("cursor_param", "cursor")
            cursor_path = pagination_cfg.get("cursor_path", "next_cursor")

            cursor: Optional[str] = None
            first_iteration = True

            while first_iteration or cursor is not None:
                if first_iteration:
                    first_iteration = False
                request_params = dict(params)
                if cursor is not None:
                    request_params[cursor_param] = cursor

                resp = self._make_request(
                    session, url, headers, request_params, timeout, auth
                )
                data = resp.json()

                records = self._extract_records(data, api_cfg)
                if not records:
                    break

                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records (total: {len(all_records)})"
                )

                cursor_val: Optional[str] = None
                obj: Any = data
                if isinstance(obj, dict):
                    for key in cursor_path.split("."):
                        obj = obj.get(key) if isinstance(obj, dict) else None
                        if obj is None:
                            break
                    cursor_val = obj

                cursor = cursor_val

        else:
            raise ValueError(f"Unsupported pagination type: '{pagination_type}'")

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

        all_records: List[Dict[str, Any]] = []
        pagination_cfg = api_cfg.get("pagination", {})
        pagination_type = pagination_cfg.get("type", "none")
        params = dict(api_cfg.get("params", {}))

        async def _get(params_local: Dict[str, Any]) -> Dict[str, Any]:
            if limiter:
                await limiter.async_acquire()
            with trace_span("api.request"):
                return await client.get(endpoint, params=params_local)

        if pagination_type == "none":
            data = await _get(params)
            all_records = self._extract_records(data, api_cfg)
            logger.info(f"Fetched {len(all_records)} records (no pagination, async)")

        elif pagination_type == "offset":
            offset_param = pagination_cfg.get("offset_param", "offset")
            limit_param = pagination_cfg.get("limit_param", "limit")
            page_size = pagination_cfg.get("page_size", 100)
            max_records = pagination_cfg.get("max_records", 0)
            offset = 0
            params[limit_param] = page_size
            while True:
                params[offset_param] = offset
                data = await _get(params)
                records = self._extract_records(data, api_cfg)
                if not records:
                    break
                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records at offset {offset} (total: {len(all_records)})"
                )
                if len(records) < page_size:
                    break
                if max_records > 0 and len(all_records) >= max_records:
                    all_records = all_records[:max_records]
                    logger.info(f"Reached max_records limit of {max_records}")
                    break
                offset += page_size

        elif pagination_type == "page":
            page_param = pagination_cfg.get("page_param", "page")
            page_size_param = pagination_cfg.get("page_size_param", "page_size")
            page_size = pagination_cfg.get("page_size", 100)
            max_pages = pagination_cfg.get("max_pages", 0)
            page = 1
            params[page_size_param] = page_size

            # Prefetch next page while processing current page
            next_page_task: Optional[asyncio.Task[Dict[str, Any]]] = None

            while True:
                if max_pages > 0 and page > max_pages:
                    logger.info(f"Reached max_pages limit of {max_pages}")
                    break

                params_copy = dict(params)
                params_copy[page_param] = page

                # If we have a prefetch task running, await it; otherwise fetch current page
                if next_page_task is not None:
                    data = await next_page_task
                else:
                    data = await _get(params_copy)

                # Start prefetching next page in parallel while we process current data
                if max_pages == 0 or page + 1 <= max_pages:
                    next_params = dict(params)
                    next_params[page_param] = page + 1
                    next_page_task = asyncio.create_task(_get(next_params))
                else:
                    next_page_task = None

                records = self._extract_records(data, api_cfg)
                if not records:
                    if next_page_task:
                        next_page_task.cancel()
                    break

                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records from page {page} (total: {len(all_records)})"
                )

                if len(records) < page_size:
                    if next_page_task is not None:
                        next_page_task.cancel()
                    break

                page += 1

        elif pagination_type == "cursor":
            cursor_param = pagination_cfg.get("cursor_param", "cursor")
            cursor_path = pagination_cfg.get("cursor_path", "next_cursor")

            cursor: Optional[str] = None
            first_iteration = True
            while first_iteration or cursor is not None:
                if first_iteration:
                    first_iteration = False
                request_params = dict(params)
                if cursor is not None:
                    request_params[cursor_param] = cursor
                data = await _get(request_params)
                records = self._extract_records(data, api_cfg)
                if not records:
                    break
                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records (total: {len(all_records)})"
                )
                # Extract next cursor
                cursor_val = None
                obj: Any = data
                if isinstance(obj, dict):
                    for key in cursor_path.split("."):
                        obj = obj.get(key) if isinstance(obj, dict) else None
                        if obj is None:
                            break
                    cursor_val = obj
                cursor = cursor_val
        else:
            raise ValueError(f"Unsupported pagination type: '{pagination_type}'")

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
