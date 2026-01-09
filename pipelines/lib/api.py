"""API extraction for Bronze layer.

Provides full-featured REST API extraction with:
- Multiple authentication methods (bearer, API key, basic)
- Multiple pagination strategies (offset, page, cursor)
- Rate limiting
- Retry with exponential backoff
- Watermark-based incremental extraction

Example:
    from pipelines.lib.api import ApiSource, AuthConfig, AuthType
    from pipelines.lib.api import PaginationConfig, PaginationStrategy

    source = ApiSource(
        system="github",
        entity="repos",
        base_url="https://api.github.com",
        endpoint="/users/{user}/repos",
        target_path="./bronze/github/repos/dt={run_date}/",
        auth=AuthConfig(
            auth_type=AuthType.BEARER,
            token="${GITHUB_TOKEN}",
        ),
        pagination=PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
            page_param="page",
            page_size_param="per_page",
        ),
        path_params={"user": "anthropics"},
    )

    result = source.run("2025-01-15")
"""

from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from pipelines.lib.env import utc_now_iso
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

import httpx
import requests_toolbelt
import tenacity
from requests_toolbelt.utils.user_agent import user_agent
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from pipelines.lib._path_utils import path_has_data, resolve_target_path
from pipelines.lib.checksum import write_checksum_manifest
from pipelines.lib.env import expand_env_vars, expand_options
from pipelines.lib.io import OutputMetadata, infer_column_types, maybe_dry_run, maybe_skip_if_exists
from pipelines.lib.state import get_watermark, save_watermark

logger = logging.getLogger(__name__)

# ============================================================================
# Authentication (formerly auth.py)
# ============================================================================


class AuthType(Enum):
    """Supported API authentication methods."""

    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"


@dataclass
class AuthConfig:
    """Configuration for API authentication.

    All credential fields support environment variable expansion using
    ${VAR_NAME} syntax. Use this instead of hardcoding secrets.

    Examples:
        # No authentication
        auth = AuthConfig(auth_type=AuthType.NONE)

        # Bearer token
        auth = AuthConfig(
            auth_type=AuthType.BEARER,
            token="${API_TOKEN}",
        )

        # API key in header
        auth = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="${API_KEY}",
            api_key_header="X-API-Key",
        )

        # Basic auth
        auth = AuthConfig(
            auth_type=AuthType.BASIC,
            username="${API_USER}",
            password="${API_PASSWORD}",
        )
    """

    auth_type: AuthType = AuthType.NONE

    # Bearer token authentication
    token: Optional[str] = None

    # API key authentication
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"

    # Basic authentication
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate configuration based on auth type."""
        if self.auth_type == AuthType.BEARER and not self.token:
            raise ValueError("Bearer authentication requires 'token' to be set")
        if self.auth_type == AuthType.API_KEY and not self.api_key:
            raise ValueError("API key authentication requires 'api_key' to be set")
        if self.auth_type == AuthType.BASIC and not (self.username and self.password):
            raise ValueError(
                "Basic authentication requires both 'username' and 'password'"
            )


def build_auth_headers(
    config: Optional[AuthConfig],
    *,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build HTTP headers and auth tuple from authentication config.

    Args:
        config: Authentication configuration (None = no auth)
        extra_headers: Additional headers to include

    Returns:
        Tuple of (headers dict, optional basic auth tuple)

    Raises:
        ValueError: If environment variables cannot be resolved

    Example:
        auth = AuthConfig(auth_type=AuthType.BEARER, token="${MY_TOKEN}")
        headers, auth_tuple = build_auth_headers(auth)
        response = requests.get(url, headers=headers, auth=auth_tuple)
    """
    headers: Dict[str, str] = {"Accept": "application/json"}
    auth_tuple: Optional[Tuple[str, str]] = None

    if config is None or config.auth_type == AuthType.NONE:
        logger.debug("No authentication configured")

    elif config.auth_type == AuthType.BEARER:
        token = expand_env_vars(config.token or "", strict=True)
        if not token:
            raise ValueError("Bearer token resolved to empty string")
        headers["Authorization"] = f"Bearer {token}"
        logger.debug("Added bearer token authentication")

    elif config.auth_type == AuthType.API_KEY:
        api_key = expand_env_vars(config.api_key or "", strict=True)
        if not api_key:
            raise ValueError("API key resolved to empty string")
        headers[config.api_key_header] = api_key
        logger.debug("Added API key authentication in header '%s'", config.api_key_header)

    elif config.auth_type == AuthType.BASIC:
        username = expand_env_vars(config.username or "", strict=True)
        password = expand_env_vars(config.password or "", strict=True)
        if not (username and password):
            raise ValueError("Basic auth username or password resolved to empty string")
        auth_tuple = (username, password)
        logger.debug("Prepared basic authentication")

    # Add any extra headers
    if extra_headers:
        # Expand env vars in extra headers too
        for key, value in extra_headers.items():
            headers[key] = expand_env_vars(value, strict=False)

    return headers, auth_tuple


def build_auth_headers_from_dict(
    api_options: Dict[str, str],
) -> Tuple[Dict[str, str], Optional[Tuple[str, str]]]:
    """Build auth headers from a dictionary of options.

    This is a convenience function for building AuthConfig from
    the options dict commonly used in BronzeSource.

    Expected keys:
        - auth_type: "none", "bearer", "api_key", "basic"
        - token: Bearer token (for auth_type=bearer)
        - api_key: API key value (for auth_type=api_key)
        - api_key_header: Header name for API key (default: X-API-Key)
        - username: Username (for auth_type=basic)
        - password: Password (for auth_type=basic)

    Args:
        api_options: Dictionary with auth configuration (supports ${VAR} expansion)

    Returns:
        Tuple of (headers dict, optional basic auth tuple)
    """
    # Expand ${VAR} patterns BEFORE creating AuthConfig
    # This ensures validation happens on actual values, not placeholders
    expanded = expand_options(api_options)

    auth_type_str = expanded.get("auth_type", "none").lower()

    try:
        auth_type = AuthType(auth_type_str)
    except ValueError:
        raise ValueError(
            f"Unsupported auth_type: '{auth_type_str}'. "
            f"Use 'bearer', 'api_key', 'basic', or 'none'"
        )

    config = AuthConfig(
        auth_type=auth_type,
        token=expanded.get("token"),
        api_key=expanded.get("api_key"),
        api_key_header=expanded.get("api_key_header", "X-API-Key"),
        username=expanded.get("username"),
        password=expanded.get("password"),
    )

    raw_headers: Any = expanded.get("headers", {})
    extra_headers: Dict[str, str] = raw_headers if isinstance(raw_headers, dict) else {}
    return build_auth_headers(config, extra_headers=extra_headers)


# ============================================================================
# Pagination (formerly pagination.py)
# ============================================================================


class PaginationStrategy(Enum):
    """Supported pagination strategies."""

    NONE = "none"
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"


@dataclass
class PaginationConfig:
    """Configuration for API pagination.

    Examples:
        # No pagination (single request)
        config = PaginationConfig(strategy=PaginationStrategy.NONE)

        # Offset pagination (offset/limit params)
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
            offset_param="offset",
            limit_param="limit",
        )

        # Page pagination (page/per_page params)
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=50,
            page_param="page",
            page_size_param="per_page",
            max_pages=10,  # Optional limit
        )

        # Cursor pagination (cursor-based)
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_param="cursor",
            cursor_path="meta.next_cursor",
        )
    """

    strategy: PaginationStrategy = PaginationStrategy.NONE
    page_size: int = 100

    # Offset pagination params
    offset_param: str = "offset"
    limit_param: str = "limit"

    # Page pagination params
    page_param: str = "page"
    page_size_param: str = "page_size"
    max_pages: Optional[int] = None

    # Cursor pagination params
    cursor_param: str = "cursor"
    cursor_path: str = "next_cursor"

    # Global limit (stop after this many records regardless of pagination)
    max_records: int = 0  # 0 = unlimited


class PaginationState(ABC):
    """Base class for pagination state machines.

    Pagination states track the progress through paginated API responses
    and build the appropriate query parameters for each request.
    """

    def __init__(
        self,
        config: PaginationConfig,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize pagination state.

        Args:
            config: Pagination configuration
            base_params: Additional query parameters to include in every request
        """
        self.config = config
        self.base_params = dict(base_params or {})
        self.max_records = config.max_records

    @abstractmethod
    def should_fetch_more(self) -> bool:
        """Check if more pages should be fetched.

        Returns:
            True if there are more pages to fetch
        """
        ...

    @abstractmethod
    def build_params(self) -> Dict[str, Any]:
        """Build query parameters for the current page.

        Returns:
            Dictionary of query parameters
        """
        ...

    @abstractmethod
    def on_response(self, records: List[Dict[str, Any]], data: Any) -> bool:
        """Process a page response and update state.

        Args:
            records: Records extracted from the response
            data: Full response data (for cursor extraction)

        Returns:
            True if there are more pages to fetch
        """
        ...

    @abstractmethod
    def describe(self) -> str:
        """Get a human-readable description of current pagination state.

        Returns:
            Description string for logging
        """
        ...


class NoPaginationState(PaginationState):
    """State for single-request (non-paginated) APIs."""

    def __init__(
        self,
        config: PaginationConfig,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(config, base_params)
        self._fetched = False

    def should_fetch_more(self) -> bool:
        return not self._fetched

    def build_params(self) -> Dict[str, Any]:
        return dict(self.base_params)

    def on_response(self, records: List[Dict[str, Any]], data: Any) -> bool:
        self._fetched = True
        return False

    def describe(self) -> str:
        return "(no pagination)"


class OffsetPaginationState(PaginationState):
    """State for offset/limit pagination.

    Typical API pattern:
        GET /items?offset=0&limit=100
        GET /items?offset=100&limit=100
        ...
    """

    def __init__(
        self,
        config: PaginationConfig,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(config, base_params)
        self.offset = 0
        self._last_offset = 0

    def should_fetch_more(self) -> bool:
        return True  # Controlled by on_response

    def build_params(self) -> Dict[str, Any]:
        params = dict(self.base_params)
        params[self.config.limit_param] = self.config.page_size
        params[self.config.offset_param] = self.offset
        self._last_offset = self.offset
        return params

    def on_response(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records or len(records) < self.config.page_size:
            return False
        self.offset += self.config.page_size
        return True

    def describe(self) -> str:
        return f"at offset {self._last_offset}"


class PagePaginationState(PaginationState):
    """State for page number pagination.

    Typical API pattern:
        GET /items?page=1&page_size=100
        GET /items?page=2&page_size=100
        ...
    """

    def __init__(
        self,
        config: PaginationConfig,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(config, base_params)
        self.page = 1
        self._last_page = 1
        self._max_pages_reached = False

    def should_fetch_more(self) -> bool:
        if self.config.max_pages and self.page > self.config.max_pages:
            self._max_pages_reached = True
            return False
        return True

    def build_params(self) -> Dict[str, Any]:
        params = dict(self.base_params)
        params[self.config.page_param] = self.page
        params[self.config.page_size_param] = self.config.page_size
        self._last_page = self.page
        return params

    def on_response(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records or len(records) < self.config.page_size:
            return False
        self.page += 1
        return True

    def describe(self) -> str:
        return f"from page {self._last_page}"

    @property
    def max_pages_limit_hit(self) -> bool:
        """Check if max_pages limit was reached."""
        return self._max_pages_reached


class CursorPaginationState(PaginationState):
    """State for cursor-based pagination.

    Typical API pattern:
        GET /items
        -> Response: {"items": [...], "next_cursor": "abc123"}
        GET /items?cursor=abc123
        -> Response: {"items": [...], "next_cursor": "def456"}
        ...
    """

    def __init__(
        self,
        config: PaginationConfig,
        base_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(config, base_params)
        self.cursor: Optional[str] = None

    def should_fetch_more(self) -> bool:
        return True  # Controlled by on_response

    def build_params(self) -> Dict[str, Any]:
        params = dict(self.base_params)
        if self.cursor:
            params[self.config.cursor_param] = self.cursor
        return params

    def on_response(self, records: List[Dict[str, Any]], data: Any) -> bool:
        if not records:
            return False
        self.cursor = self._extract_cursor(data)
        return bool(self.cursor)

    def describe(self) -> str:
        if self.cursor:
            return f"(cursor={self.cursor[:20]}...)" if len(self.cursor) > 20 else f"(cursor={self.cursor})"
        return "(cursor pagination, first page)"

    def _extract_cursor(self, data: Any) -> Optional[str]:
        """Extract next cursor from response data.

        Supports nested paths like "meta.pagination.next_cursor".
        """
        if not isinstance(data, dict):
            return None

        obj: Any = data
        for key in self.config.cursor_path.split("."):
            if isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
            if obj is None:
                return None

        if isinstance(obj, str):
            return obj
        elif obj is not None:
            # Try to convert to string
            return str(obj)
        return None


def build_pagination_state(
    config: PaginationConfig,
    base_params: Optional[Dict[str, Any]] = None,
) -> PaginationState:
    """Create appropriate pagination state from configuration.

    Args:
        config: Pagination configuration
        base_params: Additional query parameters for every request

    Returns:
        Appropriate PaginationState subclass instance
    """
    if config.strategy == PaginationStrategy.OFFSET:
        return OffsetPaginationState(config, base_params)
    elif config.strategy == PaginationStrategy.PAGE:
        return PagePaginationState(config, base_params)
    elif config.strategy == PaginationStrategy.CURSOR:
        return CursorPaginationState(config, base_params)
    else:
        return NoPaginationState(config, base_params)


def build_pagination_config_from_dict(
    options: Dict[str, Any],
) -> PaginationConfig:
    """Build PaginationConfig from a dictionary of options.

    Convenience function for creating config from BronzeSource options.

    Expected keys:
        - pagination_type: "none", "offset", "page", "cursor"
        - page_size: Number of records per page (default: 100)
        - offset_param: Query param for offset (default: "offset")
        - limit_param: Query param for limit (default: "limit")
        - page_param: Query param for page number (default: "page")
        - page_size_param: Query param for page size (default: "page_size")
        - max_pages: Maximum pages to fetch (default: None)
        - cursor_param: Query param for cursor (default: "cursor")
        - cursor_path: Path to cursor in response (default: "next_cursor")
        - max_records: Stop after this many records (default: 0 = unlimited)

    Args:
        options: Dictionary with pagination options

    Returns:
        PaginationConfig instance
    """
    pagination_type = options.get("pagination_type", "none").lower()

    try:
        strategy = PaginationStrategy(pagination_type)
    except ValueError:
        raise ValueError(
            f"Unsupported pagination_type: '{pagination_type}'. "
            f"Use 'offset', 'page', 'cursor', or 'none'"
        )

    return PaginationConfig(
        strategy=strategy,
        page_size=options.get("page_size", 100),
        offset_param=options.get("offset_param", "offset"),
        limit_param=options.get("limit_param", "limit"),
        page_param=options.get("page_param", "page"),
        page_size_param=options.get("page_size_param", "page_size"),
        max_pages=options.get("max_pages"),
        cursor_param=options.get("cursor_param", "cursor"),
        cursor_path=options.get("cursor_path", "next_cursor"),
        max_records=options.get("max_records", 0),
    )


# ============================================================================
# Rate Limiting (formerly rate_limiter.py)
# ============================================================================

F = TypeVar("F", bound=Callable[..., Any])


class RateLimiter:
    """Token-bucket rate limiter.

    Limits the rate of operations to a specified number per second.
    Thread-safe for concurrent usage.

    Example:
        limiter = RateLimiter(requests_per_second=10)

        for item in items:
            limiter.acquire()  # Blocks until allowed
            make_api_call(item)
    """

    def __init__(
        self,
        requests_per_second: float,
        *,
        burst_size: int | None = None,
    ) -> None:
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum sustained rate
            burst_size: Maximum burst capacity (defaults to 1)
        """
        self.rate = requests_per_second
        self.burst_size = burst_size or 1
        self.tokens = float(self.burst_size)
        self.last_update = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a token, blocking until available.

        Args:
            timeout: Maximum time to wait (None = wait forever)

        Returns:
            True if token acquired, False if timeout expired
        """
        start_time = time.monotonic()

        while True:
            with self._lock:
                self._refill_tokens()

                if self.tokens >= 1:
                    self.tokens -= 1
                    return True

                # Calculate wait time for next token
                wait_time = (1 - self.tokens) / self.rate

            # Check timeout
            if timeout is not None:
                elapsed = time.monotonic() - start_time
                if elapsed + wait_time > timeout:
                    return False

            # Wait and retry
            time.sleep(min(wait_time, 0.1))  # Cap wait at 100ms increments

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(
            self.burst_size,
            self.tokens + elapsed * self.rate,
        )
        self.last_update = now

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if token acquired, False if not available
        """
        with self._lock:
            self._refill_tokens()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False


def rate_limited(
    requests_per_second: float,
    *,
    burst_size: int | None = None,
) -> Callable[[F], F]:
    """Decorator to rate-limit a function.

    Example:
        @rate_limited(10)  # 10 requests per second
        def call_api(item):
            return requests.get(f"https://api.example.com/{item}")

        # Can also be used with burst capacity
        @rate_limited(5, burst_size=10)  # 5 RPS sustained, burst of 10
        def call_api(item):
            ...
    """
    limiter = RateLimiter(requests_per_second, burst_size=burst_size)

    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            limiter.acquire()
            return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


# ============================================================================
# API Source
# ============================================================================

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_USER_AGENT = user_agent(
    "bronze-foundry",
    "dev",
    extras=[
        ("httpx", getattr(httpx, "__version__", "unknown")),
        ("tenacity", getattr(tenacity, "__version__", "unknown")),
        ("requests-toolbelt", getattr(requests_toolbelt, "__version__", "unknown")),
    ],
)

__all__ = [
    # Authentication
    "AuthType",
    "AuthConfig",
    "build_auth_headers",
    "build_auth_headers_from_dict",
    # Pagination
    "PaginationStrategy",
    "PaginationConfig",
    "PaginationState",
    "NoPaginationState",
    "OffsetPaginationState",
    "PagePaginationState",
    "CursorPaginationState",
    "build_pagination_state",
    "build_pagination_config_from_dict",
    # Rate Limiting
    "RateLimiter",
    "rate_limited",
    # API Source
    "ApiSource",
    "ApiOutputMetadata",
    "create_api_source_from_options",
]

# Backwards compatibility: ApiOutputMetadata is now OutputMetadata
ApiOutputMetadata = OutputMetadata


@dataclass
class ApiSource:
    """Declarative API extraction source.

    Provides full-featured REST API extraction with authentication,
    pagination, rate limiting, and watermark-based incremental loading.

    Example:
        source = ApiSource(
            system="sales_api",
            entity="orders",
            base_url="https://api.example.com",
            endpoint="/v1/orders",
            target_path="./bronze/sales/orders/dt={run_date}/",
            auth=AuthConfig(auth_type=AuthType.BEARER, token="${API_TOKEN}"),
            pagination=PaginationConfig(
                strategy=PaginationStrategy.OFFSET,
                page_size=100,
            ),
            data_path="data.orders",
            watermark_column="updated_at",
            watermark_param="since",
        )
        result = source.run("2025-01-15")
    """

    # Identity
    system: str  # Source system name
    entity: str  # API endpoint/resource name

    # API endpoint
    base_url: str  # Base URL (e.g., "https://api.example.com")
    endpoint: str  # Endpoint path (e.g., "/v1/orders")

    # Target
    target_path: str  # Where to land in Bronze

    # Authentication
    auth: Optional[AuthConfig] = None

    # Pagination
    pagination: Optional[PaginationConfig] = None

    # Data extraction
    data_path: Optional[str] = None  # Path to records in response (e.g., "data.items")

    # Rate limiting
    requests_per_second: Optional[float] = None
    burst_size: Optional[int] = None

    # Watermarking for incremental loads
    watermark_column: Optional[str] = None  # Column in response with timestamp
    watermark_param: Optional[str] = None  # Query param to pass last watermark

    # Request options
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)
    path_params: Dict[str, str] = field(default_factory=dict)  # URL path substitution
    timeout: float = 30.0

    # Session configuration
    max_retries: int = 3
    backoff_factor: float = 0.5
    pool_connections: int = 10
    pool_maxsize: int = 10

    # Artifact generation
    write_checksums: bool = True
    write_metadata: bool = True

    def __post_init__(self) -> None:
        """Validate configuration on instantiation."""
        errors = self._validate()
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"ApiSource configuration errors for {self.system}.{self.entity}:\n"
                f"{error_msg}\n\n"
                "Fix the configuration and try again."
            )

    def _validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors: List[str] = []

        if not self.system:
            errors.append("system is required (source system name)")

        if not self.entity:
            errors.append("entity is required (API resource name)")

        if not self.base_url:
            errors.append("base_url is required (e.g., 'https://api.example.com')")

        if not self.endpoint:
            errors.append("endpoint is required (e.g., '/v1/orders')")

        if not self.target_path:
            errors.append("target_path is required")

        # Watermark validation
        if self.watermark_column and not self.watermark_param:
            logger.warning(
                "watermark_column is set but watermark_param is not. "
                "The watermark value won't be passed to the API."
            )

        return errors

    def run(
        self,
        run_date: str,
        *,
        target_override: Optional[str] = None,
        skip_if_exists: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute the API extraction.

        Args:
            run_date: The date for this extraction run (YYYY-MM-DD)
            target_override: Override the target path
            skip_if_exists: Skip if data already exists
            dry_run: Validate without extracting

        Returns:
            Dictionary with extraction results
        """
        target = self._resolve_target(run_date, target_override)

        skip_result = maybe_skip_if_exists(
            skip_if_exists=skip_if_exists,
            exists_fn=lambda: self._already_exists(target),
            target=target,
            logger=logger,
            context=f"{self.system}.{self.entity}",
        )
        if skip_result:
            return skip_result

        dry_run_result = maybe_dry_run(
            dry_run=dry_run,
            logger=logger,
            message="[DRY RUN] Would extract %s.%s from %s%s to %s",
            message_args=(self.system, self.entity, self.base_url, self.endpoint, target),
            target=target,
            extra={"base_url": self.base_url, "endpoint": self.endpoint},
        )
        if dry_run_result:
            return dry_run_result

        # Get watermark for incremental loads
        last_watermark = None
        if self.watermark_column:
            last_watermark = get_watermark(self.system, self.entity)
            if last_watermark:
                logger.info(
                    "Incremental load for %s.%s from watermark: %s",
                    self.system,
                    self.entity,
                    last_watermark,
                )

        # Fetch records from API
        records, pages_fetched, total_requests = self._fetch_all(
            run_date, last_watermark
        )

        if not records:
            logger.warning("No records fetched from API for %s.%s", self.system, self.entity)
            return {
                "row_count": 0,
                "target": target,
                "pages_fetched": pages_fetched,
                "total_requests": total_requests,
            }

        # Add Bronze metadata
        now = utc_now_iso()
        for record in records:
            record["_load_date"] = run_date
            record["_extracted_at"] = now
            record["_source_system"] = self.system
            record["_source_entity"] = self.entity

        # Write to target
        result = self._write(records, target, run_date, last_watermark, pages_fetched, total_requests)

        # Save new watermark if applicable
        if self.watermark_column and records:
            new_watermark = self._compute_max_watermark(records)
            if new_watermark:
                save_watermark(self.system, self.entity, str(new_watermark))
                result["new_watermark"] = str(new_watermark)

        return result

    def _resolve_target(self, run_date: str, target_override: Optional[str]) -> str:
        """Resolve target path with template substitution."""
        return resolve_target_path(
            template=self.target_path,
            target_override=target_override,
            env_var="BRONZE_TARGET_ROOT",
            format_vars={
                "system": self.system,
                "entity": self.entity,
                "run_date": run_date,
            },
        )

    def _already_exists(self, target: str) -> bool:
        """Check if data already exists at target."""
        return path_has_data(target)

    def _fetch_all(
        self,
        run_date: str,
        last_watermark: Optional[str],
    ) -> tuple[List[Dict[str, Any]], int, int]:
        """Fetch all records from the API with pagination.

        Returns:
            Tuple of (records, pages_fetched, total_requests)
        """
        headers, auth_tuple = build_auth_headers(self.auth, extra_headers=self.headers)
        headers.setdefault("User-Agent", _USER_AGENT)

        endpoint = self._format_endpoint()

        base_params = dict(expand_options(self.params))
        if last_watermark and self.watermark_param:
            base_params[self.watermark_param] = last_watermark

        limiter = None
        if self.requests_per_second:
            limiter = RateLimiter(
                self.requests_per_second,
                burst_size=self.burst_size,
            )

        pagination_config = self.pagination or PaginationConfig(
            strategy=PaginationStrategy.NONE
        )
        state = build_pagination_state(pagination_config, base_params)

        all_records: List[Dict[str, Any]] = []
        pages_fetched = 0
        total_requests = 0

        with self._create_httpx_client() as client:
            while state.should_fetch_more():
                if limiter:
                    limiter.acquire()

                params = state.build_params()
                response, attempts = self._fetch_page_with_retry(
                    client=client,
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    auth=auth_tuple,
                )
                total_requests += attempts
                pages_fetched += 1

                data = response.json()
                records = self._extract_records(data)

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

                if not state.on_response(records, data):
                    break

        if (
            isinstance(state, PagePaginationState)
            and state.max_pages_limit_hit
        ):
            logger.info("Reached max_pages limit of %d", pagination_config.max_pages)

        logger.info(
            "Successfully fetched %d records from %s.%s in %d pages (%d requests)",
            len(all_records),
            self.system,
            self.entity,
            pages_fetched,
            total_requests,
        )

        return all_records, pages_fetched, total_requests

    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """Extract records from API response.

        Uses data_path to navigate nested response structures.
        """
        # Navigate to data using data_path
        if self.data_path:
            for key in self.data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                else:
                    logger.warning("Cannot navigate path '%s' in response", self.data_path)
                    break

        # Convert to list of records
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Try common patterns
            for key in ("items", "data", "results", "records"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # Wrap single record
            return [data]
        else:
            logger.warning("Unexpected data type: %s", type(data))
            return []

    def _compute_max_watermark(self, records: List[Dict[str, Any]]) -> Optional[str]:
        """Compute the maximum watermark value from records.

        Handles different value types appropriately:
        - datetime objects: compared directly
        - ISO 8601 strings: parsed to datetime for comparison
        - numeric values: compared numerically
        - other strings: compared lexicographically (fallback)
        """
        if not self.watermark_column:
            return None

        watermark_values = [
            r[self.watermark_column]
            for r in records
            if self.watermark_column in r and r[self.watermark_column] is not None
        ]

        if not watermark_values:
            return None

        try:
            # Try to determine the type and compare appropriately
            sample = watermark_values[0]

            # If already datetime, compare directly
            if isinstance(sample, datetime):
                max_val = max(watermark_values)
                return max_val.isoformat() if hasattr(max_val, 'isoformat') else str(max_val)

            # If numeric, compare numerically
            if isinstance(sample, (int, float)):
                return str(max(watermark_values))

            # If string, try to parse as ISO 8601 datetime
            if isinstance(sample, str):
                # Try parsing as datetime for proper temporal comparison
                try:
                    parsed_values = []
                    for v in watermark_values:
                        # Handle various ISO formats
                        v_str = str(v)
                        # Try fromisoformat (handles most ISO 8601 formats)
                        try:
                            parsed = datetime.fromisoformat(v_str.replace('Z', '+00:00'))
                        except ValueError:
                            # Try parsing date-only format
                            parsed = datetime.strptime(v_str[:10], '%Y-%m-%d')
                        parsed_values.append((parsed, v_str))

                    # Find max by parsed datetime, return original string
                    max_parsed, max_original = max(parsed_values, key=lambda x: x[0])
                    return max_original
                except (ValueError, TypeError):
                    # Fall back to string comparison if parsing fails
                    pass

            # Fallback: convert to strings and compare
            str_values = [str(v) for v in watermark_values]
            return max(str_values)
        except (TypeError, ValueError) as e:
            logger.warning(
                "Could not compute max watermark from '%s': %s",
                self.watermark_column,
                e,
            )
            return None

    def _write(
        self,
        records: List[Dict[str, Any]],
        target: str,
        run_date: str,
        last_watermark: Optional[str],
        pages_fetched: int,
        total_requests: int,
    ) -> Dict[str, Any]:
        """Write records to target with metadata and checksums."""
        import ibis

        # Create Ibis table from records
        t = ibis.memtable(records)

        row_count = len(records)
        # Use shared column inference function
        columns = infer_column_types(records)
        now = utc_now_iso()

        # Write to target
        output_dir = Path(target)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self.entity}.parquet"
        t.to_parquet(str(output_file))

        data_files = [str(output_file)]

        # API-specific metadata fields
        api_extra = {
            "system": self.system,
            "entity": self.entity,
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "watermark_column": self.watermark_column,
            "last_watermark": last_watermark,
            "pages_fetched": pages_fetched,
            "total_requests": total_requests,
        }

        # Write metadata
        if self.write_metadata:
            metadata = OutputMetadata(
                row_count=row_count,
                columns=columns,
                written_at=now,
                run_date=run_date,
                data_files=[Path(f).name for f in data_files],
                extra=api_extra,
            )
            metadata_file = output_dir / "_metadata.json"
            metadata_file.write_text(metadata.to_json(), encoding="utf-8")
            logger.debug("Wrote metadata to %s", metadata_file)

        # Write checksums
        if self.write_checksums:
            write_checksum_manifest(
                output_dir,
                [Path(f) for f in data_files],
                entity_kind="bronze",
                row_count=row_count,
                extra_metadata={
                    "system": self.system,
                    "entity": self.entity,
                    "source_type": "api_rest",
                },
            )

        logger.info(
            "Wrote %d rows for %s.%s to %s",
            row_count,
            self.system,
            self.entity,
            target,
        )

        result: Dict[str, Any] = {
            "row_count": row_count,
            "target": target,
            "columns": [c["name"] for c in columns],
            "files": [Path(f).name for f in data_files],
            "pages_fetched": pages_fetched,
            "total_requests": total_requests,
        }

        if self.write_metadata:
            result["metadata_file"] = "_metadata.json"
        if self.write_checksums:
            result["checksums_file"] = "_checksums.json"

        return result

    def _format_endpoint(self) -> str:
        endpoint = self.endpoint
        for key, value in self.path_params.items():
            endpoint = endpoint.replace(f"{{{key}}}", expand_env_vars(value))
        return endpoint

    def _create_httpx_client(self) -> httpx.Client:
        base_url = self.base_url.rstrip("/")
        limits = httpx.Limits(
            max_connections=self.pool_maxsize,
            max_keepalive_connections=self.pool_connections,
        )
        return httpx.Client(
            base_url=base_url,
            timeout=self.timeout,
            limits=limits,
        )

    def _fetch_page_with_retry(
        self,
        *,
        client: httpx.Client,
        endpoint: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        auth: Optional[tuple[str, str]],
    ) -> tuple[httpx.Response, int]:
        attempts = 0

        @retry(
            stop=stop_after_attempt(max(self.max_retries, 1)),
            wait=wait_exponential(
                multiplier=self.backoff_factor,
                min=0.5,
                max=30,
            ),
            retry=retry_if_exception(self._should_retry),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def do_request() -> httpx.Response:
            nonlocal attempts
            attempts += 1
            logger.debug("Fetching %s with params %s", endpoint, params)
            response = client.get(
                endpoint,
                headers=headers,
                params=params,
                auth=auth,
                timeout=self.timeout,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response and exc.response.status_code == 429:
                    self._respect_retry_after(exc.response)
                raise
            return response

        return do_request(), attempts

    def _should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code if exc.response else None
            return status_code in RETRYABLE_STATUS_CODES
        return isinstance(exc, httpx.RequestError)

    def _respect_retry_after(self, response: httpx.Response) -> None:
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return
        try:
            wait_seconds = float(retry_after)
        except (TypeError, ValueError):
            return
        if wait_seconds > 0:
            logger.warning(
                "Rate limited by API; sleeping %.1f seconds before retrying",
                wait_seconds,
            )
            time.sleep(wait_seconds)


def create_api_source_from_options(
    system: str,
    entity: str,
    options: Dict[str, Any],
    target_path: str,
) -> ApiSource:
    """Create ApiSource from a dictionary of options.

    Convenience function for creating ApiSource from BronzeSource options dict.

    Expected option keys:
        - base_url: API base URL
        - endpoint: API endpoint path
        - auth_type: "none", "bearer", "api_key", "basic"
        - token: Bearer token (for auth_type=bearer)
        - api_key: API key (for auth_type=api_key)
        - api_key_header: Header for API key (default: X-API-Key)
        - username: Username (for auth_type=basic)
        - password: Password (for auth_type=basic)
        - pagination_type: "none", "offset", "page", "cursor"
        - page_size: Records per page
        - data_path: Path to records in response
        - requests_per_second: Rate limit
        - watermark_column: Column for incremental loads
        - watermark_param: Query param for watermark
        - headers: Additional headers dict
        - params: Additional query params dict
        - path_params: URL path substitutions

    Args:
        system: Source system name
        entity: Entity/resource name
        options: Configuration options
        target_path: Where to write data

    Returns:
        Configured ApiSource instance
    """
    # Build auth config
    auth_type_str = options.get("auth_type", "none").lower()
    try:
        auth_type = AuthType(auth_type_str)
    except ValueError:
        raise ValueError(f"Unsupported auth_type: '{auth_type_str}'")

    auth = None
    if auth_type != AuthType.NONE:
        auth = AuthConfig(
            auth_type=auth_type,
            token=options.get("token"),
            api_key=options.get("api_key"),
            api_key_header=options.get("api_key_header", "X-API-Key"),
            username=options.get("username"),
            password=options.get("password"),
        )

    # Build pagination config
    pagination = None
    if "pagination_type" in options:
        pagination = build_pagination_config_from_dict(options)

    return ApiSource(
        system=system,
        entity=entity,
        base_url=options.get("base_url", ""),
        endpoint=options.get("endpoint", ""),
        target_path=target_path,
        auth=auth,
        pagination=pagination,
        data_path=options.get("data_path"),
        requests_per_second=options.get("requests_per_second"),
        burst_size=options.get("burst_size"),
        watermark_column=options.get("watermark_column"),
        watermark_param=options.get("watermark_param"),
        headers=options.get("headers", {}),
        params=options.get("params", {}),
        path_params=options.get("path_params", {}),
        timeout=options.get("timeout", 30.0),
        max_retries=options.get("max_retries", 3),
        write_checksums=options.get("write_checksums", True),
        write_metadata=options.get("write_metadata", True),
    )
