"""Async HTTP client for API extractors using httpx.

Provides optional async path with bounded concurrency for improved throughput
on pagination-heavy workloads.

Connection Pooling:
    The AsyncApiClient now supports connection pooling via HttpPoolConfig.
    A single httpx.AsyncClient instance is reused across requests, significantly
    reducing connection overhead for high-volume API extractions.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.platform.resilience import RetryPolicy, CircuitBreaker, execute_with_retry_async

logger = logging.getLogger(__name__)

# Conditional import - httpx is optional
httpx: Any = None
try:
    import httpx as _httpx

    HTTPX_AVAILABLE = True
    httpx = _httpx
except Exception:
    HTTPX_AVAILABLE = False
    httpx = None


@dataclass
class HttpPoolConfig:
    """Configuration for HTTP connection pooling (async httpx).

    Attributes:
        max_connections: Maximum total connections in the pool
        max_keepalive_connections: Maximum idle connections to keep alive
        keepalive_expiry: Seconds before idle connections expire
        http2: Enable HTTP/2 support (requires h2 package)
    """

    max_connections: int = 100
    max_keepalive_connections: int = 20
    keepalive_expiry: float = 30.0
    http2: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "HttpPoolConfig":
        """Create from dictionary configuration."""
        if data is None:
            return cls()
        return cls(
            max_connections=data.get("max_connections", 100),
            max_keepalive_connections=data.get("max_keepalive_connections", 20),
            keepalive_expiry=data.get("keepalive_expiry", 30.0),
            http2=data.get("http2", False),
        )


@dataclass
class SyncPoolConfig:
    """Configuration for synchronous HTTP connection pooling (requests).

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


@dataclass
class ConnectionMetrics:
    """Metrics for connection pool usage.

    Attributes:
        requests_made: Total number of requests made
        connections_created: Number of new connections created
        connections_reused: Number of times existing connections were reused
    """

    requests_made: int = 0
    connections_created: int = 0
    connections_reused: int = 0

    @property
    def reuse_ratio(self) -> float:
        """Calculate connection reuse ratio (0.0 to 1.0)."""
        if self.requests_made == 0:
            return 0.0
        return self.connections_reused / self.requests_made


class AsyncApiClient:
    """Async HTTP client with retry, rate limiting, and connection pooling support.

    The client maintains a persistent httpx.AsyncClient for connection reuse.
    Use as a context manager for proper cleanup, or call close() explicitly.

    Example:
        async with AsyncApiClient(base_url, headers) as client:
            data = await client.get("/endpoint")

        # Or manual management:
        client = AsyncApiClient(base_url, headers)
        try:
            data = await client.get("/endpoint")
        finally:
            await client.close()
    """

    _breaker = CircuitBreaker(
        failure_threshold=5, cooldown_seconds=30.0, half_open_max_calls=1
    )

    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str],
        auth: Optional[Tuple[str, str]] = None,
        timeout: int = 30,
        max_concurrent: int = 5,
        pool_config: Optional[HttpPoolConfig] = None,
    ):
        if not HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for async HTTP. Install via: pip install httpx"
            )

        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.auth = auth
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._pool_config = pool_config or HttpPoolConfig()
        self._client: Optional[Any] = None  # httpx.AsyncClient
        self._metrics = ConnectionMetrics()

    async def _get_client(self) -> Any:
        """Get or create the pooled httpx.AsyncClient.

        Creates the client lazily on first use with configured pool limits.
        """
        if self._client is None:
            limits = httpx.Limits(
                max_connections=self._pool_config.max_connections,
                max_keepalive_connections=self._pool_config.max_keepalive_connections,
                keepalive_expiry=self._pool_config.keepalive_expiry,
            )
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=limits,
                http2=self._pool_config.http2,
            )
            self._metrics.connections_created += 1
            logger.debug(
                "Created pooled httpx client with limits: max=%d, keepalive=%d",
                self._pool_config.max_connections,
                self._pool_config.max_keepalive_connections,
            )
        else:
            self._metrics.connections_reused += 1
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client and release connections."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.debug(
                "Closed pooled client. Metrics: requests=%d, created=%d, reused=%d, ratio=%.2f",
                self._metrics.requests_made,
                self._metrics.connections_created,
                self._metrics.connections_reused,
                self._metrics.reuse_ratio,
            )

    async def __aenter__(self) -> "AsyncApiClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit - closes the client."""
        await self.close()

    @property
    def metrics(self) -> ConnectionMetrics:
        """Get connection pool metrics."""
        return self._metrics

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make async GET request with concurrency control and retry.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            httpx.HTTPError: On request failures after retries
        """
        url = f"{self.base_url}{endpoint}"

        async def _once() -> Dict[str, Any]:
            async with self._semaphore:
                client = await self._get_client()
                self._metrics.requests_made += 1
                logger.debug("Async request to %s with params %s", url, params)

                kwargs: Dict[str, Any] = {
                    "headers": self.headers,
                    "params": params or {},
                }
                if self.auth:
                    kwargs["auth"] = self.auth

                response = await client.get(url, **kwargs)
                response.raise_for_status()
                from typing import cast

                return cast(Dict[str, Any], response.json())

        def _retry_if(exc: BaseException) -> bool:
            # Retry timeouts, connection errors, and 5xx/429
            if "httpx" in str(type(exc).__module__):
                # httpx.TimeoutException, httpx.ConnectError, etc.
                if "Timeout" in type(exc).__name__ or "Connect" in type(exc).__name__:
                    return True
                # httpx.HTTPStatusError
                if hasattr(exc, "response"):
                    status = getattr(exc.response, "status_code", None)
                    if status is not None:
                        try:
                            status_int = int(status)
                            return status_int == 429 or 500 <= status_int < 600
                        except Exception:
                            return False
            return False

        def _delay_from_exc(
            exc: BaseException, attempt: int, default_delay: float
        ) -> Optional[float]:
            if hasattr(exc, "response"):
                resp = getattr(exc, "response", None)
                if resp and hasattr(resp, "headers"):
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            return float(retry_after)
                        except Exception:
                            pass
            return None

        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=8.0,
            backoff_multiplier=2.0,
            jitter=0.2,
            retry_if=_retry_if,
            delay_from_exception=_delay_from_exc,
        )

        return await execute_with_retry_async(
            _once,
            policy=policy,
            breaker=AsyncApiClient._breaker,
            operation_name="async_api_get",
        )

    async def get_many(
        self,
        requests: List[Tuple[str, Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """Execute multiple GET requests concurrently.

        Args:
            requests: List of (endpoint, params) tuples

        Returns:
            List of JSON responses
        """
        tasks = [self.get(endpoint, params) for endpoint, params in requests]
        return await asyncio.gather(*tasks)


def is_async_enabled(api_cfg: Dict[str, Any]) -> bool:
    """Check if async HTTP should be used.

    Args:
        api_cfg: API configuration dictionary

    Returns:
        True if async is enabled and httpx is available
    """
    if not HTTPX_AVAILABLE:
        return False

    # Check config flag
    async_enabled = bool(api_cfg.get("async", False))

    # Check environment override
    if os.environ.get("BRONZE_ASYNC_HTTP"):
        async_enabled = os.environ["BRONZE_ASYNC_HTTP"].lower() in ("1", "true", "yes")

    return bool(async_enabled)
