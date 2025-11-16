"""Async HTTP client for API extractors using httpx.

Provides optional async path with bounded concurrency for improved throughput
on pagination-heavy workloads.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Conditional import - httpx is optional
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore


class AsyncApiClient:
    """Async HTTP client with retry and rate limiting support."""
    
    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str],
        auth: Optional[Tuple[str, str]] = None,
        timeout: int = 30,
        max_concurrent: int = 5,
    ):
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx is required for async HTTP. Install via: pip install httpx")
        
        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.auth = auth
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make async GET request with concurrency control.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dict
            
        Raises:
            httpx.HTTPError: On request failures
        """
        url = f"{self.base_url}{endpoint}"
        
        async with self._semaphore:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.debug(f"Async request to {url} with params {params}")
                
                kwargs: Dict[str, Any] = {
                    "headers": self.headers,
                    "params": params or {},
                }
                if self.auth:
                    kwargs["auth"] = self.auth
                
                response = await client.get(url, **kwargs)
                response.raise_for_status()
                return response.json()
    
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
    async_enabled = api_cfg.get("async", False)
    
    # Check environment override
    if os.environ.get("BRONZE_ASYNC_HTTP"):
        async_enabled = os.environ["BRONZE_ASYNC_HTTP"].lower() in ("1", "true", "yes")
    
    return async_enabled
