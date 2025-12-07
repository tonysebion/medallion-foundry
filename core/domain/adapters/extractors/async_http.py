"""Backward compatibility shim for async HTTP helpers."""

from core.infrastructure.io.http.session import AsyncApiClient, is_async_enabled

__all__ = ["AsyncApiClient", "is_async_enabled"]
