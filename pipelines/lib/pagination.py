"""Pagination strategies for API extraction.

Provides state machines for different pagination patterns commonly used
by REST APIs. Supports offset-based, page-based, and cursor-based pagination.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

__all__ = [
    "PaginationStrategy",
    "PaginationConfig",
    "PaginationState",
    "NoPaginationState",
    "OffsetPaginationState",
    "PagePaginationState",
    "CursorPaginationState",
    "build_pagination_state",
]


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
