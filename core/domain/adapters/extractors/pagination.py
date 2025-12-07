"""Pagination helpers shared by API extractor."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class PaginationState:
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


class NoPaginationState(PaginationState):
    def on_records(self, records: List[Dict[str, Any]], data: Any) -> bool:
        return False


class OffsetPaginationState(PaginationState):
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


class PagePaginationState(PaginationState):
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


class CursorPaginationState(PaginationState):
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


def build_pagination_state(
    pagination_cfg: Dict[str, Any], params: Dict[str, Any]
) -> PaginationState:
    pagination_type = (pagination_cfg or {}).get("type", "none")
    if pagination_type == "offset":
        return OffsetPaginationState(pagination_cfg, params)
    if pagination_type == "page":
        return PagePaginationState(pagination_cfg, params)
    if pagination_type == "cursor":
        return CursorPaginationState(pagination_cfg, params)
    return NoPaginationState(pagination_cfg, params)
