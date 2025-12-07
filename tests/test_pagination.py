"""Unit tests for pagination helpers used by API extractors."""


from core.domain.adapters.extractors.pagination import (
    PaginationState,
    NoPaginationState,
    OffsetPaginationState,
    PagePaginationState,
    CursorPaginationState,
    build_pagination_state,
)


class TestBuildPaginationState:
    """Tests for the build_pagination_state factory function."""

    def test_none_type_returns_no_pagination(self):
        """Test that 'none' type returns NoPaginationState."""
        state = build_pagination_state({"type": "none"}, {})
        assert isinstance(state, NoPaginationState)

    def test_missing_type_returns_no_pagination(self):
        """Test that missing type defaults to NoPaginationState."""
        state = build_pagination_state({}, {})
        assert isinstance(state, NoPaginationState)

    def test_none_config_returns_no_pagination(self):
        """Test that None config returns NoPaginationState."""
        state = build_pagination_state(None, {})
        assert isinstance(state, NoPaginationState)

    def test_offset_type_returns_offset_pagination(self):
        """Test that 'offset' type returns OffsetPaginationState."""
        state = build_pagination_state({"type": "offset"}, {})
        assert isinstance(state, OffsetPaginationState)

    def test_page_type_returns_page_pagination(self):
        """Test that 'page' type returns PagePaginationState."""
        state = build_pagination_state({"type": "page"}, {})
        assert isinstance(state, PagePaginationState)

    def test_cursor_type_returns_cursor_pagination(self):
        """Test that 'cursor' type returns CursorPaginationState."""
        state = build_pagination_state({"type": "cursor"}, {})
        assert isinstance(state, CursorPaginationState)


class TestPaginationStateBase:
    """Tests for base PaginationState class."""

    def test_default_should_fetch_more(self):
        """Test default should_fetch_more returns True."""
        state = PaginationState({}, {})
        assert state.should_fetch_more() is True

    def test_build_params_returns_base_params(self):
        """Test build_params returns base params."""
        state = PaginationState({}, {"filter": "active"})
        params = state.build_params()
        assert params == {"filter": "active"}

    def test_on_records_returns_false(self):
        """Test on_records returns False by default."""
        state = PaginationState({}, {})
        assert state.on_records([{"id": 1}], {}) is False

    def test_describe_returns_no_pagination(self):
        """Test describe returns correct string."""
        state = PaginationState({}, {})
        assert state.describe() == "(no pagination)"

    def test_max_records_from_config(self):
        """Test max_records is extracted from config."""
        state = PaginationState({"max_records": 1000}, {})
        assert state.max_records == 1000


class TestNoPaginationState:
    """Tests for NoPaginationState."""

    def test_on_records_always_returns_false(self):
        """Test that NoPaginationState never continues."""
        state = NoPaginationState({}, {})
        # Even with many records, should stop
        assert state.on_records([{"id": i} for i in range(1000)], {}) is False


class TestOffsetPaginationState:
    """Tests for OffsetPaginationState."""

    def test_default_parameters(self):
        """Test default offset/limit parameter names."""
        state = OffsetPaginationState({"type": "offset"}, {})
        assert state.offset_param == "offset"
        assert state.limit_param == "limit"
        assert state.page_size == 100

    def test_custom_parameters(self):
        """Test custom parameter names from config."""
        cfg = {
            "type": "offset",
            "offset_param": "skip",
            "limit_param": "take",
            "page_size": 50,
        }
        state = OffsetPaginationState(cfg, {})
        assert state.offset_param == "skip"
        assert state.limit_param == "take"
        assert state.page_size == 50

    def test_build_params_includes_offset_and_limit(self):
        """Test build_params includes offset and limit."""
        state = OffsetPaginationState({"type": "offset", "page_size": 25}, {"query": "test"})
        params = state.build_params()

        assert params["offset"] == 0
        assert params["limit"] == 25
        assert params["query"] == "test"

    def test_offset_increments_on_full_page(self):
        """Test offset increments when full page received."""
        state = OffsetPaginationState({"type": "offset", "page_size": 10}, {})

        # First page - full
        records = [{"id": i} for i in range(10)]
        assert state.on_records(records, {}) is True
        assert state.offset == 10

        # Second page - full
        assert state.on_records(records, {}) is True
        assert state.offset == 20

    def test_offset_stops_on_partial_page(self):
        """Test offset stops when partial page received."""
        state = OffsetPaginationState({"type": "offset", "page_size": 10}, {})

        # Partial page
        records = [{"id": i} for i in range(5)]
        assert state.on_records(records, {}) is False

    def test_offset_stops_on_empty_page(self):
        """Test offset stops when empty page received."""
        state = OffsetPaginationState({"type": "offset"}, {})
        assert state.on_records([], {}) is False

    def test_describe_shows_offset(self):
        """Test describe shows current offset."""
        state = OffsetPaginationState({"type": "offset", "page_size": 10}, {})
        state.build_params()  # Initialize _last_offset
        assert state.describe() == "at offset 0"

        state.on_records([{"id": i} for i in range(10)], {})
        state.build_params()
        assert state.describe() == "at offset 10"


class TestPagePaginationState:
    """Tests for PagePaginationState."""

    def test_default_parameters(self):
        """Test default page parameter names."""
        state = PagePaginationState({"type": "page"}, {})
        assert state.page_param == "page"
        assert state.page_size_param == "page_size"
        assert state.page_size == 100
        assert state.page == 1

    def test_custom_parameters(self):
        """Test custom parameter names from config."""
        cfg = {
            "type": "page",
            "page_param": "p",
            "page_size_param": "size",
            "page_size": 20,
        }
        state = PagePaginationState(cfg, {})
        assert state.page_param == "p"
        assert state.page_size_param == "size"
        assert state.page_size == 20

    def test_build_params_includes_page_and_size(self):
        """Test build_params includes page and page_size."""
        state = PagePaginationState({"type": "page", "page_size": 25}, {"filter": "all"})
        params = state.build_params()

        assert params["page"] == 1
        assert params["page_size"] == 25
        assert params["filter"] == "all"

    def test_page_increments_on_full_page(self):
        """Test page increments when full page received."""
        state = PagePaginationState({"type": "page", "page_size": 10}, {})

        records = [{"id": i} for i in range(10)]
        assert state.on_records(records, {}) is True
        assert state.page == 2

    def test_page_stops_on_partial_page(self):
        """Test page stops when partial page received."""
        state = PagePaginationState({"type": "page", "page_size": 10}, {})

        records = [{"id": i} for i in range(5)]
        assert state.on_records(records, {}) is False

    def test_max_pages_limit(self):
        """Test max_pages limits pagination."""
        state = PagePaginationState({"type": "page", "page_size": 10, "max_pages": 3}, {})

        # Page 1
        records = [{"id": i} for i in range(10)]
        state.on_records(records, {})
        assert state.page == 2
        assert state.should_fetch_more() is True

        # Page 2
        state.on_records(records, {})
        assert state.page == 3
        assert state.should_fetch_more() is True

        # Page 3
        state.on_records(records, {})
        assert state.page == 4
        # Now should stop
        assert state.should_fetch_more() is False
        assert state.max_pages_limit_hit is True

    def test_describe_shows_page(self):
        """Test describe shows current page."""
        state = PagePaginationState({"type": "page"}, {})
        state.build_params()
        assert state.describe() == "from page 1"


class TestCursorPaginationState:
    """Tests for CursorPaginationState."""

    def test_default_parameters(self):
        """Test default cursor parameter names."""
        state = CursorPaginationState({"type": "cursor"}, {})
        assert state.cursor_param == "cursor"
        assert state.cursor_path == "next_cursor"
        assert state.cursor is None

    def test_custom_parameters(self):
        """Test custom parameter names from config."""
        cfg = {
            "type": "cursor",
            "cursor_param": "after",
            "cursor_path": "pagination.next",
        }
        state = CursorPaginationState(cfg, {})
        assert state.cursor_param == "after"
        assert state.cursor_path == "pagination.next"

    def test_build_params_excludes_cursor_initially(self):
        """Test build_params excludes cursor on first call."""
        state = CursorPaginationState({"type": "cursor"}, {"query": "test"})
        params = state.build_params()

        assert "cursor" not in params
        assert params["query"] == "test"

    def test_build_params_includes_cursor_after_records(self):
        """Test build_params includes cursor after on_records."""
        state = CursorPaginationState({"type": "cursor"}, {})

        # First page - no cursor
        params = state.build_params()
        assert "cursor" not in params

        # Receive records with next cursor
        data = {"next_cursor": "abc123"}
        state.on_records([{"id": 1}], data)

        # Second page - cursor included
        params = state.build_params()
        assert params["cursor"] == "abc123"

    def test_cursor_extracted_from_nested_path(self):
        """Test cursor is extracted from nested path."""
        cfg = {
            "type": "cursor",
            "cursor_path": "meta.pagination.next_cursor",
        }
        state = CursorPaginationState(cfg, {})

        data = {
            "meta": {
                "pagination": {
                    "next_cursor": "nested_value"
                }
            }
        }
        state.on_records([{"id": 1}], data)

        assert state.cursor == "nested_value"

    def test_cursor_stops_on_missing_cursor(self):
        """Test pagination stops when cursor is missing."""
        state = CursorPaginationState({"type": "cursor"}, {})

        # No next_cursor in response
        result = state.on_records([{"id": 1}], {"data": "value"})
        assert result is False

    def test_cursor_stops_on_empty_records(self):
        """Test pagination stops on empty records."""
        state = CursorPaginationState({"type": "cursor"}, {})
        result = state.on_records([], {"next_cursor": "abc"})
        assert result is False

    def test_cursor_continues_with_cursor(self):
        """Test pagination continues when cursor present."""
        state = CursorPaginationState({"type": "cursor"}, {})
        result = state.on_records([{"id": 1}], {"next_cursor": "abc"})
        assert result is True

    def test_describe_shows_cursor(self):
        """Test describe shows cursor info."""
        state = CursorPaginationState({"type": "cursor"}, {})
        assert "cursor pagination" in state.describe()

        state.on_records([{"id": 1}], {"next_cursor": "xyz"})
        assert "xyz" in state.describe()


class TestPaginationWithBaseParams:
    """Tests for pagination preserving base params."""

    def test_offset_preserves_base_params(self):
        """Test offset pagination preserves base params."""
        base = {"api_key": "secret", "format": "json"}
        state = OffsetPaginationState({"type": "offset"}, base)

        params = state.build_params()
        assert params["api_key"] == "secret"
        assert params["format"] == "json"
        assert "offset" in params
        assert "limit" in params

    def test_page_preserves_base_params(self):
        """Test page pagination preserves base params."""
        base = {"filter": "active", "sort": "name"}
        state = PagePaginationState({"type": "page"}, base)

        params = state.build_params()
        assert params["filter"] == "active"
        assert params["sort"] == "name"
        assert "page" in params
        assert "page_size" in params

    def test_cursor_preserves_base_params(self):
        """Test cursor pagination preserves base params."""
        base = {"include": "all", "fields": "id,name"}
        state = CursorPaginationState({"type": "cursor"}, base)

        params = state.build_params()
        assert params["include"] == "all"
        assert params["fields"] == "id,name"
