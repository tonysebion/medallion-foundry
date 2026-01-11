"""Unit tests for pagination state machines and configuration.

These tests validate the pagination logic that was formerly in pagination.py
and is now integrated into api.py.

Tests cover:
- Offset pagination (offset/limit parameters)
- Page pagination (page/page_size parameters)
- Cursor pagination (cursor extraction from nested JSON)
- Edge cases: empty responses, partial pages, max_pages limits
"""

from pipelines.lib.api import (
    PaginationConfig,
    PaginationStrategy,
    NoPaginationState,
    OffsetPaginationState,
    PagePaginationState,
    CursorPaginationState,
    build_pagination_state,
    build_pagination_config_from_dict,
)


# ============================================================================
# PaginationConfig Tests
# ============================================================================


class TestPaginationConfig:
    """Tests for PaginationConfig dataclass."""

    def test_default_config_is_none_strategy(self):
        """Default pagination config should use NONE strategy."""
        config = PaginationConfig()
        assert config.strategy == PaginationStrategy.NONE
        assert config.page_size == 100

    def test_offset_config_defaults(self):
        """Offset pagination should have sensible defaults."""
        config = PaginationConfig(strategy=PaginationStrategy.OFFSET)
        assert config.offset_param == "offset"
        assert config.limit_param == "limit"
        assert config.page_size == 100

    def test_page_config_defaults(self):
        """Page pagination should have sensible defaults."""
        config = PaginationConfig(strategy=PaginationStrategy.PAGE)
        assert config.page_param == "page"
        assert config.page_size_param == "page_size"
        assert config.max_pages is None

    def test_cursor_config_defaults(self):
        """Cursor pagination should have sensible defaults."""
        config = PaginationConfig(strategy=PaginationStrategy.CURSOR)
        assert config.cursor_param == "cursor"
        assert config.cursor_path == "next_cursor"

    def test_custom_offset_params(self):
        """Custom offset/limit parameter names should be respected."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=50,
            offset_param="skip",
            limit_param="take",
        )
        assert config.offset_param == "skip"
        assert config.limit_param == "take"
        assert config.page_size == 50

    def test_max_records_default_is_unlimited(self):
        """max_records should default to 0 (unlimited)."""
        config = PaginationConfig()
        assert config.max_records == 0


# ============================================================================
# NoPaginationState Tests
# ============================================================================


class TestNoPaginationState:
    """Tests for single-request (non-paginated) APIs."""

    def test_should_fetch_once(self):
        """Should only fetch one page."""
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = NoPaginationState(config)

        assert state.should_fetch_more() is True
        state.on_response([{"id": 1}], {})
        assert state.should_fetch_more() is False

    def test_build_params_returns_base_params_only(self):
        """Should return only base parameters, no pagination params."""
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = NoPaginationState(config, base_params={"filter": "active"})

        params = state.build_params()
        assert params == {"filter": "active"}

    def test_describe_shows_no_pagination(self):
        """describe() should indicate no pagination."""
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = NoPaginationState(config)

        assert "no pagination" in state.describe().lower()

    def test_empty_response_still_marks_fetched(self):
        """Empty response should still mark as fetched."""
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = NoPaginationState(config)

        state.on_response([], {})
        assert state.should_fetch_more() is False


# ============================================================================
# OffsetPaginationState Tests
# ============================================================================


class TestOffsetPaginationState:
    """Tests for offset/limit pagination."""

    def test_initial_offset_is_zero(self):
        """First request should start at offset 0."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        params = state.build_params()
        assert params["offset"] == 0
        assert params["limit"] == 100

    def test_offset_increments_by_page_size(self):
        """Offset should increment by page_size after each response."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=50,
        )
        state = OffsetPaginationState(config)

        # First request
        state.build_params()
        state.on_response([{"id": i} for i in range(50)], {})

        # Second request should have offset=50
        params = state.build_params()
        assert params["offset"] == 50
        assert params["limit"] == 50

    def test_stops_on_empty_response(self):
        """Should stop fetching when response is empty."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        more = state.on_response([], {})
        assert more is False

    def test_stops_on_partial_page(self):
        """Should stop when response has fewer records than page_size."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        # Response with 50 records (less than page_size of 100)
        more = state.on_response([{"id": i} for i in range(50)], {})
        assert more is False

    def test_continues_on_full_page(self):
        """Should continue when response has exactly page_size records."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        more = state.on_response([{"id": i} for i in range(100)], {})
        assert more is True

    def test_custom_param_names(self):
        """Custom parameter names should be used."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=25,
            offset_param="skip",
            limit_param="take",
        )
        state = OffsetPaginationState(config)

        params = state.build_params()
        assert params["skip"] == 0
        assert params["take"] == 25
        assert "offset" not in params
        assert "limit" not in params

    def test_base_params_preserved(self):
        """Base parameters should be included in every request."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(
            config, base_params={"status": "active", "sort": "name"}
        )

        params = state.build_params()
        assert params["status"] == "active"
        assert params["sort"] == "name"
        assert params["offset"] == 0
        assert params["limit"] == 100

    def test_describe_shows_offset(self):
        """describe() should show current offset."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        state.on_response([{"id": i} for i in range(100)], {})
        state.build_params()

        assert "100" in state.describe()


class TestOffsetPaginationEdgeCases:
    """Edge case tests for offset pagination."""

    def test_single_record_response_stops_pagination(self):
        """A single record (less than page_size) should stop pagination."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=100,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        more = state.on_response([{"id": 1}], {})
        assert more is False

    def test_exactly_one_page_then_empty(self):
        """Full page followed by empty page should work correctly."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=10,
        )
        state = OffsetPaginationState(config)

        # First page - full
        state.build_params()
        more = state.on_response([{"id": i} for i in range(10)], {})
        assert more is True

        # Second page - empty
        state.build_params()
        more = state.on_response([], {})
        assert more is False

    def test_large_page_size(self):
        """Large page sizes should work correctly."""
        config = PaginationConfig(
            strategy=PaginationStrategy.OFFSET,
            page_size=10000,
        )
        state = OffsetPaginationState(config)

        state.build_params()
        state.on_response([{"id": i} for i in range(10000)], {})

        params = state.build_params()
        assert params["offset"] == 10000


# ============================================================================
# PagePaginationState Tests
# ============================================================================


class TestPagePaginationState:
    """Tests for page number pagination."""

    def test_initial_page_is_one(self):
        """First request should be page 1."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=50,
        )
        state = PagePaginationState(config)

        params = state.build_params()
        assert params["page"] == 1
        assert params["page_size"] == 50

    def test_page_increments_after_response(self):
        """Page number should increment after each response."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=50,
        )
        state = PagePaginationState(config)

        state.build_params()
        state.on_response([{"id": i} for i in range(50)], {})

        params = state.build_params()
        assert params["page"] == 2

    def test_stops_on_partial_page(self):
        """Should stop when response has fewer records than page_size."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
        )
        state = PagePaginationState(config)

        state.build_params()
        more = state.on_response([{"id": i} for i in range(50)], {})
        assert more is False

    def test_max_pages_limit(self):
        """Should respect max_pages limit."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=10,
            max_pages=3,
        )
        state = PagePaginationState(config)

        # Page 1
        assert state.should_fetch_more() is True
        state.build_params()
        state.on_response([{"id": i} for i in range(10)], {})

        # Page 2
        assert state.should_fetch_more() is True
        state.build_params()
        state.on_response([{"id": i} for i in range(10)], {})

        # Page 3
        assert state.should_fetch_more() is True
        state.build_params()
        state.on_response([{"id": i} for i in range(10)], {})

        # Page 4 should be blocked
        assert state.should_fetch_more() is False
        assert state.max_pages_limit_hit is True

    def test_custom_param_names(self):
        """Custom parameter names should be used."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=25,
            page_param="p",
            page_size_param="per_page",
        )
        state = PagePaginationState(config)

        params = state.build_params()
        assert params["p"] == 1
        assert params["per_page"] == 25
        assert "page" not in params
        assert "page_size" not in params

    def test_describe_shows_page(self):
        """describe() should show current page number."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
        )
        state = PagePaginationState(config)

        state.build_params()
        assert "1" in state.describe()


class TestPagePaginationEdgeCases:
    """Edge case tests for page pagination."""

    def test_max_pages_one_stops_after_first(self):
        """max_pages=1 should stop after first page."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
            max_pages=1,
        )
        state = PagePaginationState(config)

        assert state.should_fetch_more() is True
        state.build_params()
        state.on_response([{"id": i} for i in range(100)], {})

        # Should stop after max_pages
        assert state.should_fetch_more() is False

    def test_empty_first_page_stops_immediately(self):
        """Empty first page should stop pagination."""
        config = PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
        )
        state = PagePaginationState(config)

        state.build_params()
        more = state.on_response([], {})
        assert more is False


# ============================================================================
# CursorPaginationState Tests
# ============================================================================


class TestCursorPaginationState:
    """Tests for cursor-based pagination."""

    def test_first_request_has_no_cursor(self):
        """First request should not include cursor parameter."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_param="cursor",
        )
        state = CursorPaginationState(config)

        params = state.build_params()
        assert "cursor" not in params

    def test_subsequent_requests_include_cursor(self):
        """Subsequent requests should include cursor from response."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_param="cursor",
            cursor_path="next_cursor",
        )
        state = CursorPaginationState(config)

        # First request
        state.build_params()
        state.on_response([{"id": 1}], {"next_cursor": "abc123"})

        # Second request should have cursor
        params = state.build_params()
        assert params["cursor"] == "abc123"

    def test_stops_when_no_cursor_in_response(self):
        """Should stop when response has no next cursor."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next_cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        more = state.on_response([{"id": 1}], {"next_cursor": None})
        assert more is False

    def test_stops_when_cursor_missing_from_response(self):
        """Should stop when cursor key is missing from response."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next_cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        more = state.on_response([{"id": 1}], {"other_field": "value"})
        assert more is False

    def test_stops_on_empty_response(self):
        """Should stop when response has no records."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next_cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        more = state.on_response([], {"next_cursor": "abc123"})
        assert more is False

    def test_custom_cursor_param_name(self):
        """Custom cursor parameter name should be used."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_param="continuation_token",
            cursor_path="meta.continuation",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response([{"id": 1}], {"meta": {"continuation": "xyz789"}})

        params = state.build_params()
        assert params["continuation_token"] == "xyz789"


class TestCursorExtractionFromNestedJson:
    """Tests for cursor extraction from deeply nested JSON responses."""

    def test_single_level_path(self):
        """Should extract cursor from single-level path."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response([{"id": 1}], {"cursor": "single_level"})

        params = state.build_params()
        assert params["cursor"] == "single_level"

    def test_two_level_path(self):
        """Should extract cursor from two-level nested path."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="meta.next",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response([{"id": 1}], {"meta": {"next": "two_level"}})

        params = state.build_params()
        assert params["cursor"] == "two_level"

    def test_three_level_path(self):
        """Should extract cursor from deeply nested path."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="response.pagination.next_cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response(
            [{"id": 1}],
            {"response": {"pagination": {"next_cursor": "deep_value"}}},
        )

        params = state.build_params()
        assert params["cursor"] == "deep_value"

    def test_partial_path_returns_none(self):
        """Should return None when path is partially valid."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="meta.pagination.cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        # Path exists up to 'meta.pagination' but 'cursor' is missing
        more = state.on_response(
            [{"id": 1}],
            {"meta": {"pagination": {"other": "value"}}},
        )
        assert more is False

    def test_non_dict_intermediate_returns_none(self):
        """Should return None when intermediate path is not a dict."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="meta.cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        # 'meta' is a string, not a dict
        more = state.on_response([{"id": 1}], {"meta": "not_a_dict"})
        assert more is False

    def test_integer_cursor_converted_to_string(self):
        """Should convert integer cursor to string."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next_page",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response([{"id": 1}], {"next_page": 12345})

        params = state.build_params()
        assert params["cursor"] == "12345"

    def test_non_dict_response_returns_none(self):
        """Should return None when response is not a dict."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="cursor",
        )
        state = CursorPaginationState(config)

        state.build_params()
        # Response is a list, not a dict
        more = state.on_response([{"id": 1}], [{"cursor": "value"}])
        assert more is False


class TestCursorDescribe:
    """Tests for cursor pagination describe() method."""

    def test_first_page_description(self):
        """Should describe first page correctly."""
        config = PaginationConfig(strategy=PaginationStrategy.CURSOR)
        state = CursorPaginationState(config)

        desc = state.describe()
        assert "first page" in desc.lower()

    def test_subsequent_page_shows_cursor(self):
        """Should show cursor value in description."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next",
        )
        state = CursorPaginationState(config)

        state.build_params()
        state.on_response([{"id": 1}], {"next": "abc123"})

        desc = state.describe()
        assert "abc123" in desc

    def test_long_cursor_truncated(self):
        """Long cursor values should be truncated in description."""
        config = PaginationConfig(
            strategy=PaginationStrategy.CURSOR,
            cursor_path="next",
        )
        state = CursorPaginationState(config)

        state.build_params()
        long_cursor = "a" * 50
        state.on_response([{"id": 1}], {"next": long_cursor})

        desc = state.describe()
        assert "..." in desc
        assert len(desc) < len(long_cursor) + 30  # Should be truncated


# ============================================================================
# build_pagination_state Factory Tests
# ============================================================================


class TestBuildPaginationState:
    """Tests for build_pagination_state factory function."""

    def test_none_strategy_returns_no_pagination(self):
        """NONE strategy should return NoPaginationState."""
        config = PaginationConfig(strategy=PaginationStrategy.NONE)
        state = build_pagination_state(config)

        assert isinstance(state, NoPaginationState)

    def test_offset_strategy_returns_offset_state(self):
        """OFFSET strategy should return OffsetPaginationState."""
        config = PaginationConfig(strategy=PaginationStrategy.OFFSET)
        state = build_pagination_state(config)

        assert isinstance(state, OffsetPaginationState)

    def test_page_strategy_returns_page_state(self):
        """PAGE strategy should return PagePaginationState."""
        config = PaginationConfig(strategy=PaginationStrategy.PAGE)
        state = build_pagination_state(config)

        assert isinstance(state, PagePaginationState)

    def test_cursor_strategy_returns_cursor_state(self):
        """CURSOR strategy should return CursorPaginationState."""
        config = PaginationConfig(strategy=PaginationStrategy.CURSOR)
        state = build_pagination_state(config)

        assert isinstance(state, CursorPaginationState)

    def test_base_params_passed_to_state(self):
        """Base parameters should be passed to the state."""
        config = PaginationConfig(strategy=PaginationStrategy.OFFSET)
        state = build_pagination_state(config, base_params={"filter": "active"})

        params = state.build_params()
        assert params["filter"] == "active"


# ============================================================================
# build_pagination_config_from_dict Tests
# ============================================================================


class TestBuildPaginationConfigFromDict:
    """Tests for build_pagination_config_from_dict helper."""

    def test_empty_dict_returns_none_strategy(self):
        """Empty dict should return NONE strategy config."""
        config = build_pagination_config_from_dict({})

        assert config.strategy == PaginationStrategy.NONE

    def test_offset_from_dict(self):
        """Should build offset config from dict."""
        config = build_pagination_config_from_dict(
            {
                "pagination_type": "offset",
                "page_size": 50,
                "offset_param": "skip",
                "limit_param": "take",
            }
        )

        assert config.strategy == PaginationStrategy.OFFSET
        assert config.page_size == 50
        assert config.offset_param == "skip"
        assert config.limit_param == "take"

    def test_page_from_dict(self):
        """Should build page config from dict."""
        config = build_pagination_config_from_dict(
            {
                "pagination_type": "page",
                "page_size": 25,
                "page_param": "p",
                "page_size_param": "per_page",
                "max_pages": 10,
            }
        )

        assert config.strategy == PaginationStrategy.PAGE
        assert config.page_size == 25
        assert config.page_param == "p"
        assert config.page_size_param == "per_page"
        assert config.max_pages == 10

    def test_cursor_from_dict(self):
        """Should build cursor config from dict."""
        config = build_pagination_config_from_dict(
            {
                "pagination_type": "cursor",
                "cursor_param": "next_token",
                "cursor_path": "meta.next",
            }
        )

        assert config.strategy == PaginationStrategy.CURSOR
        assert config.cursor_param == "next_token"
        assert config.cursor_path == "meta.next"

    def test_none_pagination_explicit(self):
        """Explicit 'none' should work."""
        config = build_pagination_config_from_dict(
            {
                "pagination_type": "none",
            }
        )

        assert config.strategy == PaginationStrategy.NONE

    def test_case_insensitive_strategy(self):
        """Strategy name should be case insensitive."""
        config = build_pagination_config_from_dict(
            {
                "pagination_type": "OFFSET",
            }
        )

        assert config.strategy == PaginationStrategy.OFFSET
