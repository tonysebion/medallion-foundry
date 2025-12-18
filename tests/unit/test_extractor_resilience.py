"""Tests for ResilientExtractorMixin and extractor circuit breaker integration."""

from unittest.mock import MagicMock

from core.domain.adapters.extractors.resilience import ResilientExtractorMixin
from core.platform.resilience import CircuitBreaker, CircuitState


class MockExtractor(ResilientExtractorMixin):
    """Mock extractor for testing the resilience mixin."""

    def __init__(self):
        self._init_resilience(
            failure_threshold=3,
            cooldown_seconds=5.0,
            half_open_max_calls=1,
        )
        self.call_count = 0
        self.should_fail = False
        self.fail_count = 0

    def do_operation(self) -> str:
        """Simulated operation that can fail."""
        self.call_count += 1
        if self.should_fail and self.fail_count > 0:
            self.fail_count -= 1
            raise ConnectionError("Simulated failure")
        return "success"


class TestResilientExtractorMixin:
    """Tests for the ResilientExtractorMixin class."""

    def test_init_resilience_creates_circuit_breaker(self) -> None:
        extractor = MockExtractor()
        assert hasattr(extractor, "_breaker")
        assert isinstance(extractor._breaker, CircuitBreaker)

    def test_init_resilience_with_custom_thresholds(self) -> None:
        class CustomExtractor(ResilientExtractorMixin):
            def __init__(self):
                self._init_resilience(
                    failure_threshold=10,
                    cooldown_seconds=60.0,
                    half_open_max_calls=3,
                )

        extractor = CustomExtractor()
        assert extractor._breaker.failure_threshold == 10
        assert extractor._breaker.cooldown_seconds == 60.0
        assert extractor._breaker.half_open_max_calls == 3

    def test_get_circuit_state_returns_closed_initially(self) -> None:
        extractor = MockExtractor()
        assert extractor.get_circuit_state() == "closed"

    def test_reset_circuit_resets_to_closed(self) -> None:
        extractor = MockExtractor()
        # Manually set state to open
        extractor._breaker._state = CircuitState.OPEN
        extractor.reset_circuit()
        assert extractor.get_circuit_state() == "closed"

    def test_build_retry_policy_creates_policy(self) -> None:
        extractor = MockExtractor()
        policy = extractor._build_retry_policy()

        assert policy.max_attempts == 5  # default
        assert policy.base_delay == 0.5
        assert policy.jitter == 0.2

    def test_build_retry_policy_with_custom_params(self) -> None:
        extractor = MockExtractor()
        policy = extractor._build_retry_policy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=10.0,
            jitter=0.5,
        )

        assert policy.max_attempts == 3
        assert policy.base_delay == 1.0
        assert policy.max_delay == 10.0
        assert policy.jitter == 0.5


class TestDefaultRetryIf:
    """Tests for the default retry predicate."""

    def test_retries_on_requests_connection_error(self) -> None:
        extractor = MockExtractor()

        # Create mock exception that looks like requests.ConnectionError
        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "ConnectionError"
        mock_exc.__class__.__module__ = "requests.exceptions"

        assert extractor._default_retry_if(mock_exc) is True

    def test_retries_on_requests_timeout(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "Timeout"
        mock_exc.__class__.__module__ = "requests.exceptions"

        assert extractor._default_retry_if(mock_exc) is True

    def test_retries_on_httpx_timeout(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "TimeoutException"
        mock_exc.__class__.__module__ = "httpx"

        assert extractor._default_retry_if(mock_exc) is True

    def test_retries_on_429_status(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "HTTPError"
        mock_exc.__class__.__module__ = "requests.exceptions"
        mock_exc.response = MagicMock()
        mock_exc.response.status_code = 429

        assert extractor._default_retry_if(mock_exc) is True

    def test_retries_on_500_status(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "HTTPError"
        mock_exc.__class__.__module__ = "requests.exceptions"
        mock_exc.response = MagicMock()
        mock_exc.response.status_code = 500

        assert extractor._default_retry_if(mock_exc) is True

    def test_retries_on_503_status(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "HTTPError"
        mock_exc.__class__.__module__ = "requests.exceptions"
        mock_exc.response = MagicMock()
        mock_exc.response.status_code = 503

        assert extractor._default_retry_if(mock_exc) is True

    def test_does_not_retry_on_400_status(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "HTTPError"
        mock_exc.__class__.__module__ = "requests.exceptions"
        mock_exc.response = MagicMock()
        mock_exc.response.status_code = 400

        assert extractor._default_retry_if(mock_exc) is False

    def test_does_not_retry_on_404_status(self) -> None:
        extractor = MockExtractor()

        mock_exc = MagicMock()
        mock_exc.__class__.__name__ = "HTTPError"
        mock_exc.__class__.__module__ = "requests.exceptions"
        mock_exc.response = MagicMock()
        mock_exc.response.status_code = 404

        assert extractor._default_retry_if(mock_exc) is False

    def test_does_not_retry_on_unknown_exception(self) -> None:
        extractor = MockExtractor()

        mock_exc = ValueError("Unknown error")
        assert extractor._default_retry_if(mock_exc) is False


class TestExecuteWithResilience:
    """Tests for _execute_with_resilience method."""

    def test_successful_operation_returns_result(self) -> None:
        extractor = MockExtractor()

        result = extractor._execute_with_resilience(
            lambda: "test_result",
            "test_operation",
        )

        assert result == "test_result"

    def test_operation_with_custom_retry_if(self) -> None:
        extractor = MockExtractor()

        # Custom retry_if that never retries
        result = extractor._execute_with_resilience(
            lambda: "success",
            "test_operation",
            retry_if=lambda exc: False,
        )

        assert result == "success"

    def test_operation_with_max_attempts_override(self) -> None:
        extractor = MockExtractor()

        result = extractor._execute_with_resilience(
            lambda: "success",
            "test_operation",
            max_attempts=1,
        )

        assert result == "success"


class TestApiExtractorResilience:
    """Integration tests for ApiExtractor's use of ResilientExtractorMixin."""

    def test_api_extractor_initializes_circuit_breaker(self) -> None:
        from core.domain.adapters.extractors.api_extractor import ApiExtractor

        extractor = ApiExtractor()
        assert hasattr(extractor, "_breaker")
        assert isinstance(extractor._breaker, CircuitBreaker)
        assert extractor.get_circuit_state() == "closed"

    def test_api_extractor_retry_predicate_handles_http_errors(self) -> None:
        from core.domain.adapters.extractors.api_extractor import ApiExtractor
        import requests

        extractor = ApiExtractor()

        # Create a mock HTTPError with 503 status
        mock_response = MagicMock()
        mock_response.status_code = 503

        exc = requests.exceptions.HTTPError()
        exc.response = mock_response

        assert extractor._should_retry(exc) is True

    def test_api_extractor_retry_predicate_handles_timeout(self) -> None:
        from core.domain.adapters.extractors.api_extractor import ApiExtractor
        import requests

        extractor = ApiExtractor()

        exc = requests.exceptions.Timeout()
        assert extractor._should_retry(exc) is True

    def test_api_extractor_delay_from_retry_after_header(self) -> None:
        from core.domain.adapters.extractors.api_extractor import ApiExtractor
        import requests

        extractor = ApiExtractor()

        # Create mock HTTPError with Retry-After header
        mock_response = MagicMock()
        mock_response.headers = {"Retry-After": "30"}

        exc = requests.exceptions.HTTPError()
        exc.response = mock_response

        delay = extractor._delay_from_api_exc(exc, 1, 1.0)
        assert delay == 30.0

    def test_api_extractor_delay_returns_none_without_header(self) -> None:
        from core.domain.adapters.extractors.api_extractor import ApiExtractor
        import requests

        extractor = ApiExtractor()

        # Create mock HTTPError without Retry-After header
        mock_response = MagicMock()
        mock_response.headers = {}

        exc = requests.exceptions.HTTPError()
        exc.response = mock_response

        delay = extractor._delay_from_api_exc(exc, 1, 1.0)
        assert delay is None
