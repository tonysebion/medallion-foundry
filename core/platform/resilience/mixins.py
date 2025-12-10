"""Shared resilience patterns mixin for extractors and storage backends.

This module provides a unified ResilienceMixin that can be used by both
extractors (domain layer) and storage backends (infrastructure layer) to gain
consistent circuit breaker and retry capabilities.

The mixin extracts common resilience logic that was previously duplicated
between ResilientExtractorMixin and BaseCloudStorage.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Mapping, Optional, TypeVar, TYPE_CHECKING

from core.platform.resilience.config import parse_retry_config
from core.platform.resilience.constants import (
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_HALF_OPEN_MAX_CALLS,
)

if TYPE_CHECKING:
    from core.platform.resilience.circuit_breaker import CircuitBreaker
    from core.platform.resilience.retry import RetryPolicy

logger = logging.getLogger(__name__)

StateChangeHook = Callable[[str, Optional[str], str], None]

T = TypeVar("T")


class ResilienceMixin:
    """Mixin providing circuit breaker and retry capabilities.

    This mixin can be used by both storage backends and extractors to gain
    consistent resilience patterns. It supports:
    - Multiple named circuit breakers for different operations
    - Configurable retry policies with exponential backoff
    - Unified execute_with_resilience wrapper

    Usage for single breaker (extractors):
        class MyExtractor(BaseExtractor, ResilienceMixin):
            def __init__(self):
                self._init_resilience_single("MyExtractor")

            def fetch_records(self, ...):
                return self._execute_with_resilience(
                    lambda: self._do_fetch(...),
                    "fetch",
                    retry_if=self._should_retry,
                )

    Usage for multiple breakers (storage):
        class MyStorage(StorageBackend, ResilienceMixin):
            def __init__(self):
                self._init_resilience_multi(
                    ["upload", "download", "list", "delete"],
                    "MyStorage",
                )

            def upload_file(self, ...):
                return self._execute_with_resilience(
                    lambda: self._do_upload(...),
                    "upload",
                    breaker_key="upload",
                )
    """

    _breaker: "CircuitBreaker"  # Single breaker mode
    _breakers: Dict[str, "CircuitBreaker"]  # Multi-breaker mode
    _component_name: str

    def _init_resilience_single(
        self,
        component_name: str,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        half_open_max_calls: int = DEFAULT_HALF_OPEN_MAX_CALLS,
        on_state_change: Optional[Callable[[str], None]] = None,
        observability_hook: Optional[StateChangeHook] = None,
    ) -> None:
        """Initialize a single circuit breaker for this component.

        Use this for components with a single operation type (e.g., extractors).

        Args:
            component_name: Name for logging/metrics
            failure_threshold: Number of failures before opening circuit
            cooldown_seconds: Seconds to wait before trying again
            half_open_max_calls: Max calls in half-open state
            on_state_change: Optional callback when state changes
            observability_hook: Optional hook to emit resilience state via
                `core.platform.observability` (component, breaker_key, state)
        """
        from core.platform.resilience import CircuitBreaker

        self._component_name = component_name
        self._breakers = {}
        self._observability_hook = observability_hook

        def _emit_state(state: str) -> None:
            logger.info(
                "metric=breaker_state component=%s state=%s",
                component_name,
                state,
            )
            if on_state_change:
                on_state_change(state)
            self._notify_observability(None, state)

        self._breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
            half_open_max_calls=half_open_max_calls,
            on_state_change=_emit_state,
        )

    def _init_resilience_multi(
        self,
        operations: List[str],
        component_name: str,
        failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        half_open_max_calls: int = DEFAULT_HALF_OPEN_MAX_CALLS,
        on_state_change: Optional[Callable[[str], None]] = None,
        observability_hook: Optional[StateChangeHook] = None,
    ) -> None:
        """Initialize multiple circuit breakers for different operation types.

        Use this for components with multiple operation types (e.g., storage backends
        with upload, download, list, delete).

        Args:
            operations: List of operation names (e.g., ["upload", "download"])
            component_name: Name for logging/metrics
            failure_threshold: Number of failures before opening circuit
            cooldown_seconds: Seconds to wait before trying again
            half_open_max_calls: Max calls in half-open state
            on_state_change: Optional callback when state changes
        """
        from core.platform.resilience import CircuitBreaker

        self._component_name = component_name
        self._observability_hook = observability_hook

        def _make_emit_state(op_name: str) -> Callable[[str], None]:
            def _emit_state(state: str) -> None:
                logger.info(
                    "metric=breaker_state component=%s operation=%s state=%s",
                    component_name,
                    op_name,
                    state,
                )
                if on_state_change:
                    on_state_change(state)
                self._notify_observability(op_name, state)

            return _emit_state

        self._breakers = {}
        for op in operations:
            self._breakers[op] = CircuitBreaker(
                failure_threshold=failure_threshold,
                cooldown_seconds=cooldown_seconds,
                half_open_max_calls=half_open_max_calls,
                on_state_change=_make_emit_state(op),
            )

    def _notify_observability(
        self, breaker_key: Optional[str], state: str
    ) -> None:
        hook = getattr(self, "_observability_hook", None)
        if not hook:
            return
        try:
            hook(self._component_name, breaker_key, state)
        except Exception as exc:  # pragma: no cover - best effort
            logger.debug("Observability hook failed: %s", exc)

    def _get_breaker(self, breaker_key: Optional[str] = None) -> "CircuitBreaker":
        """Get the appropriate circuit breaker.

        Args:
            breaker_key: For multi-breaker mode, the operation name

        Returns:
            The circuit breaker to use
        """
        if breaker_key and hasattr(self, "_breakers") and breaker_key in self._breakers:
            return self._breakers[breaker_key]
        if hasattr(self, "_breaker"):
            return self._breaker
        raise RuntimeError(
            f"No circuit breaker found. Call _init_resilience_single or "
            f"_init_resilience_multi first. breaker_key={breaker_key}"
        )

    def _build_retry_policy(
        self,
        retry_if: Optional[Callable[[BaseException], bool]] = None,
        delay_from_exception: Optional[
            Callable[[BaseException, int, float], Optional[float]]
        ] = None,
        max_attempts: Optional[int] = None,
        base_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        backoff_multiplier: Optional[float] = None,
        jitter: Optional[float] = None,
        retry_config: Optional[Mapping[str, Any]] = None,
    ) -> "RetryPolicy":
        """Build a retry policy for operations.

        Args:
            retry_if: Custom predicate to determine if exception is retryable.
                      If None, falls back to self._default_retry_if if it exists.
            delay_from_exception: Optional callback to extract delay from exception
            max_attempts: Maximum retry attempts
            base_delay: Initial delay between retries
            max_delay: Maximum delay between retries
            backoff_multiplier: Multiplier for exponential backoff
            jitter: Random jitter factor (0.0 to 1.0)

        Returns:
            Configured RetryPolicy instance
        """
        from core.platform.resilience import RetryPolicy

        # Fall back to instance method if no retry_if provided
        actual_retry_if = retry_if
        if actual_retry_if is None:
            default_method = getattr(self, "_default_retry_if", None)
            if callable(default_method):
                actual_retry_if = default_method

        policy_kwargs: Dict[str, Any] = parse_retry_config(retry_config)
        if max_attempts is not None:
            policy_kwargs["max_attempts"] = max_attempts
        if base_delay is not None:
            policy_kwargs["base_delay"] = base_delay
        if max_delay is not None:
            policy_kwargs["max_delay"] = max_delay
        if backoff_multiplier is not None:
            policy_kwargs["backoff_multiplier"] = backoff_multiplier
        if jitter is not None:
            policy_kwargs["jitter"] = jitter

        policy_kwargs.update(
            {
                "retry_on_exceptions": (),
                "retry_if": actual_retry_if,
                "delay_from_exception": delay_from_exception,
            }
        )
        return RetryPolicy(**policy_kwargs)

    def _execute_with_resilience(
        self,
        operation: Callable[[], T],
        operation_name: str,
        breaker_key: Optional[str] = None,
        retry_if: Optional[Callable[[BaseException], bool]] = None,
        delay_from_exception: Optional[
            Callable[[BaseException, int, float], Optional[float]]
        ] = None,
        max_attempts: Optional[int] = None,
        base_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        backoff_multiplier: Optional[float] = None,
        jitter: Optional[float] = None,
        retry_config: Optional[Mapping[str, Any]] = None,
    ) -> T:
        """Execute an operation with circuit breaker and retry logic.

        Args:
            operation: The operation to execute (should be a callable)
            operation_name: Name for logging/metrics
            breaker_key: For multi-breaker mode, the operation type key
            retry_if: Optional custom retry predicate
        delay_from_exception: Optional delay extraction callback
            max_attempts: Override default max attempts
            base_delay: Override default base delay
            max_delay: Override default max delay
            backoff_multiplier: Override default backoff multiplier
            jitter: Override default jitter
            retry_config: Optional mapping containing `retry` block values

        Returns:
            Result of the operation

        Raises:
            Exception: From operation if all retries exhausted or circuit open
        """
        from core.platform.resilience import execute_with_retry

        policy_kwargs: Dict[str, Any] = parse_retry_config(retry_config)
        if max_attempts is not None:
            policy_kwargs["max_attempts"] = max_attempts
        if base_delay is not None:
            policy_kwargs["base_delay"] = base_delay
        if max_delay is not None:
            policy_kwargs["max_delay"] = max_delay
        if backoff_multiplier is not None:
            policy_kwargs["backoff_multiplier"] = backoff_multiplier
        if jitter is not None:
            policy_kwargs["jitter"] = jitter

        policy = self._build_retry_policy(
            retry_if=retry_if,
            delay_from_exception=delay_from_exception,
            retry_config=retry_config,
            **policy_kwargs,  # type: ignore[arg-type]
        )

        breaker = self._get_breaker(breaker_key)
        return execute_with_retry(
            operation,
            policy=policy,
            breaker=breaker,
            operation_name=operation_name,
        )

    def get_circuit_state(self, breaker_key: Optional[str] = None) -> str:
        """Get the current circuit breaker state.

        Args:
            breaker_key: For multi-breaker mode, the operation type key

        Returns:
            Current state: "closed", "open", or "half_open"
        """
        try:
            breaker = self._get_breaker(breaker_key)
            return breaker.state.value
        except RuntimeError:
            return "unknown"

    def reset_circuit(self, breaker_key: Optional[str] = None) -> None:
        """Reset the circuit breaker to closed state.

        Args:
            breaker_key: For multi-breaker mode, the operation type key.
                         If None and in multi-breaker mode, resets all breakers.
        """
        if breaker_key:
            try:
                breaker = self._get_breaker(breaker_key)
                breaker.record_success()
            except RuntimeError:
                pass
        elif hasattr(self, "_breakers") and self._breakers:
            # Reset all breakers
            for breaker in self._breakers.values():
                breaker.record_success()
        elif hasattr(self, "_breaker"):
            self._breaker.record_success()
