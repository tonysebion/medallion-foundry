# Retry Configuration

Retry behavior is already wired into the Bronze extraction, Silver processor, and storage backends via the resilient execution helpers in `core.platform.resilience`. This guide explains how to customize that behavior so each source type can align with its uptime guarantees, throughput goals, and error budget.

## Why tune retry settings?

- **Avoid overwhelming APIs** with retries that pile up after a transient failure.
- **Stop fast** on non-retryable errors (authentication, bad requests, not found) while retrying the ones that clear up naturally (timeouts, network blips, server errors).
- **Add jitter and backoff** so retries spread out when a backend is already struggling.
- **Match storage vs API workloads**: uploads to storage backends typically tolerate more attempts than synchronous API calls.

## Runtime configuration

Each extractor exposes a `retry` block under its `source` definition. The values map directly to `RetryPolicy` in `core/platform/resilience/retry.py`, so the same terminology applies in docs and code:

```yaml
source:
  type: api

  retry:
    max_attempts: 8           # Total tries before giving up (default 5)
    base_delay: 0.5           # Starting backoff delay in seconds
    max_delay: 10.0           # Upper bound on backoff delay
    multiplier: 2.0           # Exponential multiplier (default 2.0)
    jitter: 0.25              # Add +/-25% randomness to avoid thundering herds
    retry_on_exceptions:
      - TimeoutError
      - ConnectionError
      - asyncio.TimeoutError
```

- `base_delay`/`max_delay`/`multiplier` compose the classic exponential backoff curve.
- `jitter` inserts configurable randomness so simultaneous retries don't collide.
- `retry_on_exceptions` accepts built-in exception names that the extractor should treat as retryable; the implementation resolves them back to exception classes before constructing `RetryPolicy`.

If you omit the `retry` block, the default policy still applies (5 attempts, 0.5s → 8.0s). Providing any of these fields overrides only that value while the others fall back to defaults.

## Per-source-type tweaks

Extractors can switch the retry predicate depending on domain knowledge:

- **API extractors** call `_should_retry_api` to skip retries on 4xx errors while honoring 429/5xx. You can set more aggressive values (e.g., `max_attempts: 10`) for flaky APIs if they otherwise succeed.
- **Database extractors** override `_should_retry_db_error` and often retry on transient SQL errors or deadlock notifications. Use `retry_on_exceptions` to include `pyodbc.Error` subclasses.
- **Storage/upload helpers** (S3/Azure/local) keep their own `_should_retry` helpers; the shared `retry` block still controls the policy.

Use intent configs to vary retry policies across datasets:

```yaml
bronze:
  ...
source:
  type: api
  retry:
    max_attempts: 10
    base_delay: 0.2
    jitter: 0.3

silver:
  ...
```

## Logging and observability

Resilience helpers log each retry attempt at DEBUG level:

```
DEBUG Waiting 0.75s before retry (attempt 3/7) [...]
```

If you need richer telemetry, hook into:

- `core.platform.resilience.retry.execute_with_retry`, which accepts an optional `retry_if` predicate per call.
- `core.platform.resilience.mixins.ResilienceMixin`, which surfaces `RetryPolicy`.

Tune log levels or ship the message to your observability stack so every dataset run reports how many retries it issued and why they eventually succeeded or failed.

## Futures & suggestions

- Consider exposing a `retry.retry_if` config key that accepts named predicates in the future.
- Allow dataset-specific retry timeouts (e.g., short circuits for fast-fail workflows) by decorating `execute_with_retry`.
- Combine with rate-limiting (see the API guide) so retries automatically respect downstream throttles.
