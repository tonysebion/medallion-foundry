# Retry Configuration

Retry support is available for handling transient failures in database connections, API calls, and other flaky operations. This guide explains how to add retry logic to your pipelines.

> **Note:** Retry configuration is currently only available for Python pipelines. YAML pipelines do not support retry configuration. If you need retry logic with a YAML pipeline, wrap the call in a Python script.

## Philosophy

Bronze-Foundry keeps `BronzeSource` and `SilverEntity` simple and deterministic. Retry logic is **opt-in** via the `@with_retry` decorator when you know a source is flaky.

## Basic Usage (Python Only)

```python
from pipelines.lib.resilience import with_retry

# Simple case - no retry (most sources)
def run(run_date: str):
    return bronze.run(run_date)

# Flaky database - add retry
@with_retry(max_attempts=3)
def run(run_date: str):
    return bronze.run(run_date)

# API with rate limiting - longer backoff
@with_retry(max_attempts=5, backoff_seconds=5.0)
def run(run_date: str):
    return bronze.run(run_date)
```

## Decorator Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_attempts` | 3 | Maximum number of attempts before giving up |
| `backoff_seconds` | 1.0 | Base delay between retries |
| `exponential` | True | Use exponential backoff (delay doubles each attempt) |
| `jitter` | True | Add random jitter to avoid thundering herd |
| `retry_exceptions` | None | Only retry on these exception types (None = all) |

## Backoff Behavior

With default settings (`exponential=True`, `jitter=True`, `backoff_seconds=1.0`):

| Attempt | Base Delay | With Jitter (example) |
|---------|------------|----------------------|
| 1 → 2 | 1.0s | 1.0-1.5s |
| 2 → 3 | 2.0s | 2.0-3.0s |
| 3 → 4 | 4.0s | 4.0-6.0s |

## Common Patterns

### Database with Transient Failures

```python
from pipelines.lib.resilience import with_retry

@with_retry(max_attempts=3, backoff_seconds=2.0)
def run_bronze(run_date: str, **kwargs):
    """Extract with retry on transient failures."""
    return bronze.run(run_date, **kwargs)
```

### Rate-Limited API

```python
from pipelines.lib.resilience import with_retry

@with_retry(
    max_attempts=5,
    backoff_seconds=10.0,  # Start with longer delay
    retry_exceptions=(ConnectionError, TimeoutError),
)
def run_bronze(run_date: str, **kwargs):
    """Extract from rate-limited API."""
    return bronze.run(run_date, **kwargs)
```

### Retry Only Specific Exceptions

```python
import pyodbc
from pipelines.lib.resilience import with_retry

@with_retry(
    max_attempts=3,
    retry_exceptions=(pyodbc.OperationalError, pyodbc.InterfaceError),
)
def run_bronze(run_date: str, **kwargs):
    """Retry only on transient database errors."""
    return bronze.run(run_date, **kwargs)
```

## Alternative: RetryConfig Class

For programmatic retry configuration:

```python
from pipelines.lib.resilience import RetryConfig, retry_operation

# Predefined configurations
config = RetryConfig.default()      # 3 attempts, 1s backoff
config = RetryConfig.aggressive()   # 5 attempts, 5s backoff
config = RetryConfig.none()         # No retry (fail immediately)

# Custom configuration
config = RetryConfig(
    max_attempts=10,
    backoff_seconds=2.0,
    exponential=True,
    jitter=True,
)

# Use with retry_operation helper
result = retry_operation(
    lambda: database.execute(query),
    config,
    "database query"  # Operation name for logging
)
```

## Circuit Breaker

For services that may be completely unavailable, use the circuit breaker to fail fast:

```python
from pipelines.lib.resilience import CircuitBreaker, CircuitBreakerOpen

breaker = CircuitBreaker(
    failure_threshold=5,   # Open circuit after 5 failures
    recovery_time=60.0,    # Try again after 60 seconds
)

try:
    with breaker:
        result = api.call()
except CircuitBreakerOpen:
    # Service is down - use cached data or fail gracefully
    result = get_cached_data()
```

## Logging

Retry attempts are logged at WARNING level:

```
WARNING Attempt 1/3 failed: Connection refused. Retrying in 1.2s...
WARNING Attempt 2/3 failed: Connection refused. Retrying in 2.4s...
ERROR All 3 attempts failed. Last error: Connection refused
```

## YAML Pipeline Workaround

If you need retry with a YAML pipeline, create a wrapper Python script:

```python
# run_with_retry.py
from pipelines.lib.config_loader import load_pipeline
from pipelines.lib.resilience import with_retry

@with_retry(max_attempts=3, backoff_seconds=2.0)
def run_pipeline(config_path: str, run_date: str):
    pipeline = load_pipeline(config_path)
    return pipeline.run(run_date)

if __name__ == "__main__":
    import sys
    result = run_pipeline(sys.argv[1], sys.argv[2])
    print(result)
```

Then run:
```bash
python run_with_retry.py ./my_pipeline.yaml 2025-01-15
```

## When to Use Retry

| Scenario | Recommendation |
|----------|---------------|
| Stable internal database | No retry needed |
| Cloud database (Azure SQL, RDS) | Add retry (transient network issues) |
| Rate-limited external API | Add retry with longer backoff |
| File system operations | Usually no retry needed |
| Network file shares | Add retry (connection drops) |

## Future: YAML Retry Support

YAML retry configuration may be added in a future version:

```yaml
# FUTURE - NOT YET IMPLEMENTED
bronze:
  system: sales
  entity: orders
  source_type: database_mssql
  retry:
    max_attempts: 3
    backoff_seconds: 2.0
```

Track progress: [GitHub Issue #TBD]
