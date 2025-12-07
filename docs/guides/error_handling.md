# Error Handling & Resilience Configuration

This guide covers how Bronze Foundry handles errors, retries, rate limiting, and circuit breakers to build robust extraction pipelines.

---

## Overview

Bronze Foundry includes built-in resilience patterns that protect your pipelines from transient failures:

| Pattern | Purpose | Default Behavior |
|---------|---------|------------------|
| **Retry** | Retry failed operations with exponential backoff | 5 attempts, 0.5s-8s delays |
| **Circuit Breaker** | Stop calling failing services to prevent cascading failures | Opens after 5 failures, 30s cooldown |
| **Rate Limiter** | Throttle requests to avoid hitting API rate limits | Configurable via config or env var |
| **Error Mapping** | Convert third-party errors to domain exceptions | Automatic for S3, Azure, API, local |

---

## Retry Behavior

### How Retries Work

When an operation fails with a retryable error, Bronze Foundry:

1. Waits with **exponential backoff** (delay doubles each attempt)
2. Adds **jitter** (random variation) to avoid thundering herd
3. Retries up to **max_attempts** times
4. Raises `RetryExhaustedError` if all attempts fail

### Default Retry Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `max_attempts` | 5 | Maximum retry attempts before giving up |
| `base_delay` | 0.5s | Initial delay between retries |
| `max_delay` | 8.0s | Maximum delay cap (regardless of backoff) |
| `backoff_multiplier` | 2.0 | Delay multiplier per attempt |
| `jitter` | 0.2 | Random variation (20% of delay) |

### Retry Delay Calculation

```
delay = min(max_delay, base_delay * (backoff_multiplier ^ attempt))
actual_delay = delay +/- (delay * jitter)
```

**Example with defaults:**
| Attempt | Calculated Delay | With Jitter (20%) |
|---------|------------------|-------------------|
| 1 | 0.5s | 0.4s - 0.6s |
| 2 | 1.0s | 0.8s - 1.2s |
| 3 | 2.0s | 1.6s - 2.4s |
| 4 | 4.0s | 3.2s - 4.8s |
| 5 | 8.0s | 6.4s - 9.6s |

### Retryable vs Non-Retryable Errors

**Retryable (will retry):**
- `TimeoutError` - Connection or read timeout
- `ConnectionError` - Network connectivity issues
- `OSError` - Filesystem/network errors
- HTTP 429 (Too Many Requests)
- HTTP 5xx (Server errors: 500, 502, 503, 504)

**Non-Retryable (immediate failure):**
- HTTP 400 (Bad Request) - Fix your request
- HTTP 401 (Unauthorized) - Fix credentials
- HTTP 403 (Forbidden) - Fix permissions
- HTTP 404 (Not Found) - Fix URL/resource
- `ValueError`, `TypeError` - Fix your code

---

## Circuit Breaker

### How Circuit Breakers Work

Circuit breakers prevent your pipeline from repeatedly calling a failing service:

```
    CLOSED ────(failures >= threshold)───► OPEN
       ▲                                      │
       │                                      │
       │                           (cooldown expires)
       │                                      │
       │                                      ▼
       └────(success)──────────────── HALF_OPEN
                                    (test request)
```

**States:**
- **CLOSED** (normal): Requests flow through normally
- **OPEN** (failing): Requests are rejected immediately without calling the service
- **HALF_OPEN** (testing): One request allowed through to test if service recovered

### Default Circuit Breaker Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `failure_threshold` | 5 | Failures before circuit opens |
| `cooldown_seconds` | 30.0s | Time before trying again |
| `half_open_max_calls` | 1 | Test calls during recovery |

### When Circuit Opens

When the circuit breaker opens, operations fail immediately with:
```
RetryExhaustedError: Circuit open; refusing to execute operation_name
```

This protects your pipeline from:
- Wasting time on a known-failing service
- Overloading a struggling service
- Cascading failures to downstream systems

### Monitoring Circuit State

Circuit state changes are logged:
```
INFO metric=breaker_state component=ApiExtractor state=open
INFO metric=breaker_state component=ApiExtractor state=half_open
INFO metric=breaker_state component=ApiExtractor state=closed
```

---

## Rate Limiting

### How Rate Limiting Works

Rate limiting uses a **token bucket algorithm**:
- Tokens are added at `requests_per_second` rate
- Each request consumes one token
- If no tokens available, request waits until one is available
- `burst` capacity allows short bursts above the rate

### Configuring Rate Limits

**Option 1: Environment Variable**
```bash
export BRONZE_API_RPS=10  # 10 requests per second
```

**Option 2: Config File**
```yaml
source:
  type: api
  url: https://api.example.com/data
  rate_limit:
    rps: 10      # requests per second
    burst: 20    # optional burst capacity
```

**Option 3: Run Config**
```yaml
run:
  rate_limit_rps: 10
```

### Rate Limit Priority

Rate limits are resolved in this order (first match wins):
1. Per-extractor `rate_limit.rps` in source config
2. `rate_limit_rps` in run config
3. `BRONZE_API_RPS` environment variable

### Handling 429 Responses

When an API returns HTTP 429 (Too Many Requests):
1. The request is marked as retryable
2. If the response includes `Retry-After` header, that delay is used
3. Otherwise, exponential backoff applies
4. Rate limiter automatically adjusts

---

## Error Types and Handling

### Domain Exceptions

Bronze Foundry maps third-party errors to domain exceptions:

| Exception | Typical Causes | Error Code |
|-----------|----------------|------------|
| `ExtractionError` | API/DB/file extraction failures | EXT001 |
| `StorageError` | S3/Azure/local storage failures | STG001 |
| `AuthenticationError` | Invalid credentials, expired tokens | AUTH001 |
| `RetryExhaustedError` | All retries failed, circuit open | RETRY001 |
| `ConfigurationError` | Invalid config, missing fields | CFG001 |

### Error Mapping by Backend

Errors are automatically mapped based on backend type:

**S3 (boto3/botocore):**
- `ClientError` → `StorageError` with error code and HTTP status
- `BotoCoreError` → `StorageError`

**Azure:**
- `ResourceNotFoundError` → `StorageError`
- `AzureError` → `StorageError`

**API (requests):**
- `Timeout` → `ExtractionError` (retryable)
- `ConnectionError` → `ExtractionError` (retryable)
- `HTTPError` → `ExtractionError` or `AuthenticationError`

**Local filesystem:**
- `FileNotFoundError` → `StorageError`
- `PermissionError` → `StorageError`

---

## Failure Modes

### Fail-Fast Mode

The default behavior stops on first non-retryable error:
- Validates config before starting
- Stops immediately on authentication failures
- Fails if critical resources are missing

### Continue-On-Error Mode

For some operations, partial success is acceptable:
- Individual record failures don't stop the pipeline
- Errors are logged and counted
- Final status reflects partial success

Configure in your source:
```yaml
run:
  continue_on_error: true
  max_error_rate: 0.05  # fail if >5% of records error
```

---

## Debugging Retry Behavior

### Enable Debug Logging

```bash
export BRONZE_LOG_LEVEL=DEBUG
python bronze_extract.py --config config.yaml --date 2025-01-15
```

### Understanding Retry Logs

```
DEBUG Attempt 1 failed: ConnectionError: Connection refused
DEBUG Waiting 0.52s before retry (attempt 2/5)
DEBUG Attempt 2 failed: ConnectionError: Connection refused
DEBUG Waiting 1.1s before retry (attempt 3/5)
DEBUG Attempt 3 succeeded
```

### Identifying Persistent Failures

If you see `RetryExhaustedError`, check:
1. Is the service actually down? Test manually
2. Are credentials valid? Check `AUTH001` errors
3. Is the URL correct? Check `404` responses
4. Is the network blocked? Check firewall rules

---

## Best Practices

### For Unreliable APIs

```yaml
source:
  type: api
  url: https://flaky-api.example.com
  rate_limit:
    rps: 5  # Conservative rate
    burst: 10
  # Consider these approaches:
  # - Lower RPS to reduce pressure
  # - Increase timeouts for slow responses
  # - Use async mode for better throughput
  async: true
```

### For Rate-Limited APIs

```yaml
source:
  type: api
  url: https://api.example.com
  rate_limit:
    rps: 10  # Match API's documented limit
    burst: 1  # No bursting to stay under limit
```

### For Database Connections

Database connections typically fail fast or succeed:
- Use lower `max_attempts` (3 instead of 5)
- Use longer `base_delay` (1.0s instead of 0.5s)
- Connection pooling handles most transient issues

### For Large File Uploads

Storage operations have separate circuit breakers per operation type:
- `upload` - File uploads to cloud storage
- `download` - File downloads from cloud storage
- `list` - Directory listings
- `delete` - File deletions

Each operation type fails independently.

---

## Metrics and Monitoring

### Available Metrics

Rate limiter emits metrics when a component label is provided:
```
INFO metric=rate_limit component=ApiExtractor phase=acquire tokens=4.50 capacity=10 rate=10.00
```

Circuit breaker emits state change metrics:
```
INFO metric=breaker_state component=S3Storage state=open
```

### Alerting Recommendations

Set up alerts for:
- `RETRY001` errors - Indicates persistent failures
- Circuit breaker state changes - Indicates service issues
- High error rates in extraction logs

---

## Quick Reference

### Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `BRONZE_API_RPS` | Global API rate limit | `10` |
| `BRONZE_LOG_LEVEL` | Logging verbosity | `DEBUG` |

### Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| RETRY001 | Retries exhausted / circuit open | Check upstream service health |
| EXT001 | Extraction failed | Check source connectivity |
| STG001 | Storage operation failed | Check storage credentials/permissions |
| AUTH001 | Authentication failed | Check API keys/tokens |

### See Also

- [Troubleshooting Guide](../framework/operations/troubleshooting-guide.md) - General debugging
- [Error Codes Reference](../framework/operations/ERROR_CODES.md) - All error codes
- [Config Reference](../framework/reference/CONFIG_REFERENCE.md) - All configuration options
