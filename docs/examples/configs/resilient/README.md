# Resilient Pipeline Configuration Examples

These examples demonstrate how to configure Bronze Foundry for maximum reliability when dealing with unreliable sources, rate-limited APIs, or flaky network connections.

## Examples in This Directory

| File | Description | Use Case |
|------|-------------|----------|
| `resilient_api_example.yaml` | API with rate limiting and retry hints | Rate-limited REST APIs |
| `resilient_db_example.yaml` | Database with connection retry | Unreliable DB connections |
| `resilient_file_example.yaml` | File source with storage resilience | Cloud storage with transient errors |

## Key Resilience Features

### Rate Limiting
```yaml
source:
  rate_limit:
    rps: 10      # Requests per second
    burst: 20    # Burst capacity
```

### Async Mode for APIs
```yaml
source:
  async: true  # Non-blocking HTTP requests
```

### Continue on Partial Errors
```yaml
run:
  continue_on_error: true
  max_error_rate: 0.05  # Fail if >5% errors
```

## Built-In Defaults

Bronze Foundry includes sensible defaults for resilience:

| Pattern | Default Value |
|---------|---------------|
| Max retry attempts | 5 |
| Base retry delay | 0.5 seconds |
| Max retry delay | 8.0 seconds |
| Backoff multiplier | 2.0x |
| Jitter | 20% |
| Circuit breaker threshold | 5 failures |
| Circuit cooldown | 30 seconds |

## Environment Variables

Override defaults with environment variables:

```bash
export BRONZE_API_RPS=10       # Global API rate limit
export BRONZE_LOG_LEVEL=DEBUG  # Verbose logging for debugging
```

## See Also

- [Error Handling Guide](../../../guides/error_handling.md) - Complete resilience documentation
- [Troubleshooting Guide](../../../framework/operations/troubleshooting-guide.md) - Common issues
