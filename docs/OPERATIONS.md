# Operations Guide

This guide covers testing, troubleshooting, error handling, and operational concerns.

## Testing

### Quick Start

```bash
# Run all tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=pipelines

# Unit tests only
python -m pytest tests/unit/

# Integration tests
export RUN_INTEGRATION=1
python -m pytest tests/ -m integration
```

### Test Categories

- `unit` - Fast tests, no external dependencies
- `integration` - Requires emulators (MinIO, Azurite)
- `slow` - Long-running tests
- `async` - Async HTTP extraction tests

### Code Quality

```bash
# Linting
ruff check .

# Formatting
ruff format .

# Type checking
mypy pipelines/
```

## Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| CFG001 | Configuration validation error | Check config YAML/schema |
| EXT001 | Extractor failure (API/DB/file) | Check source connectivity |
| STG001 | Storage backend error | Check S3/Azure credentials |
| AUTH001 | Authentication failure | Verify API keys/tokens |
| PAGE001 | Pagination error | Check API response format |
| STATE001 | Invalid state transition | Check watermark files |
| QUAL001 | Data quality threshold exceeded | Review rejected records |
| RETRY001 | Retry exhaustion / circuit open | Check upstream service health |

## Troubleshooting

### Quick Diagnosis

```bash
# Enable debug logging
export BRONZE_LOG_LEVEL=DEBUG
python -m pipelines myteam.orders --date 2025-01-15

# Validate config
python -m pipelines myteam.orders --date 2025-01-15 --dry-run

# Check connectivity
python -m pipelines test-connection db --host ... --database ...
```

### Common Issues

**Access Denied (S3)**
```bash
aws sts get-caller-identity  # Verify credentials
aws s3 ls s3://my-bucket/    # Test bucket access
```

**Invalid Credentials (API)**
- Check environment variables: `echo $MY_API_TOKEN`
- Verify token expiration
- Test manually with curl

**No Data Found**
- Check source data exists for date range
- Use `--dry-run` to see planned paths
- Verify watermark isn't ahead of data

**Memory Issues**
- Reduce `max_rows_per_file`
- Enable chunked processing
- Increase system memory

## Error Handling & Resilience

### Built-in Patterns

| Pattern | Purpose | Default |
|---------|---------|---------|
| Retry | Retry with exponential backoff | 5 attempts, 0.5s-8s delays |
| Circuit Breaker | Stop calling failing services | Opens after 5 failures, 30s cooldown |
| Rate Limiter | Throttle API requests | Configurable via config or env |

### Retry Behavior

Retryable errors (will retry):
- Timeouts, connection errors
- HTTP 429 (Too Many Requests)
- HTTP 5xx (Server errors)

Non-retryable (immediate failure):
- HTTP 400, 401, 403, 404
- ValueError, TypeError

### Rate Limiting

```bash
# Via environment
export BRONZE_API_RPS=10

# Via config
source:
  rate_limit:
    rps: 10
    burst: 20
```

The `RateLimiter` class in `pipelines.lib.rate_limiter` uses token-bucket algorithm.

## S3 Setup

### Configure Credentials

```bash
# Option A: AWS CLI
aws configure

# Option B: Environment
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

### Create Bucket

```bash
aws s3 mb s3://my-medallion-bucket
aws s3api put-bucket-versioning --bucket my-medallion-bucket --versioning-configuration Status=Enabled
```

### Test Connection

```bash
aws sts get-caller-identity
aws s3 ls s3://my-medallion-bucket/
```

### MinIO (Local S3)

```bash
# Start MinIO
docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"

# Configure
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
```

## Day-to-Day Operations

### Running Pipelines

```bash
# Bronze runs are idempotent per config+date
python -m pipelines myteam.orders --date 2025-01-15

# Dry run to smoke test
python -m pipelines myteam.orders --date 2025-01-15 --dry-run
```

### Output Artifacts

Bronze and Silver outputs include:
- `_metadata.json` - Source info, watermarks, lineage
- `_checksums.json` - SHA256 hashes for data integrity

### Secrets & Credentials

- Use `${VAR_NAME}` syntax in configs for environment variables
- Never commit real credentials
- Inject secrets via scheduler, `.env`, or secret manager

### Disaster Recovery

- Directories are deterministic (`system/entity/dt=YYYY-MM-DD/`)
- Re-running same config+date is the simplest recovery
- Keep `_metadata.json` and `_checksums.json` backed up

## Monitoring

### Log Patterns

```bash
# Search for errors
grep "ERROR" logs/*.log

# Count error types
grep "ERROR" logs/*.log | cut -d' ' -f4 | sort | uniq -c | sort -nr
```

### Metrics

Rate limiter logs:
```
INFO metric=rate_limit component=ApiExtractor phase=acquire tokens=4.50 capacity=10 rate=10.00
```

Circuit breaker state changes:
```
INFO metric=breaker_state component=S3Storage state=open
```

### Alerting

Set up alerts for:
- `RETRY001` errors - persistent failures
- Circuit breaker state changes
- High error rates

## Azure Setup (Optional)

### Azurite Emulator

```bash
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite

export AZURE_STORAGE_ACCOUNT=devstoreaccount1
export AZURE_STORAGE_KEY="Eby8vdM02xNOcqFeq..."  # default azurite key
export AZURE_STORAGE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1
```
