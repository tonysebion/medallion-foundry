# Performance Tuning Guide

This guide provides recommended settings for optimal Bronze/Silver performance based on data characteristics.

## Bronze File Size Strategy

**Rule of thumb:** Keep each Bronze file between 128 MB and 512 MB so the built-in Silver chunking keeps memory bounded.

| Bronze Partition Size | Guideline | Notes |
|-----------------------|-----------|-------|
| < 100 MB              | 1 file    | Entire partition fits in memory; Silver can process it as one chunk. |
| 100 MB - 1 GB         | 2-6 files | Split by `max_file_size_mb` or `parallel_workers` to balance CPU and I/O. |
| 1 GB - 10 GB          | 8-32 files | Many files keep Silver chunk sizes predictable; consider `max_rows_per_file` to control row counts. |
| > 10 GB               | 32+ files | Use finer-grained Bronze files and parallel Bronze workers to let Silver stream them automatically.

`SilverProcessor` handles chunking internally; you control throughput by tuning how Bronze writes files (see `bronze.options.max_file_size_mb`, `max_rows_per_file`, and partition strategies). Legacy streaming flags remain documented in `docs/framework/operations/legacy-streaming.md`.

## API Extraction Performance

**Async vs Sync:**
- Enable async (`source.api.async: true`) when:
  - Pagination depth > 10 pages
  - Network latency > 50ms
  - RPS limits allow concurrent requests
- Async prefetch improves page-based pagination by ~30-50% (overlaps next page fetch)

**Rate Limiting:**
- Set `source.api.rate_limit.rps` to 80% of upstream API limit to leave headroom for retries
- Example: If API allows 100 RPS, configure `rps: 80`
- Lower RPS when circuit breaker opens frequently (indicates upstream pressure)

**Retry/Circuit Breaker Tuning:**
- Default retry: 5 attempts, exponential backoff (0.5s → 8s max)
- Circuit breaker: opens after 5 failures, cools down for 30s
- Adjust via `source.run` config:
  ```yaml
  run:
    max_retry_attempts: 3  # fewer retries if API is unstable
    retry_base_delay: 1.0  # longer initial delay for slow APIs
  ```

## Parallelism

**Bronze Extraction:**
- `--parallel-workers N`: Runs N configs concurrently
  - Set to # of CPU cores for CPU-bound transforms
  - Set to 2-4x cores for I/O-bound API/DB extracts (overlaps network waits)


## Memory Profiling

Large Bronze partitions still risk growth when Silver loads them into pandas. Monitor system memory (Task Manager, `top`, etc.) and adjust the Bronze chunking controls listed above before running Silver. Use `python -m scripts.benchmark --scenario rate_limiting` or custom harnesses to exercise the CPU/I/O profile of your environment, and instrument `silver_extract.py` with `python -m memory_profiler silver_extract.py --config myconfig.yaml` to inspect hotspots (no streaming flags are needed today). Historical streaming controls are documented in `docs/framework/operations/legacy-streaming.md` if you need reference material.

## Storage Backend Optimization

**S3:**
- Enable multipart upload for files > 100 MB (automatic in boto3)
- Use same region as compute to minimize latency

**Azure:**
- Use Blob Storage (not ADLS Gen2) if append operations aren't needed
- Configure `max_concurrency` in SDK for parallel block uploads

**Local:**
- Use SSD for Bronze/Silver partitions when ingesting large Bronze chunks
- Pre-create output directories to avoid mkdir overhead per chunk

## Observability

**Tracing:**
- Enable `BRONZE_TRACING=1` to measure time in API requests, transforms, writes
- Use OTLP collector to visualize span waterfall in Jaeger/Zipkin

**Metrics:**
- Track: API response time (p50/p95), chunk processing rate, circuit breaker state
- Alert on: retry exhaustion, circuit open > 5 min, chunk throughput falling below baseline

## Quick Wins

1. **Enable async for paginated APIs:** Add `api.async: true`
2. **Control Bronze file sizes:** Start with `bronze.options.max_file_size_mb` at ~256 and adjust `max_rows_per_file` to keep each file manageable.
3. **Rate limit to avoid 429s:** Configure `api.rate_limit.rps` at 80% of API limit
4. **Verify metadata before reruns:** Use `_metadata.json`/`_checksums.json` to ensure Bronze/Silver stay in sync before retrying.
5. **Benchmark first:** Run `scripts/benchmark.py` to establish baselines before tuning
