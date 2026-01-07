# API Rate Limiting

APIs often have shared quotas or burst limits, so Bronze Foundry ships with a token-bucket rate limiter that guards every HTTP request. The limiter ensures downstream services stay happy while still letting your connector make progress.

## How it works

The `RateLimiter` class in `pipelines.lib.rate_limiter` provides token-bucket rate limiting. The rate limiter:

- refills tokens at `rps` (requests per second) and allows bursts up to the `burst` capacity.
- emits log lines when tokens are refilled or acquired (`metric=rate_limit component=<name>…`).
- supports both sync (`acquire`) and async (`async_acquire`) callers so both `_call_api` and `_call_async_api` paths respect the same limit.

Because the limiter is built in, you only need to configure your intent file.

## Configuring rate limits

Set a `rate_limit` block inside your `source` definition:

```yaml
source:
  type: api

  rate_limit:
    rps: 8            # Max requests per second (fractional values allowed)
    burst: 16         # Maximum bucket capacity for short bursts (defaults to ceil(rps))

run:
  rate_limit_rps: 5    # Fallback if the extractor config omits `rate_limit`
```

Priority order:

1. `source.<type>.rate_limit.rps`
2. `run.rate_limit_rps`
3. Environment variable (`BRONZE_API_RPS`, `BRONZE_DB_RPS`, etc.)

`burst` defaults to `ceil(rps)` when omitted, but set it smaller if your service cannot tolerate sudden spikes.

## Best practices

- **Start conservative**: choose 80% of the documented API limit so intermittent retries don’t push you above quota.
- **Combine rate limiting with retries**: when `RateLimiter` pauses execution, retry backoff continues from that moment. The limiter therefore smooths bursts before retries even begin.
- **Monitor metrics**: the library logs lines such as
  ```
  INFO metric=rate_limit component=ApiExtractor phase=acquire tokens=3.20 capacity=10 rate=8.00
  ```
  Use these to verify you’re staying below upstream thresholds.
- **Use different limits for test vs production** by swapping configs or environment variables, which lets you run faster tests without affecting production API quotas.

## Async vs sync enforcement

Whether HTTP requests happen inside async tasks or synchronous code, Bronze Foundry routes both through the same limiter:

- Async extractors call `async_acquire`, so `asyncio` sleeps until tokens are available.
- Sync extractors block on `acquire`, so the worker pauses politely rather than spinning.

No additional code changes are required; the rate limiter hooks are part of every HTTP extractor via the `ResilientExtractorMixin`.

## Troubleshooting

- If you still get 429 responses:
  - Verify the `rps` value accounts for all concurrent workers.
  - Check that you are not overriding `rate_limit` with a higher `run.rate_limit_rps`.
  - Inspect logs for `metric=rate_limit` to see whether tokens were exhausted (`phase=acquire` with low `tokens`).
- When rate limiting throttles other components (silver, storage uploads), consider raising `burst` temporarily while keeping `rps` steady.

Use this guide together with the [Error Handling & Resilience guide](error_handling.md) for full visibility into retry, breaker, and rate-limit interactions.
