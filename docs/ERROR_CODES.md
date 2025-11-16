# Error Codes

This project standardizes error codes across core exceptions to make on-call diagnosis faster. Codes appear in logs and exception strings.

Common codes:

- CFG001: Configuration validation errors (missing/invalid fields, schema mismatch)
- EXT001: Extractor failures (API/DB/file)
- STG001: Storage backend errors (S3/Azure/local)
- AUTH001: Authentication failures (missing/invalid credentials)
- PAGE001: Pagination logic errors
- STATE001: Invalid state transitions
- QUAL001: Data quality thresholds exceeded
- RETRY001: Retry exhaustion or circuit breaker open

Where to find them:

- Implementation: `core/exceptions.py`
- Wrappers mapping SDK errors: `core/error_wrapper.py`

Operational guidance:

- Group alerts by code to route to the right team (platform vs. source owner).
- For `AUTH001`, verify secret injection and token scopes.
- For `RETRY001`, check upstream health, increase backoff, or lower RPS.
- For `QUAL001`, inspect Silver error handling config and sample rejected rows.
