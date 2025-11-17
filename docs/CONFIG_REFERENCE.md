# Config Reference

Each config file is a YAML document with two top-level sections:

- `platform` – owned and managed by the data platform / central team.
- `source` – owned by the domain/product/data team responsible for a given data product.

```yaml
platform:
  ...

source:
  ...
```

## `platform` section

### `platform.bronze`

Controls where Bronze data is written and how it is structured.

```yaml
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"
```

- `s3_bucket` – target bucket for Bronze data.
- `s3_prefix` – prefix under which all Bronze data will live (e.g., `bronze`).
- `partitioning.use_dt_partition` – when true, includes `dt=YYYY-MM-DD/` in the path.
- `partitioning.partition_strategy` – *(NEW)* partition strategy for multiple daily loads:
  - `"date"` (default) – one partition per day: `dt=YYYY-MM-DD/`
  - `"hourly"` – one partition per hour: `dt=YYYY-MM-DD/hour=HH/`
  - `"timestamp"` – minute-level granularity: `dt=YYYY-MM-DD/batch=YYYYMMDD_HHMM/`
  - `"batch_id"` – custom batch tracking: `dt=YYYY-MM-DD/batch_id=<custom>/`
- `output_defaults` – default behavior for output formats.
  - `allow_csv` – globally allow CSV creation.
  - `allow_parquet` – globally allow Parquet creation.
  - `parquet_compression` – default compression for Parquet (e.g., `snappy`, `gzip`, `brotli`, `lz4`, `zstd`).
- `storage_metadata` – classification metadata for the target:
  - `boundary` (required when `--storage-scope onprem` is used): `onprem` or `cloud`.
  - `provider_type`: e.g., `s3_local`, `s3_cloud`, `azure_blob`, `azure_adls`, `local_generic`.
  - `cloud_provider`: `azure`, `aws`, `gcp`, or `null`.

  Use this metadata when you need to distinguish storage targets in governance policies (on-prem vs. cloud).

### Storage policy enforcement

- `--storage-scope`/`--onprem-only` flags (Available on both Bronze/Silver CLIs) enforce the metadata above. When set to `onprem`, all storage targets must declare `boundary: onprem` and Azure/backed cloud providers are rejected up front.

### `platform.s3_connection`

Controls how the S3-compatible client is configured.

```yaml
platform:
  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"
```

Values are environment variable **names**, not secrets themselves.

## `source` section

### Common fields

```yaml
source:
  type: "api"            # "api" | "db" | "custom"
  system: "claims"
  table: "claims_header"
  run:
    max_rows_per_file: 500000
    write_csv: true
    write_parquet: true
    s3_enabled: true
    local_output_dir: "./output"
```

- `type` – extractor type; determines which built-in or custom class will be used.
- `system` – logical system identifier (becomes `system=<system>` in the path).
- `table` – logical table identifier (becomes `table=<table>` in the path).
- `run.max_rows_per_file` – max rows per file; if 0 or omitted, writes a single file.
- `run.max_file_size_mb` – *(NEW)* maximum file size in MB (takes precedence over `max_rows_per_file`). Recommended: 128-512 for analytics optimization.
- `run.parallel_workers` – *(NEW)* number of parallel threads for chunk processing within this extraction (default: 1). Use 4-8 for large extractions with many chunks.
- `run.write_csv` – whether to write CSV locally (allowed only if platform allows).
- `run.write_parquet` – whether to write Parquet locally (allowed only if platform allows).
- `run.s3_enabled` – whether to upload written files to S3.
- `run.local_output_dir` – base directory for local writes.
- `run.cleanup_on_failure` – *(default: true)* whether to delete local files if extraction fails.
- `run.batch_id` – *(optional)* custom batch ID for `partition_strategy: "batch_id"`. Auto-generated if omitted.

### `source.api` (for `type: api`)

```yaml
source:
  type: "api"
  system: "example_product"
  table: "tickets"

  api:
    base_url: "https://api.example.com"
    endpoint: "/v1/tickets"
    method: "GET"

    auth_type: "bearer"              # "bearer" | "api_key" | "none"
    auth_token_env: "EXAMPLE_API_TOKEN"

    params:
      page_size: 100

    pagination:
      type: "page"                   # "page" | "none" | (future: "cursor")
      page_param: "page"
      start_page: 1
      max_pages: 1000

    incremental:
      enabled: true
      cursor_field: "updated_at"     # field in the JSON payload
      cursor_param: "updated_since"  # query param to set when re-running
      initial_cursor_value: "2025-01-01T00:00:00Z"

    # Optional: Async + Rate Limiting + Tracing
    async: true                # Enable async httpx-based extractor (overridden by BRONZE_ASYNC_HTTP)
    rate_limit:                # Per-API rate limiting (token bucket)
      rps: 5.0                 # Requests per second (supports fractional)
    # (Tracing is enabled globally via env BRONZE_TRACING)

```

Additional API options:

- `api.async` – when `true` and `httpx` is installed, uses the async HTTP path with prefetch pagination and cooperative backpressure. Override globally with environment variable `BRONZE_ASYNC_HTTP=1` / `true`.
- `api.rate_limit.rps` – requests-per-second token bucket for this API; fractional values allowed (e.g., `0.5` for one request every 2 seconds). Falls back to `run.rate_limit_rps` when absent.
- Environment overrides:
  - `BRONZE_ASYNC_HTTP` – force enable/disable async path (`1/true/yes` to enable).
  - `BRONZE_API_RPS` – global default RPS if neither `api.rate_limit.rps` nor `run.rate_limit_rps` supplied.
  - `BRONZE_TRACING` – enable OpenTelemetry spans (set to `1` / `true`). Safe no-op if instrumentation not present.
- Resilience (implicit): unified retry + circuit breaker automatically wrap API calls; no config needed for defaults.
```

### `source.db` (for `type: db`)

```yaml
source:
  type: "db"
  system: "claims_dw"
  table: "claims_header"

  db:
    conn_str_env: "CLAIMS_DB_CONN_STR"  # env var name for ODBC connection string

    base_query: |
      SELECT
          ClaimID,
          MemberID,
          ServiceDate,
          Amount,
          UpdatedAt
      FROM dbo.ClaimsHeader
      WHERE 1=1

    incremental:
      enabled: true
      cursor_column: "UpdatedAt"
      state_file: "./.state/claims_header.cursor"
      cursor_type: "datetime"           # or "string"
```

### `source.custom` (for `type: custom`)

```yaml
source:
  type: "custom"
  system: "salesforce"
  table: "accounts"

  custom_extractor:
    module: "my_org.custom_extractors.salesforce"
    class_name: "SalesforceExtractor"

  # Optional custom-specific config; your extractor decides what to read:
  salesforce:
    username_env: "SF_USER"
    password_env: "SF_PASS"
    token_env: "SF_TOKEN"
    soql_query: "SELECT Id, Name, LastModifiedDate FROM Account"
    incremental:
      cursor_field: "LastModifiedDate"
```

The framework does not interpret `source.salesforce` directly; it is available for your custom extractor to consume.

### Optional: rate limiting fallback (non-API types)

For certain custom or future extractors, you can declare a run-level rate limit:

```yaml
source:
  run:
    rate_limit_rps: 10  # Fallback if api.rate_limit.rps not set
```

Priority order for determining the active rate: `api.rate_limit.rps` → `run.rate_limit_rps` → env `BRONZE_API_RPS`.

If no value resolves, no throttling is applied.

## Enforcement and ownership

- Changes to the **`platform`** section should be coordinated and reviewed by the platform team.
- Changes in the **`source`** section are expected and are where domain teams declare their data product responsibilities.

## `silver` section (optional)

When you plan to promote Bronze data to Silver with `silver_extract.py`, you can include a `silver` section in the shared config file. This section keeps Silver settings aligned with the Bronze declaration so both CLIs can work from the same artifact.

```yaml
silver:
  output_dir: "./silver_output"
  domain: "claims"
  entity: "claim_header"
  version: 1
  load_partition_name: "load_date"
  include_pattern_folder: false
  write_parquet: true
  write_csv: false
  parquet_compression: "snappy"
  require_checksum: false
  primary_keys: ["claim_id"]
  order_column: "updated_at"
  partitioning:
    columns: ["status"]
  schema:
    rename_map:
      ClaimID: claim_id
    column_order:
      - claim_id
      - policy_id
      - updated_at
  normalization:
    trim_strings: true
    empty_strings_as_null: true
  error_handling:
    enabled: true
    max_bad_records: 100
    max_bad_percent: 5.0
```

- `output_dir` – base folder for Silver medallion layout (default `./silver_output`).
- `domain`, `entity`, `version` – govern the medallion hierarchy: `domain=<domain>/entity=<entity>/v<version>/`.
- `load_partition_name` and `include_pattern_folder` – control the partition folder names (`load_date=YYYY-MM-DD` plus optional `pattern=<full|cdc|current_history>`).
- `write_parquet` / `write_csv` / `parquet_compression` – output format controls that mirror Bronze defaults but scoped to Silver.
- `primary_keys` and `order_column` – required when `source.run.load_pattern` is `current_history` to derive current vs history tables.
- `partitioning.columns` – split Silver outputs into subdirectories (e.g., `status=new/is_current=1`).
- `schema.rename_map` and `schema.column_order` – declarative schema normalization (rename or reorder columns without business logic).
- `normalization.trim_strings` / `empty_strings_as_null` – safe string clean-up routines for Silver.
- `error_handling` – optionally route invalid records to `_errors/<artifact>.csv` before failing hard.
- `require_checksum` – when `true`, `silver_extract` enforces the checksum manifest that Bronze writes to `_checksums.json`. You can also pass `--require-checksum` on the CLI; Silver aborts if the manifest is missing, mismatched, or reports a different load pattern. This keeps Bronze and Silver in lockstep before promoting data.
- `model` – choose the curated Silver asset type. Supported values: `scd_type_1`, `scd_type_2`, `incremental_merge`, `full_merge_dedupe`, `periodic_snapshot`. Defaults derive from the Bronze load pattern (`full` -> `periodic_snapshot`, `cdc` -> `incremental_merge`, `current_history` -> `scd_type_2`). Override via `silver.model` or the CLI `--silver-model` flag.
- Streaming resume: use `--stream` with `--resume` to skip already processed chunks using checkpoint files in the output directory (see `core/checkpoint.py`). Safe for long-running promotions; checkpoints cleared on success.

If the `silver` section is omitted, `silver_extract` falls back to sensible defaults and you can override individual settings with CLI switches.

### Developer notes: typed Silver schema

If you need to enforce new validations or defaults, edit the dataclasses in `core/config_models.py`. The `SilverConfig` model (and its child classes such as `SilverSchema`, `SilverNormalization`, etc.) backs `_normalize_silver_config` in `core/config.py`, so every CLI automatically inherits the same behavior. This keeps docs and implementation aligned—update the dataclass once, add a test, and both Bronze and Silver parsing flows receive the new rules.

### Streaming Silver

- Use `silver_extract.py --stream` to process Bronze chunks sequentially when partitions exceed available memory. The streaming mode respects all existing `silver` settings (schema, normalization, partitioning, error handling, checksum) while writing files chunk-by-chunk and updating metadata/lineage in place.
- Resume support: pass `--resume` to skip chunks already checkpointed after a partial prior run. Combines with `--stream` only. Checkpoints stored alongside output; automatically cleaned at successful completion.
