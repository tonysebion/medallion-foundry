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

### Optional: `platform.dlt`

If you use `dlt` for state or more advanced pipelines, you can add a semantic section like:

```yaml
platform:
  dlt:
    enabled: true
    pipeline_name_prefix: "api_bronze_"
```

The scaffold does not fully implement all dlt features; this section is here to standardize how you might configure it later.

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

## Enforcement and ownership

- Changes to the **`platform`** section should be coordinated and reviewed by the platform team.
- Changes in the **`source`** section are expected and are where domain teams declare their data product responsibilities.
