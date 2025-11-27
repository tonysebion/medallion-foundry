# Real-World Example Configurations

- API: `api_simple.yaml` / `api_complex.yaml`
- DB: `db_simple.yaml` / `db_complex.yaml`
- File-based: `file_simple.yaml` / `file_complex.yaml`
- Custom extractor: `custom_simple.yaml` / `custom_complex.yaml`


Example configurations for popular SaaS platforms and APIs.

## Shopify API

Extract orders from Shopify:

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
      partition_strategy: "date"
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "shopify"
  table: "orders"

  api:
    base_url: "https://your-store.myshopify.com"
    endpoint: "/admin/api/2024-01/orders.json"
    method: "GET"

    # Shopify uses custom token in header
    auth_type: "api_key"
    auth_key_header: "X-Shopify-Access-Token"
    auth_key_env: "SHOPIFY_ACCESS_TOKEN"

    # Shopify uses page-based pagination with link headers
    pagination:
      type: "page"
      page_size: 250  # Shopify max
      page_param: "page"
      limit_param: "limit"

    # Extract orders from nested response
    data_path: "orders"

  run:
    max_rows_per_file: 50000
    max_file_size_mb: 256
    write_csv: false
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
    timeout_seconds: 60
```

## Salesforce REST API

Extract Account records from Salesforce:

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "salesforce"
  table: "accounts"

  api:
    base_url_env: "SALESFORCE_INSTANCE_URL"  # e.g., https://yourorg.my.salesforce.com
    endpoint: "/services/data/v59.0/query"
    method: "GET"

    # Salesforce uses OAuth bearer token
    auth_type: "bearer"
    auth_token_env: "SALESFORCE_ACCESS_TOKEN"

    # SOQL query
    params:
      q: "SELECT Id, Name, Industry, AnnualRevenue, CreatedDate, LastModifiedDate FROM Account WHERE LastModifiedDate >= LAST_N_DAYS:7"

    # Salesforce uses cursor-based pagination
    pagination:
      type: "cursor"
      cursor_param: "nextRecordsUrl"
      cursor_path: "nextRecordsUrl"

    # Extract records from response
    data_path: "records"

  run:
    max_rows_per_file: 100000
    max_file_size_mb: 512
    write_csv: true
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
    timeout_seconds: 120
```

## Stripe API

Extract payment transactions:

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
      partition_strategy: "hourly"  # High-frequency data
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "stripe"
  table: "charges"

  api:
    base_url: "https://api.stripe.com"
    endpoint: "/v1/charges"
    method: "GET"

    # Stripe uses bearer token (secret key)
    auth_type: "bearer"
    auth_token_env: "STRIPE_SECRET_KEY"

    # Stripe cursor-based pagination
    pagination:
      type: "cursor"
      page_size: 100  # Stripe max
      cursor_param: "starting_after"
      cursor_path: "data.-1.id"  # Last record's ID

    data_path: "data"

    # Rate limiting considerations
    headers:
      Stripe-Version: "2023-10-16"

  run:
    max_rows_per_file: 50000
    max_file_size_mb: 256
    write_csv: false
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
    timeout_seconds: 30
    parallel_workers: 2  # Conservative for rate limits
```

## GitHub API

Extract repository issues:

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: true
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "github"
  table: "issues"

  api:
    base_url: "https://api.github.com"
    endpoint: "/repos/owner/repo/issues"
    method: "GET"

    # GitHub personal access token
    auth_type: "bearer"
    auth_token_env: "GITHUB_TOKEN"

    # GitHub uses page-based pagination
    pagination:
      type: "page"
      page_size: 100  # GitHub max
      page_param: "page"
      limit_param: "per_page"

    # Optional filters
    params:
      state: "all"
      sort: "updated"
      direction: "desc"

    headers:
      Accept: "application/vnd.github+json"
      X-GitHub-Api-Version: "2022-11-28"

  run:
    max_rows_per_file: 10000
    max_file_size_mb: 128
    write_csv: true
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
    timeout_seconds: 30
```

## HubSpot CRM API

Extract contacts:

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "api"
  system: "hubspot"
  table: "contacts"

  api:
    base_url: "https://api.hubapi.com"
    endpoint: "/crm/v3/objects/contacts"
    method: "GET"

    # HubSpot private app token
    auth_type: "bearer"
    auth_token_env: "HUBSPOT_ACCESS_TOKEN"

    # HubSpot pagination
    pagination:
      type: "cursor"
      page_size: 100
      cursor_param: "after"
      cursor_path: "paging.next.after"

    data_path: "results"

    # Request specific properties
    params:
      properties: "email,firstname,lastname,createdate,lastmodifieddate"

  run:
    max_rows_per_file: 50000
    max_file_size_mb: 256
    write_csv: false
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
    timeout_seconds: 60
```

## Google Analytics Data API

Extract website sessions (requires service account):

```yaml
platform:
  bronze:
    s3_bucket: "my-data-lake"
    s3_prefix: "bronze"
    storage_backend: "s3"
    partitioning:
      use_dt_partition: true
      partition_strategy: "date"
    output_defaults:
      allow_csv: false
      allow_parquet: true
      parquet_compression: "snappy"

  s3_connection:
    endpoint_url_env: "AWS_S3_ENDPOINT"
    access_key_env: "AWS_ACCESS_KEY_ID"
    secret_key_env: "AWS_SECRET_ACCESS_KEY"

source:
  type: "custom"  # Requires custom extractor for GA4 API
  system: "google_analytics"
  table: "sessions"

  custom_extractor:
    module: "custom_extractors.google_analytics"
    class_name: "GoogleAnalyticsExtractor"
    config:
      property_id: "123456789"
      credentials_env: "GOOGLE_APPLICATION_CREDENTIALS"
      dimensions:
        - "date"
        - "deviceCategory"
        - "country"
      metrics:
        - "sessions"
        - "users"
        - "pageviews"
      date_range_days: 7

  run:
    max_rows_per_file: 100000
    max_file_size_mb: 512
    write_csv: false
    write_parquet: true
    storage_enabled: true
    local_output_dir: "./output"
```

## Notes

### Authentication

- **API Keys**: Store in environment variables, never in config files
- **OAuth Tokens**: Use service accounts or long-lived tokens for automation
- **Refresh Logic**: Not built-in; handle token refresh externally

### Rate Limiting

Configure `parallel_workers` and `timeout_seconds` based on API rate limits:

- **Stripe**: 100 requests/second (use `parallel_workers: 2-4`)
- **GitHub**: 5,000 requests/hour (use `parallel_workers: 1-2`)
- **Shopify**: 2 requests/second (use `parallel_workers: 1`)
- **Salesforce**: 15,000 requests/24 hours (monitor carefully)

### Pagination

Different APIs use different pagination strategies:

- **Offset-based**: Skip N records (`offset`, `skip`)
- **Page-based**: Request page numbers (`page`)
- **Cursor-based**: Use opaque tokens (`cursor`, `next_token`)
- **Link-based**: Follow `Link` headers (not yet supported - use cursor)

### Testing

Always test with small date ranges first:

```bash
# Test configuration
python bronze_extract.py --config config/shopify_orders.yaml --validate-only

# Dry run
python bronze_extract.py --config config/shopify_orders.yaml --dry-run

# Small extraction
python bronze_extract.py --config config/shopify_orders.yaml --date 2025-01-12
```

See [DOCUMENTATION.md](../framework/reference/DOCUMENTATION.md) for complete API extractor documentation.
