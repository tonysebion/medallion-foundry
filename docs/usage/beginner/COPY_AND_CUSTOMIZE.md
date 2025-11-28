# Copy & Customize Guide

This guide shows you how to take existing example configs and customize them for your specific data sources.

## Quick Start: Copy a Config

### 1. Choose Your Starting Point

| Your Data Source | Start With | Why |
|------------------|------------|-----|
| **REST API** | `docs/examples/configs/examples/api_example.yaml` | Complete API setup with auth, pagination |
| **Database** | `docs/examples/configs/examples/db_example.yaml` | SQL query patterns and incremental loading |
| **CSV/JSON Files** | `docs/examples/configs/examples/file_example.yaml` | File format detection and column mapping |
| **Custom Source** | `docs/examples/configs/examples/custom_example.yaml` | Base template for custom extractors |

### 2. Copy to Your Config Directory

```bash
# Create config directory if it doesn't exist
mkdir -p config

# Copy your chosen template
cp docs/examples/configs/examples/api_example.yaml config/my_api.yaml
```

### 3. Customize the Config

Edit `config/my_api.yaml` and update these key sections:

#### Basic Identity
```yaml
environment: prod          # dev | staging | prod
domain: sales             # business domain
system: shopify           # source system name
entity: orders            # table/entity name
```

#### Connection Details
```yaml
source:
  type: api
  api:
    base_url: https://your-api.com
    endpoint: /v1/orders
    auth:
      bearer_token_env: MY_API_TOKEN  # Set this env var
```

#### Data Mapping
```yaml
silver:
  natural_keys:
    - order_id              # Your primary key
  event_ts_column: created_at
  attributes:
    - customer_id
    - total_amount
    - status
```

## Step-by-Step Customization

### For API Sources

1. **Update URLs and endpoints:**
   ```yaml
   api:
     base_url: https://api.yourcompany.com
     endpoint: /v2/orders
   ```

2. **Set authentication:**
   ```yaml
   auth:
     bearer_token_env: MY_API_TOKEN  # Export: export MY_API_TOKEN="your-token"
   # OR for API keys:
   auth:
     header_name: X-API-Key
     header_value_env: MY_API_KEY
   ```

3. **Configure pagination:**
   ```yaml
   pagination:
     type: offset        # offset | cursor | page
     limit_param: limit
     offset_param: offset
   ```

4. **Map your data fields:**
   ```yaml
   silver:
     natural_keys: [id]
     event_ts_column: created_at
     attributes: [name, email, status]
   ```

### For Database Sources

1. **Update connection:**
   ```yaml
   db:
     conn_str_env: MY_DB_CONN  # export MY_DB_CONN="mssql://user:pass@host/db"
   ```

2. **Modify the query:**
   ```yaml
   query: |
     SELECT id, name, created_at, status
     FROM orders
     WHERE created_at >= '{{start_date}}'
   ```

3. **Set incremental loading:**
   ```yaml
   incremental:
     cursor_column: id
     state_file: .cursor/orders.state
   ```

### For File Sources

1. **Update file path:**
   ```yaml
   file:
     path: ./data/my_orders.csv
     format: csv  # csv | json | parquet
   ```

2. **Configure columns:**
   ```yaml
   columns: [id, name, amount, date]  # Optional: specify columns to load
   ```

## Testing Your Custom Config

### 1. Validate Configuration
```bash
python bronze_extract.py --config config/my_api.yaml --validate-only
```

### 2. Dry Run (No Data Changes)
```bash
python bronze_extract.py --config config/my_api.yaml --dry-run
```

### 3. Test with Small Date Range
```bash
python bronze_extract.py --config config/my_api.yaml --date 2025-11-13
```

### 4. Check Results
```bash
# View metadata
cat output/system=your_system/table=your_entity/dt=2025-11-13/_metadata.json

# View sample data
python -c "import pandas as pd; print(pd.read_parquet('output/system=your_system/table=your_entity/dt=2025-11-13/part-0001.parquet').head())"
```

## Advanced Customization

### Environment-Specific Configs
Create multiple versions for different environments:

```bash
cp config/my_api.yaml config/my_api_dev.yaml
cp config/my_api.yaml config/my_api_prod.yaml
```

### Using Templates with Variables
For multiple similar sources, use a base template and customize:

```yaml
# config/base_api.yaml
source:
  type: api
  api:
    base_url: https://api.yourcompany.com
    auth:
      bearer_token_env: API_TOKEN

# Then inherit and customize
# config/orders_api.yaml
_extends: config/base_api.yaml
source:
  api:
    endpoint: /v1/orders
silver:
  entity: orders
```

## Common Customizations

### Adding Error Handling
```yaml
run:
  error_handling:
    enabled: true
    max_bad_records: 100
    max_bad_percent: 5.0
```

### Configuring File Sizes
```yaml
run:
  max_file_size_mb: 256
  max_rows_per_file: 100000
```

### Setting Up Parallel Processing
```yaml
run:
  parallel_workers: 4
```

## Troubleshooting

### Config Validation Errors
- Check YAML syntax with: `python -c "import yaml; yaml.safe_load(open('config/my_config.yaml'))"`
- Ensure all required fields are present
- Verify environment variables are set

### No Data Extracted
- Check API/database connectivity
- Verify authentication credentials
- Review date ranges and filters
- Check logs for specific error messages

### Performance Issues
- Reduce `max_rows_per_file` for faster processing
- Increase `parallel_workers` for multi-core systems
- Use `--dry-run` to test without data transfer

## Next Steps

- **Run Silver Promotion**: `python silver_extract.py --config config/my_config.yaml --date 2025-11-13`
- **Set Up Monitoring**: Add webhook URLs for success/failure notifications
- **Automate**: Create cron jobs or orchestration workflows
- **Scale**: Configure partitioning and parallel processing

## Need Help?

- **Pattern Selection**: `docs/usage/patterns/QUICK_REFERENCE.md`
- **Detailed Config Reference**: `docs/framework/reference/CONFIG_REFERENCE.md`
- **Onboarding Guide**: `docs/usage/onboarding/intent-owner-guide.md`
