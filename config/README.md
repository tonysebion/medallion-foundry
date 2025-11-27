# Config Directory

This directory is your **workspace** for storing project-specific configuration files.

## Purpose

Store your YAML configuration files here for different data sources and extraction jobs. This directory is **git-ignored** to prevent accidentally committing sensitive credentials or environment-specific settings.

## Getting Started

### 1. Copy Example Configurations

Choose a template from the examples directory:

```bash
# Quick API test (recommended for first-time users)
cp docs/examples/configs/quickstart/quick_test.yaml config/my_first_api.yaml

# Full API configuration
cp docs/examples/configs/examples/api_example.yaml config/my_api.yaml

# Database extraction
cp docs/examples/configs/examples/db_example.yaml config/my_db.yaml

# Custom extractor
cp docs/examples/configs/examples/custom_example.yaml config/my_custom.yaml
```

### 2. Edit Configuration

Open your copied file and update:

- **System and table names**: Identify your data source
- **Connection details**: API URLs, database connections
- **Authentication**: Environment variable names for credentials
- **Storage settings**: S3/Azure bucket details
- **Output preferences**: File formats, partitioning strategy

### 3. Set Environment Variables

```bash
# Example for API with bearer token
export MY_API_TOKEN="your-token-here"

# Example for S3 storage
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export BRONZE_S3_ENDPOINT="https://s3.amazonaws.com"
```

### 4. Run Extraction

```bash
python bronze_extract.py --config config/my_first_api.yaml
```

## Directory Structure Example

Organize your configs by purpose:

```
config/
├── README.md                    # This file
├── production/
│   ├── sales_api.yaml          # Production sales data
│   ├── customer_db.yaml        # Customer database
│   └── inventory_api.yaml      # Inventory system
├── staging/
│   ├── sales_api_staging.yaml
│   └── customer_db_staging.yaml
├── development/
│   └── test_api.yaml           # Local development testing
└── .gitkeep                     # Keeps directory in git
```

## Configuration Templates

### Available Examples

All example configurations are in `docs/examples/configs/`:

| Template | Purpose | Use When |
|----------|---------|----------|
| `quickstart/quick_test.yaml` | Fast API validation | Testing if API is data-ready |
| `examples/api_example.yaml` | Full API extraction | Production API jobs |
| `examples/db_example.yaml` | Database extraction | SQL Server, PostgreSQL, MySQL |
| `examples/custom_example.yaml` | Custom Python extractor | Special data sources |
| `advanced/enhanced_example.yaml` | Advanced features | File sizing, parallelism, hourly loads |

### Configuration Reference

See complete documentation:
- **Full reference**: [docs/framework/reference/CONFIG_REFERENCE.md](../docs/framework/reference/CONFIG_REFERENCE.md)
- **Quick reference**: [docs/framework/reference/QUICK_REFERENCE.md](../docs/framework/reference/QUICK_REFERENCE.md)
- **Examples README**: [docs/examples/README.md](../docs/examples/README.md)

## Best Practices

### 1. Never Commit Credentials

❌ **Don't do this:**
```yaml
api:
  auth_token: "hardcoded-secret-token-123"  # NEVER!
```

✅ **Do this instead:**
```yaml
api:
  auth_token_env: "MY_API_TOKEN"  # Reference environment variable
```

### 2. Use Descriptive Names

```yaml
source:
  system: "salesforce"        # Not "api1"
  table: "opportunities"      # Not "table_a"
```

### 3. One Config Per Data Source

Create separate configuration files for each:
- API endpoint / database table
- Environment (prod, staging, dev)
- Schedule (daily, hourly, realtime)

### 4. Document Custom Settings

Add comments explaining non-obvious settings:

```yaml
run:
  max_file_size_mb: 256     # Optimized for Snowflake queries
  parallel_workers: 4       # Matches available CPU cores
```

### 5. Version Control Strategy

If you want to version control configs (without secrets):

1. Create a separate repo for configs
2. Use template files with placeholder values
3. Use tools like `envsubst` or `j2cli` for templating
4. Keep actual secrets in a secret manager

## Environment-Specific Configs

### Using Different Environments

```bash
# Development
python bronze_extract.py --config config/development/my_api.yaml

# Staging
python bronze_extract.py --config config/staging/my_api.yaml

# Production
python bronze_extract.py --config config/production/my_api.yaml
```

### Environment Variables Per Environment

```bash
# .env.development
export BRONZE_S3_ENDPOINT="http://localhost:9000"  # Local MinIO
export MY_API_TOKEN="dev-token"

# .env.production
export BRONZE_S3_ENDPOINT="https://s3.amazonaws.com"
export MY_API_TOKEN="prod-token"
```

Load with:
```bash
source .env.development
python bronze_extract.py --config config/development/my_api.yaml
```

## Troubleshooting

### Config Not Found

```
FileNotFoundError: Config file not found: config/my_api.yaml
```

**Solution**: Ensure file exists and path is correct:
```bash
ls -la config/
pwd  # Check you're in project root
```

### Validation Errors

```
ValueError: Missing required key 'source.api.base_url' in config
```

**Solution**: Compare your config to example templates:
```bash
diff config/my_api.yaml docs/examples/configs/examples/api_example.yaml
```

### Environment Variable Not Set

```
ValueError: Environment variable 'MY_API_TOKEN' not set for bearer token
```

**Solution**: Set the environment variable:
```bash
export MY_API_TOKEN="your-token-here"
# Verify
echo $MY_API_TOKEN
```

## Need Help?

- **Quick Start**: See [docs/QUICKSTART.md](../docs/QUICKSTART.md)
- **Full Documentation**: See [DOCUMENTATION.md](../DOCUMENTATION.md)
- **Examples**: Browse [docs/examples/](../docs/examples/)
- **Issues**: Open a GitHub issue

## Security Note

⚠️ **This directory is git-ignored by default**

The `.gitignore` file prevents `config/` from being committed to version control. This protects you from accidentally exposing:
- API keys and tokens
- Database passwords
- AWS/Azure credentials
- Internal system URLs
- Business logic in queries

Keep it that way! 🔒
