# YAML vs Python Pipelines: Feature Comparison

Bronze-Foundry supports two configuration approaches: **YAML** (recommended) and **Python**. This guide explains when to use each and documents feature parity.

## Quick Decision Guide

| Question | Answer → Use |
|----------|--------------|
| Do I need custom transformations or logic? | Yes → Python |
| Do I need retry/resilience decorators? | Yes → Python |
| Do I need runtime-computed configuration? | Yes → Python |
| Do I prefer minimal boilerplate? | Yes → YAML |
| Is my team non-Python developers? | Yes → YAML |
| Do I want editor autocomplete? | Yes → YAML (with JSON Schema) |

## Feature Parity Matrix

| Feature | YAML | Python | Notes |
|---------|:----:|:------:|-------|
| **Source Types** | | | |
| CSV files | ✅ | ✅ | |
| Parquet files | ✅ | ✅ | |
| JSON/JSONL files | ✅ | ✅ | |
| Excel files | ✅ | ✅ | |
| Fixed-width files | ✅ | ✅ | |
| MSSQL databases | ✅ | ✅ | |
| PostgreSQL databases | ✅ | ✅ | |
| MySQL databases | ✅ | ✅ | |
| DB2 databases | ✅ | ✅ | |
| REST APIs | ✅ | ✅ | |
| **Load Patterns** | | | |
| Full snapshot | ✅ | ✅ | |
| Incremental append | ✅ | ✅ | |
| CDC | ✅ | ✅ | |
| **Silver History Modes** | | | |
| SCD Type 1 (current only) | ✅ | ✅ | |
| SCD Type 2 (full history) | ✅ | ✅ | |
| **API Features** | | | |
| Bearer auth | ✅ | ✅ | |
| API key auth | ✅ | ✅ | |
| Basic auth | ✅ | ✅ | |
| Offset pagination | ✅ | ✅ | |
| Page pagination | ✅ | ✅ | |
| Cursor pagination | ✅ | ✅ | |
| Rate limiting | ✅ | ✅ | |
| Custom headers | ✅ | ✅ | |
| **Advanced Features** | | | |
| Environment variables | ✅ | ✅ | `${VAR_NAME}` syntax |
| Chunked processing | ✅ | ✅ | `chunk_size` parameter |
| Periodic full refresh | ✅ | ✅ | `full_refresh_days` parameter |
| Custom SQL queries | ✅ | ✅ | `query` field |
| Source validation | ❌ | ✅ | `validate_source` parameter |
| **Python-Only Features** | | | |
| Retry decorators | ❌ | ✅ | `@with_retry` decorator |
| Circuit breakers | ❌ | ✅ | `CircuitBreaker` class |
| Custom transformations | ❌ | ✅ | Any Python code |
| Runtime config | ❌ | ✅ | Computed at execution time |
| Quality rules | ❌ | ✅ | `check_quality()` function |
| Late data handling | ❌ | ✅ | `filter_late_data()` function |

## YAML Pipeline Example

```yaml
# yaml-language-server: $schema=./pipelines/schema/pipeline.schema.json
name: retail_orders
description: Load retail orders with SCD Type 1

bronze:
  system: retail
  entity: orders
  source_type: file_csv
  source_path: ./data/orders_{run_date}.csv

silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  history_mode: current_only
  attributes:
    - customer_id
    - order_total
    - status
```

Run: `python -m pipelines ./my_pipeline.yaml --date 2025-01-15`

## Python Pipeline Example

```python
from pipelines.lib.bronze import BronzeSource, SourceType
from pipelines.lib.silver import SilverEntity, HistoryMode
from pipelines.lib.resilience import with_retry
from pipelines.lib.quality import check_quality, not_null

bronze = BronzeSource(
    system="retail",
    entity="orders",
    source_type=SourceType.FILE_CSV,
    source_path="./data/orders_{run_date}.csv",
)

silver = SilverEntity(
    natural_keys=["order_id"],
    change_timestamp="updated_at",
    history_mode=HistoryMode.CURRENT_ONLY,
)

@with_retry(max_attempts=3)  # Python-only: retry on failures
def run(run_date: str, **kwargs):
    bronze_result = bronze.run(run_date, **kwargs)

    # Python-only: data quality checks
    rules = not_null("order_id", "customer_id")
    quality = check_quality(bronze_result["data"], rules)
    if not quality.passed:
        raise ValueError(f"Quality check failed: {quality.failures}")

    silver_result = silver.run(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}
```

Run: `python -m pipelines retail.orders --date 2025-01-15`

## Workarounds for YAML Limitations

### Retry with YAML Pipelines

Wrap the YAML pipeline execution in a Python script:

```python
# run_with_retry.py
from pipelines.lib.config_loader import load_pipeline
from pipelines.lib.resilience import with_retry

@with_retry(max_attempts=3, backoff_seconds=2.0)
def run_pipeline(config_path: str, run_date: str):
    pipeline = load_pipeline(config_path)
    return pipeline.run(run_date)

if __name__ == "__main__":
    import sys
    result = run_pipeline(sys.argv[1], sys.argv[2])
    print(result)
```

Then run:
```bash
python run_with_retry.py ./my_pipeline.yaml 2025-01-15
```

### Quality Checks with YAML Pipelines

Run quality checks as a separate post-processing step:

```python
# quality_check.py
from pipelines.lib.io import read_bronze
from pipelines.lib.quality import check_quality, not_null, unique_key

bronze_data = read_bronze("retail", "orders", "2025-01-15")
rules = not_null("order_id") + [unique_key("order_id")]
result = check_quality(bronze_data, rules)

if not result.passed:
    print(f"FAILED: {result.failures}")
    exit(1)
print("Quality checks passed")
```

## Migration Guide

### YAML to Python

If you outgrow YAML, migration is straightforward:

1. Read the YAML structure
2. Create equivalent Python dataclasses
3. Add the advanced features you need

### Python to YAML

For simpler pipelines, you can convert to YAML:

1. Extract BronzeSource/SilverEntity parameters
2. Map enum values to YAML strings (e.g., `SourceType.FILE_CSV` → `file_csv`)
3. Remove Python-specific features (decorators, functions)

## Recommendations

1. **Start with YAML** for new pipelines - it's simpler and sufficient for most use cases
2. **Use Python** only when you need:
   - Retry/resilience patterns
   - Custom data transformations
   - Quality rules within the pipeline
   - Runtime-computed configuration
3. **Use the wizard** to generate your initial pipeline: `python -m pipelines.create`
4. **Enable JSON Schema** for YAML autocomplete by adding the schema reference at the top of your YAML file
