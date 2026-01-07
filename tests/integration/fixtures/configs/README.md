# Integration Test Configuration Fixtures

This directory contains YAML configuration files used for full pipeline integration tests.
These files use the standard pipeline schema at `pipelines/schema/pipeline.schema.json`.

## Files

### bronze_claims_test.yaml
Bronze extraction configuration for healthcare claims synthetic data.
- Source type: file_parquet
- Load pattern: full_snapshot
- Entity: synthetic/claims

### silver_claims_test.yaml
Silver processing configuration for claims data.
- Entity kind: event
- Natural keys: claim_id
- Change timestamp: updated_at

## Usage

These configs follow the standard pipeline schema and can be run with:

```bash
python -m pipelines ./tests/integration/fixtures/configs/bronze_claims_test.yaml --date 2025-01-15
```

For tests, paths can be overridden using environment variables:
- `${INPUT_DIR}` - Replaced with temp directory containing synthetic parquet files
- `${OUTPUT_DIR}` - Replaced with temp directory for Bronze output

## Synthetic Data

Tests use deterministic generators from `tests/synthetic_data.py`:
- Seed: 42 (produces identical data across runs)
- Row count: 100 (configurable)
- Columns: claim_id, patient_id, provider_id, claim_type, service_date,
          billed_amount, paid_amount, diagnosis_code, procedure_code,
          status, created_at, updated_at
