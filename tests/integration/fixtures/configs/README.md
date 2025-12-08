# Integration Test Configuration Fixtures

This directory contains YAML configuration files used for full pipeline integration tests.

## Files

### bronze_claims_test.yaml
Bronze extraction configuration for healthcare claims synthetic data.
- Uses file source type with parquet format
- Outputs to local storage (configurable via test fixtures)
- Pattern: snapshot load

### silver_claims_test.yaml
Silver processing configuration for claims data.
- Entity kind: event
- Natural keys: claim_id
- Order column: updated_at

## Usage

These configs are loaded by test fixtures in `conftest.py` with path substitution:
- `${INPUT_DIR}` - Replaced with temp directory containing synthetic parquet files
- `${OUTPUT_DIR}` - Replaced with temp directory for Bronze output

## Synthetic Data

Tests use deterministic generators from `tests/synthetic_data.py`:
- Seed: 42 (produces identical data across runs)
- Row count: 100 (configurable)
- Columns: claim_id, patient_id, provider_id, claim_type, service_date,
           billed_amount, paid_amount, diagnosis_code, procedure_code,
           status, created_at, updated_at
