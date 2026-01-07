# Testing Strategy Guide

This guide explains how to write and run tests for Bronze Foundry, including the test pyramid, synthetic data generation, MinIO-backed integration testing, and fixture patterns.

---

## Test Pyramid

Bronze Foundry follows a layered testing strategy:

```
                 ┌─────────────┐
                 │    E2E      │  ← Full pipeline tests (MinIO, Bronze→Silver)
                 │   Tests     │     Slowest, but highest confidence
                 └─────────────┘
              ┌─────────────────────┐
              │   Integration Tests │  ← Component interactions (storage, extractors)
              │                     │     Medium speed, realistic scenarios
              └─────────────────────┘
         ┌───────────────────────────────┐
         │         Unit Tests            │  ← Individual functions and classes
         │                               │     Fast, deterministic, isolated
         └───────────────────────────────┘
```

| Test Type | Location | Speed | Dependencies | Coverage Goal |
|-----------|----------|-------|--------------|---------------|
| Unit | `tests/test_*.py` | Fast (<1s) | None | Logic, validation, edge cases |
| Integration | `tests/integration/` | Medium (1-30s) | MinIO, temp files | Storage, extractors, state |
| E2E | `tests/integration/test_*_e2e.py` | Slow (30s+) | Full stack | Complete workflows |

---

## Running Tests

### Basic Commands

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_bronze_patterns.py -v

# Run specific test
python -m pytest tests/test_bronze_patterns.py::TestBronzePatterns::test_snapshot_pattern -v
```

### Test Markers

Use markers to run specific test categories:

```bash
# Unit tests only (fast)
python -m pytest -m unit

# Integration tests (requires MinIO)
python -m pytest -m integration

# Pattern verification tests
python -m pytest -m pattern_verification

# Slow tests (performance, large data)
python -m pytest -m slow
```

### Scaling Test Data

Control synthetic data size with `TEST_SCALE_FACTOR`:

```bash
# Double the default row counts
TEST_SCALE_FACTOR=2.0 pytest tests/test_time_series.py -v

# Half the default (faster tests)
TEST_SCALE_FACTOR=0.5 pytest tests/ -v
```

---

## MinIO Setup for Integration Testing

Integration and E2E tests use MinIO as an S3-compatible storage backend. MinIO provides a local, ephemeral environment for testing Bronze/Silver pipelines without AWS credentials.

### Starting MinIO

**Option 1: Docker (Recommended)**

```bash
docker run -d \
  --name minio-test \
  -p 9000:9000 \
  -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin123" \
  quay.io/minio/minio server /data --console-address ":9001"
```

**Option 2: Binary Installation**

```bash
# Download MinIO for your platform
# See: https://min.io/download

# Start MinIO server
minio server /tmp/minio-data --console-address ":9001"
```

### Creating the Test Bucket

```bash
# Install MinIO client
pip install minio

# Or use the mc command-line tool
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/mdf
```

### MinIO Console Access

- **API Endpoint:** http://localhost:9000
- **Console UI:** http://localhost:9001
- **Username:** `minioadmin`
- **Password:** `minioadmin123`
- **Test Bucket:** `mdf` (medallion-data-foundry)

### Skipping MinIO Tests

If MinIO is unavailable, integration tests are automatically skipped:

```
SKIPPED [1] tests/integration/conftest.py:62: MinIO not available at http://localhost:9000
```

---

## Synthetic Data Generation

Bronze Foundry includes domain-specific synthetic data generators for reproducible testing.

### Available Generators

| Generator | Domain | Key Columns | Use Case |
|-----------|--------|-------------|----------|
| `ClaimsGenerator` | Healthcare | claim_id, patient_id, provider_id | Insurance claims processing |
| `OrdersGenerator` | E-commerce | order_id, customer_id, product_id | Retail order tracking |
| `TransactionsGenerator` | Finance | transaction_id, account_id | Banking transactions |
| `StateChangeGenerator` | Generic | entity_id, version | SCD Type 2 patterns |

### Time Series Scenarios

Each generator supports T0/T1/T2 scenarios:

| Scenario | Method | Description |
|----------|--------|-------------|
| T0 | `generate_t0(date)` | Initial full load |
| T1 | `generate_t1(date, t0_df)` | Incremental changes (inserts + updates) |
| T2 | `generate_t2_late(date, t0_df)` | Late-arriving data (backfills) |

### Usage Example

```python
from datetime import date
from tests.synthetic_data import ClaimsGenerator, generate_time_series_data

# Single generator
gen = ClaimsGenerator(seed=42, row_count=100)
t0_df = gen.generate_t0(date(2024, 1, 15))
t1_df = gen.generate_t1(date(2024, 1, 16), t0_df)

# Complete time series (all scenarios)
time_series = generate_time_series_data(
    domain="claims",
    t0_date=date(2024, 1, 15),
    seed=42,
    row_count=100,
)
# Returns: {"t0": DataFrame, "t1": DataFrame, "t2_late": DataFrame}
```

### Deterministic Seeding

All generators use deterministic seeding for reproducibility:

```python
# Same seed = identical data
gen1 = ClaimsGenerator(seed=42, row_count=100)
gen2 = ClaimsGenerator(seed=42, row_count=100)
assert gen1.generate_t0(date(2024, 1, 15)).equals(gen2.generate_t0(date(2024, 1, 15)))
```

---

## Test Fixture Patterns

### Common Fixtures

**Temporary Directories:**

```python
@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def bronze_output_dir(temp_dir: Path) -> Path:
    """Create Bronze output directory."""
    bronze_dir = temp_dir / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir
```

**Standard Test Dates:**

```python
@pytest.fixture
def t0_date() -> date:
    return date(2024, 1, 15)

@pytest.fixture
def t1_date() -> date:
    return date(2024, 1, 16)
```

**Synthetic Data Fixtures:**

```python
@pytest.fixture
def claims_generator() -> ClaimsGenerator:
    return ClaimsGenerator(seed=42, row_count=100)

@pytest.fixture
def claims_t0_df(claims_generator, t0_date) -> pd.DataFrame:
    return claims_generator.generate_t0(t0_date)
```

### MinIO Integration Fixtures

Located in `tests/integration/conftest.py`:

```python
# Session-scoped MinIO client (reused across tests)
@pytest.fixture(scope="session")
def minio_client():
    if not is_minio_available():
        pytest.skip("MinIO not available")
    return boto3.client("s3", endpoint_url=MINIO_ENDPOINT, ...)

# Test isolation with unique prefixes
@pytest.fixture
def cleanup_prefix(minio_client, minio_bucket, test_prefix):
    yield test_prefix
    # Automatic cleanup after test
    _delete_all_objects_with_prefix(...)

# Platform config for S3 backend
@pytest.fixture
def minio_platform_config(minio_bucket, cleanup_prefix) -> Dict[str, Any]:
    return {
        "bronze": {"storage_backend": "s3", "s3_bucket": minio_bucket, ...},
        "silver": {"storage_backend": "s3", ...},
    }
```

### Helper Functions

```python
# Upload DataFrame to MinIO
from tests.integration.conftest import upload_dataframe_to_minio

path = upload_dataframe_to_minio(
    client=minio_client,
    bucket="mdf",
    key="test-123/claims.parquet",
    df=claims_df,
    format="parquet",  # or "csv"
)

# Download parquet from MinIO
from tests.integration.conftest import download_parquet_from_minio

df = download_parquet_from_minio(client, "mdf", "test-123/claims.parquet")
```

---

## Writing Tests

### Unit Test Example

```python
import pytest
from pipelines.lib.bronze import LoadPattern

class TestLoadPatterns:
    """Unit tests for load pattern logic."""

    def test_load_pattern_values(self):
        """LoadPattern enum should have expected values."""
        assert LoadPattern.FULL_SNAPSHOT.value == "full_snapshot"
        assert LoadPattern.INCREMENTAL_APPEND.value == "incremental"
```

### Integration Test Example

```python
import pytest
from tests.integration.conftest import requires_minio

@requires_minio
class TestBronzeMinIOExtraction:
    """Integration tests for Bronze extraction with MinIO backend."""

    def test_extracts_parquet_to_minio(
        self,
        minio_client,
        minio_bucket,
        cleanup_prefix,
        claims_t0_df,
    ):
        """Bronze extraction should write parquet files to MinIO."""
        # Arrange: Upload source data
        input_key = f"{cleanup_prefix}/input/claims.parquet"
        upload_dataframe_to_minio(minio_client, minio_bucket, input_key, claims_t0_df)

        # Act: Run Bronze extraction
        result = run_bronze_extraction(config, date(2024, 1, 15))

        # Assert: Verify output in MinIO
        output_objects = list_objects_in_prefix(
            minio_client, minio_bucket, f"{cleanup_prefix}/bronze"
        )
        assert len(output_objects) > 0
        assert any(obj.endswith(".parquet") for obj in output_objects)
```

### E2E Test Example

```python
@requires_minio
class TestBronzeToSilverE2E:
    """End-to-end tests for complete Bronze→Silver pipeline."""

    def test_full_pipeline_with_claims(
        self,
        minio_client,
        minio_bucket,
        cleanup_prefix,
        claims_time_series,
    ):
        """Full pipeline should transform claims through Bronze to Silver."""
        # T0: Initial load
        t0_df = claims_time_series["t0"]
        bronze_result = run_bronze_extraction(t0_df, date(2024, 1, 15))
        assert bronze_result["exit_code"] == 0

        # Run Silver transformation
        silver_result = run_silver_transformation(bronze_result["bronze_path"])
        assert silver_result["exit_code"] == 0

        # Verify deduplication worked
        silver_df = load_silver_output(silver_result["silver_path"])
        assert len(silver_df) == len(t0_df["claim_id"].unique())
```

---

## Test Cleanup Strategies

### Automatic Cleanup with Fixtures

Use the `cleanup_prefix` fixture to automatically delete test data:

```python
@pytest.fixture
def cleanup_prefix(minio_client, minio_bucket, test_prefix):
    yield test_prefix
    # Cleanup runs after test completes
    for obj in list_objects(minio_client, minio_bucket, test_prefix):
        minio_client.delete_object(Bucket=minio_bucket, Key=obj)
```

### Manual Cleanup

For debugging, skip cleanup by accessing MinIO directly:

```bash
# List objects in test bucket
mc ls local/mdf/test-abc123/

# Remove test prefix manually
mc rm --recursive local/mdf/test-abc123/
```

### Checkpoint Cleanup

The test session automatically cleans checkpoint state:

```python
@pytest.fixture(scope="session", autouse=True)
def clean_checkpoint_state():
    """Ensure checkpoint store starts empty for each test session."""
    checkpoint_dir = Path(".state/checkpoints")
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
```

---

## Testing Custom Connectors

When adding a new source connector, follow this test template:

### 1. Unit Tests (Required)

```python
# tests/test_my_connector.py
class TestMyConnector:
    def test_connection_config_validation(self):
        """Connector should validate required config fields."""
        ...

    def test_extracts_data_correctly(self, mock_api_response):
        """Connector should transform API response to DataFrame."""
        ...

    def test_handles_pagination(self, mock_paginated_response):
        """Connector should handle multi-page responses."""
        ...

    def test_handles_errors_gracefully(self, mock_error_response):
        """Connector should raise appropriate exceptions."""
        ...
```

### 2. Integration Tests (Recommended)

```python
# tests/integration/test_my_connector_integration.py
@requires_minio
class TestMyConnectorIntegration:
    def test_extracts_to_minio(self, minio_platform_config):
        """Connector should write data to MinIO backend."""
        ...
```

### 3. E2E Tests (For Critical Connectors)

```python
@requires_minio
class TestMyConnectorE2E:
    def test_full_bronze_pipeline(self):
        """Full Bronze extraction with real-ish data."""
        ...
```

---

## Pattern Verification Tests

The pattern verification framework validates load pattern behavior:

```python
from tests.integration.pattern_data import PatternTestDataGenerator

# Generate multi-batch test data
gen = PatternTestDataGenerator(seed=42, base_rows=1000)

# Test SNAPSHOT: Full replacement
snapshot_data = gen.generate_snapshot_scenario(include_replacement=True)

# Test INCREMENTAL_APPEND: Append-only
append_data = gen.generate_incremental_append_scenario(batches=4)

# Test INCREMENTAL_MERGE: Upsert by key
merge_data = gen.generate_incremental_merge_scenario(
    batches=4,
    update_rate=0.2,  # 20% of rows updated per batch
    insert_rate=0.1,  # 10% new rows per batch
)

# Test SCD Type 2: History tracking
scd_data = gen.generate_scd2_scenario(entities=500, changes_per_entity=3)
```

---

## Troubleshooting

### MinIO Connection Errors

```
botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL
```

**Solution:** Ensure MinIO is running and accessible at `http://localhost:9000`.

### Flaky Integration Tests

If tests fail intermittently:
1. Check for shared state between tests (use unique prefixes)
2. Ensure fixtures properly clean up resources
3. Use deterministic seeds for synthetic data

### Slow Test Runs

```bash
# Run only fast unit tests during development
python -m pytest -m unit

# Run full suite in CI
python -m pytest tests/ --timeout=300
```

---

## Summary

| Task | Approach |
|------|----------|
| Test logic/validation | Unit tests with mocks |
| Test storage/extractors | Integration tests with MinIO |
| Test full workflows | E2E tests with synthetic data |
| Test custom connectors | Follow the 3-tier template |
| Ensure reproducibility | Use deterministic seeds |
| Isolate tests | Use unique prefixes + cleanup fixtures |
