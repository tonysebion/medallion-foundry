"""
Example Pipeline: Resilient API Extraction
===========================================
Demonstrates API extraction with retry, rate limiting, and pagination.

This example shows:
- REST API extraction with authentication
- Cursor-based pagination
- Rate limiting (10 requests/second)
- Retry with exponential backoff
- Watermark-based incremental loads

To run:
    python -m pipelines examples.resilient_api --date 2025-01-15
"""

from pathlib import Path

from pipelines.lib.api import ApiSource
from pipelines.lib.auth import AuthConfig, AuthType
from pipelines.lib.pagination import PaginationConfig, PaginationStrategy
from pipelines.lib.resilience import with_retry, CircuitBreaker
from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity

# Output paths
OUTPUT_DIR = Path("./output")

# ============================================
# BRONZE: Extract from paginated API
# ============================================

# API authentication configuration
auth = AuthConfig(
    auth_type=AuthType.BEARER,
    token="${API_TOKEN}",  # Uses environment variable
)

# Cursor-based pagination (common in modern APIs)
pagination = PaginationConfig(
    strategy=PaginationStrategy.CURSOR,
    cursor_param="cursor",
    cursor_path="meta.next_cursor",  # Path in JSON response to next cursor
    page_size=100,
)

bronze = ApiSource(
    system="crm",
    entity="contacts",
    base_url="https://api.example.com",
    endpoint="/v2/contacts",
    target_path=str(OUTPUT_DIR / "bronze" / "crm" / "contacts"),
    auth=auth,
    pagination=pagination,
    # Rate limiting: max 10 requests per second
    requests_per_second=10.0,
    # Navigate into JSON response to get the data array
    data_path="data.contacts",
    # Incremental loading via watermark
    watermark_column="updated_at",
    watermark_param="modified_since",
)

# ============================================
# SILVER: Curate contacts (SCD Type 1)
# ============================================

silver = SilverEntity(
    source_path=str(OUTPUT_DIR / "bronze" / "crm" / "contacts" / "*.parquet"),
    target_path=str(OUTPUT_DIR / "silver" / "crm" / "contacts"),
    natural_keys=["contact_id"],
    change_timestamp="updated_at",
    entity_kind=EntityKind.STATE,
    history_mode=HistoryMode.CURRENT_ONLY,
    attributes=["email", "first_name", "last_name", "company", "status"],
)

# Circuit breaker for API protection
api_breaker = CircuitBreaker(failure_threshold=5, recovery_time=60.0)


@with_retry(max_attempts=5, backoff_seconds=5.0)
def run_bronze(run_date: str, **kwargs):
    """Extract contacts from API with retry and circuit breaker.

    Uses circuit breaker to fail fast when API is unavailable,
    and retry with exponential backoff for transient failures.
    """
    with api_breaker:
        return bronze.run(run_date, **kwargs)


def run_silver(run_date: str, **kwargs):
    """Curate Bronze contacts to Silver."""
    return silver.run(run_date, **kwargs)


def run(run_date: str, **kwargs):
    """Run full pipeline: Bronze → Silver with resilience patterns."""
    bronze_result = run_bronze(run_date, **kwargs)
    silver_result = run_silver(run_date, **kwargs)
    return {"bronze": bronze_result, "silver": silver_result}


if __name__ == "__main__":
    import sys

    run_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-15"

    print(f"Running resilient API pipeline for {run_date}...")
    print("Note: Requires API_TOKEN environment variable")

    try:
        result = run(run_date)
        print(f"\nResults:")
        print(f"  Bronze: {result['bronze'].get('row_count', 0)} rows")
        print(f"  Silver: {result['silver'].get('row_count', 0)} rows")
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure API_TOKEN is set and API endpoint is accessible.")
        sys.exit(1)
