"""Example: GitHub API extraction pipeline.

This example shows how to use ApiSource to extract data from GitHub's REST API.
It demonstrates:
- Bearer token authentication via environment variable
- Cursor-based pagination (GitHub's Link header pagination)
- Rate limiting to respect API limits
- Watermark-based incremental loads

Usage:
    # Set your GitHub token
    export GITHUB_TOKEN="ghp_xxxxxxxxxxxx"

    # Run the pipeline
    python -m pipelines examples.github_api --date 2025-01-15

    # Dry run to see configuration
    python -m pipelines examples.github_api --date 2025-01-15 --dry-run
"""

from pipelines.lib.api import ApiSource
from pipelines.lib.auth import AuthConfig, AuthType
from pipelines.lib.pagination import PaginationConfig, PaginationStrategy
from pipelines.lib.runner import pipeline

# Configure API source for GitHub repository events
github_events = ApiSource(
    system="github",
    entity="repo_events",
    base_url="https://api.github.com",
    endpoint="/repos/anthropics/claude-code/events",
    target_path="./bronze/system=github/entity=repo_events/dt={run_date}/",

    # Authentication - use GitHub personal access token
    auth=AuthConfig(
        auth_type=AuthType.BEARER,
        token="${GITHUB_TOKEN}",  # Read from environment variable
    ),

    # GitHub uses page-based pagination with Link headers
    # For simplicity, we use page pagination here
    pagination=PaginationConfig(
        strategy=PaginationStrategy.PAGE,
        page_size=30,  # GitHub default
        page_param="page",
        page_size_param="per_page",
        max_pages=3,  # Limit for demo
    ),

    # Rate limiting - GitHub allows 5000 requests/hour for authenticated users
    # That's about 1.4 requests/second, we'll be conservative
    requests_per_second=1.0,

    # Extra headers required by GitHub
    headers={
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    },

    # Request timeout
    timeout=30.0,
)


@pipeline(name="github_events")
def run(run_date: str, dry_run: bool = False) -> dict:
    """Extract GitHub repository events to Bronze layer.

    Args:
        run_date: The date for this pipeline run (YYYY-MM-DD)
        dry_run: If True, validate config without making API calls

    Returns:
        Pipeline result with extraction metadata
    """
    return github_events.run(run_date, dry_run=dry_run)


# For direct execution
if __name__ == "__main__":
    import sys
    from datetime import date

    run_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    dry_run = "--dry-run" in sys.argv

    result = run(run_date, dry_run=dry_run)
    print(f"Result: {result}")
