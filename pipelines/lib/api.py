"""API extraction for Bronze layer.

Provides full-featured REST API extraction with:
- Multiple authentication methods (bearer, API key, basic)
- Multiple pagination strategies (offset, page, cursor)
- Rate limiting
- Retry with exponential backoff
- Watermark-based incremental extraction

Example:
    from pipelines.lib.api import ApiSource
    from pipelines.lib.auth import AuthConfig, AuthType
    from pipelines.lib.pagination import PaginationConfig, PaginationStrategy

    source = ApiSource(
        system="github",
        entity="repos",
        base_url="https://api.github.com",
        endpoint="/users/{user}/repos",
        target_path="./bronze/github/repos/dt={run_date}/",
        auth=AuthConfig(
            auth_type=AuthType.BEARER,
            token="${GITHUB_TOKEN}",
        ),
        pagination=PaginationConfig(
            strategy=PaginationStrategy.PAGE,
            page_size=100,
            page_param="page",
            page_size_param="per_page",
        ),
        path_params={"user": "anthropics"},
    )

    result = source.run("2025-01-15")
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.lib.auth import AuthConfig, build_auth_headers
from pipelines.lib.checksum import write_checksum_manifest
from pipelines.lib.env import expand_env_vars, expand_options
from pipelines.lib.pagination import (
    PagePaginationState,
    PaginationConfig,
    PaginationStrategy,
    build_pagination_config_from_dict,
    build_pagination_state,
)
from pipelines.lib.rate_limiter import RateLimiter
from pipelines.lib.watermark import get_watermark, save_watermark

logger = logging.getLogger(__name__)

__all__ = ["ApiSource", "ApiOutputMetadata"]


@dataclass
class ApiOutputMetadata:
    """Metadata for API extraction outputs.

    Written alongside data files to track lineage.
    """

    # Basic metadata
    row_count: int
    columns: List[Dict[str, Any]]
    written_at: str

    # Source information
    system: str
    entity: str
    base_url: str
    endpoint: str

    # Temporal
    run_date: str
    watermark_column: Optional[str] = None
    last_watermark: Optional[str] = None
    new_watermark: Optional[str] = None

    # Pagination info
    pages_fetched: int = 0
    total_requests: int = 0

    # Files written
    data_files: List[str] = field(default_factory=list)

    # Format info
    format: str = "parquet"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


def _create_session(
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    pool_connections: int = 10,
    pool_maxsize: int = 10,
) -> requests.Session:
    """Create a requests Session with connection pooling and retry logic.

    Args:
        max_retries: Maximum retry attempts
        backoff_factor: Backoff multiplier between retries
        pool_connections: Number of urllib3 connection pools
        pool_maxsize: Maximum connections per pool

    Returns:
        Configured requests.Session
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,  # We'll handle status codes ourselves
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    logger.debug(
        "Created session with pool_connections=%d, pool_maxsize=%d",
        pool_connections,
        pool_maxsize,
    )

    return session


@dataclass
class ApiSource:
    """Declarative API extraction source.

    Provides full-featured REST API extraction with authentication,
    pagination, rate limiting, and watermark-based incremental loading.

    Example:
        source = ApiSource(
            system="sales_api",
            entity="orders",
            base_url="https://api.example.com",
            endpoint="/v1/orders",
            target_path="./bronze/sales/orders/dt={run_date}/",
            auth=AuthConfig(auth_type=AuthType.BEARER, token="${API_TOKEN}"),
            pagination=PaginationConfig(
                strategy=PaginationStrategy.OFFSET,
                page_size=100,
            ),
            data_path="data.orders",
            watermark_column="updated_at",
            watermark_param="since",
        )
        result = source.run("2025-01-15")
    """

    # Identity
    system: str  # Source system name
    entity: str  # API endpoint/resource name

    # API endpoint
    base_url: str  # Base URL (e.g., "https://api.example.com")
    endpoint: str  # Endpoint path (e.g., "/v1/orders")

    # Target
    target_path: str  # Where to land in Bronze

    # Authentication
    auth: Optional[AuthConfig] = None

    # Pagination
    pagination: Optional[PaginationConfig] = None

    # Data extraction
    data_path: Optional[str] = None  # Path to records in response (e.g., "data.items")

    # Rate limiting
    requests_per_second: Optional[float] = None
    burst_size: Optional[int] = None

    # Watermarking for incremental loads
    watermark_column: Optional[str] = None  # Column in response with timestamp
    watermark_param: Optional[str] = None  # Query param to pass last watermark

    # Request options
    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, str] = field(default_factory=dict)
    path_params: Dict[str, str] = field(default_factory=dict)  # URL path substitution
    timeout: float = 30.0

    # Session configuration
    max_retries: int = 3
    backoff_factor: float = 0.5
    pool_connections: int = 10
    pool_maxsize: int = 10

    # Artifact generation
    write_checksums: bool = True
    write_metadata: bool = True

    def __post_init__(self) -> None:
        """Validate configuration on instantiation."""
        errors = self._validate()
        if errors:
            error_msg = "\n".join(f"  - {e}" for e in errors)
            raise ValueError(
                f"ApiSource configuration errors for {self.system}.{self.entity}:\n"
                f"{error_msg}\n\n"
                "Fix the configuration and try again."
            )

    def _validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors: List[str] = []

        if not self.system:
            errors.append("system is required (source system name)")

        if not self.entity:
            errors.append("entity is required (API resource name)")

        if not self.base_url:
            errors.append("base_url is required (e.g., 'https://api.example.com')")

        if not self.endpoint:
            errors.append("endpoint is required (e.g., '/v1/orders')")

        if not self.target_path:
            errors.append("target_path is required")

        # Watermark validation
        if self.watermark_column and not self.watermark_param:
            logger.warning(
                "watermark_column is set but watermark_param is not. "
                "The watermark value won't be passed to the API."
            )

        return errors

    def run(
        self,
        run_date: str,
        *,
        target_override: Optional[str] = None,
        skip_if_exists: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute the API extraction.

        Args:
            run_date: The date for this extraction run (YYYY-MM-DD)
            target_override: Override the target path
            skip_if_exists: Skip if data already exists
            dry_run: Validate without extracting

        Returns:
            Dictionary with extraction results
        """
        target = self._resolve_target(run_date, target_override)

        if skip_if_exists and self._already_exists(target):
            logger.info(
                "Skipping %s.%s - data already exists at %s",
                self.system,
                self.entity,
                target,
            )
            return {"skipped": True, "reason": "already_exists", "target": target}

        if dry_run:
            logger.info(
                "[DRY RUN] Would extract %s.%s from %s%s to %s",
                self.system,
                self.entity,
                self.base_url,
                self.endpoint,
                target,
            )
            return {
                "dry_run": True,
                "target": target,
                "base_url": self.base_url,
                "endpoint": self.endpoint,
            }

        # Get watermark for incremental loads
        last_watermark = None
        if self.watermark_column:
            last_watermark = get_watermark(self.system, self.entity)
            if last_watermark:
                logger.info(
                    "Incremental load for %s.%s from watermark: %s",
                    self.system,
                    self.entity,
                    last_watermark,
                )

        # Fetch records from API
        records, pages_fetched, total_requests = self._fetch_all(
            run_date, last_watermark
        )

        if not records:
            logger.warning("No records fetched from API for %s.%s", self.system, self.entity)
            return {
                "row_count": 0,
                "target": target,
                "pages_fetched": pages_fetched,
                "total_requests": total_requests,
            }

        # Add Bronze metadata
        now = datetime.now(timezone.utc).isoformat()
        for record in records:
            record["_load_date"] = run_date
            record["_extracted_at"] = now
            record["_source_system"] = self.system
            record["_source_entity"] = self.entity

        # Write to target
        result = self._write(records, target, run_date, last_watermark, pages_fetched, total_requests)

        # Save new watermark if applicable
        if self.watermark_column and records:
            new_watermark = self._compute_max_watermark(records)
            if new_watermark:
                save_watermark(self.system, self.entity, str(new_watermark))
                result["new_watermark"] = str(new_watermark)

        return result

    def _resolve_target(self, run_date: str, target_override: Optional[str]) -> str:
        """Resolve target path with template substitution."""
        import os

        base = target_override or os.environ.get("BRONZE_TARGET_ROOT") or self.target_path
        return base.format(
            system=self.system,
            entity=self.entity,
            run_date=run_date,
        )

    def _already_exists(self, target: str) -> bool:
        """Check if data already exists at target."""
        path = Path(target)
        return path.exists() and any(path.iterdir())

    def _fetch_all(
        self,
        run_date: str,
        last_watermark: Optional[str],
    ) -> tuple[List[Dict[str, Any]], int, int]:
        """Fetch all records from the API with pagination.

        Returns:
            Tuple of (records, pages_fetched, total_requests)
        """
        session = _create_session(
            max_retries=self.max_retries,
            backoff_factor=self.backoff_factor,
            pool_connections=self.pool_connections,
            pool_maxsize=self.pool_maxsize,
        )

        try:
            # Build auth headers
            headers, auth_tuple = build_auth_headers(self.auth, extra_headers=self.headers)

            # Build URL
            endpoint = self.endpoint
            for key, value in self.path_params.items():
                endpoint = endpoint.replace(f"{{{key}}}", expand_env_vars(value))

            url = self.base_url.rstrip("/") + endpoint

            # Build base params
            base_params = dict(expand_options(self.params))
            if last_watermark and self.watermark_param:
                base_params[self.watermark_param] = last_watermark

            # Set up rate limiter
            limiter = None
            if self.requests_per_second:
                limiter = RateLimiter(
                    self.requests_per_second,
                    burst_size=self.burst_size,
                )

            # Set up pagination
            pagination_config = self.pagination or PaginationConfig(
                strategy=PaginationStrategy.NONE
            )
            state = build_pagination_state(pagination_config, base_params)

            # Fetch loop
            all_records: List[Dict[str, Any]] = []
            pages_fetched = 0
            total_requests = 0

            while state.should_fetch_more():
                if limiter:
                    limiter.acquire()

                params = state.build_params()
                total_requests += 1

                logger.debug("Fetching %s with params %s", url, params)

                response = session.get(
                    url,
                    headers=headers,
                    params=params,
                    auth=auth_tuple,
                    timeout=self.timeout,
                )

                # Handle rate limiting with Retry-After header
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        import time

                        wait_seconds = float(retry_after)
                        logger.warning(
                            "Rate limited. Waiting %s seconds...",
                            wait_seconds,
                        )
                        time.sleep(wait_seconds)
                        continue

                response.raise_for_status()

                data = response.json()
                records = self._extract_records(data)
                pages_fetched += 1

                if not records:
                    break

                all_records.extend(records)

                logger.info(
                    "Fetched %d records %s (total: %d)",
                    len(records),
                    state.describe(),
                    len(all_records),
                )

                # Check max_records limit
                if state.max_records > 0 and len(all_records) >= state.max_records:
                    all_records = all_records[: state.max_records]
                    logger.info("Reached max_records limit of %d", state.max_records)
                    break

                # Update pagination state
                if not state.on_response(records, data):
                    break

            # Log if max_pages was reached
            if (
                isinstance(state, PagePaginationState)
                and state.max_pages_limit_hit
            ):
                logger.info("Reached max_pages limit of %d", pagination_config.max_pages)

            logger.info(
                "Successfully fetched %d records from %s.%s in %d pages (%d requests)",
                len(all_records),
                self.system,
                self.entity,
                pages_fetched,
                total_requests,
            )

            return all_records, pages_fetched, total_requests

        finally:
            session.close()

    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """Extract records from API response.

        Uses data_path to navigate nested response structures.
        """
        # Navigate to data using data_path
        if self.data_path:
            for key in self.data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                else:
                    logger.warning("Cannot navigate path '%s' in response", self.data_path)
                    break

        # Convert to list of records
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Try common patterns
            for key in ("items", "data", "results", "records"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            # Wrap single record
            return [data]
        else:
            logger.warning("Unexpected data type: %s", type(data))
            return []

    def _compute_max_watermark(self, records: List[Dict[str, Any]]) -> Optional[str]:
        """Compute the maximum watermark value from records."""
        if not self.watermark_column:
            return None

        watermark_values = [
            r[self.watermark_column]
            for r in records
            if self.watermark_column in r and r[self.watermark_column] is not None
        ]

        if not watermark_values:
            return None

        try:
            # Convert all to strings for comparison
            str_values = [str(v) for v in watermark_values]
            return max(str_values)
        except (TypeError, ValueError) as e:
            logger.warning(
                "Could not compute max watermark from '%s': %s",
                self.watermark_column,
                e,
            )
            return None

    def _write(
        self,
        records: List[Dict[str, Any]],
        target: str,
        run_date: str,
        last_watermark: Optional[str],
        pages_fetched: int,
        total_requests: int,
    ) -> Dict[str, Any]:
        """Write records to target with metadata and checksums."""
        import ibis

        # Create Ibis table from records
        t = ibis.memtable(records)

        row_count = len(records)
        columns = self._infer_columns(records)
        now = datetime.now(timezone.utc).isoformat()

        # Write to target
        output_dir = Path(target)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self.entity}.parquet"
        t.to_parquet(str(output_file))

        data_files = [str(output_file)]

        # Write metadata
        if self.write_metadata:
            metadata = ApiOutputMetadata(
                row_count=row_count,
                columns=columns,
                written_at=now,
                system=self.system,
                entity=self.entity,
                base_url=self.base_url,
                endpoint=self.endpoint,
                run_date=run_date,
                watermark_column=self.watermark_column,
                last_watermark=last_watermark,
                pages_fetched=pages_fetched,
                total_requests=total_requests,
                data_files=[Path(f).name for f in data_files],
            )
            metadata_file = output_dir / "_metadata.json"
            metadata_file.write_text(metadata.to_json(), encoding="utf-8")
            logger.debug("Wrote metadata to %s", metadata_file)

        # Write checksums
        if self.write_checksums:
            write_checksum_manifest(
                output_dir,
                [Path(f) for f in data_files],
                entity_kind="bronze",
                row_count=row_count,
                extra_metadata={
                    "system": self.system,
                    "entity": self.entity,
                    "source_type": "api_rest",
                },
            )

        logger.info(
            "Wrote %d rows for %s.%s to %s",
            row_count,
            self.system,
            self.entity,
            target,
        )

        result: Dict[str, Any] = {
            "row_count": row_count,
            "target": target,
            "columns": [c["name"] for c in columns],
            "files": [Path(f).name for f in data_files],
            "pages_fetched": pages_fetched,
            "total_requests": total_requests,
        }

        if self.write_metadata:
            result["metadata_file"] = "_metadata.json"
        if self.write_checksums:
            result["checksums_file"] = "_checksums.json"

        return result

    def _infer_columns(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Infer column types from records."""
        if not records:
            return []

        # Collect all keys
        all_keys: set[str] = set()
        for record in records:
            all_keys.update(record.keys())

        # Infer types from first non-null value
        columns = []
        for key in sorted(all_keys):
            sample_value = None
            for record in records:
                if key in record and record[key] is not None:
                    sample_value = record[key]
                    break

            if sample_value is None:
                type_name = "string"
            else:
                type_name = type(sample_value).__name__

            columns.append({
                "name": key,
                "type": type_name,
                "nullable": True,
            })

        return columns


def create_api_source_from_options(
    system: str,
    entity: str,
    options: Dict[str, Any],
    target_path: str,
) -> ApiSource:
    """Create ApiSource from a dictionary of options.

    Convenience function for creating ApiSource from BronzeSource options dict.

    Expected option keys:
        - base_url: API base URL
        - endpoint: API endpoint path
        - auth_type: "none", "bearer", "api_key", "basic"
        - token: Bearer token (for auth_type=bearer)
        - api_key: API key (for auth_type=api_key)
        - api_key_header: Header for API key (default: X-API-Key)
        - username: Username (for auth_type=basic)
        - password: Password (for auth_type=basic)
        - pagination_type: "none", "offset", "page", "cursor"
        - page_size: Records per page
        - data_path: Path to records in response
        - requests_per_second: Rate limit
        - watermark_column: Column for incremental loads
        - watermark_param: Query param for watermark
        - headers: Additional headers dict
        - params: Additional query params dict
        - path_params: URL path substitutions

    Args:
        system: Source system name
        entity: Entity/resource name
        options: Configuration options
        target_path: Where to write data

    Returns:
        Configured ApiSource instance
    """
    from pipelines.lib.auth import AuthConfig, AuthType

    # Build auth config
    auth_type_str = options.get("auth_type", "none").lower()
    try:
        auth_type = AuthType(auth_type_str)
    except ValueError:
        raise ValueError(f"Unsupported auth_type: '{auth_type_str}'")

    auth = None
    if auth_type != AuthType.NONE:
        auth = AuthConfig(
            auth_type=auth_type,
            token=options.get("token"),
            api_key=options.get("api_key"),
            api_key_header=options.get("api_key_header", "X-API-Key"),
            username=options.get("username"),
            password=options.get("password"),
        )

    # Build pagination config
    pagination = None
    if "pagination_type" in options:
        pagination = build_pagination_config_from_dict(options)

    return ApiSource(
        system=system,
        entity=entity,
        base_url=options.get("base_url", ""),
        endpoint=options.get("endpoint", ""),
        target_path=target_path,
        auth=auth,
        pagination=pagination,
        data_path=options.get("data_path"),
        requests_per_second=options.get("requests_per_second"),
        burst_size=options.get("burst_size"),
        watermark_column=options.get("watermark_column"),
        watermark_param=options.get("watermark_param"),
        headers=options.get("headers", {}),
        params=options.get("params", {}),
        path_params=options.get("path_params", {}),
        timeout=options.get("timeout", 30.0),
        max_retries=options.get("max_retries", 3),
        write_checksums=options.get("write_checksums", True),
        write_metadata=options.get("write_metadata", True),
    )
