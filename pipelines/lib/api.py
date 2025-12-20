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

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import requests_toolbelt
import tenacity
from requests_toolbelt.utils.user_agent import user_agent
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from pipelines.lib._path_utils import path_has_data, resolve_target_path
from pipelines.lib.auth import AuthConfig, build_auth_headers
from pipelines.lib.checksum import write_checksum_manifest
from pipelines.lib.env import expand_env_vars, expand_options
from pipelines.lib.io import OutputMetadata, infer_column_types, maybe_dry_run, maybe_skip_if_exists
from pipelines.lib.pagination import (
    PagePaginationState,
    PaginationConfig,
    PaginationStrategy,
    build_pagination_config_from_dict,
    build_pagination_state,
)
from pipelines.lib.rate_limiter import RateLimiter
from pipelines.lib.state import get_watermark, save_watermark

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_USER_AGENT = user_agent(
    "bronze-foundry",
    "dev",
    extras=[
        ("httpx", getattr(httpx, "__version__", "unknown")),
        ("tenacity", getattr(tenacity, "__version__", "unknown")),
        ("requests-toolbelt", getattr(requests_toolbelt, "__version__", "unknown")),
    ],
)

__all__ = ["ApiSource", "ApiOutputMetadata"]

# Backwards compatibility: ApiOutputMetadata is now OutputMetadata
ApiOutputMetadata = OutputMetadata


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

        skip_result = maybe_skip_if_exists(
            skip_if_exists=skip_if_exists,
            exists_fn=lambda: self._already_exists(target),
            target=target,
            logger=logger,
            context=f"{self.system}.{self.entity}",
        )
        if skip_result:
            return skip_result

        dry_run_result = maybe_dry_run(
            dry_run=dry_run,
            logger=logger,
            message="[DRY RUN] Would extract %s.%s from %s%s to %s",
            message_args=(self.system, self.entity, self.base_url, self.endpoint, target),
            target=target,
            extra={"base_url": self.base_url, "endpoint": self.endpoint},
        )
        if dry_run_result:
            return dry_run_result

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
        return resolve_target_path(
            template=self.target_path,
            target_override=target_override,
            env_var="BRONZE_TARGET_ROOT",
            format_vars={
                "system": self.system,
                "entity": self.entity,
                "run_date": run_date,
            },
        )

    def _already_exists(self, target: str) -> bool:
        """Check if data already exists at target."""
        return path_has_data(target)

    def _fetch_all(
        self,
        run_date: str,
        last_watermark: Optional[str],
    ) -> tuple[List[Dict[str, Any]], int, int]:
        """Fetch all records from the API with pagination.

        Returns:
            Tuple of (records, pages_fetched, total_requests)
        """
        headers, auth_tuple = build_auth_headers(self.auth, extra_headers=self.headers)
        headers.setdefault("User-Agent", _USER_AGENT)

        endpoint = self._format_endpoint()

        base_params = dict(expand_options(self.params))
        if last_watermark and self.watermark_param:
            base_params[self.watermark_param] = last_watermark

        limiter = None
        if self.requests_per_second:
            limiter = RateLimiter(
                self.requests_per_second,
                burst_size=self.burst_size,
            )

        pagination_config = self.pagination or PaginationConfig(
            strategy=PaginationStrategy.NONE
        )
        state = build_pagination_state(pagination_config, base_params)

        all_records: List[Dict[str, Any]] = []
        pages_fetched = 0
        total_requests = 0

        with self._create_httpx_client() as client:
            while state.should_fetch_more():
                if limiter:
                    limiter.acquire()

                params = state.build_params()
                response, attempts = self._fetch_page_with_retry(
                    client=client,
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    auth=auth_tuple,
                )
                total_requests += attempts
                pages_fetched += 1

                data = response.json()
                records = self._extract_records(data)

                if not records:
                    break

                all_records.extend(records)

                logger.info(
                    "Fetched %d records %s (total: %d)",
                    len(records),
                    state.describe(),
                    len(all_records),
                )

                if state.max_records > 0 and len(all_records) >= state.max_records:
                    all_records = all_records[: state.max_records]
                    logger.info("Reached max_records limit of %d", state.max_records)
                    break

                if not state.on_response(records, data):
                    break

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
        """Compute the maximum watermark value from records.

        Handles different value types appropriately:
        - datetime objects: compared directly
        - ISO 8601 strings: parsed to datetime for comparison
        - numeric values: compared numerically
        - other strings: compared lexicographically (fallback)
        """
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
            # Try to determine the type and compare appropriately
            sample = watermark_values[0]

            # If already datetime, compare directly
            if isinstance(sample, datetime):
                max_val = max(watermark_values)
                return max_val.isoformat() if hasattr(max_val, 'isoformat') else str(max_val)

            # If numeric, compare numerically
            if isinstance(sample, (int, float)):
                return str(max(watermark_values))

            # If string, try to parse as ISO 8601 datetime
            if isinstance(sample, str):
                # Try parsing as datetime for proper temporal comparison
                try:
                    parsed_values = []
                    for v in watermark_values:
                        # Handle various ISO formats
                        v_str = str(v)
                        # Try fromisoformat (handles most ISO 8601 formats)
                        try:
                            parsed = datetime.fromisoformat(v_str.replace('Z', '+00:00'))
                        except ValueError:
                            # Try parsing date-only format
                            parsed = datetime.strptime(v_str[:10], '%Y-%m-%d')
                        parsed_values.append((parsed, v_str))

                    # Find max by parsed datetime, return original string
                    max_parsed, max_original = max(parsed_values, key=lambda x: x[0])
                    return max_original
                except (ValueError, TypeError):
                    # Fall back to string comparison if parsing fails
                    pass

            # Fallback: convert to strings and compare
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
        # Use shared column inference function
        columns = infer_column_types(records)
        now = datetime.now(timezone.utc).isoformat()

        # Write to target
        output_dir = Path(target)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self.entity}.parquet"
        t.to_parquet(str(output_file))

        data_files = [str(output_file)]

        # API-specific metadata fields
        api_extra = {
            "system": self.system,
            "entity": self.entity,
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "watermark_column": self.watermark_column,
            "last_watermark": last_watermark,
            "pages_fetched": pages_fetched,
            "total_requests": total_requests,
        }

        # Write metadata
        if self.write_metadata:
            metadata = OutputMetadata(
                row_count=row_count,
                columns=columns,
                written_at=now,
                run_date=run_date,
                data_files=[Path(f).name for f in data_files],
                extra=api_extra,
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

    def _format_endpoint(self) -> str:
        endpoint = self.endpoint
        for key, value in self.path_params.items():
            endpoint = endpoint.replace(f"{{{key}}}", expand_env_vars(value))
        return endpoint

    def _create_httpx_client(self) -> httpx.Client:
        base_url = self.base_url.rstrip("/")
        limits = httpx.Limits(
            max_connections=self.pool_maxsize,
            max_keepalive_connections=self.pool_connections,
        )
        return httpx.Client(
            base_url=base_url,
            timeout=self.timeout,
            limits=limits,
        )

    def _fetch_page_with_retry(
        self,
        *,
        client: httpx.Client,
        endpoint: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        auth: Optional[tuple[str, str]],
    ) -> tuple[httpx.Response, int]:
        attempts = 0

        @retry(
            stop=stop_after_attempt(max(self.max_retries, 1)),
            wait=wait_exponential(
                multiplier=self.backoff_factor,
                min=0.5,
                max=30,
            ),
            retry=retry_if_exception(self._should_retry),
            reraise=True,
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def do_request() -> httpx.Response:
            nonlocal attempts
            attempts += 1
            logger.debug("Fetching %s with params %s", endpoint, params)
            response = client.get(
                endpoint,
                headers=headers,
                params=params,
                auth=auth,
                timeout=self.timeout,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response and exc.response.status_code == 429:
                    self._respect_retry_after(exc.response)
                raise
            return response

        return do_request(), attempts

    def _should_retry(self, exc: BaseException) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code if exc.response else None
            return status_code in RETRYABLE_STATUS_CODES
        return isinstance(exc, httpx.RequestError)

    def _respect_retry_after(self, response: httpx.Response) -> None:
        retry_after = response.headers.get("Retry-After")
        if not retry_after:
            return
        try:
            wait_seconds = float(retry_after)
        except (TypeError, ValueError):
            return
        if wait_seconds > 0:
            logger.warning(
                "Rate limited by API; sleeping %.1f seconds before retrying",
                wait_seconds,
            )
            time.sleep(wait_seconds)


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
