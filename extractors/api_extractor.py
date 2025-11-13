"""API-based extraction with authentication and pagination support."""

import logging
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import date

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class ApiExtractor(BaseExtractor):
    """Extractor for REST API sources with authentication and pagination."""

    def _build_auth_headers(self, api_cfg: Dict[str, Any]) -> Dict[str, str]:
        """Build authentication headers based on config."""
        headers = {"Accept": "application/json"}
        
        auth_type = api_cfg.get("auth_type", "none")
        
        if auth_type == "bearer":
            token_env = api_cfg.get("auth_token_env")
            if not token_env:
                raise ValueError("auth_type='bearer' requires 'auth_token_env' in config")
            
            token = os.environ.get(token_env)
            if not token:
                raise ValueError(f"Environment variable '{token_env}' not set for bearer token")
            
            headers["Authorization"] = f"Bearer {token}"
            logger.debug("Added bearer token authentication")
        
        elif auth_type == "api_key":
            key_env = api_cfg.get("auth_key_env")
            key_header = api_cfg.get("auth_key_header", "X-API-Key")
            
            if not key_env:
                raise ValueError("auth_type='api_key' requires 'auth_key_env' in config")
            
            api_key = os.environ.get(key_env)
            if not api_key:
                raise ValueError(f"Environment variable '{key_env}' not set for API key")
            
            headers[key_header] = api_key
            logger.debug(f"Added API key authentication in header '{key_header}'")
        
        elif auth_type == "basic":
            username_env = api_cfg.get("auth_username_env")
            password_env = api_cfg.get("auth_password_env")
            
            if not username_env or not password_env:
                raise ValueError("auth_type='basic' requires 'auth_username_env' and 'auth_password_env'")
            
            # Basic auth is handled separately via requests auth parameter
            logger.debug("Using basic authentication")
        
        elif auth_type != "none":
            raise ValueError(f"Unsupported auth_type: '{auth_type}'. Use 'bearer', 'api_key', 'basic', or 'none'")
        
        # Add any custom headers from config
        custom_headers = api_cfg.get("headers", {})
        headers.update(custom_headers)
        
        return headers

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, requests.exceptions.Timeout)),
        reraise=True
    )
    def _make_request(
        self,
        session: requests.Session,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        timeout: int,
        auth: Optional[Tuple[str, str]] = None
    ) -> requests.Response:
        """Make HTTP request with retry logic."""
        logger.debug(f"Making request to {url} with params {params}")
        resp = session.get(url, headers=headers, params=params, timeout=timeout, auth=auth)
        resp.raise_for_status()
        return resp

    def _extract_records(self, data: Any, api_cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract records from API response data."""
        # Support custom data path
        data_path = api_cfg.get("data_path", "")
        
        if data_path:
            # Navigate nested structure (e.g., "data.items")
            for key in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, [])
                else:
                    logger.warning(f"Cannot navigate path '{data_path}' in response")
                    break
        
        # Convert to list of records
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Check common patterns
            for key in ["items", "data", "results", "records"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            # If no common pattern, wrap single record
            return [data]
        else:
            logger.warning(f"Unexpected data type: {type(data)}")
            return []

    def _paginate(
        self,
        session: requests.Session,
        base_url: str,
        endpoint: str,
        headers: Dict[str, str],
        api_cfg: Dict[str, Any],
        run_cfg: Dict[str, Any],
        auth: Optional[Tuple[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """Handle pagination and collect all records."""
        all_records: List[Dict[str, Any]] = []
        url = base_url.rstrip("/") + endpoint
        
        timeout = run_cfg.get("timeout_seconds", 30)
        pagination_cfg = api_cfg.get("pagination", {})
        pagination_type = pagination_cfg.get("type", "none")
        
        params = dict(api_cfg.get("params", {}))
        
        if pagination_type == "none":
            # Single request, no pagination
            resp = self._make_request(session, url, headers, params, timeout, auth)
            data = resp.json()
            all_records = self._extract_records(data, api_cfg)
            logger.info(f"Fetched {len(all_records)} records (no pagination)")
        
        elif pagination_type == "offset":
            # Offset-based pagination
            offset_param = pagination_cfg.get("offset_param", "offset")
            limit_param = pagination_cfg.get("limit_param", "limit")
            page_size = pagination_cfg.get("page_size", 100)
            max_records = pagination_cfg.get("max_records", 0)
            
            offset = 0
            params[limit_param] = page_size
            
            while True:
                params[offset_param] = offset
                resp = self._make_request(session, url, headers, params, timeout, auth)
                data = resp.json()
                
                records = self._extract_records(data, api_cfg)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"Fetched {len(records)} records at offset {offset} (total: {len(all_records)})")
                
                if len(records) < page_size:
                    break
                
                if max_records > 0 and len(all_records) >= max_records:
                    all_records = all_records[:max_records]
                    logger.info(f"Reached max_records limit of {max_records}")
                    break
                
                offset += page_size
        
        elif pagination_type == "page":
            # Page-based pagination
            page_param = pagination_cfg.get("page_param", "page")
            page_size_param = pagination_cfg.get("page_size_param", "page_size")
            page_size = pagination_cfg.get("page_size", 100)
            max_pages = pagination_cfg.get("max_pages", 0)
            
            page = 1
            params[page_size_param] = page_size
            
            while True:
                if max_pages > 0 and page > max_pages:
                    logger.info(f"Reached max_pages limit of {max_pages}")
                    break
                
                params[page_param] = page
                resp = self._make_request(session, url, headers, params, timeout, auth)
                data = resp.json()
                
                records = self._extract_records(data, api_cfg)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"Fetched {len(records)} records from page {page} (total: {len(all_records)})")
                
                if len(records) < page_size:
                    break
                
                page += 1
        
        elif pagination_type == "cursor":
            # Cursor-based pagination
            cursor_param = pagination_cfg.get("cursor_param", "cursor")
            cursor_path = pagination_cfg.get("cursor_path", "next_cursor")
            
            cursor = None
            
            while True:
                if cursor:
                    params[cursor_param] = cursor
                
                resp = self._make_request(session, url, headers, params, timeout, auth)
                data = resp.json()
                
                records = self._extract_records(data, api_cfg)
                if not records:
                    break
                
                all_records.extend(records)
                logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
                
                # Extract next cursor
                cursor = None
                if isinstance(data, dict):
                    for key in cursor_path.split("."):
                        data = data.get(key) if isinstance(data, dict) else None
                        if data is None:
                            break
                    cursor = data
                
                if not cursor:
                    break
        
        else:
            raise ValueError(f"Unsupported pagination type: '{pagination_type}'")
        
        return all_records

    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch records from API with authentication and pagination."""
        source_cfg = cfg["source"]
        api_cfg = source_cfg["api"]
        run_cfg = source_cfg["run"]

        session = requests.Session()
        headers = self._build_auth_headers(api_cfg)
        
        # Handle basic auth separately
        auth = None
        if api_cfg.get("auth_type") == "basic":
            username = os.environ.get(api_cfg["auth_username_env"])
            password = os.environ.get(api_cfg["auth_password_env"])
            if username and password:
                auth = (username, password)
        
        base_url = api_cfg["base_url"]
        endpoint = api_cfg["endpoint"]
        
        logger.info(f"Starting API extraction from {base_url}{endpoint}")
        
        try:
            records = self._paginate(session, base_url, endpoint, headers, api_cfg, run_cfg, auth)
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        finally:
            session.close()
        
        # Compute cursor for incremental loading (if configured)
        new_cursor: Optional[str] = None
        cursor_field = api_cfg.get("cursor_field")
        
        if cursor_field and records:
            # Find the maximum cursor value from records
            try:
                cursor_values = [r.get(cursor_field) for r in records if cursor_field in r]
                if cursor_values:
                    new_cursor = str(max(cursor_values))
                    logger.info(f"Computed new cursor: {new_cursor}")
            except (TypeError, ValueError) as e:
                logger.warning(f"Could not compute cursor from field '{cursor_field}': {e}")
        
        logger.info(f"Successfully extracted {len(records)} records from API")
        return records, new_cursor
