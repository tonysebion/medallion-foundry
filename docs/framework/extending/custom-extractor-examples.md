# Custom Extractor Examples

This guide shows how to create custom extractors for medallion-foundry. Extractors handle data extraction from various sources (APIs, databases, files) and convert them to standardized Bronze layer format.

## Extractor Architecture

All extractors implement the `Extractor` base class:

```python
from core.extractors.base import Extractor
from typing import Iterator, Dict, Any
import pandas as pd

class MyCustomExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        """Extract data and yield DataFrames"""
        # Your extraction logic here
        pass
```

## Example 1: REST API Extractor

### Basic REST API Extractor

```python
import requests
from core.extractors.base import Extractor
from core.context import ExtractionContext
from typing import Iterator
import pandas as pd

class RestApiExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        # Get API configuration
        api_config = context.source_config.get('api', {})
        url = api_config['url']
        headers = api_config.get('headers', {})
        params = api_config.get('params', {})

        # Add authentication if configured
        if 'auth' in api_config:
            auth_config = api_config['auth']
            if auth_config['type'] == 'bearer':
                headers['Authorization'] = f"Bearer {auth_config['token']}"
            elif auth_config['type'] == 'basic':
                # Handle basic auth
                pass

        # Make API request
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        # Parse JSON response
        data = response.json()

        # Convert to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and 'results' in data:
            df = pd.DataFrame(data['results'])
        else:
            # Handle other response formats
            df = pd.DataFrame([data])

        # Add metadata columns
        df['_extracted_at'] = pd.Timestamp.now()
        df['_source_url'] = url

        yield df
```

### Configuration for REST API Extractor

```yaml
sources:
  - name: user_api_data
    source:
      system: user_system
      table: users
      type: api
      api:
        url: https://api.example.com/users
        headers:
          Accept: application/json
        params:
          limit: 1000
        auth:
          type: bearer
          token_env: API_TOKEN
      run:
        load_pattern: full
        write_parquet: true
```

## Example 2: Database Extractor

### Custom SQL Database Extractor

```python
import pyodbc
from core.extractors.base import Extractor
from core.context import ExtractionContext
from typing import Iterator
import pandas as pd

class SqlExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        # Get database configuration
        db_config = context.source_config.get('database', {})
        connection_string = db_config['connection_string']
        query = db_config['query']

        # Establish connection
        conn = pyodbc.connect(connection_string)

        try:
            # Execute query
            df = pd.read_sql_query(query, conn)

            # Add metadata
            df['_extracted_at'] = pd.Timestamp.now()
            df['_source_table'] = db_config.get('table', 'unknown')

            yield df

        finally:
            conn.close()
```

### Configuration for SQL Extractor

```yaml
sources:
  - name: user_database_data
    source:
      system: user_system
      table: users
      type: db
      database:
        connection_string: "DRIVER={ODBC Driver 17 for SQL Server};SERVER=myserver;DATABASE=mydb;UID=user;PWD=password"
        query: "SELECT id, name, email, created_at FROM users WHERE created_at >= ?"
        table: users
      run:
        load_pattern: cdc
        write_parquet: true
        incremental_column: created_at
```

## Example 3: File-based Extractor

### Custom CSV/JSON File Extractor

```python
import pandas as pd
from pathlib import Path
from core.extractors.base import Extractor
from core.context import ExtractionContext
from typing import Iterator

class FileExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        # Get file configuration
        file_config = context.source_config.get('file', {})
        file_path = Path(file_config['path'])
        file_format = file_config.get('format', 'csv')

        # Read file based on format
        if file_format == 'csv':
            df = pd.read_csv(file_path, **file_config.get('read_options', {}))
        elif file_format == 'json':
            df = pd.read_json(file_path, **file_config.get('read_options', {}))
        elif file_format == 'parquet':
            df = pd.read_parquet(file_path, **file_config.get('read_options', {}))
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        # Add metadata
        df['_extracted_at'] = pd.Timestamp.now()
        df['_source_file'] = str(file_path)

        yield df
```

### Configuration for File Extractor

```yaml
sources:
  - name: local_csv_data
    source:
      system: local_system
      table: products
      type: file
      file:
        path: ./data/products.csv
        format: csv
        read_options:
          sep: ','
          encoding: utf-8
      run:
        load_pattern: full
        write_parquet: true
```

## Example 4: Paginated API Extractor

### Advanced REST API with Pagination

```python
import requests
from time import sleep
from core.extractors.base import Extractor
from core.context import ExtractionContext
from typing import Iterator, Dict, Any
import pandas as pd

class PaginatedApiExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        api_config = context.source_config.get('api', {})
        base_url = api_config['url']
        headers = api_config.get('headers', {})
        params = api_config.get('params', {}).copy()

        # Pagination settings
        page_size = api_config.get('page_size', 100)
        max_pages = api_config.get('max_pages', 1000)
        rate_limit = api_config.get('rate_limit', 1.0)  # requests per second

        params['limit'] = page_size
        page = 1
        total_records = 0

        while page <= max_pages:
            # Set page parameter
            params['page'] = page

            # Make request
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            # Check if we have data
            if isinstance(data, dict) and 'results' in data:
                records = data['results']
            elif isinstance(data, list):
                records = data
            else:
                break

            if not records:
                break

            # Convert to DataFrame
            df = pd.DataFrame(records)
            df['_extracted_at'] = pd.Timestamp.now()
            df['_page'] = page
            df['_source_url'] = response.url

            yield df

            total_records += len(df)

            # Check for next page
            if len(records) < page_size:
                break

            page += 1

            # Rate limiting
            if rate_limit > 0:
                sleep(1.0 / rate_limit)
```

### Configuration for Paginated API

```yaml
sources:
  - name: paginated_api_data
    source:
      system: api_system
      table: events
      type: api
      api:
        url: https://api.example.com/events
        headers:
          Authorization: Bearer ${API_TOKEN}
        params:
          status: active
        page_size: 500
        max_pages: 100
        rate_limit: 2.0  # 2 requests per second
      run:
        load_pattern: full
        write_parquet: true
```

## Example 5: Incremental Extractor

### CDC-style Incremental Extractor

```python
from datetime import datetime, timedelta
from core.extractors.base import Extractor
from core.context import ExtractionContext
from typing import Iterator
import pandas as pd

class IncrementalExtractor(Extractor):
    def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
        # Get incremental settings
        incremental_config = context.source_config.get('incremental', {})
        incremental_column = incremental_config.get('column', 'updated_at')
        lookback_days = incremental_config.get('lookback_days', 1)

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        # Get last successful run date from context
        if context.last_successful_run:
            start_date = max(start_date, context.last_successful_run)

        # Build query or API call with date filter
        # This is pseudo-code - adapt to your source
        data = self._fetch_data_since(start_date, end_date)

        df = pd.DataFrame(data)
        df['_extracted_at'] = pd.Timestamp.now()
        df['_incremental_start'] = start_date
        df['_incremental_end'] = end_date

        yield df

    def _fetch_data_since(self, start_date: datetime, end_date: datetime) -> list:
        # Implement your data fetching logic here
        # Could be API call, database query, etc.
        pass
```

### Configuration for Incremental Extractor

```yaml
sources:
  - name: incremental_api_data
    source:
      system: api_system
      table: orders
      type: api
      incremental:
        column: updated_at
        lookback_days: 7
      api:
        url: https://api.example.com/orders
        params:
          updated_since: ${INCREMENTAL_START}
      run:
        load_pattern: cdc
        write_parquet: true
```

## Registering Custom Extractors

### 1. Create Extractor Module

Create `core/extractors/custom.py`:

```python
from .rest_api import RestApiExtractor
from .sql_db import SqlExtractor
from .file_extractor import FileExtractor

# Export custom extractors
__all__ = ['RestApiExtractor', 'SqlExtractor', 'FileExtractor']
```

### 2. Update Extractor Factory

Modify `core/extractors/__init__.py`:

```python
from .custom import RestApiExtractor, SqlExtractor, FileExtractor

EXTRACTOR_REGISTRY = {
    'rest_api': RestApiExtractor,
    'sql_db': SqlExtractor,
    'file': FileExtractor,
    # Add your custom extractors here
}
```

### 3. Use in Configuration

```yaml
sources:
  - name: my_custom_data
    source:
      system: my_system
      table: my_table
      type: rest_api  # Matches registry key
      # Your custom config here
```

## Best Practices

### Error Handling
```python
def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
    try:
        # Your extraction logic
        yield df
    except requests.RequestException as e:
        context.logger.error(f"API request failed: {e}")
        raise
    except Exception as e:
        context.logger.error(f"Extraction failed: {e}")
        raise
```

### Logging
```python
def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
    context.logger.info(f"Starting extraction from {self.source_type}")
    # ... extraction logic ...
    context.logger.info(f"Extracted {len(df)} records")
```

### Configuration Validation
```python
def __init__(self, config: Dict[str, Any]):
    super().__init__(config)
    # Validate required config
    if 'url' not in config.get('api', {}):
        raise ValueError("API URL is required")
```

### Testing Extractors
```python
import pytest
from unittest.mock import Mock

def test_rest_api_extractor():
    config = {'api': {'url': 'https://api.example.com/data'}}
    extractor = RestApiExtractor(config)

    # Mock the API response
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = [{'id': 1, 'name': 'test'}]

        context = Mock()
        dfs = list(extractor.extract(context))

        assert len(dfs) == 1
        assert len(dfs[0]) == 1
```

## Common Patterns

### Retry Logic
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def _make_api_call(self, url, headers):
    return requests.get(url, headers=headers)
```

### Rate Limiting
```python
from core.rate_limit import RateLimiter

def extract(self, context: ExtractionContext) -> Iterator[pd.DataFrame]:
    rate_limiter = RateLimiter(requests_per_second=10)

    # ... in your loop ...
    rate_limiter.wait_if_needed()
    response = requests.get(url)
```

### Data Type Conversion
```python
def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
    # Convert date strings to datetime
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])

    # Ensure consistent data types
    df = df.astype({
        'id': 'int64',
        'amount': 'float64',
        'is_active': 'bool'
    })

    return df
```

This covers the basics of creating custom extractors. Check the existing extractors in `core/extractors/` for more advanced examples and patterns.