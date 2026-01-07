# Storage Backend Architecture

This document describes the pluggable storage backend architecture implemented in medallion-foundry v1.0.

## Overview

Bronze-foundry now supports multiple storage backends through a clean abstraction layer. This allows you to:
- Use **S3** (AWS, MinIO, or any S3-compatible storage) - default
- Use **Azure** (Blob Storage or ADLS Gen2)
- Use **local filesystem** for development/testing
- Implement **custom storage backends** for other systems

## Architecture

### StorageBackend Interface

All storage backends implement the abstract `StorageBackend` base class defined in `pipelines/lib/storage/`:

```python
class StorageBackend(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to remote storage."""
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from remote storage."""
        pass

    @abstractmethod
    def list_files(self, prefix: str) -> List[str]:
        """List files in remote storage with given prefix."""
        pass

    @abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from remote storage."""
        pass

    @abstractmethod
    def get_backend_type(self) -> str:
        """Get backend type identifier."""
        pass
```

### Factory Pattern

The `get_storage_backend()` factory function instantiates the appropriate backend based on configuration:

```python
from pipelines.lib.storage import get_backend

# Automatically selects backend based on path prefix
storage = get_backend("s3://my-bucket/bronze/")

# Use the backend
storage.put("local_file.parquet", "s3://my-bucket/bronze/data.parquet")
```

## Supported Backends

### S3 Storage (Default)

**Implementation**: `pipelines/lib/storage/s3.py`

**Configuration**:
```yaml
platform:
  bronze:
    storage_backend: "s3"  # optional, defaults to s3
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"

  s3_connection:
    endpoint_url_env: "BRONZE_S3_ENDPOINT"
    access_key_env: "BRONZE_S3_ACCESS_KEY"
    secret_key_env: "BRONZE_S3_SECRET_KEY"
```

**Features**:
- Works with AWS S3, MinIO, and any S3-compatible storage
- Automatic retries with exponential backoff
- Environment-based credentials
- Optional endpoint URL for non-AWS S3
- Integrated with unified retry + circuit breaker for resilience
- Rate limiting support via external configuration

### Azure Storage

**Implementation**: Not included in core, see extension example

**Configuration**:
```yaml
platform:
  bronze:
    storage_backend: "azure"
    azure_storage_account: "myaccount"
    azure_container: "bronze"
    azure_prefix: "bronze"

  azure_connection:
    connection_string_env: "AZURE_STORAGE_CONNECTION_STRING"
    # Or use SAS token, service principal, etc.
```

**Implementation**: `pipelines/lib/storage/adls.py`

### Local Filesystem

**Implementation**: `pipelines/lib/storage/local.py`

**Configuration**:
```yaml
platform:
  bronze:
    storage_backend: "local"
    local_path: "/data/bronze"
```

## Configuration

### Required Fields by Backend

| Backend | Required Bronze Fields | Required Connection Section |
|---------|------------------------|----------------------------|
| S3 | `s3_bucket`, `s3_prefix` | `s3_connection` |
| Azure | `azure_storage_account`, `azure_container` | `azure_connection` |
| Local | `local_path` | None |

### Backward Compatibility

For backward compatibility with existing configs:
- If `storage_backend` is not specified, defaults to `"s3"`
- `s3_enabled: true` is supported, but `storage_enabled: true` is preferred
- Legacy helpers such as `S3StorageBackend` and `upload_to_s3()` have been removed; call the standard storage backend API directly

## Implementation Guide

### Creating a Custom Storage Backend

See the existing implementations in `pipelines/lib/storage/` for examples:
- `local.py` - Local filesystem
- `s3.py` - AWS S3 and compatible services
- `adls.py` - Azure Data Lake Storage
- `fsspec_backend.py` - Generic fsspec-based backend

## Usage

### Usage Example

```python
from pipelines.lib.storage import get_backend

# Get backend based on path prefix
storage = get_backend("s3://my-bucket/bronze/")

# Write data
storage.put(local_path, "s3://my-bucket/bronze/data.parquet")

# Read data
data = storage.get("s3://my-bucket/bronze/data.parquet")

# List files
files = storage.glob("s3://my-bucket/bronze/*.parquet")
```

## Benefits

1. **Vendor Neutrality** - Not locked into any single cloud provider
2. **Extensibility** - Easy to add new backends without modifying core code
3. **Testability** - Can use local backend for testing without cloud dependencies
4. **Consistency** - Same interface for all storage systems
5. **Backward Compatibility** - Existing S3 configs work without changes

## Migration Guide

### From S3-Only to Pluggable Backends

If you have existing configs, they will continue to work. To explicitly specify S3:

```yaml
# Before (still works)
platform:
  bronze:
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"

# After (recommended)
platform:
  bronze:
    storage_backend: "s3"  # Explicit backend selection
    s3_bucket: "analytics-bronze"
    s3_prefix: "bronze"
```

### Updating Code

Use the storage backend directly:

```python
from pipelines.lib.storage import get_backend

storage = get_backend(target_path)
storage.put(local_path, target_path)
```

## Testing

The storage backend architecture is designed for testability:

1. **Mock backends** for unit tests
2. **Local backend** for integration tests
3. **MinIO** for S3-compatible testing without AWS costs

Example test:

```python
def test_extraction_with_mock_storage():
    # Create mock storage backend
    class MockStorage(StorageBackend):
        def __init__(self):
            self.uploaded_files = []

        def upload_file(self, local_path, remote_path):
            self.uploaded_files.append((local_path, remote_path))
            return True
        # ... implement other methods ...

    # Inject into runner
    # Test extraction
    # Assert uploaded_files contains expected files
```

## Future Enhancements

Potential future additions:
- **HDFS** support for Hadoop ecosystems
- **SFTP** support for legacy systems
- **Multi-backend** support (upload to multiple destinations)
- **Caching layer** for frequently accessed files
- **Compression** at storage layer
- **Encryption** at rest

---

For implementation examples, see `pipelines/lib/storage/`.
