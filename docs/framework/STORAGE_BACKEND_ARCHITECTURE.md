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

All storage backends implement the abstract `StorageBackend` base class defined in `core/storage.py`:

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
from core.storage import get_storage_backend

# Automatically selects backend based on config
storage = get_storage_backend(platform_config)

# Use the backend
storage.upload_file("local_file.parquet", "system=api/table=users/file.parquet")
```

## Supported Backends

### S3 Storage (Default)

**Implementation**: `core/s3.py` - `S3Storage` class

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

**See**: [Azure Storage Extension Example](../examples/extensions/azure_storage/README.md) for complete implementation

### Local Filesystem

**Implementation**: Not included in core, requires custom implementation

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
- Old S3 helper functions (`upload_to_s3()`) are deprecated but still work

## Implementation Guide

### Creating a Custom Storage Backend

1. **Create backend class** implementing `StorageBackend`:

```python
# core/my_storage.py
from core.storage import StorageBackend
from typing import List

class MyStorage(StorageBackend):
    def __init__(self, config: Dict[str, Any]):
        # Initialize from config
        self.bucket = config["bronze"]["my_bucket"]
        # ... setup client ...

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        # Upload implementation
        pass

    def download_file(self, remote_path: str, local_path: str) -> bool:
        # Download implementation
        pass

    def list_files(self, prefix: str) -> List[str]:
        # List implementation
        pass

    def delete_file(self, remote_path: str) -> bool:
        # Delete implementation
        pass

    def get_backend_type(self) -> str:
        return "my_storage"
```

2. **Register in factory** (`core/storage.py`):

```python
def get_storage_backend(config: Dict[str, Any]) -> StorageBackend:
    backend_type = config.get("bronze", {}).get("storage_backend", "s3")

    if backend_type == "s3":
        from core.s3 import S3Storage
        return S3Storage(config)
    elif backend_type == "my_storage":
        from core.my_storage import MyStorage
        return MyStorage(config)
    # ... other backends ...
```

3. **Update config validation** (`core/config.py`):

```python
valid_backends = ["s3", "azure", "local", "my_storage"]
```

4. **Document configuration requirements**

## Usage

### In Extractors/Runners

The runner automatically uses the configured storage backend:

```python
# core/runner.py
from core.storage import get_storage_backend

# Initialize storage backend
storage_backend = get_storage_backend(platform_cfg)

# Upload files
storage_backend.upload_file(str(local_path), remote_path)
```

### Direct Usage

You can also use storage backends directly:

```python
from core.storage import get_storage_backend

# Load config
config = load_config("my_config.yaml")

# Get storage backend
storage = get_storage_backend(config["platform"])

# Upload
storage.upload_file("output/file.parquet", "system=api/table=users/file.parquet")

# List files
files = storage.list_files("system=api/table=users/")

# Download
storage.download_file("system=api/table=users/file.parquet", "downloaded.parquet")

# Delete
storage.delete_file("system=api/table=users/old_file.parquet")
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

If you have custom extractors using the old S3 helpers:

```python
# Before (deprecated)
from core.s3 import upload_to_s3
upload_to_s3(local_path, platform_cfg, relative_path)

# After (recommended)
from core.storage import get_storage_backend
storage = get_storage_backend(platform_cfg)
storage.upload_file(str(local_path), f"{relative_path}{local_path.name}")
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

For implementation examples, see:
- [Azure Storage Extension](../examples/extensions/azure_storage/README.md)
