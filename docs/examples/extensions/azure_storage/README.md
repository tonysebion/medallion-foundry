# Example: Adding Azure Blob Storage / ADLS Gen2 Support

This document demonstrates how to extend bronze-foundry to support Azure Blob Storage or Azure Data Lake Storage Gen2 (ADLS Gen2) as an alternative to S3-compatible storage.

> **Status**: This is a **complete, working example** provided in `docs/examples/`. Azure storage is not included in core v1.0 to keep dependencies minimal, but you can add it yourself by following the steps below.

## Quick Start

### 1. Install Azure Dependencies

```bash
pip install -r requirements-azure.txt
```

Or manually:

```bash
pip install azure-storage-blob>=12.19.0 azure-identity>=1.15.0
```

### 2. Copy Azure Backend to Core

```bash
# Windows PowerShell
copy docs\examples\extensions\azure_storage\azure_storage.py core\azure_storage.py

# Linux/Mac
cp docs/examples/extensions/azure_storage/azure_storage.py core/azure_storage.py
```

### 3. Update Storage Factory

Edit `core/storage.py` and update the `get_storage_backend()` function:

```python
def get_storage_backend(config: Dict[str, Any]) -> StorageBackend:
    """Factory function to get appropriate storage backend."""
    backend_type = config.get("bronze", {}).get("storage_backend", "s3")
    
    if backend_type == "s3":
        from core.s3 import S3Storage
        return S3Storage(config)
    
    elif backend_type == "azure":
        # Azure backend (optional, requires azure-storage-blob)
        try:
            from core.azure_storage import AzureStorage
            return AzureStorage(config)
        except ImportError as e:
            raise ImportError(
                "Azure storage backend requires azure dependencies. "
                "Install with: pip install -r requirements-azure.txt"
            ) from e
    
    # ... rest of backends ...
```

### 4. Use Azure Configuration

Copy the example config:

```bash
copy docs\examples\extensions\azure_storage\azure_config.yaml configs\my_azure_config.yaml
```

Set your Azure credentials:

```powershell
# Using connection string (easiest)
$env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=..."

# OR using account key
$env:AZURE_STORAGE_ACCOUNT = "myaccount"
$env:AZURE_STORAGE_KEY = "your-account-key"
```

### 5. Run Extraction

```bash
python bronze_extract.py --config configs/my_azure_config.yaml
```

Data will be uploaded to Azure Blob Storage at:
```
azure://bronze/system=<system>/table=<table>/dt=YYYY-MM-DD/part-0001.parquet
```

---

## Implementation Details

The complete Azure storage backend implementation is provided in **`docs/examples/azure_storage_example.py`**.

### Key Features

The Azure backend (`AzureStorage` class) implements the `StorageBackend` interface with:

- **Multiple Authentication Methods**:
  - Connection string (easiest for dev/test)
  - Account key
  - Service principal (recommended for production)
  - Managed identity (for Azure VMs/Functions)

- **Automatic Retries**: Uses `tenacity` for exponential backoff on failures

- **Container Management**: Automatically creates container if it doesn't exist

- **ADLS Gen2 Support**: Works with both Blob Storage and ADLS Gen2

- **Path Prefix Support**: Optional prefix for all blob paths (like S3 prefix)

### Implementation Architecture

```
bronze-foundry/
├── core/
│   ├── storage.py          # ✅ Already in core - abstract base class
│   ├── s3.py              # ✅ Already in core - S3 implementation
│   └── azure_storage.py   # ⬅️ Copy from docs/examples/azure_storage_example.py
├── docs/examples/
│   ├── azure_storage_example.py    # 📄 Complete Azure backend code
│   ├── azure_storage_example.yaml  # 📄 Example configuration
│   └── AZURE_STORAGE_EXTENSION.md  # 📄 This documentation
└── requirements-azure.txt  # 📄 Azure dependencies
```

### Storage Backend Interface

The framework already provides the `StorageBackend` abstract base class in `core/storage.py`:

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
        """List files with given prefix."""
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

### Azure Backend Implementation

The `AzureStorage` class in `azure_storage_example.py` implements all these methods:

**Key Implementation Highlights**:

1. **`__init__()`** - Builds Azure BlobServiceClient with flexible authentication
2. **`upload_file()`** - Uploads with retry logic using `tenacity`
3. **`download_file()`** - Downloads blobs to local filesystem
4. **`list_files()`** - Lists blobs with prefix filtering
5. **`delete_file()`** - Deletes blobs from container

See **`azure_storage.py`** in this directory for the complete 300+ line implementation with:
- Full error handling
- Multiple auth methods
- Comprehensive logging
- Production-ready retry logic

---

## Configuration

### Example Configuration

See **`azure_config.yaml`** in this directory for a complete configuration example.

**Key configuration sections**:

```yaml
platform:
  bronze:
    storage_backend: "azure"  # Select Azure backend
    azure_container: "bronze" # Container name (like S3 bucket)
    azure_prefix: "bronze"    # Optional path prefix
    
  azure_connection:
    # Choose ONE authentication method:
    
    # Method 1: Connection string (easiest)
    connection_string_env: "AZURE_STORAGE_CONNECTION_STRING"
    
    # Method 2: Account key
    # account_name_env: "AZURE_STORAGE_ACCOUNT"
    # account_key_env: "AZURE_STORAGE_KEY"
    
    # Method 3: Service principal (production)
    # account_name_env: "AZURE_STORAGE_ACCOUNT"
    # tenant_id_env: "AZURE_TENANT_ID"
    # client_id_env: "AZURE_CLIENT_ID"
    # client_secret_env: "AZURE_CLIENT_SECRET"
    
    # Method 4: Managed identity
    # account_name_env: "AZURE_STORAGE_ACCOUNT"
    # use_managed_identity: true
```

### Configuration Validation

When you add Azure support, the config validator in `core/config.py` will check:

- `storage_backend: "azure"` is set
- `azure_container` is provided
- `azure_connection` section exists
- At least one authentication method is configured

---

## Authentication Methods

### Method 1: Connection String (Recommended for Development)

**Easiest to set up**, get from Azure Portal:

1. Navigate to Storage Account → Access keys
2. Copy Connection string
3. Set environment variable:

```powershell
$env:AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=..."
```

### Method 2: Account Key

**Simple but less secure**, requires account name + key:

```powershell
$env:AZURE_STORAGE_ACCOUNT = "mystorageaccount"
$env:AZURE_STORAGE_KEY = "your-account-key-here"
```

### Method 3: Service Principal (Recommended for Production)

**Most secure for production**, uses Azure AD:

1. Create service principal in Azure AD
2. Grant "Storage Blob Data Contributor" role
3. Set environment variables:

```powershell
$env:AZURE_STORAGE_ACCOUNT = "mystorageaccount"
$env:AZURE_TENANT_ID = "your-tenant-id"
$env:AZURE_CLIENT_ID = "your-client-id"
$env:AZURE_CLIENT_SECRET = "your-client-secret"
```

### Method 4: Managed Identity

**Best for Azure VMs/Functions**, no credentials needed:

```yaml
azure_connection:
  account_name_env: "AZURE_STORAGE_ACCOUNT"
  use_managed_identity: true
```

---

## Usage Examples

### Basic Extraction

```bash
# 1. Install Azure dependencies
pip install -r requirements-azure.txt

# 2. Copy backend to core
copy docs\examples\extensions\azure_storage\azure_storage.py core\azure_storage.py

# 3. Update core/storage.py factory (see "Update Storage Factory" above)

# 4. Set credentials
$env:AZURE_STORAGE_CONNECTION_STRING = "your-connection-string"
$env:EXAMPLE_API_TOKEN = "your-api-token"

# 5. Run extraction
python bronze_extract.py --config docs/examples/extensions/azure_storage/azure_config.yaml
```

### Verify Upload in Azure Portal

1. Open Azure Portal
2. Navigate to: Storage Accounts → [your account] → Containers → bronze
3. Browse to: `bronze/system=example_system/table=example_table/dt=2025-01-12/`
4. Verify Parquet files exist

### Testing with Azure Storage Explorer

Download [Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer/):

```bash
# View uploaded files
# Connect using connection string or account key
# Navigate to bronze container
# Browse partitioned directory structure
```

---

## Azure Data Lake Storage Gen2 (ADLS Gen2)

The Azure backend works seamlessly with ADLS Gen2:

### What is ADLS Gen2?

- **Blob Storage with hierarchical namespace enabled**
- Optimized for big data analytics
- Better performance for directory operations
- Compatible with Hadoop/Spark/Databricks

### Using ADLS Gen2

1. **Create storage account with hierarchical namespace**:
   - In Azure Portal: Create Storage Account
   - Advanced tab → Enable "Hierarchical namespace"

2. **Use same configuration** - no code changes needed!

3. **Benefits**:
   - Faster directory listing
   - Atomic directory operations
   - Better integration with analytics platforms

The example code works identically for both Blob Storage and ADLS Gen2.

---

## Production Considerations

### Security

- **Use Service Principal** in production (Method 3)
- **Enable firewall rules** on storage account
- **Use Private Endpoints** for enhanced security
- **Enable soft delete** for data protection
- **Rotate credentials** regularly

### Performance

- **Choose appropriate region** - colocate with compute
- **Use Premium tier** for low-latency requirements
- **Enable CDN** for frequently accessed data
- **Monitor throttling** - Azure has rate limits

### Cost Optimization

- **Use lifecycle management** - auto-tier to cool/archive
- **Enable compression** - Parquet with Snappy/GZIP
- **Monitor storage metrics** - track growth and costs
- **Use reserved capacity** for predictable workloads

### Monitoring

```python
# Add custom metrics in azure_storage_example.py
import time
from azure.monitor.opentelemetry import configure_azure_monitor

# Track upload times, sizes, failures
logger.info(f"Uploaded {file_size_mb}MB in {duration_sec}s")
```

---

## Troubleshooting

### Common Issues

**1. Import Error: "No module named 'azure.storage.blob'"**

```bash
# Install Azure dependencies
pip install azure-storage-blob azure-identity
```

**2. Authentication Error: "Connection string is invalid"**

- Verify connection string has no extra spaces
- Ensure environment variable is set correctly
- Try using account key method instead

**3. Container Not Found**

- Azure backend auto-creates containers
- Check permissions: need "Storage Blob Data Contributor" role
- Verify container name doesn't have invalid characters

**4. Slow Upload Performance**

- Check network bandwidth
- Verify storage account region
- Consider using Premium tier
- Enable parallel chunk processing in config

**5. Permission Denied**

```bash
# Grant required role
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <service-principal-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>
```

---

## Files Provided

This example includes three complete files in this directory:

1. **`azure_storage.py`** - Full Azure backend implementation (300+ lines)
2. **`azure_config.yaml`** - Complete example configuration
3. **`README.md`** - This documentation

All files are production-ready and fully documented.

---

## Summary

Adding Azure storage support to bronze-foundry is straightforward:

1. Install dependencies (`pip install -r requirements-azure.txt`)
2. Copy backend file (`azure_storage_example.py` → `core/azure_storage.py`)
3. Update storage factory in `core/storage.py`
4. Configure and run!

The pluggable architecture makes this a 5-minute task. The example code handles authentication, retries, error handling, and works with both Blob Storage and ADLS Gen2.

**Questions?** See the complete implementation in `azure_storage.py` or open an issue on GitHub.

**Note**: Azure dependencies are listed in `requirements-azure.txt` in the project root.
