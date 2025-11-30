# Medallion Architecture Glossary

This glossary defines key terms and concepts used in medallion-foundry and the broader medallion architecture pattern.

## Core Concepts

### Medallion Architecture
A data architecture pattern with three layers:
- **Bronze**: Raw, unprocessed data from source systems
- **Silver**: Cleaned, transformed, and enriched data
- **Gold**: Business-ready data for analytics and reporting

### Bronze Layer
The landing zone for raw data extraction. Data is stored in its original format with minimal transformation. Purpose: preserve source data integrity, enable reprocessing, support multiple downstream consumers.

### Silver Layer
The transformation and curation layer. Data is cleaned, deduplicated, and standardized. Purpose: create reliable datasets for business logic, enable data discovery, support multiple Gold layer consumers.

### Gold Layer
The consumption layer with business-specific aggregations and metrics. Purpose: provide optimized datasets for reporting, analytics, and machine learning.

## Data Patterns

### Full Load
Complete refresh of all data from the source system. Used for: dimension tables, small datasets, when CDC is not available.

### Change Data Capture (CDC)
Incremental loading of only changed records. Tracks inserts, updates, and deletes. Used for: large tables, real-time requirements, audit trails.

### Current History (SCD Type 2)
Maintains full history of changes with effective dates. Each change creates a new record with start/end dates. Used for: slowly changing dimensions, audit requirements, point-in-time analysis.

### Natural Key
Business identifier that uniquely identifies a record (e.g., customer_id, order_number). Used for deduplication and change detection.

### Surrogate Key
System-generated unique identifier for records. Independent of business keys. Used for: performance optimization, handling key changes, data warehouse best practices.

## Configuration Terms

### Source
A data extraction configuration defining:
- System and table names
- Extraction type (API, database, file)
- Load pattern and schedule
- Storage and processing options

### Platform
Infrastructure configuration defining:
- Storage backends (S3, Azure, local)
- Connection settings and credentials
- Performance and reliability options

### Extractor
A component that connects to data sources and retrieves data. Types include:
- **API Extractor**: REST/GraphQL APIs
- **Database Extractor**: SQL databases via ODBC
- **File Extractor**: Local/network files (CSV, JSON, Parquet)

### Storage Backend
Pluggable storage implementation. Supported:
- **S3**: AWS S3 buckets
- **Azure**: Azure Blob Storage
- **Local**: Local filesystem

## Processing Terms

### Extraction Context
Runtime information passed to extractors including:
- Configuration settings
- Run date and parameters
- Logging and tracing objects
- Previous run metadata

### DataFrame
In-memory data structure (Pandas) used for data manipulation. Extractors yield DataFrames that are written to Bronze storage.

### Partitioning
Data organization strategy using folder hierarchies:
- **Hive-style**: `key1=value1/key2=value2/`
- **Date partitioning**: `dt=2025-11-27/`
- **Benefits**: Query optimization, data lifecycle management

### Metadata Files
JSON files written alongside data:
- **_metadata.json**: Run information, record counts, schema
- **_checksums.json**: File integrity verification
- **Purpose**: Data lineage, validation, debugging

## Quality and Reliability

### Idempotency
Ability to run the same operation multiple times with identical results. Ensures safe retries and reprocessing.

### Data Validation
Schema and content verification including:
- Column presence and types
- Value ranges and formats
- Referential integrity
- Business rule compliance

### Circuit Breaker
Error handling pattern that stops processing when failure thresholds are exceeded. Prevents cascade failures and resource waste.

### Retry Logic
Automatic retry of failed operations with exponential backoff. Configurable per operation type.

## Performance Terms

### Parallel Workers
Concurrent processing threads for:
- Multiple source extractions
- File uploads/downloads
- Data transformations

### Chunking
Splitting large datasets into smaller pieces for:
- Memory management
- Parallel processing
- Storage optimization

### Rate Limiting
Throttling request frequency to respect API limits and prevent overloading source systems.

### Streaming
Processing data without loading everything into memory. Used for large datasets and real-time processing.

## Operational Terms

### Dry Run
Validation mode that simulates extraction without writing data. Used for: testing configurations, estimating resource needs, debugging.

### Validation Only
Configuration validation without execution. Checks syntax, connectivity, and permissions.

### Run Options
CLI flags controlling execution behavior:
- `--dry-run`: Simulate without writing
- `--validate-only`: Check configuration only
- `--parallel-workers`: Concurrent processing count
- `--force`: Override safety checks

### Context Building
Creating the runtime environment with:
- Configuration parsing
- Storage backend initialization
- Logging setup
- Credential resolution

## Storage Terms

### Bucket
Top-level storage container:
- S3: Bucket name
- Azure: Container name
- Local: Directory path

### Prefix
Storage path within a bucket (like a folder). Used for organization and access control.

### Object Key
Full path to a stored file including prefix and filename.

### Multipart Upload
Large file upload strategy splitting files into parts for:
- Improved reliability
- Better performance
- Resume capability

## Error Handling

### Exit Codes
Process termination status:
- `0`: Success
- `1-99`: Configuration errors
- `100+`: Runtime errors

### Structured Logging
Consistent log format with:
- Timestamps and log levels
- Context information
- Error details and stack traces
- Performance metrics

### Exception Wrapping
Converting source-specific errors to standardized framework exceptions with consistent error codes.

## Development Terms

### Extractor Registry
Central registry mapping extractor types to implementation classes. Enables pluggable extractor architecture.

### Hook System
Extension points for custom logic:
- Pre/post extraction hooks
- Validation hooks
- Notification hooks

### Tracing
Distributed tracing for monitoring complex operations across multiple components and services.

### Testing Markers
Pytest markers for different test types:
- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.slow`: Performance tests

## Advanced Concepts

### Data Contracts
Agreements between data producers and consumers defining:
- Schema expectations
- Quality requirements
- Update frequencies
- Support responsibilities

### Data Mesh
Decentralized data architecture where domains own their data pipelines and products.

### Data Lakehouse
Architecture combining data lake flexibility with data warehouse performance and governance.

### Lakehouse Tables
Tables supporting both file-based (Parquet) and table-based (Delta) access patterns.

This glossary covers the most common terms. For implementation details, see the CONFIG_REFERENCE.md and API documentation.