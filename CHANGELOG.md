# Changelog

All notable changes to medallion-foundry will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Typed configuration models (Pydantic) exposed via `__typed_model__` on loaded configs for gradual adoption.
- Public `SilverArtifactWriter` protocol and `DefaultSilverArtifactWriter` implementation.
- Partition abstraction classes: `BronzePartition`, `SilverPartition` used in `silver_extract` path resolution.
- Explicit sample bootstrap command (`core.samples.bootstrap`) for deterministic Bronze fixtures.
- Structured deprecation & compatibility warning framework (`core.deprecation`).
- Config schema versioning (`config_version`, default 1) with compatibility warning (CFG004 when missing).
- Streaming Silver promotion now reuses `DatasetWriter` chunk APIs (consistent partition/error handling).

### Deprecated
- Implicit fallback from `platform.bronze.output_dir` to `platform.bronze.local_path` (CFG001, removal in 1.3.0).
- Legacy `source.api.url` key (CFG002, removal in 1.3.0) – use `base_url`.
- Missing `source.api.endpoint` defaulting to '/' (CFG003, removal in 1.3.0).
- Positional legacy wrapper `write_silver_outputs` (API001, removal in 1.3.0) – migrate to `DefaultSilverArtifactWriter`.
- `StreamingSilverWriter` class (replaced by direct `DatasetWriter` usage in streaming path; removal in 1.3.0).

### Changed
- `generate_silver_samples.py` no longer auto-synthesizes Bronze data; tests and users should call bootstrap command explicitly.

### Migration Guidance
Add explicit `platform.bronze.local_path`; rename `source.api.url` to `base_url`; specify `source.api.endpoint`; replace calls to `write_silver_outputs` wrapper with `DefaultSilverArtifactWriter().write(...)` before v1.3.0.


## [1.0.0] - 2025-11-12

### Added

#### Core Framework
- **Custom Exception Classes** (`core/exceptions.py`): Specific exception types for better error handling and debugging
  - `BronzeFoundryError`: Base exception with context details
  - `ConfigValidationError`: Configuration validation failures
  - `ExtractionError`: Data extraction failures
  - `StorageError`: Storage operation failures
  - `AuthenticationError`, `PaginationError`, `StateManagementError`, `DataQualityError`, `RetryExhaustedError`

- **Enhanced Logging** (`core/logging_config.py`): Configurable logging system
  - JSON formatter for production monitoring
  - Human-readable formatter with optional colors
  - Environment variable-based configuration (`BRONZE_LOG_LEVEL`, `BRONZE_LOG_FORMAT`)
  - File logging with rotation
  - Structured logging with context

- **CLI Enhancements** (`bronze_extract.py`): New command-line flags
  - `--dry-run`: Validate configuration and connections without extraction
  - `--validate-only`: Configuration validation only
  - `--verbose` / `-v`: Debug-level logging
  - `--quiet` / `-q`: Error-only logging
  - `--version`: Show version information
  - `--list-backends`: List available storage backends
  - `--log-format`: Choose log format (human/json/simple)

- **Performance Monitoring**: Enhanced metadata tracking
  - Extended `write_batch_metadata()` to include performance metrics
  - Support for extraction duration, records/second, network statistics
  - Data quality metrics support

#### Testing & Quality
- **Test Runner** (`run_tests.py`): Unified testing script
  - Run tests, type checking, linting, formatting in one command
  - `--all-checks`: Run complete quality suite
  - `--coverage`: Generate coverage reports
  - CI/CD ready (Jenkins, GitLab, Azure DevOps, Travis, CircleCI)

- **Test Configuration**:
  - `pytest.ini`: Pytest configuration with markers (unit, integration, slow)
  - `mypy.ini`: Type checking configuration
  - Improved test coverage with pagination, auth, and error handling tests

- **Testing Documentation** (`TESTING.md`): Comprehensive testing guide
  - Test execution guide
  - CI/CD integration examples
  - Writing tests guide
  - Coverage goals

#### Documentation
- **docs/CONTRIBUTING.md**: Complete contribution guidelines
  - Development workflow
  - Code style requirements
  - Testing requirements
  - PR process

- **config/README.md**: Config directory documentation
  - Purpose and usage
  - Best practices
  - Security guidelines
  - Environment management

- **MkDocs Setup** (`mkdocs.yml`): Documentation website configuration
  - Material theme
  - Dark mode support
  - Code highlighting
  - Navigation structure

- **Real-World Examples** (`docs/examples/REAL_WORLD_EXAMPLES.md`):
  - Shopify API integration
  - Salesforce REST API
  - Stripe payments
  - GitHub API
  - HubSpot CRM
  - Google Analytics (custom extractor example)

#### Architecture
- **Pluggable Storage Backends**: Abstract `StorageBackend` interface
  - S3 storage in core
  - Azure storage as optional extension example
  - Support for local filesystem

- **Type Hints**: Improved type coverage throughout codebase
  - Optional types for better null safety
  - Generic types for collections
  - Return type annotations

### Changed
- Updated `bronze_extract.py` to use new logging configuration
- Enhanced `write_batch_metadata()` signature for performance and quality metrics
- Improved error messages with contextual information
- Updated setup.py to Development Status 5 - Production/Stable

### Fixed
- Type hint issues in exception classes
- Empty record chunking behavior
- Test compatibility with new metadata signature

### Documentation
- Reorganized examples directory into configs/, extensions/, custom_extractors/
- Updated all documentation for v1.0 release
- Removed personal references, now organization/community project
- Added comprehensive testing and contribution guides

### Infrastructure
- CI/CD setup without GitHub Actions requirement
- Support for multiple CI/CD platforms
- Automated quality checks (tests, linting, type checking, formatting)

## [0.x.x] - Pre-release

Earlier versions were development releases without formal changelog tracking.

---

## Guidelines for Future Releases

### Version Numbering
- **Major (X.0.0)**: Breaking changes, major new features
- **Minor (1.X.0)**: New features, backwards compatible
- **Patch (1.0.X)**: Bug fixes, minor improvements

### Categories
- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Features marked for removal
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security improvements

### Future Roadmap

#### Planned for v1.1.0
- Configurable retry logic (attempts, backoff timing)
- API rate limiting with Retry-After header support
- Enhanced data quality checks module
- Centralized state management for all extractors

#### Planned for v1.2.0
- Database connection pooling
- Enhanced storage backend features (multipart upload, resumable uploads)
- Additional real-world integration examples

#### Planned for v2.0.0
- Pydantic configuration validation (breaking change)
- Enhanced async support
- Streaming processing for large datasets
