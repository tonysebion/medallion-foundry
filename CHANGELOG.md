# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Pre-commit hooks configuration for code quality
- Comprehensive contributing guidelines (CONTRIBUTING.md)
- Ruff configuration in pyproject.toml for linting and formatting
- Missing dependencies in pyproject.toml (fsspec, polars, pandera, structlog, ibis-framework)
- GitHub Actions workflows for CI/CD (planned)
- Security policy documentation (planned)

### Changed
- Updated pyproject.toml to include all required dependencies from requirements.txt

### Fixed
- Missing fsspec dependency causing import errors
- Inconsistent dependency declarations between requirements.txt and pyproject.toml

## [0.0.2] - 2025-01-07

### Added
- YAML-first pipeline configuration with JSON Schema validation
- Bronze layer: Raw data landing with metadata and checksums
- Silver layer: SCD1/SCD2 history tracking and deduplication
- Support for multiple source types: CSV, Parquet, JSON, Excel, fixed-width files
- Database connectors: MSSQL, PostgreSQL, MySQL, DB2
- REST API support with pagination, retry logic, and rate limiting
- Pluggable storage backends: Local, S3, Azure Blob/ADLS
- CLI for pipeline discovery and execution
- Interactive pipeline creator (TUI)
- Sample data generation scripts
- Comprehensive documentation

### Changed
- Refactored documentation structure
- Improved CLI interface

## [0.0.1] - Initial Release

### Added
- Initial project structure
- Core Bronze and Silver layer functionality
- Basic source connectors
- Storage backend abstraction
- Testing framework
- Documentation

[Unreleased]: https://github.com/tonysebion/medallion-foundry/compare/v0.0.2...HEAD
[0.0.2]: https://github.com/tonysebion/medallion-foundry/releases/tag/v0.0.2
[0.0.1]: https://github.com/tonysebion/medallion-foundry/releases/tag/v0.0.1
