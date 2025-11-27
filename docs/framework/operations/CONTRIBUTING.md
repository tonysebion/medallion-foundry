# Contributing to medallion-foundry

Thank you for considering contributing to medallion-foundry! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Testing Requirements](#testing-requirements)
- [Pull Request Process](#pull-request-process)
- [Project Structure](#project-structure)
- [Adding New Features](#adding-new-features)

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Assume good intentions

## Getting Started

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR-USERNAME/medallion-foundry.git
cd medallion-foundry

# Add upstream remote
git remote add upstream https://github.com/medallion-foundry/medallion-foundry.git
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Verify setup
python run_tests.py
```

### 3. Create a Branch

```bash
# Update main branch
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

## Development Workflow

### 1. Make Changes

- Write code following our [code style](#code-style)
- Add tests for new functionality
- Update documentation as needed
- Keep commits focused and atomic

### 2. Run Tests Locally

```bash
# Run all quality checks
python run_tests.py --all-checks

# Run only tests
python run_tests.py --coverage

# Run specific test categories
python run_tests.py --unit
pytest -m integration
```

### 3. Commit Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "Add feature: brief description

Detailed explanation of what changed and why.
Fixes #issue-number"
```

#### Commit Message Guidelines

- Use present tense ("Add feature" not "Added feature")
- First line: brief summary (50 chars or less)
- Reference issues and PRs when relevant
- Examples:
  - `Add support for Azure storage backend`
  - `Fix pagination bug in API extractor`
  - `Update documentation for retry configuration`
  - `Refactor: simplify config validation logic`

### 4. Push and Create Pull Request

```bash
# Push to your fork
git push origin feature/your-feature-name

# Create Pull Request on GitHub
# - Fill out the PR template
# - Link related issues
# - Request review
```

## Code Style

### Python Code Style

We follow **PEP 8** with some modifications:

- **Line length**: 120 characters (not 79)
- **Formatting**: Use `black` for automatic formatting
- **Imports**: Organized with `isort`
- **Docstrings**: Google-style docstrings

#### Formatting Code

```bash
# Auto-format code
black core extractors tests bronze_extract.py

# Check formatting
black --check core extractors tests bronze_extract.py

# Sort imports
isort core extractors tests bronze_extract.py
```

#### Linting

```bash
# Run linter
flake8 core extractors tests bronze_extract.py

# Fix common issues automatically
autopep8 --in-place --aggressive --aggressive <file>
```

### Type Hints

- Add type hints to all new functions
- Use `typing` module for complex types
- Check with mypy: `mypy core extractors bronze_extract.py`

Example:

```python
from typing import Dict, Any, List, Optional

def process_records(
    records: List[Dict[str, Any]],
    max_size: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Process records with optional size limit.

    Args:
        records: List of record dictionaries
        max_size: Maximum number of records to process

    Returns:
        Processed records
    """
    # Implementation
    pass
```

### Documentation Style

- Use **Markdown** for all documentation
- Use **Google-style docstrings** for Python code
- Include examples in docstrings when helpful
- Keep README.md and docs synchronized

Example docstring:

```python
def fetch_records(self, cfg: Dict[str, Any], run_date: date) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch records from the data source.

    Args:
        cfg: Configuration dictionary containing platform and source settings
        run_date: The logical date for this extraction run

    Returns:
        Tuple of (records, cursor) where:
        - records: List of extracted record dictionaries
        - cursor: Optional cursor value for incremental loads

    Raises:
        ValueError: If configuration is invalid
        ConnectionError: If unable to connect to data source

    Example:
        >>> extractor = ApiExtractor()
        >>> records, cursor = extractor.fetch_records(config, date(2025, 1, 1))
        >>> print(f"Extracted {len(records)} records")
    """
```

## Testing Requirements

### Test Coverage

- **New features**: 100% coverage required
- **Bug fixes**: Add test that reproduces the bug
- **Refactoring**: Maintain existing coverage

### Writing Tests

```python
import pytest
from datetime import date

class TestMyFeature:
    """Test suite for my feature."""

    @pytest.mark.unit
    def test_basic_functionality(self):
        """Test that basic functionality works."""
        # Arrange
        input_data = {"key": "value"}

        # Act
        result = my_function(input_data)

        # Assert
        assert result["key"] == "value"

    @pytest.mark.integration
    def test_with_dependencies(self, sample_config):
        """Test with external dependencies."""
        # Use fixtures from conftest.py
        pass
```

### Test Markers

- `@pytest.mark.unit`: Fast unit tests, no external dependencies
- `@pytest.mark.integration`: Integration tests, may use mocks
- `@pytest.mark.slow`: Long-running tests

### Running Tests

See [TESTING.md](TESTING.md) for complete testing guide.

```bash
# Quick check before committing
python run_tests.py --all-checks

# Run with coverage
python run_tests.py --coverage --html-coverage
```

## Pull Request Process

### Before Submitting

- [ ] All tests pass locally
- [ ] Code is formatted with `black`
- [ ] Linting passes with `flake8`
- [ ] Type checking passes with `mypy`
- [ ] Documentation is updated
- [ ] CHANGELOG.md is updated (if applicable)
- [ ] Commit messages are clear and descriptive

### PR Checklist

- [ ] PR title clearly describes the change
- [ ] Description explains what and why
- [ ] Links to related issues
- [ ] Screenshots/examples for UI changes
- [ ] Breaking changes are noted
- [ ] Tests added/updated
- [ ] Documentation added/updated

### Review Process

1. **Automated checks**: Must pass before review
2. **Code review**: At least one approval required
3. **Discussion**: Address reviewer feedback
4. **Approval**: Once approved, maintainer will merge

### After Merge

- Update your local repository
- Delete your feature branch
- Close related issues

## Project Structure

```
medallion-foundry/
├── core/               # Core framework modules
│   ├── config.py      # Configuration loading and validation
│   ├── runner.py      # Main extraction orchestration
│   ├── io.py          # File I/O operations
│   ├── storage.py     # Storage backend abstraction
│   ├── s3.py          # S3 storage implementation
│   └── parallel.py    # Parallel execution
├── extractors/        # Data source extractors
│   ├── base.py        # Base extractor interface
│   ├── api_extractor.py
│   └── db_extractor.py
├── tests/             # Test suite
│   ├── conftest.py    # Pytest fixtures
│   └── test_*.py      # Test modules
├── docs/              # Documentation
│   ├── examples/      # Example configurations
│   └── *.md           # Feature documentation
└── bronze_extract.py  # CLI entrypoint
```

## Adding New Features

### 1. New Extractor Type

To add a new extractor (e.g., `FileExtractor`):

1. Create `extractors/file_extractor.py`
2. Inherit from `BaseExtractor`
3. Implement `fetch_records()` method
4. Add to `build_extractor()` in `core/runner.py`
5. Add configuration validation in `core/config.py`
6. Create example config in `docs/examples/configs/`
7. Add tests in `tests/test_extractors.py`
8. Update documentation

See [EXTENDING_EXTRACTORS.md](../EXTENDING_EXTRACTORS.md) for details.

### 2. New Storage Backend

To add a new storage backend (e.g., `GoogleCloudStorage`):

1. Create implementation in `docs/examples/extensions/<storage_name>/`
2. Implement `StorageBackend` interface
3. Add example configuration
4. Add README with setup instructions
5. Update `STORAGE_BACKEND_ARCHITECTURE.md`

See [STORAGE_BACKEND_ARCHITECTURE.md](../STORAGE_BACKEND_ARCHITECTURE.md) for details.

### 3. New Configuration Options

1. Add validation in `core/config.py`
2. Update `CONFIG_REFERENCE.md`
3. Add example in `docs/examples/configs/`
4. Update YAML examples
5. Add tests for validation

### 4. Core Feature Enhancement

1. Discuss in GitHub issue first
2. Update relevant core modules
3. Maintain backward compatibility
4. Add deprecation warnings if needed
5. Update all documentation
6. Add comprehensive tests

## Getting Help

- **Questions**: Open a GitHub Discussion
- **Bugs**: Open a GitHub Issue with reproduction steps
- **Feature Requests**: Open a GitHub Issue with use case
- **Security Issues**: Email maintainers directly (see SECURITY.md)

## Recognition

Contributors are recognized in:
- Git commit history
- Release notes
- Special mentions for significant contributions

Thank you for contributing to medallion-foundry! 🎉
