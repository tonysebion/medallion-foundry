# Testing Guide

This document explains how to run tests and quality checks for medallion-foundry.

## Quick Start

```bash
# Run all tests
python run_tests.py

# Run all tests with coverage
python run_tests.py --coverage

# Run all quality checks (tests + linting + type checking)
python run_tests.py --all-checks
```

## Test Categories

Tests are marked with categories for selective execution:

- **unit**: Fast unit tests with no external dependencies
- **integration**: Tests that may require external services (mocked)
- **slow**: Long-running tests

### Running Specific Test Categories

```bash
# Run only unit tests
python run_tests.py --unit

# Run only integration tests
python run_tests.py --integration

# Run tests with pytest directly and filter by marker
pytest -m unit
pytest -m "not slow"
```

## Coverage Reports

Generate coverage reports to identify untested code:

```bash
# Terminal coverage report
python run_tests.py --coverage

# HTML coverage report (opens in browser)
python run_tests.py --html-coverage
open htmlcov/index.html  # Linux/Mac
start htmlcov/index.html # Windows
```

## Code Quality Checks

### Type Checking (mypy)

```bash
# Run type checking
python run_tests.py --mypy

# Or directly with mypy
mypy core extractors bronze_extract.py
```

Configuration in `mypy.ini`.

### Linting (flake8)

```bash
# Run linting
python run_tests.py --flake8

# Or directly with flake8
flake8 core extractors tests bronze_extract.py
```

### Code Formatting (black)

```bash
# Check formatting
python run_tests.py --black-check

# Auto-format code
black core extractors tests bronze_extract.py
```

## CI/CD Integration

The `run_tests.py` script is designed for easy CI/CD integration:

### Jenkins

```groovy
pipeline {
    agent any
    stages {
        stage('Setup') {
            steps {
                sh 'python -m venv .venv'
                sh '.venv/bin/pip install -r requirements.txt -r requirements-dev.txt'
            }
        }
        stage('Test') {
            steps {
                sh '.venv/bin/python run_tests.py --all-checks'
            }
        }
    }
}
```

### GitLab CI

```yaml
test:
  image: python:3.11
  script:
    - pip install -r requirements.txt -r requirements-dev.txt
    - python run_tests.py --all-checks
  coverage: '/TOTAL.*\s+(\d+%)$/'
```

### Azure DevOps

```yaml
steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.11'
- script: |
    pip install -r requirements.txt -r requirements-dev.txt
    python run_tests.py --all-checks
  displayName: 'Run Tests'
```

### Travis CI

```yaml
language: python
python:
  - "3.8"
  - "3.9"
  - "3.10"
  - "3.11"
install:
  - pip install -r requirements.txt -r requirements-dev.txt
script:
  - python run_tests.py --all-checks
```

### CircleCI

```yaml
version: 2.1
jobs:
  test:
    docker:
      - image: cimg/python:3.11
    steps:
      - checkout
      - run:
          name: Install dependencies
          command: |
            pip install -r requirements.txt -r requirements-dev.txt
      - run:
          name: Run tests
          command: python run_tests.py --all-checks
workflows:
  test:
    jobs:
      - test
```

## Running Tests Manually with pytest

For more control, use pytest directly:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_config.py

# Run specific test
pytest tests/test_config.py::TestConfigValidation::test_valid_config

# Run tests matching pattern
pytest -k "test_api"

# Stop on first failure
pytest -x

# Run last failed tests
pytest --lf

# Show local variables on failure
pytest -l

# Run in parallel (requires pytest-xdist)
pytest -n auto
```

## Writing Tests

### Test Structure

```python
import pytest
from pathlib import Path

class TestMyFeature:
    """Test suite for MyFeature."""
    
    @pytest.mark.unit
    def test_basic_functionality(self):
        """Test basic functionality."""
        # Arrange
        input_data = {"key": "value"}
        
        # Act
        result = my_function(input_data)
        
        # Assert
        assert result == expected_value
    
    @pytest.mark.integration
    def test_with_external_service(self, tmp_path):
        """Test integration with external service."""
        # Use tmp_path fixture for temporary files
        config_file = tmp_path / "test.yaml"
        config_file.write_text("test: data")
        
        # Test code here
        pass
```

### Using Fixtures

Common fixtures are defined in `tests/conftest.py`:

```python
def test_with_config(sample_config):
    """Use the sample_config fixture."""
    assert sample_config["platform"]["bronze"]["s3_bucket"] == "test-bronze-bucket"
```

### Mocking External Services

```python
from unittest.mock import Mock, patch

@pytest.mark.unit
def test_api_call():
    """Test API call with mocked response."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {"data": []}
        mock_get.return_value.status_code = 200
        
        # Your test code here
        pass
```

## Continuous Testing During Development

Use pytest-watch for automatic test runs on file changes:

```bash
# Install pytest-watch
pip install pytest-watch

# Run tests automatically
ptw

# Run with coverage
ptw -- --cov=core --cov=extractors
```

## Test Coverage Goals

- **Overall**: 80%+ coverage target
- **Core modules**: 90%+ coverage (config, runner, io)
- **Extractors**: 85%+ coverage
- **New features**: 100% coverage required

## Troubleshooting

### Tests fail with import errors

Ensure you're in the project root and virtual environment is activated:

```bash
cd /path/to/medallion-foundry
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

### Coverage report shows wrong files

Make sure you're running from project root and coverage source paths are correct.

### Mypy reports errors in dependencies

Add ignore rules to `mypy.ini` for third-party packages without type stubs.

## Best Practices

1. **Run tests before committing**: Use `python run_tests.py --all-checks`
2. **Write tests first**: Follow TDD when adding new features
3. **Keep tests fast**: Mock external services, use fixtures efficiently
4. **One assertion per test**: Makes failures easier to diagnose
5. **Use descriptive names**: Test names should describe what they test
6. **Clean up resources**: Use fixtures and context managers properly

## Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)
- [mypy Documentation](https://mypy.readthedocs.io/)
- [flake8 Documentation](https://flake8.pycqa.org/)
- [black Documentation](https://black.readthedocs.io/)
