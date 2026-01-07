# Contributing to Medallion Foundry

Thank you for your interest in contributing to Medallion Foundry! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for all contributors.

## Getting Started

### Prerequisites

- Python 3.9 or higher
- Git
- Virtual environment tool (venv, conda, etc.)

### Setting Up Development Environment

1. **Fork and clone the repository**

   ```bash
   git clone https://github.com/YOUR_USERNAME/medallion-foundry.git
   cd medallion-foundry
   ```

2. **Create a virtual environment**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -e .
   pip install -r requirements-dev.txt
   ```

4. **Install pre-commit hooks**

   ```bash
   pre-commit install
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run unit tests only (fast)
make test-fast

# Run with coverage
make test-cov

# Run specific test file
pytest tests/unit/test_bronze.py -v
```

### Code Quality

```bash
# Run linter
make lint

# Auto-format code
make format

# Fix linting issues automatically
make fix

# Type checking
make type-check
```

### Testing Your Changes

Before submitting a pull request:

1. **Run the full test suite**: `make test`
2. **Run linting**: `make lint`
3. **Run type checking**: `make type-check`
4. **Test with sample data**: `python scripts/generate_bronze_samples.py --all`

## Submitting Changes

### Pull Request Process

1. **Create a feature branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write clear, concise commit messages
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

3. **Run tests and quality checks**

   ```bash
   make all  # Runs install, lint, type-check, and test
   ```

4. **Push your branch and create a PR**

   ```bash
   git push origin feature/your-feature-name
   ```

5. **Fill out the PR template** with:
   - Description of changes
   - Related issue numbers
   - Testing performed
   - Documentation updates

### Commit Message Guidelines

- Use present tense ("Add feature" not "Added feature")
- Use imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit first line to 72 characters
- Reference issues and PRs when relevant

Examples:
```
Add support for PostgreSQL sources

- Implement PostgreSQL connector
- Add connection pooling
- Update documentation

Fixes #123
```

## Coding Standards

### Python Style

- Follow PEP 8 guidelines
- Use type hints for function signatures
- Maximum line length: 100 characters (enforced by ruff)
- Use double quotes for strings
- Use 4 spaces for indentation

### Documentation

- Add docstrings to all public modules, functions, classes, and methods
- Use Google-style docstrings
- Update README.md and docs/ when adding features
- Include examples in docstrings when helpful

Example:
```python
def process_data(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Process DataFrame by selecting and cleaning specified columns.

    Args:
        data: Input DataFrame to process
        columns: List of column names to keep

    Returns:
        Processed DataFrame with selected columns

    Raises:
        ValueError: If any specified column is missing from data

    Example:
        >>> df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        >>> process_data(df, ["a"])
           a
        0  1
        1  2
    """
    # Implementation here
```

### Testing

- Write tests for all new functionality
- Aim for >80% code coverage
- Use descriptive test names: `test_bronze_source_handles_missing_file`
- Use fixtures from `tests/conftest.py` when applicable
- Mark integration tests with `@pytest.mark.integration`

### Module Structure

```
pipelines/
├── lib/          # Core framework (stable, well-tested)
├── examples/     # Reference implementations
├── templates/    # Copy-paste scaffolds
└── tui/          # Text UI components
```

## Areas for Contribution

### High Priority

1. **Additional Source Connectors**
   - PostgreSQL, MySQL enhancements
   - Kafka, Kinesis streaming sources
   - Snowflake, Databricks sources

2. **Storage Backend Enhancements**
   - GCS (Google Cloud Storage) support
   - HDFS support
   - Performance optimizations

3. **Data Quality**
   - Additional validation rules
   - Anomaly detection
   - Data profiling utilities

### Documentation

- Tutorial videos or blog posts
- Architecture diagrams
- Additional examples
- Translation to other languages

### Testing

- Increase test coverage
- Add integration test scenarios
- Performance benchmarks
- Load testing

## Release Process

Maintainers handle releases:

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create release tag: `git tag v1.2.3`
4. Push tag: `git push origin v1.2.3`
5. GitHub Actions builds and publishes to PyPI

## Getting Help

- **Documentation**: Check the [docs/](docs/) directory
- **Issues**: Search [existing issues](https://github.com/tonysebion/medallion-foundry/issues)
- **Discussions**: Start a [GitHub Discussion](https://github.com/tonysebion/medallion-foundry/discussions)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
