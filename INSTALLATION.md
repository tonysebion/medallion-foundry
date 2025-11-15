# Installation Guide

## Prerequisites

- Python 3.8 or higher
- pip
- Virtual environment (recommended)

## Installation Steps

### Clone Repository

```bash
git clone https://github.com/medallion-foundry/medallion-foundry.git
cd medallion-foundry
```

### 2. Create a Virtual Environment

**On Windows:**
```powershell
python -m venv .venv
.venv\Scripts\activate
```

**On Linux/Mac:**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Install the Package (Optional)

For development mode:
```bash
pip install -e .
```

For production:
```bash
pip install .
```

## Running Tests

Install test dependencies:
```bash
pip install pytest pytest-cov
```

Run tests:
```bash
pytest tests/
```

Run with coverage:
```bash
pytest --cov=core --cov=extractors tests/
```

## Configuration

1. Copy an example config:
```bash
cp docs/examples/configs/api_example.yaml config/my_source.yaml
```

2. Edit the config file with your source details

3. Set required environment variables (e.g., API tokens, DB connection strings)

## Running an Extraction

```bash
python bronze_extract.py --config config/my_source.yaml --date 2025-11-12
```

## Troubleshooting

### Import Errors

Make sure you've installed the package or are running from the project root directory.

### Missing Dependencies

Install pyodbc system dependencies (for database extraction):
- Windows: Install Microsoft ODBC Driver for SQL Server
- Linux: Install unixodbc and appropriate database drivers
- Mac: Use homebrew to install unixodbc

### Permission Errors

Ensure the script has write permissions to the output directory and .state directory.
