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
- Windows: Install Microsoft ODBC Driver for SQL Server (e.g., ODBC Driver 18)
- Linux: Install unixodbc and appropriate database drivers
- Mac: Use homebrew to install unixodbc

### MSSQL on Windows (pyodbc)

1. Install the Microsoft ODBC Driver for SQL Server:
	 https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server

2. Use a connection string referencing the installed driver, for example:

	 `Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver.database.windows.net,1433;Database=mydb;Uid=myuser;Pwd={${ENV_PASSWORD}};Encrypt=yes;TrustServerCertificate=no;`

3. Provide the connection string via environment variable referenced by your config (`source.db.conn_str_env`). Example YAML:

```
source:
	type: db
	system: my_mssql
	table: customers
	db:
		driver: pyodbc  # default
		conn_str_env: MSSQL_CONN_STR
		base_query: |
			SELECT * FROM dbo.Customers
		incremental:
			enabled: true
			cursor_column: updated_at
```

Then set the variable before running:

```powershell
$env:MSSQL_CONN_STR = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:...;Database=...;Uid=...;Pwd=...;Encrypt=yes;TrustServerCertificate=no;"
```

Note: `db.driver: pymssql` is recognized but not yet implemented at runtime; prefer `pyodbc`.

### Permission Errors

Ensure the script has write permissions to the output directory and .state directory.
