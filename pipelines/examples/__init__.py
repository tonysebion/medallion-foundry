"""Example pipelines for testing and demonstration.

YAML examples (recommended for most use cases):
- retail_orders.yaml: Basic CSV to Silver pipeline
- customer_scd2.yaml: SCD Type 2 history tracking
- mssql_incremental.yaml: Database incremental load
- github_api.yaml: REST API with bearer auth
- api_with_cursor.yaml: API with cursor pagination
- file_to_silver.yaml: Parquet files with SCD2
- resilient_api.yaml: API extraction basics
- multi_source_parallel.yaml: Single source extraction

Python examples (for advanced features):
- multi_source_parallel.py: Parallel processing with concurrent.futures
- resilient_api.py: Retry decorators and circuit breakers
- file_to_silver.py: Data quality validation with check_quality()

Use Python examples when you need features not available in YAML.
"""
