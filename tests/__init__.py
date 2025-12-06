"""Bronze-Foundry Test Suite.

Test organization per spec Section 10:
- test_bronze_patterns.py: Bronze ingestion patterns (db_table, db_query, db_multi, file_batch, api)
- test_silver_patterns.py: Silver transformation patterns (single_source, multi_source_join, lookups)
- test_time_series.py: T0/T1/T2 time series scenarios
- test_quality_rules.py: Quality rules engine
- test_polybase_patterns.py: PolyBase DDL generation
- test_idempotency.py: Re-run verification

Synthetic data generators in synthetic_data.py support:
- Claims domain (healthcare)
- Orders domain (retail)
- Transactions domain (financial)
- State change patterns (SCD)
"""
