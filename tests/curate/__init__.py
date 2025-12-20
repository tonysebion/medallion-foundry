"""Tests for curate.py transformation functions.

These tests validate the low-level Ibis operations used for Silver layer curation:
- dedupe_latest: SCD1 - keep latest record per key
- dedupe_earliest: Keep first record per key
- build_history: SCD2 - build effective date ranges
- dedupe_exact: Remove exact duplicate rows
- filter_incremental: Filter by watermark
- rank_by_keys: Add rank column by partition
- coalesce_columns: Handle schema evolution
- union_dedupe: Union and deduplicate tables
"""
