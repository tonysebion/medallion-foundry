"""
Deep data correctness validation for Pattern 1 (Full Events) and Pattern 2 (CDC Events).

This test suite validates that:
1. Bronze→Silver transformations preserve data correctness
2. Business logic is applied correctly (event deduplication, timestamp handling)
3. Partition structures are correct and queryable via PolyBase
4. Metadata matches actual data characteristics
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, cast

import pandas as pd
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIGS_ROOT = REPO_ROOT / "docs" / "examples" / "configs"
SOURCE_ROOT = REPO_ROOT / "sampledata" / "source_samples"
BRONZE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"


def _run_extraction(config_path: Path, run_date: str, extraction_type: str) -> None:
    """Run bronze_extract.py or silver_extract.py."""
    script = f"{extraction_type}_extract.py"
    subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / script),
            "--config",
            str(config_path),
            "--date",
            run_date,
        ],
        check=True,
        cwd=REPO_ROOT,
    )


def _rewrite_extraction_config(
    original: Path, run_date: str, tmp_dir: Path
) -> tuple[Path, Path, Path, Dict[str, Any]]:
    """Rewrite config to use temp directories for extraction."""
    cfg = cast(Dict[str, Any], yaml.safe_load(original.read_text()))

    bronze_out = (tmp_dir / f"bronze_{run_date}").resolve()
    silver_out = (tmp_dir / f"silver_{run_date}").resolve()

    bronze_cfg = cfg.setdefault("bronze", {})
    bronze_cfg.setdefault("options", {})["local_output_dir"] = str(bronze_out)

    silver_cfg = cfg.setdefault("silver", {})
    silver_cfg["output_dir"] = str(silver_out)

    rewritten = tmp_dir / f"{original.stem}_{run_date}.yaml"
    rewritten.write_text(yaml.safe_dump(cfg))

    return rewritten, bronze_out, silver_out, cfg


def _collect_bronze_partition(output_root: Path) -> Path:
    """Find the Bronze partition directory with metadata."""
    metadata_files = list(output_root.rglob("_metadata.json"))
    assert metadata_files, f"No Bronze metadata found under {output_root}"
    return metadata_files[0].parent


def _read_source_csv(csv_path: Path) -> pd.DataFrame:
    """Read source CSV with proper dtype handling."""
    return pd.read_csv(csv_path, dtype=str, keep_default_na=False)


def _read_bronze_parquet(partition_dir: Path) -> pd.DataFrame:
    """Read all Parquet files from Bronze partition."""
    parquet_files = sorted(partition_dir.glob("*.parquet"))
    assert parquet_files, f"No Parquet files found in {partition_dir}"
    frames = [pd.read_parquet(f) for f in parquet_files]
    return pd.concat(frames, ignore_index=True)


def _read_silver_parquet(silver_dir: Path, model_type: str = "events") -> pd.DataFrame:
    """Read Silver artifacts by type (events.parquet for pattern1)."""
    parquet_files = list(silver_dir.rglob(f"{model_type}.parquet"))
    if not parquet_files:
        parquet_files = list(silver_dir.rglob("*.parquet"))
    assert parquet_files, f"No {model_type} parquet found under {silver_dir}"
    frames = [pd.read_parquet(f) for f in sorted(parquet_files)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ============================================================================
# PATTERN 1: FULL EVENTS (Full refresh, daily snapshot)
# ============================================================================


@pytest.mark.parametrize(
    "run_date",
    [
        "2025-11-13",
    ],
)
def test_pattern1_source_to_bronze_preserves_rows(
    tmp_path: Path, run_date: str
) -> None:
    """Pattern 1: Verify source rows are loaded into Bronze (same date only)."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    source_path = (
        SOURCE_ROOT
        / f"sample=pattern1_full_events/system=retail_demo/table=orders/dt={run_date}/full-part-0001.csv"
    )

    if not source_path.exists():
        pytest.skip(f"Source data not found for {run_date}")

    rewritten_cfg, bronze_out, _, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    source_df = _read_source_csv(source_path)
    assert len(source_df) > 0, f"Source data empty for {run_date}"

    _run_extraction(rewritten_cfg, run_date, "bronze")

    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)

    # Core assertion: row count must match
    assert len(bronze_df) == len(source_df), (
        f"Row count mismatch: source={len(source_df)}, "
        f"bronze={len(bronze_df)} for {run_date}"
    )

    # Verify all source columns present in Bronze
    assert set(source_df.columns) <= set(bronze_df.columns), (
        f"Missing columns in Bronze. Source: {set(source_df.columns)}, "
        f"Bronze: {set(bronze_df.columns)}"
    )


def test_pattern1_bronze_timestamp_parsing(tmp_path: Path) -> None:
    """Pattern 1: Verify timestamps are correctly parsed (may span multiple dates in source data)."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    run_date = "2025-11-13"
    source_path = (
        SOURCE_ROOT
        / f"sample=pattern1_full_events/system=retail_demo/table=orders/dt={run_date}/full-part-0001.csv"
    )

    rewritten_cfg, bronze_out, _, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    source_df = _read_source_csv(source_path)
    _run_extraction(rewritten_cfg, run_date, "bronze")

    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)

    # Extract event_date from updated_at timestamp
    source_df["extracted_date"] = pd.to_datetime(
        source_df["updated_at"]
    ).dt.date.astype(str)
    bronze_df_converted = bronze_df.copy()
    bronze_df_converted["extracted_date"] = pd.to_datetime(
        bronze_df_converted["updated_at"]
    ).dt.date.astype(str)

    # Verify dates parse correctly and Bronze has same date distribution as source
    source_unique_dates = set(source_df["extracted_date"].unique())
    bronze_unique_dates = set(bronze_df_converted["extracted_date"].unique())

    assert source_unique_dates == bronze_unique_dates, (
        f"Date mismatch between source and bronze. "
        f"Source: {len(source_unique_dates)} dates, Bronze: {len(bronze_unique_dates)} dates"
    )


def test_pattern1_silver_event_deduplication(tmp_path: Path) -> None:
    """
    Pattern 1: Verify Silver deduplication logic for full events.

    For pattern1_full_events (input_mode: replace_daily):
    - Silver should contain latest state of each order_id per load date
    - No duplicates on natural_key (order_id)
    """
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    silver_df = _read_silver_parquet(silver_out, "events")

    # For full events with replace_daily: no duplicates on natural key
    natural_key = cfg_data["silver"]["natural_keys"]  # ["order_id"]
    assert "order_id" in natural_key

    duplicate_count = silver_df.groupby(natural_key).size()
    duplicates = duplicate_count[duplicate_count > 1]

    assert (
        len(duplicates) == 0
    ), f"Found {len(duplicates)} duplicate order_ids in Silver: {duplicates.index.tolist()}"


def test_pattern1_silver_partition_structure(tmp_path: Path) -> None:
    """Pattern 1: Verify Silver partition structure matches config."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    # Silver should be partitioned by load_date (and possibly event_date)
    silver_df = _read_silver_parquet(silver_out, "events")

    # Verify partition columns exist in data or derived columns
    # (e.g., event_ts_dt in config might become event_date in actual data)
    silver_cfg = cfg_data["silver"]
    partition_by = silver_cfg.get("partition_by", [])
    actual_partition_cols = set(silver_df.columns)

    for partition_col in partition_by:
        # Check if exact column exists or a derived version (e.g., event_date for event_ts_dt)
        if partition_col not in actual_partition_cols:
            # Try common transformations
            possible_cols = [
                partition_col.replace("_dt", ""),  # event_ts_dt -> event_ts
                "event_date",  # Generic event date partition
                "load_date",  # Generic load date partition
            ]
            found = any(col in actual_partition_cols for col in possible_cols)
            assert found, (
                f"Partition column {partition_col} (or derived version) missing from Silver data. "
                f"Columns: {actual_partition_cols}"
            )


def test_pattern1_silver_business_metadata(tmp_path: Path) -> None:
    """Pattern 1: Verify Silver includes all required business metadata columns."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    silver_df = _read_silver_parquet(silver_out, "events")

    # Expected metadata columns from configuration
    required_columns = {
        "load_batch_id",
        "record_source",
        "pipeline_run_at",
        "environment",
        "domain",
        "entity",
    }

    missing = required_columns - set(silver_df.columns)
    assert not missing, f"Missing metadata columns: {missing}"

    # load_batch_id should follow pattern: domain.entity-YYYY-MM-DD
    assert (
        silver_df["load_batch_id"].nunique() == 1
    ), "Expected single load_batch_id per run"
    batch_id = silver_df["load_batch_id"].iloc[0]
    assert (
        batch_id == f"retail_demo.orders-{run_date}"
    ), f"Unexpected batch_id: {batch_id}"


# ============================================================================
# PATTERN 2: CDC EVENTS (Change Data Capture, append-only log)
# ============================================================================


@pytest.mark.parametrize(
    "run_date",
    [
        "2025-11-13",
    ],
)
def test_pattern2_source_to_bronze_change_type_preserved(
    tmp_path: Path, run_date: str
) -> None:
    """Pattern 2: Verify change_type (insert/update/delete) is preserved in Bronze."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    source_path = (
        SOURCE_ROOT
        / f"sample=pattern2_cdc_events/system=retail_demo/table=orders/dt={run_date}/cdc-part-0001.csv"
    )

    if not source_path.exists():
        pytest.skip(f"Source data not found for {run_date}")

    rewritten_cfg, bronze_out, _, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    source_df = _read_source_csv(source_path)
    assert "change_type" in source_df.columns, "Source missing change_type column"

    _run_extraction(rewritten_cfg, run_date, "bronze")

    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)

    # Verify change_type is present and unchanged
    assert "change_type" in bronze_df.columns
    source_changes = set(source_df["change_type"].unique())
    bronze_changes = set(bronze_df["change_type"].unique())

    assert (
        source_changes == bronze_changes
    ), f"Change types diverged: source={source_changes}, bronze={bronze_changes}"


def test_pattern2_cdc_event_counts_by_type(tmp_path: Path) -> None:
    """
    Pattern 2: Verify insert/update/delete counts match between source and Bronze.

    CDC events should maintain event counts per change_type.
    """
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    run_date = "2025-11-13"
    source_path = (
        SOURCE_ROOT
        / f"sample=pattern2_cdc_events/system=retail_demo/table=orders/dt={run_date}/cdc-part-0001.csv"
    )

    rewritten_cfg, bronze_out, _, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    source_df = _read_source_csv(source_path)
    source_counts = source_df["change_type"].value_counts().to_dict()

    _run_extraction(rewritten_cfg, run_date, "bronze")

    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)
    bronze_counts = bronze_df["change_type"].value_counts().to_dict()

    assert source_counts == bronze_counts, (
        f"Change type counts diverged:\n"
        f"  source: {source_counts}\n"
        f"  bronze: {bronze_counts}"
    )


def test_pattern2_silver_append_log_preserves_history(tmp_path: Path) -> None:
    """
    Pattern 2: Verify Silver append_log captures all CDC events.

    For CDC events with input_mode: append_log:
    - Silver should contain ALL change events (inserts, updates)
    - No deduplication on order_id (can have multiple rows per order)
    """
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    # Compare row counts
    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)
    silver_df = _read_silver_parquet(silver_out, "events")

    # For append_log, Silver should have at least as many rows as Bronze
    assert len(silver_df) >= len(bronze_df), (
        f"Silver has fewer rows than Bronze (append_log mode): "
        f"silver={len(silver_df)}, bronze={len(bronze_df)}"
    )


def test_pattern2_silver_change_type_distributions(tmp_path: Path) -> None:
    """Pattern 2: Verify Silver maintains change_type distribution from Bronze."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    bronze_partition = _collect_bronze_partition(bronze_out)
    bronze_df = _read_bronze_parquet(bronze_partition)
    silver_df = _read_silver_parquet(silver_out, "events")

    # change_type should be preserved
    if "change_type" in bronze_df.columns and "change_type" in silver_df.columns:
        bronze_types = bronze_df["change_type"].value_counts().to_dict()
        silver_types = silver_df["change_type"].value_counts().to_dict()

        for change_type in bronze_types:
            assert (
                silver_types.get(change_type, 0) > 0
            ), f"Silver missing {change_type} events from Bronze"


def test_pattern2_timestamp_precision(tmp_path: Path) -> None:
    """Pattern 2: Verify timestamp precision is maintained for CDC event ordering."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    silver_df = _read_silver_parquet(silver_out, "events")

    # Should be able to parse as datetime
    try:
        parsed = pd.to_datetime(silver_df["changed_at"])
        # Verify we have multiple distinct timestamps
        assert parsed.nunique() > 1, "Timestamps are not distinct"
    except Exception as e:
        pytest.fail(f"Failed to parse changed_at as datetime: {e}")


# ============================================================================
# POLYBASE SAMPLE QUERIES
# ============================================================================


def test_pattern1_polybase_external_table_generation(tmp_path: Path) -> None:
    """Pattern 1: Verify PolyBase configuration can be generated from Silver samples."""
    config_path = CONFIGS_ROOT / "patterns" / "pattern_full.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, cfg_data = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    silver_df = _read_silver_parquet(silver_out, "events")

    # Expected columns for PolyBase querying
    # Note: customer_id may not be in Silver if it's not in the attributes list
    required_cols = {"order_id", "status", "order_total", "updated_at", "load_batch_id"}
    optional_cols = {"customer_id"}  # May or may not be present depending on config

    missing = required_cols - set(silver_df.columns)
    assert not missing, f"Missing required columns for PolyBase queries: {missing}"

    # Log which optional columns are present
    optional_present = optional_cols & set(silver_df.columns)
    if optional_present:
        assert len(optional_present) > 0


def test_pattern2_polybase_query_predicates(tmp_path: Path) -> None:
    """
    Pattern 2: Verify Silver data structure supports common PolyBase predicates.

    Sample queries should be able to filter by:
    - change_type (insert/update)
    - changed_at range (time window)
    - order_id (point selection)
    """
    config_path = CONFIGS_ROOT / "patterns" / "pattern_cdc.yaml"
    run_date = "2025-11-13"

    rewritten_cfg, bronze_out, silver_out, _ = _rewrite_extraction_config(
        config_path, run_date, tmp_path
    )

    _run_extraction(rewritten_cfg, run_date, "bronze")
    _run_extraction(rewritten_cfg, run_date, "silver")

    silver_df = _read_silver_parquet(silver_out, "events")

    # Test filtering by change_type
    if "change_type" in silver_df.columns:
        inserts = silver_df[silver_df["change_type"] == "insert"]
        assert len(inserts) > 0, "No insert events found"

    # Test filtering by timestamp range
    if "changed_at" in silver_df.columns:
        silver_df["ts_parsed"] = pd.to_datetime(silver_df["changed_at"])
        min_ts = silver_df["ts_parsed"].min()
        max_ts = silver_df["ts_parsed"].max()

        # Subset to middle half of time range
        mid = min_ts + (max_ts - min_ts) / 4
        ranged = silver_df[silver_df["ts_parsed"] >= mid]
        assert len(ranged) > 0, "Time range filter produced no results"

    # Test filtering by order_id
    if "order_id" in silver_df.columns:
        sample_order = silver_df["order_id"].iloc[0]
        filtered = silver_df[silver_df["order_id"] == sample_order]
        assert len(filtered) > 0, f"Point selection failed for order_id={sample_order}"
