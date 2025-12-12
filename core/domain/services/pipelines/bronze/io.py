"""Functions for writing Bronze CSV/Parquet chunks and merging records.

This module houses Bronze-specific helpers that deal with chunk writes,
deterministic merges, and CSV/Parquet wiring required by Bronze patterns.
"""
    if not new_records:
        logger.info("No new records to merge")
        if existing_path.exists():
            return count_existing(existing_path)
        return 0

    new_df = pd.DataFrame.from_records(new_records)
    target = out_path or existing_path

    if not existing_path.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        write_new(new_df, target)
        logger.info("Created new %s with %d records at %s", format_name, len(new_df), target)
        return len(new_df)

    existing_df = read_existing(existing_path)
    merged_df, updated_count, inserted_count = DataFrameMerger.merge_upsert(
        existing_df, new_df, primary_keys
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    write_merged(merged_df, target, updated_count, inserted_count)

    return len(merged_df)


def merge_parquet_records(
    existing_path: Path,
    new_records: List[Dict[str, Any]],
    primary_keys: List[str],
    out_path: Optional[Path] = None,
    compression: str = "snappy",
) -> int:
    """Merge new records with existing Parquet file using primary keys.

    Implements INCREMENTAL_MERGE pattern: upsert semantics where new records
    replace existing records with matching primary keys.

    Args:
        existing_path: Path to existing Parquet file
        new_records: New records to merge
        primary_keys: List of columns that form the primary key
        out_path: Output path (defaults to existing_path, overwriting)
        compression: Parquet compression codec

    Returns:
        Total record count after merge
    """
    def write_new(df: pd.DataFrame, target: Path) -> None:
        df.to_parquet(target, index=False, compression=compression)

    def write_merged(df: pd.DataFrame, target: Path, updated: int, inserted: int) -> None:
        df.to_parquet(target, index=False, compression=compression)
        logger.info(
            "Merged Parquet: %d total records (%d updated, %d inserted) at %s",
            len(df), updated, inserted, target,
        )

    def count_existing(path: Path) -> int:
        return len(pd.read_parquet(path))

    return _merge_records_impl(
        existing_path=existing_path,
        new_records=new_records,
        primary_keys=primary_keys,
        out_path=out_path,
        read_existing=pd.read_parquet,
        write_new=write_new,
        write_merged=write_merged,
        count_existing=count_existing,
        format_name="Parquet",
    )


def merge_csv_records(
    existing_path: Path,
    new_records: List[Dict[str, Any]],
    primary_keys: List[str],
    out_path: Optional[Path] = None,
) -> int:
    """Merge new records with existing CSV file using primary keys.

    Implements INCREMENTAL_MERGE pattern for CSV files.

    Args:
        existing_path: Path to existing CSV file
        new_records: New records to merge
        primary_keys: List of columns that form the primary key
        out_path: Output path (defaults to existing_path, overwriting)

    Returns:
        Total record count after merge
    """
    def write_new(df: pd.DataFrame, target: Path) -> None:
        df.to_csv(target, index=False)

    def write_merged(df: pd.DataFrame, target: Path, updated: int, inserted: int) -> None:
        df.to_csv(target, index=False)
        logger.info("Merged CSV: %d total records at %s", len(df), target)

    def count_existing(path: Path) -> int:
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1  # Subtract header

    return _merge_records_impl(
        existing_path=existing_path,
        new_records=new_records,
        primary_keys=primary_keys,
        out_path=out_path,
        read_existing=pd.read_csv,
        write_new=write_new,
        write_merged=write_merged,
        count_existing=count_existing,
        format_name="CSV",
    )
