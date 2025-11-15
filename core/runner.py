"""Core runner that wires config, extractor, IO, and storage together."""

import logging
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from extractors.base import BaseExtractor
from extractors.api_extractor import ApiExtractor
from extractors.db_extractor import DbExtractor
from extractors.file_extractor import FileExtractor

from core.io import (
    chunk_records,
    write_csv_chunk,
    write_parquet_chunk,
    write_batch_metadata,
    write_checksum_manifest,
)
from core.storage import StorageBackend, get_storage_backend
from core.patterns import LoadPattern

logger = logging.getLogger(__name__)


def build_extractor(cfg: Dict[str, Any]) -> BaseExtractor:
    src = cfg["source"]
    src_type = src.get("type", "api")

    if src_type == "api":
        return ApiExtractor()
    elif src_type == "db":
        return DbExtractor()
    elif src_type == "custom":
        custom_cfg = src.get("custom_extractor", {})
        module_name = custom_cfg["module"]
        class_name = custom_cfg["class_name"]

        import importlib

        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls()
    elif src_type == "file":
        return FileExtractor()
    else:
        raise ValueError(f"Unknown source.type: {src_type}")


def _process_chunk(
    chunk_index: int,
    chunk: List[Dict[str, Any]],
    out_dir: Path,
    write_csv: bool,
    write_parquet: bool,
    parquet_compression: str,
    storage_enabled: bool,
    storage_backend: Optional[StorageBackend],
    relative_path: str,
    chunk_prefix: str,
) -> Tuple[int, List[Path], Optional[Exception]]:
    """
    Process a single chunk: write to disk and optionally upload to storage.
    
    Args:
        chunk_index: Index of this chunk
        chunk: List of records to write
        out_dir: Local output directory
        write_csv: Whether to write CSV format
        write_parquet: Whether to write Parquet format
        parquet_compression: Parquet compression algorithm
        storage_enabled: Whether to upload files to remote storage
        storage_backend: Storage backend instance (if enabled)
        relative_path: Relative path for uploaded files
    
    Returns:
        Tuple of (chunk_index, created_files, error)
        error is None on success
    """
    created_files = []
    try:
        suffix = f"{chunk_prefix}-part-{chunk_index:04d}"
        
        # Write CSV
        if write_csv:
            csv_path = out_dir / f"{suffix}.csv"
            write_csv_chunk(chunk, csv_path)
            created_files.append(csv_path)
            
            if storage_enabled and storage_backend:
                remote_path = f"{relative_path}{csv_path.name}"
                storage_backend.upload_file(str(csv_path), remote_path)
        
        # Write Parquet
        if write_parquet:
            parquet_path = out_dir / f"{suffix}.parquet"
            write_parquet_chunk(chunk, parquet_path, compression=parquet_compression)
            created_files.append(parquet_path)
            
            if storage_enabled and storage_backend:
                remote_path = f"{relative_path}{parquet_path.name}"
                storage_backend.upload_file(str(parquet_path), remote_path)
        
        logger.debug(f"Completed processing chunk {chunk_index}")
        return (chunk_index, created_files, None)
        
    except Exception as e:
        logger.error(f"Failed to process chunk {chunk_index}: {e}")
        return (chunk_index, created_files, e)


def run_extract(
    cfg: Dict[str, Any],
    run_date: date,
    local_output_base: Path,
    relative_path: str,
) -> int:
    """Run extraction with error handling and cleanup."""
    platform_cfg = cfg["platform"]
    source_cfg = cfg["source"]

    extractor = build_extractor(cfg)

    logger.info(f"Starting extract for {source_cfg['system']}.{source_cfg['table']} on {run_date}")

    # Track created files for cleanup on failure
    created_files = []
    
    try:
        # Fetch records from source
        records, new_cursor = extractor.fetch_records(cfg, run_date)
        logger.info(f"Retrieved {len(records)} records from extractor")

        if not records:
            logger.warning("No records returned from extractor")
            return 0

        # Prepare output settings
        run_cfg = source_cfg["run"]
        load_pattern = LoadPattern.normalize(run_cfg.get("load_pattern"))
        chunk_prefix = load_pattern.chunk_prefix
        logger.info(f"Load pattern: {load_pattern.value} ({load_pattern.describe()})")
        max_rows_per_file = int(run_cfg.get("max_rows_per_file", 0))
        max_file_size_mb = run_cfg.get("max_file_size_mb")  # Can be None

        chunks = chunk_records(records, max_rows_per_file, max_file_size_mb)

        bronze_output = platform_cfg["bronze"]["output_defaults"]
        write_csv = run_cfg.get("write_csv", True) and bronze_output.get("allow_csv", True)
        write_parquet = run_cfg.get("write_parquet", True) and bronze_output.get("allow_parquet", True)
        parquet_compression = bronze_output.get("parquet_compression", "snappy")

        # Initialize storage backend (supports s3_enabled for backward compatibility)
        storage_enabled = run_cfg.get("storage_enabled", run_cfg.get("s3_enabled", False))
        storage_backend = None
        
        if storage_enabled:
            storage_backend = get_storage_backend(platform_cfg)
            logger.info(f"Initialized {storage_backend.get_backend_type()} storage backend")

        # Create output directory
        out_dir = local_output_base / relative_path
        out_dir.mkdir(parents=True, exist_ok=True)

        # Get parallel workers setting
        parallel_workers = int(run_cfg.get("parallel_workers", 1))
        
        # Process chunks (parallel or sequential based on config)
        if parallel_workers > 1 and len(chunks) > 1:
            logger.info(f"Processing {len(chunks)} chunks with {parallel_workers} workers")
            
            # Parallel processing
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = {}
                
                for chunk_index, chunk in enumerate(chunks, start=1):
                    future = executor.submit(
                        _process_chunk,
                        chunk_index,
                        chunk,
                        out_dir,
                        write_csv,
                        write_parquet,
                        parquet_compression,
                        storage_enabled,
                        storage_backend,
                        relative_path,
                        chunk_prefix,
                    )
                    futures[future] = chunk_index
                
                # Collect results
                for future in as_completed(futures):
                    chunk_index, chunk_files, error = future.result()
                    
                    if error:
                        logger.error(f"Chunk {chunk_index} failed: {error}")
                        raise error
                    
                    created_files.extend(chunk_files)
                    logger.debug(f"Chunk {chunk_index} completed successfully")
        else:
            # Sequential processing (original logic)
            logger.info(f"Processing {len(chunks)} chunks sequentially")
            part_index = 1
            for chunk in chunks:
                chunk_index, chunk_files, error = _process_chunk(
                    part_index,
                    chunk,
                    out_dir,
                    write_csv,
                    write_parquet,
                    parquet_compression,
                    storage_enabled,
                    storage_backend,
                    relative_path,
                    chunk_prefix,
                )
                
                if error:
                    raise error
                
                created_files.extend(chunk_files)
                part_index += 1

        # Write metadata with extra context (load pattern, partition info, etc.)
        metadata_path = write_batch_metadata(
            out_dir,
            record_count=len(records),
            chunk_count=len(chunks),
            cursor=new_cursor,
            extra_metadata={
                "batch_timestamp": datetime.now().isoformat(),
                "run_date": run_date.isoformat(),
                "system": source_cfg["system"],
                "table": source_cfg["table"],
                "partition_path": relative_path,
                "file_formats": {"csv": write_csv, "parquet": write_parquet},
                "load_pattern": load_pattern.value,
            },
        )
        created_files.append(metadata_path)

        checksum_metadata = {
            "system": source_cfg["system"],
            "table": source_cfg["table"],
            "run_date": run_date.isoformat(),
            "config_name": source_cfg.get("config_name"),
        }
        checksum_path = write_checksum_manifest(out_dir, created_files, load_pattern.value, checksum_metadata)
        
        if storage_enabled and storage_backend:
            remote_path = f"{relative_path}{metadata_path.name}"
            storage_backend.upload_file(str(metadata_path), remote_path)

        # Handle cursor state
        if new_cursor:
            logger.info(f"Extractor returned new_cursor={new_cursor!r}")

        logger.info("Finished Bronze extract run successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Extract failed: {e}", exc_info=True)
        
        # Cleanup created files on failure
        cleanup_on_failure = source_cfg.get("run", {}).get("cleanup_on_failure", True)
        if cleanup_on_failure and created_files:
            logger.info(f"Cleaning up {len(created_files)} files due to failure")
            for file_path in created_files:
                try:
                    if file_path.exists():
                        file_path.unlink()
                        logger.debug(f"Deleted {file_path}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup {file_path}: {cleanup_error}")
        
        # Re-raise the original exception
        raise
