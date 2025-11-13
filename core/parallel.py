"""Parallel extraction support for running multiple configs concurrently."""

import logging
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.runner import run_extract
from core.config import build_relative_path

logger = logging.getLogger(__name__)


def run_parallel_extracts(
    configs: List[Dict[str, Any]],
    run_date: date,
    local_output_base: Path,
    max_workers: int = 4
) -> List[Tuple[str, int, Optional[Exception]]]:
    """
    Run multiple extraction configs in parallel.
    
    Args:
        configs: List of configuration dictionaries
        run_date: Date for the extraction run
        local_output_base: Base directory for local output
        max_workers: Maximum number of parallel workers
        
    Returns:
        List of tuples containing (config_name, status_code, error)
        status_code = 0 for success, -1 for failure
    """
    if max_workers <= 0:
        max_workers = 1
    
    logger.info(f"Starting parallel extraction with {max_workers} workers for {len(configs)} configs")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all extraction jobs
        future_to_config = {}
        
        for cfg in configs:
            config_name = f"{cfg['source']['system']}.{cfg['source']['table']}"
            relative_path = build_relative_path(cfg, run_date)
            
            future = executor.submit(
                _safe_run_extract,
                cfg,
                run_date,
                local_output_base,
                relative_path,
                config_name
            )
            future_to_config[future] = config_name
        
        # Collect results as they complete
        for future in as_completed(future_to_config):
            config_name = future_to_config[future]
            try:
                status_code, error = future.result()
                results.append((config_name, status_code, error))
                
                if status_code == 0:
                    logger.info(f"✓ Successfully completed extraction for {config_name}")
                else:
                    logger.error(f"✗ Failed extraction for {config_name}: {error}")
                    
            except Exception as e:
                logger.error(f"✗ Unexpected error for {config_name}: {e}", exc_info=True)
                results.append((config_name, -1, e))
    
    # Summary
    successful = sum(1 for _, status, _ in results if status == 0)
    failed = len(results) - successful
    
    logger.info(f"Parallel extraction complete: {successful} successful, {failed} failed out of {len(configs)} total")
    
    return results


def _safe_run_extract(
    cfg: Dict[str, Any],
    run_date: date,
    local_output_base: Path,
    relative_path: str,
    config_name: str
) -> Tuple[int, Optional[Exception]]:
    """
    Wrapper for run_extract that catches exceptions and returns status.
    
    Returns:
        Tuple of (status_code, error)
        status_code = 0 for success, -1 for failure
    """
    try:
        logger.info(f"Starting extraction for {config_name}")
        status = run_extract(cfg, run_date, local_output_base, relative_path)
        return (status, None)
    except Exception as e:
        logger.error(f"Extraction failed for {config_name}: {e}", exc_info=True)
        return (-1, e)
