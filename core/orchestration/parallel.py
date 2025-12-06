"""Parallel extraction support for running multiple configs concurrently."""

import logging
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.pipeline.runtime.context import RunContext
from core.orchestration.runner import run_extract

logger = logging.getLogger(__name__)


def run_parallel_extracts(
    contexts: List[RunContext], max_workers: int = 4
) -> List[Tuple[str, int, Optional[Exception]]]:
    """
    Run multiple extraction contexts in parallel.

    Args:
        contexts: List of RunContext instances to execute.
        max_workers: Maximum number of parallel workers.

    Returns:
        List of tuples containing (config_name, status_code, error)
        status_code = 0 for success, -1 for failure
    """
    if max_workers <= 0:
        max_workers = 1

    logger.info(
        f"Starting parallel extraction with {max_workers} workers for {len(contexts)} configs"
    )

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all extraction jobs
        future_to_context = {}

        for context in contexts:
            future = executor.submit(_safe_run_extract, context)
            future_to_context[future] = context

        # Collect results as they complete
        for future in as_completed(future_to_context):
            context = future_to_context[future]
            try:
                status_code, error = future.result()
                results.append((context.config_name, status_code, error))

                if status_code == 0:
                    logger.info(
                        f"Successfully completed extraction for {context.config_name}"
                    )
                else:
                    logger.error(
                        f"Failed extraction for {context.config_name}: {error}"
                    )
            except Exception as e:
                logger.error(
                    f"Unexpected error for {context.config_name}: {e}", exc_info=True
                )
                results.append((context.config_name, -1, e))

    # Summary
    successful = sum(1 for _, status, _ in results if status == 0)
    failed = len(results) - successful

    logger.info(
        f"Parallel extraction complete: {successful} successful, {failed} failed out of {len(contexts)} total"
    )

    return results


def _safe_run_extract(context: RunContext) -> Tuple[int, Optional[Exception]]:
    """
    Wrapper for run_extract that catches exceptions and returns status.

    Returns:
        Tuple of (status_code, error)
        status_code = 0 for success, -1 for failure
    """
    try:
        logger.info(f"Starting extraction for {context.config_name}")
        status = run_extract(context)
        return (status, None)
    except Exception as e:
        logger.error(f"Extraction failed for {context.config_name}: {e}", exc_info=True)
        return (-1, e)
