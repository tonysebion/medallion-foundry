from __future__ import annotations

from pathlib import Path
from datetime import date, datetime

from core.patterns import LoadPattern
from core.partitioning import build_bronze_partition


def build_bronze_relative_path(cfg: dict, run_date: date) -> str:
    platform_cfg = cfg["platform"]
    source_cfg = cfg["source"]

    bronze = platform_cfg["bronze"]
    partitioning = bronze.get("partitioning", {})
    use_dt = partitioning.get("use_dt_partition", True)

    partition_strategy = partitioning.get("partition_strategy", "date")

    system = source_cfg["system"]
    table = source_cfg["table"]

    base_path = f"system={system}/table={table}/"

    run_cfg = source_cfg.get("run", {})
    load_pattern = run_cfg.get("load_pattern", LoadPattern.FULL.value)
    if load_pattern:
        base_path += f"pattern={load_pattern}/"

    if not use_dt:
        return base_path

    if partition_strategy == "date":
        partition = build_bronze_partition(cfg, run_date)
        return partition.relative_path().as_posix() + "/"
    elif partition_strategy == "hourly":
        current_hour = datetime.now().strftime("%H")
        return f"{base_path}dt={run_date.isoformat()}/hour={current_hour}/"
    elif partition_strategy == "timestamp":
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        return f"{base_path}dt={run_date.isoformat()}/batch={timestamp}/"
    elif partition_strategy == "batch_id":
        from datetime import datetime as dt
        import uuid

        batch_id = (
            run_cfg.get("batch_id")
            or f"{dt.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        )
        return f"{base_path}dt={run_date.isoformat()}/batch_id={batch_id}/"
    else:
        return f"{base_path}dt={run_date.isoformat()}/"


def build_silver_partition_path(
    silver_base: Path,
    domain: str,
    entity: str,
    version: int,
    load_partition_name: str,
    include_pattern_folder: bool,
    load_pattern: LoadPattern,
    run_date: date,
) -> Path:
    path = silver_base / f"domain={domain}" / f"entity={entity}" / f"v{version}"
    if include_pattern_folder:
        path = path / f"pattern={load_pattern.value}"
    path = path / f"{load_partition_name}={run_date.isoformat()}"
    return path
