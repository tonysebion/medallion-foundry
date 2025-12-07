from __future__ import annotations

from typing import Any, Dict


def validate_v2_config_dict(cfg: Dict[str, Any]) -> None:
    """Extra validation for config_version >= 2 in strict mode.

    Raises ValueError on violations.
    """
    version = int(cfg.get("config_version", 1) or 1)
    if version < 2:
        return

    platform = cfg.get("platform") or {}
    bronze = platform.get("bronze") or {}
    if "local_path" in bronze:
        raise ValueError(
            "platform.bronze.local_path is deprecated in v2; use platform.bronze.output_dir"
        )

    source = cfg.get("source") or {}
    api = source.get("api") or {}
    if source.get("type") == "api":
        if not api.get("base_url"):
            raise ValueError("source.api.base_url is required in v2 for api sources")
        if "endpoint" not in api:
            raise ValueError("source.api.endpoint is required in v2 for api sources")

    # Silver partitioning shape must be a dict with 'columns' list if provided
    silver = cfg.get("silver") or {}
    part = silver.get("partitioning")
    if part is not None:
        if not isinstance(part, dict):
            raise ValueError("silver.partitioning must be a mapping in v2")
        cols = part.get("columns")
        if cols is not None and not isinstance(cols, list):
            raise ValueError("silver.partitioning.columns must be a list in v2")
