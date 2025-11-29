from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from core.config import build_relative_path
from core.config.typed_models import RootConfig
from core.config.environment import EnvironmentConfig
from core.patterns import LoadPattern

logger = logging.getLogger(__name__)


@dataclass
class RunContext:
    cfg: Dict[str, Any]
    run_date: date
    relative_path: str
    local_output_dir: Path
    bronze_path: Path
    source_system: str
    source_table: str
    dataset_id: str
    config_name: str
    load_pattern: LoadPattern
    env_config: Optional[EnvironmentConfig]

    @property
    def bronze_dir(self) -> Path:
        return self.bronze_path

    @property
    def bronze_output_base(self) -> Path:
        return self.local_output_dir


def build_run_context(
    cfg: Dict[str, Any] | RootConfig,
    run_date: date,
    local_output_override: Path | None = None,
    relative_override: str | None = None,
    load_pattern_override: str | None = None,
    bronze_path_override: Path | None = None,
    env_config: Optional[EnvironmentConfig] = None,
) -> RunContext:
    typed: RootConfig | None
    if isinstance(cfg, RootConfig):
        typed = cfg
        run_cfg = typed.source.run.model_dump()  # dict for compatibility
        cfg_dict = typed.model_dump()
    else:
        typed = None
        cfg_dict = cfg
        run_cfg = cfg_dict["source"].get("run", {})

    if "storage_enabled" not in run_cfg:
        bronze_backend = cfg_dict.get("platform", {}).get("bronze", {}).get("storage_backend")
        run_cfg["storage_enabled"] = str(bronze_backend).lower() == "s3"
        cfg_dict["source"]["run"]["storage_enabled"] = run_cfg["storage_enabled"]

    local_output_dir = Path(
        local_output_override or run_cfg.get("local_output_dir", "./output")
    ).resolve()
    relative_path = relative_override or build_relative_path(cfg_dict, run_date)
    bronze_path = (
        Path(bronze_path_override).resolve()
        if bronze_path_override
        else (local_output_dir / relative_path).resolve()
    )

    if typed:
        source_system = typed.source.system
        source_table = typed.source.table
    else:
        source_system = cfg_dict["source"]["system"]
        source_table = cfg_dict["source"]["table"]
    dataset_id = f"{source_system}.{source_table}"
    if typed:
        config_name = cfg_dict["source"].get("config_name", dataset_id)
    else:
        config_name = cfg_dict["source"].get("config_name", dataset_id)

    pattern_value = load_pattern_override or run_cfg.get("load_pattern")
    load_pattern = (
        LoadPattern.normalize(pattern_value) if pattern_value else LoadPattern.FULL
    )

    logger.debug(
        "Built RunContext(dataset_id=%s, run_date=%s, relative_path=%s)",
        dataset_id,
        run_date,
        relative_path,
    )

    # Return RunContext with a plain dict config so downstream runners can rely on
    # standard dict operations regardless of how the input was provided.
    return RunContext(
        cfg=cfg_dict,
        run_date=run_date,
        relative_path=relative_path,
        local_output_dir=local_output_dir,
        bronze_path=bronze_path,
        source_system=source_system,
        source_table=source_table,
        dataset_id=dataset_id,
        config_name=config_name,
        load_pattern=load_pattern,
        env_config=env_config,
    )


def run_context_to_dict(ctx: RunContext) -> Dict[str, Any]:
    return {
        "cfg": ctx.cfg,
        "run_date": ctx.run_date.isoformat(),
        "relative_path": ctx.relative_path,
        "local_output_dir": str(ctx.local_output_dir),
        "bronze_path": str(ctx.bronze_path),
        "source_system": ctx.source_system,
        "source_table": ctx.source_table,
        "dataset_id": ctx.dataset_id,
        "config_name": ctx.config_name,
        "load_pattern": ctx.load_pattern.value,
    }


def run_context_from_dict(payload: Dict[str, Any]) -> RunContext:
    cfg = payload["cfg"]
    run_date = date.fromisoformat(payload["run_date"])
    load_pattern = LoadPattern.normalize(payload.get("load_pattern"))
    return RunContext(
        cfg=cfg,
        run_date=run_date,
        relative_path=payload["relative_path"],
        local_output_dir=Path(payload["local_output_dir"]),
        bronze_path=Path(payload["bronze_path"]),
        source_system=payload["source_system"],
        source_table=payload["source_table"],
        dataset_id=payload["dataset_id"],
        config_name=payload.get("config_name", payload["dataset_id"]),
        load_pattern=load_pattern,
    )


def load_run_context(path: str | Path) -> RunContext:
    ctx_path = Path(path)
    payload = json.loads(ctx_path.read_text(encoding="utf-8"))
    context = run_context_from_dict(payload)
    logger.info(
        "Loaded RunContext for %s (run_date=%s) from %s",
        context.dataset_id,
        context.run_date,
        ctx_path,
    )
    return context


def dump_run_context(ctx: RunContext, path: str | Path) -> Path:
    target = Path(path)
    target.write_text(json.dumps(run_context_to_dict(ctx), indent=2), encoding="utf-8")
    logger.info("Wrote RunContext for %s to %s", ctx.dataset_id, target)
    return target
