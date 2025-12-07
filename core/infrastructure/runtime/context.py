from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from core.infrastructure.runtime.paths import build_bronze_relative_path
from core.infrastructure.config import EnvironmentConfig, RootConfig
from core.foundation.primitives.patterns import LoadPattern
from core.infrastructure.runtime.metadata import generate_run_id

logger = logging.getLogger(__name__)


def _normalize_config(
    cfg: Dict[str, Any] | RootConfig,
) -> tuple[Dict[str, Any], Dict[str, Any], RootConfig | None]:
    if isinstance(cfg, RootConfig):
        typed = cfg
        run_cfg = typed.source.run.model_dump()
        cfg_dict = typed.model_dump()
    else:
        typed = None
        cfg_dict = cfg
        run_cfg = cfg_dict.get("source", {}).get("run", {})
    return cfg_dict, run_cfg, typed


def _extract_source_fields(
    cfg_dict: Dict[str, Any], typed: RootConfig | None
) -> tuple[str, str, str, str]:
    source_cfg = cfg_dict["source"]
    if typed:
        system = typed.source.system
        table = typed.source.table
    else:
        system = source_cfg["system"]
        table = source_cfg["table"]
    dataset_id = f"{system}.{table}"
    config_name = source_cfg.get("config_name", dataset_id)
    return system, table, dataset_id, config_name


@dataclass
class RunContext:
    """Context for a single pipeline run.

    Contains all the configuration and state needed to execute
    a bronze extraction or silver transformation.
    """

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
    run_id: str = field(default_factory=generate_run_id)

    @property
    def bronze_dir(self) -> Path:
        return self.bronze_path

    @property
    def bronze_output_base(self) -> Path:
        return self.local_output_dir

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization.

        Note: env_config is not serialized as it contains runtime state.
        """
        return {
            "cfg": self.cfg,
            "run_date": self.run_date.isoformat(),
            "relative_path": self.relative_path,
            "local_output_dir": str(self.local_output_dir),
            "bronze_path": str(self.bronze_path),
            "source_system": self.source_system,
            "source_table": self.source_table,
            "dataset_id": self.dataset_id,
            "config_name": self.config_name,
            "load_pattern": self.load_pattern.value,
            "run_id": self.run_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunContext":
        """Create from dictionary."""
        load_pattern = LoadPattern.normalize(data.get("load_pattern"))
        return cls(
            cfg=data["cfg"],
            run_date=date.fromisoformat(data["run_date"]),
            relative_path=data["relative_path"],
            local_output_dir=Path(data["local_output_dir"]),
            bronze_path=Path(data["bronze_path"]),
            source_system=data["source_system"],
            source_table=data["source_table"],
            dataset_id=data["dataset_id"],
            config_name=data.get("config_name", data["dataset_id"]),
            load_pattern=load_pattern,
            env_config=None,
            run_id=data.get("run_id", generate_run_id()),
        )


def build_run_context(
    cfg: Dict[str, Any] | RootConfig,
    run_date: date,
    local_output_override: Path | None = None,
    relative_override: str | None = None,
    load_pattern_override: str | None = None,
    bronze_path_override: Path | None = None,
    env_config: Optional[EnvironmentConfig] = None,
    run_id: Optional[str] = None,
) -> RunContext:
    cfg_dict, run_cfg, typed = _normalize_config(cfg)

    if "storage_enabled" not in run_cfg:
        bronze_backend = (
            cfg_dict.get("platform", {}).get("bronze", {}).get("storage_backend")
        )
        run_cfg["storage_enabled"] = str(bronze_backend).lower() == "s3"
        cfg_dict["source"]["run"]["storage_enabled"] = run_cfg["storage_enabled"]

    local_output_dir = Path(
        local_output_override or run_cfg.get("local_output_dir", "./output")
    ).resolve()
    relative_path = relative_override or build_bronze_relative_path(cfg_dict, run_date)
    bronze_path = (
        Path(bronze_path_override).resolve()
        if bronze_path_override
        else (local_output_dir / relative_path).resolve()
    )

    source_system, source_table, dataset_id, config_name = _extract_source_fields(
        cfg_dict, typed
    )
    pattern_value = load_pattern_override or run_cfg.get("load_pattern")
    load_pattern = (
        LoadPattern.normalize(pattern_value) if pattern_value else LoadPattern.SNAPSHOT
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
        run_id=run_id or generate_run_id(),
    )


def load_run_context(path: str | Path) -> RunContext:
    """Load RunContext from a JSON file."""
    ctx_path = Path(path)
    payload = json.loads(ctx_path.read_text(encoding="utf-8"))
    context = RunContext.from_dict(payload)
    logger.info(
        "Loaded RunContext for %s (run_date=%s) from %s",
        context.dataset_id,
        context.run_date,
        ctx_path,
    )
    return context


def dump_run_context(ctx: RunContext, path: str | Path) -> Path:
    """Save RunContext to a JSON file."""
    target = Path(path)
    target.write_text(json.dumps(ctx.to_dict(), indent=2), encoding="utf-8")
    logger.info("Wrote RunContext for %s to %s", ctx.dataset_id, target)
    return target
