"""Generate Silver artifacts for every Bronze sample partition with hierarchical structure.

Silver samples now use a hierarchical directory structure matching Bronze:
  sampledata/silver_samples/
    {pattern_folder}/
      silver_model={model}/
        domain={domain}/
          entity={entity}/
            v{version}/
              load_date={date}/
                {artifacts}
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, cast

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "bronze_samples"
SILVER_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "silver_samples"
TEMP_SILVER_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "silver_samples_tmp"
CONFIGS_DIR = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"

# Mapping from silver entity_kind + history_mode to silver_model name
SILVER_MODEL_MAP = {
    ("state", "scd2"): "scd_type_2",
    ("state", "scd1"): "scd_type_1",
    ("state", None): "scd_type_1",
    ("event", "incremental"): "incremental_merge",
    ("event", None): "incremental_merge",
    ("events", "incremental"): "incremental_merge",
    ("events", None): "incremental_merge",
    ("snapshot", None): "periodic_snapshot",
    ("derived_event", None): "incremental_merge",
    ("derived_state", None): "scd_type_1",
    ("hybrid", "incremental"): "incremental_merge",
    ("hybrid", "cumulative"): "full_merge_dedupe",
}


def _safe_remove_path(target: Path) -> None:
    """Recursively delete a path even if shutil.rmtree previously failed."""
    try:
        if target.is_symlink():
            target.unlink()
            return
        if target.is_dir():
            for child in list(target.iterdir()):
                _safe_remove_path(child)
            target.rmdir()
        else:
            target.unlink()
    except OSError as exc:
        print(f"[WARN] Unable to delete {target}: {exc}; skipping")


def _clear_path(target: Path) -> None:
    """Remove a directory tree with best-effort cleanup."""
    if not target.exists():
        return
    try:
        shutil.rmtree(target)
        return
    except OSError as exc:
        print(f"[WARN] Unable to delete {target}: {exc}; falling back to manual cleanup")
    for child in list(target.iterdir()):
        _safe_remove_path(child)
    try:
        target.rmdir()
    except OSError:
        pass


def _promote_temp_samples() -> None:
    """Move temporarily generated samples into the final location."""
    if not TEMP_SILVER_SAMPLE_ROOT.exists():
        print("[WARN] No Silver samples generated; skipping promotion")
        return
    if SILVER_SAMPLE_ROOT.exists():
        _clear_path(SILVER_SAMPLE_ROOT)
    try:
        shutil.move(str(TEMP_SILVER_SAMPLE_ROOT), str(SILVER_SAMPLE_ROOT))
    except OSError as exc:
        print(f"[WARN] Unable to rename temp Silver samples: {exc}; copying instead")
        shutil.copytree(
            TEMP_SILVER_SAMPLE_ROOT,
            SILVER_SAMPLE_ROOT,
            dirs_exist_ok=True,
        )
        _clear_path(TEMP_SILVER_SAMPLE_ROOT)




@dataclass(frozen=True)
class PatternConfig:
    path: Path
    match_dirs: tuple[str, ...] | None
    pattern_folder: str
    silver_model: str
    domain: str
    entity: str


# Ensure project root on sys.path when executed as standalone script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _find_bronze_partitions() -> Iterable[Dict[str, Any]]:
    """Find all Bronze partitions with CSV files.

    Now looks for sample= prefixes in Bronze structure.
    """
    seen: set[str] = set()
    # Accept any partition directory containing at least one CSV even if metadata file absent
    for dir_path in BRONZE_SAMPLE_ROOT.rglob("dt=*"):
        if not dir_path.is_dir():
            continue
        csv_files = sorted(dir_path.rglob("*.csv"))
        if not csv_files:
            continue
        rel_parts = dir_path.relative_to(BRONZE_SAMPLE_ROOT).parts

        # Look for sample= prefix (new structure: sample=pattern_id)
        sample_part = next((p for p in rel_parts if p.startswith("sample=")), None)
        dt_part = next((p for p in rel_parts if p.startswith("dt=")), None)

        if not sample_part or not dt_part:
            continue

        pattern = sample_part.split("=", 1)[1]
        run_date = dt_part.split("=", 1)[1]

        chunk_dir = csv_files[0].parent
        key = f"{pattern}|{run_date}|{chunk_dir}"
        if key in seen:
            continue
        seen.add(key)

        sample = {
            "pattern": pattern,
            "run_date": run_date,
            "dir": chunk_dir,
            "file": csv_files[0],
        }
        yield sample


def _pattern_from_config(cfg: Dict[str, Any]) -> str | None:
    """Extract pattern folder name from config."""
    bronze = cfg.get("bronze", {})
    options = bronze.get("options", {}) or {}
    pattern_folder = options.get("pattern_folder") or bronze.get("pattern_folder")
    if pattern_folder:
        return str(pattern_folder)
    val = cfg.get("pattern")
    return str(val) if val is not None else None


def _silver_model_from_config(cfg: Dict[str, Any]) -> str:
    """Derive silver_model from config silver section."""
    silver = cfg.get("silver", {})
    if not isinstance(silver, dict):
        silver = {}

    # Check if explicit model specified
    if "model" in silver:
        return str(silver["model"])

    # Otherwise derive from entity_kind + history_mode
    entity_kind = silver.get("entity_kind", "state")
    history_mode = silver.get("history_mode")

    key = (entity_kind, history_mode)
    return SILVER_MODEL_MAP.get(key, "scd_type_1")


def _discover_pattern_configs() -> Dict[str, List[PatternConfig]]:
    """Discover and parse all pattern config files."""
    configs: Dict[str, List[PatternConfig]] = {}
    if not CONFIGS_DIR.is_dir():
        raise FileNotFoundError(f"Patterns directory {CONFIGS_DIR} not found")

    for path in sorted(CONFIGS_DIR.glob("pattern*.yaml")):
        if not path.is_file():
            continue

        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        pattern = _pattern_from_config(cfg)
        if not pattern:
            continue

        bronze = cfg.get("bronze", {})
        if not isinstance(bronze, dict):
            bronze = {}
        options = bronze.get("options", {}) or {}
        if not isinstance(options, dict):
            options = {}
        match_dir = options.get("match_dir")
        match_dirs: tuple[str, ...] | None = None
        if isinstance(match_dir, str):
            match_dirs = (match_dir,)
        elif isinstance(match_dir, (list, tuple)):
            match_dirs = tuple(match_dir)

        silver_model = _silver_model_from_config(cfg)
        domain = cfg.get("domain", "default")
        entity = cfg.get("entity", "dataset")

        configs.setdefault(pattern, []).append(
            PatternConfig(
                path=path,
                match_dirs=match_dirs,
                pattern_folder=pattern,
                silver_model=silver_model,
                domain=domain,
                entity=entity,
            )
        )
    return configs


# Removed _build_silver_hierarchy - silver_extract.py builds the path internally


def _write_pattern_readme(
    pattern_dir: Path,
    pattern_id: str,
    silver_model: str,
) -> None:
    """Write README for pattern directory."""
    readme = pattern_dir / "README.md"
    content = f"""# Silver Samples - {pattern_id}

## Overview
Silver artifacts derived from Bronze `{pattern_id}` samples.

## Structure
```
sample={pattern_id}/
  silver_model={silver_model}/
    domain={{domain}}/
      entity={{entity}}/
        v{{version}}/
          load_date={{YYYY-MM-DD}}/
            {{artifacts: current.parquet, history.parquet, etc.}}
```

## Silver Model
- **Model Type**: `{silver_model}`
- Derived from Bronze pattern and silver configuration

## Files
- `intent.yaml` - Original config used to generate these samples
- `_metadata.json` - Batch metadata (record counts, timestamps)
- `*.parquet` / `*.csv` - Silver artifacts

## Generation
Generated by: `python scripts/generate_silver_samples.py`
"""
    readme.write_text(content, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Silver samples with hierarchical structure matching Bronze"
    )
    parser.add_argument(
        "--formats",
        choices=["parquet", "csv", "both"],
        default="both",
        help="Which Silver artifact formats to write",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of sample partitions generated (for testing)",
    )
    return parser.parse_args()


def _run_cli(cmd: list[str]) -> None:
    subprocess.run([sys.executable, *cmd], check=True, cwd=REPO_ROOT)


def _generate_for_partition(
    partition: Dict[str, Any],
    config: PatternConfig,
    enable_parquet: bool,
    enable_csv: bool,
) -> None:
    """Generate Silver artifacts for a single Bronze partition."""
    partition_dir = partition["dir"]
    if not isinstance(partition_dir, Path):
        partition_dir = Path(str(partition_dir))
    dir_name = partition_dir.name
    if config.match_dirs and dir_name not in config.match_dirs:
        return

    run_date = str(partition["run_date"])
    pattern_id = partition["pattern"]

    # Build Silver base using sample= prefix matching Bronze
    # silver_extract.py will add domain/entity/v1/load_date internally
    silver_base = (
        TEMP_SILVER_SAMPLE_ROOT
        / f"sample={pattern_id}"
        / f"silver_model={config.silver_model}"
    )
    silver_base.mkdir(parents=True, exist_ok=True)

    cmd = [
        "silver_extract.py",
        "--config",
        str(config.path),
        "--bronze-path",
        str(partition_dir),
        "--date",
        run_date,
        "--silver-base",
        str(silver_base),  # Pass sample/silver_model level, CLI will add rest
    ]

    if enable_parquet:
        cmd.append("--write-parquet")
    else:
        cmd.append("--no-write-parquet")

    if enable_csv:
        cmd.append("--write-csv")
    else:
        cmd.append("--no-write-csv")

    print(
        f"Generating: sample={pattern_id}/silver_model={config.silver_model} for {run_date}"
    )
    _run_cli(cmd)

    # Copy config to pattern root for reference
    pattern_root = TEMP_SILVER_SAMPLE_ROOT / f"sample={pattern_id}"
    intent_dest = pattern_root / f"intent_{config.path.stem}.yaml"
    shutil.copy(config.path, intent_dest)

    # Write pattern-level README once per pattern
    if not (pattern_root / "README.md").exists():
        _write_pattern_readme(pattern_root, pattern_id, config.silver_model)


def main() -> None:
    args = parse_args()
    enable_parquet = args.formats in {"parquet", "both"}
    enable_csv = args.formats in {"csv", "both"}

    partitions = list(_find_bronze_partitions())
    if not partitions:
        raise RuntimeError("No Bronze partitions found; generate Bronze samples first.")

    _clear_path(TEMP_SILVER_SAMPLE_ROOT)
    TEMP_SILVER_SAMPLE_ROOT.mkdir(parents=True, exist_ok=True)

    pattern_configs = _discover_pattern_configs()
    generated_count = 0

    limit = args.limit if args.limit and args.limit > 0 else None
    stop = False

    for partition in partitions:
        configs = pattern_configs.get(partition["pattern"])
        if not configs:
            print(
                f"[WARN] No pattern configs found for pattern '{partition['pattern']}' - skipping"
            )
            continue

        for config_variant in configs:
            if limit is not None and generated_count >= limit:
                stop = True
                break
            _generate_for_partition(
                partition, config_variant, enable_parquet, enable_csv
            )
            generated_count += 1
        if stop:
            break

    _promote_temp_samples()
    print(
        f"\n[OK] Generated {generated_count} Silver sample(s) under {SILVER_SAMPLE_ROOT}"
    )
    print("\nDirectory structure now matches Bronze hierarchy with sample= prefix:")
    print(f"  {SILVER_SAMPLE_ROOT}/")
    print("    sample={pattern_id}/")
    print("      silver_model={model}/")
    print("        domain={domain}/...")


if __name__ == "__main__":
    main()
