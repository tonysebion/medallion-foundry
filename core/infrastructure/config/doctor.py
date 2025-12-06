from __future__ import annotations

import argparse
import copy
import difflib
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .loader import load_config


def _migrate_single(cfg: Dict[str, Any], target_version: int = 1) -> Dict[str, Any]:
    out = copy.deepcopy(cfg)
    # Ensure config_version
    out["config_version"] = target_version

    # platform.bronze: local_path -> output_dir
    platform = out.get("platform") or {}
    bronze = platform.get("bronze") or {}
    if "output_dir" not in bronze and "local_path" in bronze:
        bronze["output_dir"] = bronze["local_path"]
    # For v2, remove deprecated local_path to avoid ambiguity
    if target_version >= 2 and "local_path" in bronze:
        bronze.pop("local_path", None)
    if platform:
        platform["bronze"] = bronze
        out["platform"] = platform

    # source.api: url -> base_url; ensure endpoint
    source = out.get("source") or {}
    api = source.get("api") or {}
    if "url" in api and "base_url" not in api:
        api["base_url"] = api["url"]
    if target_version >= 2 and "url" in api:
        api.pop("url", None)
    api.setdefault("endpoint", "/")
    if source:
        source["api"] = api if api else source.get("api")
        out["source"] = source

    # Silver model explicit in v2
    silver = out.get("silver") or {}
    if target_version >= 2:
        # Ensure model is present when derivable; otherwise preserve if already set
        if "model" not in silver:
            # no inference here; leave unset if not specified to avoid surprises
            pass
        out["silver"] = silver or out.get("silver")

    # Partition settings:
    # v2 keeps bronze partitioning under platform.bronze and silver partitioning under silver.partitioning
    # but ensures shapes and defaults exist
    if target_version >= 2:
        silver.setdefault("partitioning", {})
        silver["partitioning"].setdefault(
            "columns", silver.get("partitioning", {}).get("columns", [])
        )
        out["silver"] = silver

    return out


def migrate_config_file(
    path: Path, in_place: bool = False, target_version: int = 1
) -> str:
    original_text = path.read_text(encoding="utf-8")
    data = yaml.safe_load(original_text)

    if "sources" in data:
        migrated_sources: List[Dict[str, Any]] = []
        platform = data.get("platform")
        silver = data.get("silver")
        for entry in data["sources"]:
            merged: Dict[str, Any] = {
                "platform": platform,
                "silver": silver,
                "source": entry.get("source", {}),
            }
            migrated = _migrate_single(merged, target_version)
            # store back under sources with per-entry overrides
            migrated_entry: Dict[str, Any] = {
                k: v for k, v in entry.items() if k != "source"
            }
            migrated_entry["source"] = migrated["source"]
            migrated_sources.append(migrated_entry)
        data["sources"] = migrated_sources
        # top-level platform/silver remain as defaults
        data.setdefault("config_version", target_version)
    else:
        data = _migrate_single(data, target_version)

    migrated_text = yaml.safe_dump(data, sort_keys=False)
    diff = "".join(
        difflib.unified_diff(
            original_text.splitlines(keepends=True),
            migrated_text.splitlines(keepends=True),
            fromfile=str(path),
            tofile=str(path) + " (migrated)",
        )
    )

    if in_place and original_text != migrated_text:
        path.write_text(migrated_text, encoding="utf-8")
    return diff


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="bronze-config doctor: lint and migrate configs"
    )
    parser.add_argument("paths", nargs="+", help="YAML config files to analyze")
    parser.add_argument(
        "--in-place", "-i", action="store_true", help="Write changes back to files"
    )
    parser.add_argument(
        "--target-version",
        type=int,
        default=1,
        help="Target config_version to enforce (default 1)",
    )
    args = parser.parse_args(argv)

    exit_code = 0
    for p in args.paths:
        path = Path(p)
        if not path.exists():
            print(f"! {p}: not found")
            exit_code = 2
            continue
        try:
            # Lint: attempt to load (non-strict) and report any exceptions
            try:
                _ = load_config(str(path))
                lint_msg = "ok"
            except Exception as exc:
                lint_msg = f"lint error: {exc}"
            diff = migrate_config_file(
                path, in_place=args.in_place, target_version=args.target_version
            )
            print(f"== {p} ({lint_msg})")
            if diff:
                print(diff)
            else:
                print("(no changes)")
        except Exception as exc:
            print(f"! {p}: failed: {exc}")
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
