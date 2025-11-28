"""Generate Silver join artifacts for every combination of Silver samples and output formats."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from itertools import product
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_SAMPLE_ROOT = REPO_ROOT / "sampledata" / "silver_samples"
OUTPUT_ROOT = REPO_ROOT / "docs" / "examples" / "data" / "silver_join_samples"
SCRIPT = REPO_ROOT / "silver_join.py"

FORMAT_COMBINATIONS = [
    ("parquet",),
    ("csv",),
    ("parquet", "csv"),
]

BASE_PLATFORM = {
    "bronze": {
        "storage_backend": "local",
        "storage_metadata": {
            "boundary": "onprem",
            "provider_type": "local_generic",
            "cloud_provider": None,
        },
    }
}


def _gather_silver_assets() -> list[Path]:
    assets: list[Path] = []
    for metadata_path in SILVER_SAMPLE_ROOT.rglob("_metadata.json"):
        assets.append(metadata_path.parent)
    return sorted(assets)


def _load_metadata(asset: Path) -> Dict[str, Any]:
    metadata_path = asset / "_metadata.json"
    return json.loads(metadata_path.read_text(encoding="utf-8"))


def _label_from_path(path: Path) -> str:
    rel = path.relative_to(SILVER_SAMPLE_ROOT)
    parts = rel.parts
    if len(parts) >= 6:
        pattern = parts[0]
        model = parts[1]
        domain = parts[2].split("=", 1)[-1]
        entity = parts[3].split("=", 1)[-1]
        version = parts[4]
        load_date = parts[5].split("=", 1)[-1]
        return "_".join([pattern, model, domain, entity, version, load_date])
    sanitized = [p.replace("=", "-").replace(" ", "_") for p in rel.parts]
    return "_".join(sanitized)


def _build_join_config(
    left: Path,
    right: Path,
    formats: tuple[str, ...],
    output_base: Path,
) -> Path:
    left_meta = _load_metadata(left)
    model = left_meta.get("silver_model", "scd_type_2")
    primary_keys = left_meta.get("primary_keys") or ["order_id"]
    order_column = left_meta.get("order_column")
    config = {
        "platform": BASE_PLATFORM,
        "silver_join": {
            "left": {"path": str(left)},
            "right": {"path": str(right)},
            "output": {
                "path": str(output_base),
                "model": model,
                "formats": list(formats),
                "primary_keys": primary_keys,
                "order_column": order_column,
                "checkpoint_dir": str(output_base / ".join_progress"),
                "join_strategy": "auto",
            },
        },
    }
    config_path = output_base.parent / f"{output_base.name}_config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def _run_join(config_path: Path) -> None:
    subprocess.run(
        [sys.executable, str(SCRIPT), "--config", str(config_path)],
        check=True,
        cwd=REPO_ROOT,
    )


def _write_readme(
    output_path: Path, left: Path, right: Path, formats: tuple[str, ...]
) -> None:
    readme_path = output_path / "README.md"
    readme_path.write_text(
        f"""# Silver join output

- **Left input**: `{left.relative_to(SILVER_SAMPLE_ROOT)}`
- **Right input**: `{right.relative_to(SILVER_SAMPLE_ROOT)}`
- **Formats**: {", ".join(formats)}

Generated via `python scripts/generate_silver_join_samples.py --formats both`.
""",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create silver_join samples for every combination"
    )
    parser.add_argument(
        "--formats",
        choices=["parquet", "csv", "both"],
        default="both",
        help="Which Silver artifacts to materialize (default: both).",
    )
    parser.add_argument(
        "--version",
        default="1",
        help="Version suffix written into each generated silver_join folder (default: 1).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    OUTPUT_ROOT.mkdir(parents=True)
    assets = _gather_silver_assets()
    if not assets:
        raise RuntimeError(
            "No Silver samples found; run scripts/generate_silver_samples.py first."
        )

    format_sets = {
        "parquet": [FORMAT_COMBINATIONS[0]],
        "csv": [FORMAT_COMBINATIONS[1]],
        "both": list(FORMAT_COMBINATIONS),
    }
    selected_formats = format_sets[args.formats]

    combos = list(product(assets, repeat=2))
    print(
        f"Generating {len(combos)} silver_join combos with {len(selected_formats)} format sets"
    )

    for left, right in combos:
        for format_combination in selected_formats:
            output_dir = (
                OUTPUT_ROOT
                / f"v{args.version}"
                / f"{_label_from_path(left)}__{_label_from_path(right)}"
                / f"formats-{'_'.join(format_combination)}"
            )
            if output_dir.exists():
                shutil.rmtree(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / ".join_progress").mkdir(parents=True, exist_ok=True)
            config_path = _build_join_config(
                left, right, format_combination, output_dir
            )
            _run_join(config_path)
            _write_readme(output_dir, left, right, format_combination)

    print(f"Silver join samples materialized under {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
