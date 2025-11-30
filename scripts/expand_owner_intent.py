"""Generate owner-focused guidance from intent templates."""

from __future__ import annotations

import argparse
from pathlib import Path
import textwrap
from typing import Any, Dict, Optional

import yaml

DEFAULT_BRONZE_BASE = Path("sampledata") / "bronze"
DEFAULT_SILVER_BASE = Path("./silver")


def resolve_paths(dataset: Dict[str, Any], env: Optional[str]) -> Dict[str, str]:
    bronze_base = DEFAULT_BRONZE_BASE
    silver_base = DEFAULT_SILVER_BASE
    if env:
        bronze_base = bronze_base / f"env={env}"
        silver_base = silver_base / f"env={env}"

    system = dataset["system"]
    entity = dataset["entity"]
    bronze_pattern = (
        dataset.get("bronze", {}).get("options", {}).get("load_pattern", "full")
    )
    silver_domain = dataset.get("domain", system)

    bronze_path = (
        bronze_base
        / f"system={system}"
        / f"table={entity}"
        / f"pattern={bronze_pattern}"
        / "dt=<run_date>"
    )
    silver_path = (
        silver_base
        / f"domain={silver_domain}"
        / f"entity={entity}"
        / "v1"
        / "load_date=<run_date>"
    )
    return {
        "bronze": str(bronze_path),
        "silver": str(silver_path),
    }


def load_intent(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        from typing import cast

        return cast(Dict[str, Any], yaml.safe_load(handle))


def persist_resolved(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Expand owner intent templates into resolved path guidance."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("docs/examples/configs/owner_intent_template.yaml"),
        help="Intent YAML to expand.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/examples/configs/owner_intent_template.resolved.yaml"),
        help="Where to write the annotated intent file.",
    )
    args = parser.parse_args()

    intent = load_intent(args.config)
    env = intent.get("environment")
    checklist = []

    datasets = intent.get("datasets", [])
    for dataset in datasets:
        paths = resolve_paths(dataset, env)
        dataset["derived_paths"] = paths
        summary = textwrap.dedent(
            f"""
            Dataset: {dataset['name']}
              Bronze folder (example): {paths['bronze']}
              Silver folder (example): {paths['silver']}
            """
        ).strip()
        checklist.append(summary)

    persist_resolved(args.output, intent)

    print("Owner intent expanded. Resolved paths:")
    for line in checklist:
        print(line)
        print()
    print(f"Annotated intent saved to {args.output}")


if __name__ == "__main__":
    main()
