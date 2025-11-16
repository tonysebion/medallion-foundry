"""Command to bootstrap standard Bronze sample data fixtures."""

from __future__ import annotations
from pathlib import Path
from datetime import date
from typing import Iterable
import argparse

BRONZE_ROOT = Path("docs/examples/data/bronze_samples").resolve()

PATTERNS = {
    "full": "full-part-0001.csv",
    "cdc": "cdc-part-0001.csv",
    "current_history": "current-history-part-0001.csv",
}

BASE_ROWS = {
    "full": [
        "order_id,customer_id,status,order_total,updated_at",
        "1,10,NEW,12.50,{ts}T00:00:00Z",
    ],
    "cdc": [
        "order_id,customer_id,status,order_total,changed_at",
        "1,10,NEW,12.50,{ts}T01:00:00Z",
    ],
    "current_history": [
        "order_id,customer_id,status,current_flag,effective_start,effective_end,updated_at",
        "1,10,NEW,true,{ts},{ts},{ts}T02:00:00Z",
    ],
}


def build_partition_dir(pattern: str, run_date: str) -> Path:
    return (
        BRONZE_ROOT
        / pattern
        / "system=retail_demo"
        / "table=orders"
        / f"pattern={pattern}"
        / f"dt={run_date}"
    )


def write_partition(pattern: str, run_date: str) -> Path:
    dir_path = build_partition_dir(pattern, run_date)
    dir_path.mkdir(parents=True, exist_ok=True)
    filename = PATTERNS[pattern]
    header, row = BASE_ROWS[pattern]
    content = "\n".join([header, row.format(ts=run_date)]) + "\n"
    file_path = dir_path / filename
    file_path.write_text(content, encoding="utf-8")
    return file_path


def bootstrap(run_dates: Iterable[str]) -> None:
    for rd in run_dates:
        for pattern in PATTERNS:
            write_partition(pattern, rd)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap Bronze sample data")
    p.add_argument("--dates", nargs="*", help="ISO dates to generate", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dates = args.dates or [date.today().isoformat()]
    bootstrap(dates)
    print(f"Bronze sample data bootstrapped under {BRONZE_ROOT}")


if __name__ == "__main__":  # pragma: no cover
    main()
