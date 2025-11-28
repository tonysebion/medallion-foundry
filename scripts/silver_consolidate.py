"""Consolidate chunked Silver artifacts into canonical artifacts and write metadata.

This script finds partitions containing `_metadata_chunk_*.json` files produced by
concurrent `silver_extract.py --chunk-tag` runs, merges each artifact's chunk files,
writes final artifacts and `_metadata.json`/`_checksums.json`, and optionally prunes
the chunk files afterwards.
"""
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple
import re

import pandas as pd

import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from core.bronze.io import write_batch_metadata, write_checksum_manifest
from core.storage.locks import file_lock


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consolidate chunked Silver artifacts")
    parser.add_argument("--silver-base", help="Base silver dir to search", default=None)
    parser.add_argument("--prune-chunks", action="store_true", help="Remove chunk files after consolidation")
    parser.add_argument("--dedupe", action="store_true", help="Attempt de-duplication by primary keys")
    parser.add_argument("--primary-keys", help="Comma-separated primary key columns used for dedupe", default=None)
    parser.add_argument("--order-column", help="Order column for dedupe (keep latest record)", default=None)
    parser.add_argument("--lock-timeout", type=float, default=60.0, help="Lock timeout seconds for consolidation")
    return parser.parse_args()


def find_chunk_partitions(base: Path) -> List[Path]:
    candidates = []
    # Any explicit chunk metadata files
    for p in base.rglob("_metadata_chunk_*.json"):
        candidates.append(p.parent)

    # Also consider directories containing chunk-templated artifact filenames
    pattern = re.compile(r"-[0-9a-fA-F]{8}\.(parquet|csv)$")
    for f in base.rglob("*.*"):
        if not f.is_file():
            continue
        if pattern.search(f.name):
            candidates.append(f.parent)
    return sorted(set(candidates))


def _read_chunk_files_for_artifact(partition: Path, artifact_name: str) -> List[Path]:
    files = list(partition.glob(f"{artifact_name}-*.parquet")) + list(partition.glob(f"{artifact_name}-*.csv"))
    return sorted(files)


def _consolidate_parquet(files: List[Path], final_path: Path, dedupe_keys: List[str] | None = None, order_column: str | None = None) -> None:
    frames = []
    for f in files:
        frames.append(pd.read_parquet(f))
    if frames:
        merged = pd.concat(frames, ignore_index=True)
        if dedupe_keys and order_column and order_column in merged.columns:
            merged = merged.sort_values(order_column).drop_duplicates(subset=dedupe_keys, keep='last')
        tmp = final_path.parent / f".{final_path.name}.tmp"
        merged.to_parquet(tmp, index=False)
        tmp.replace(final_path)


def _consolidate_csv(files: List[Path], final_path: Path, dedupe_keys: List[str] | None = None, order_column: str | None = None) -> None:
    frames = []
    for f in files:
        frames.append(pd.read_csv(f))
    if frames:
        merged = pd.concat(frames, ignore_index=True)
        if dedupe_keys and order_column and order_column in merged.columns:
            merged = merged.sort_values(order_column).drop_duplicates(subset=dedupe_keys, keep='last')
        tmp = final_path.parent / f".{final_path.name}.tmp"
        merged.to_csv(tmp, index=False)
        tmp.replace(final_path)


def _consolidate_partition(partition: Path, args: argparse.Namespace) -> None:
    # Parse chunk metadata files in partition
    chunk_meta_files = sorted(partition.glob("_metadata_chunk_*.json"))
    if not chunk_meta_files:
        # No explicit chunk metadata; continue if we declared partitions via artifact detection
        print(f"No `_metadata_chunk_` files under {partition}; attempting discovery via artifact filenames")
    chunk_tags = []
    for f in chunk_meta_files:
        j = json.loads(f.read_text(encoding='utf-8'))
        tag = j.get('chunk_tag')
        if tag:
            chunk_tags.append(tag)
    chunk_tags = sorted(set(chunk_tags))

    # Find all artifact names by scanning chunk files
    artifact_files_map: Dict[str, List[Path]] = {}
    for p in partition.rglob("*"):
        if p.is_file() and any(p.name.endswith(s) for s in (".parquet", ".csv")):
            name = p.name
            # normalize artifact base name by removing suffix -<chunk>
            if '-' in name:
                base = name.rsplit('-', 1)[0]
            else:
                base = name
            artifact_files_map.setdefault(base, []).append(p)

    dedupe_keys = (args.primary_keys.split(",") if args.primary_keys else None)
    order_column = args.order_column

    # Consolidate each artifact
    outputs: List[Path] = []
    for base, files in artifact_files_map.items():
        # pick parquet if exists else csv
        parquet_files = [p for p in files if p.suffix == '.parquet']
        csv_files = [p for p in files if p.suffix == '.csv']
        if parquet_files:
            final_path = partition / f"{base}.parquet"
            _consolidate_parquet(sorted(parquet_files), final_path, dedupe_keys, order_column)
            outputs.append(final_path)
        if csv_files:
            final_path = partition / f"{base}.csv"
            _consolidate_csv(sorted(csv_files), final_path, dedupe_keys, order_column)
            outputs.append(final_path)

    # Write metadata and checksum manifest
    record_count = 0
    chunk_count = 0
    for p in outputs:
        if p.exists():
            # naive counts: row counts
            try:
                if p.suffix == '.parquet':
                    df = pd.read_parquet(p)
                else:
                    df = pd.read_csv(p)
                record_count += len(df)
            except Exception:
                pass
            chunk_count += 1

    # Write metadata - simple version
    write_batch_metadata(partition, record_count, chunk_count)
    write_checksum_manifest(partition, outputs, "chunked")

    if args.prune_chunks:
        for tag in chunk_tags:
            # remove chunk artifacts
            for p in partition.glob(f"*-{tag}.parquet"):
                p.unlink()
            for p in partition.glob(f"*-{tag}.csv"):
                p.unlink()
        for f in chunk_meta_files:
            f.unlink()


def main() -> None:
    args = parse_args()
    base = Path(args.silver_base) if args.silver_base else Path("sampledata") / "silver_samples"
    if not base.exists():
        raise FileNotFoundError(f"Silver base {base} not found")

    partitions = find_chunk_partitions(base)
    if not partitions:
        print("No partitions with chunked metadata found; nothing to do")
        return

    for partition in partitions:
        print(f"Consolidating partition {partition}")
        with file_lock(partition, timeout=args.lock_timeout):
            _consolidate_partition(partition, args)
    print("Consolidation complete")


if __name__ == '__main__':
    main()
