"""Integration tests verifying that `sampledata/source_samples` and
`sampledata/bronze_samples` mirror each other for patterns 1-7.

These tests check that all files emitted by the sample generator are present
in the bronze mirror and that Bronze partition metadata and checksum manifests
are consistent with the actual files.

Run these tests only when `sampledata` has been generated locally (or set
`GENERATE_SAMPLES_FOR_TESTS=1` prior to running tests if you want tests to
invoke the generator).
"""

from __future__ import annotations

from pathlib import Path
import random
import math
import json
import os
import pytest

from core.bronze.io import verify_checksum_manifest
from core.config.loader import load_config_with_env
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem
from core.config.environment import EnvironmentConfig

REPO_ROOT = Path(__file__).resolve().parents[1]


def _local_prefix_for(storage_prefix: str) -> Path:
    """Map storage.prefix to local sampledata directory when possible.

    Example: source_samples/ -> sampledata/source_samples/
    If prefix looks like a local path (starts with ./ or /), resolve it.
    Otherwise, assume it refers to the sampledata directory under repo root.
    """
    if not storage_prefix:
        return REPO_ROOT / "sampledata"
    prefix = storage_prefix.rstrip("/")
    if prefix.startswith("./") or prefix.startswith("/"):
        return (REPO_ROOT / prefix).resolve()
    # Common conventions used in pattern configs
    if "source_samples" in prefix:
        return REPO_ROOT / "sampledata" / "source_samples"
    if "bronze_samples" in prefix:
        return REPO_ROOT / "sampledata" / "bronze_samples"
    if "silver_samples" in prefix:
        return REPO_ROOT / "sampledata" / "silver_samples"
    # Fallback to repo sampledata
    return REPO_ROOT / "sampledata"

try:
    # Import pattern mapping from the generator used to create the sample data
    from scripts.generate_sample_data import PATTERN_DIRS
except Exception:
    # Fallback mapping for safety
    PATTERN_DIRS = {
        "full": "pattern1_full_events",
        "cdc": "pattern2_cdc_events",
        "current_history": "pattern3_scd_state",
        "hybrid_cdc_point": "pattern4_hybrid_cdc_point",
        "hybrid_cdc_cumulative": "pattern5_hybrid_cdc_cumulative",
        "hybrid_incremental_point": "pattern6_hybrid_incremental_point",
        "hybrid_incremental_cumulative": "pattern7_hybrid_incremental_cumulative",
    }


def _assert_sampledata_present():
    # Compute unique storage prefixes from pattern configs
    config_dir = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"
    source_exists = False
    bronze_exists = False
    for cfg_file in config_dir.glob("pattern_*.yaml"):
        cfg = __import__("yaml").safe_load(cfg_file.read_text(encoding="utf-8"))
        storage_cfg = cfg.get("storage", {})
        source_prefix = storage_cfg.get("source", {}).get("prefix", "source_samples/")
        bronze_prefix = storage_cfg.get("bronze", {}).get("prefix", "bronze_samples/")
        if _local_prefix_for(source_prefix).exists() and any(_local_prefix_for(source_prefix).rglob("*")):
            source_exists = True
        if _local_prefix_for(bronze_prefix).exists() and any(_local_prefix_for(bronze_prefix).rglob("*")):
            bronze_exists = True
    if not source_exists:
        pytest.skip("Source sample data missing; run scripts/generate_sample_data.py")
    if not bronze_exists:
        pytest.skip("Bronze sample mirror missing; ensure `scripts/generate_sample_data.py` mirrored files")


def _is_full_check_enabled() -> bool:
    """Return True if exhaustive checking is desired.

    Use environment variable FULL_SAMPLEDATA_MIRROR_CHECK=1 to enable exhaustive checks.
    Default behavior is a lightweight sample-based check to avoid long-running tests.
    """
    return os.getenv("FULL_SAMPLEDATA_MIRROR_CHECK") == "1"


def _choose_sample(files: list[str], max_samples: int = 10) -> list[str]:
    """Return a deterministic sample of files up to `max_samples` from the provided list.

    We preserve deterministic behavior by using a fixed seed derived from a hash of
    the number of files which is consistent across runs for the same set.
    """
    if _is_full_check_enabled() or len(files) <= max_samples:
        return list(files)
    # deterministic pseudo-random picking
    seed = len(files)
    rng = random.Random(seed)
    selected = set()
    while len(selected) < max_samples:
        selected.add(rng.choice(files))
    return sorted(selected)


@pytest.mark.integration
def test_all_files_in_source_are_in_bronze_mirror() -> None:
    """For each pattern, ensure every file under source_samples exists in bronze_samples
    and the file contents match (byte-by-byte) so the mirror is exact."""
    _assert_sampledata_present()

    missing = []
    mismatched = []
    # Discover pattern config files and compute storage prefixes per pattern
    config_dir = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"
    for cfg_file in config_dir.glob("pattern_*.yaml"):
        dataset, env_config = load_config_with_env(cfg_file)
        cfg = __import__("yaml").safe_load(cfg_file.read_text(encoding="utf-8"))
        pattern_folder = (
            dataset.bronze.options.get("pattern_folder")
            or dataset.bronze.options.get("load_pattern")
            or cfg.get("pattern_id")
        )
        if not pattern_folder:
            continue
        # Extract storage prefixes (defaults to source_samples/ and bronze_samples/)
        # Resolve the source dataset root: either a local path or an s3/azure URI
        src_pattern_root = None
        if dataset.bronze.source_type == "file" and dataset.bronze.path_pattern:
            src_uri = StorageURI.parse(dataset.bronze.path_pattern)
            # If the path is S3 or Azure, compute the pattern root using prefix + sample=pattern
            if src_uri.backend in ("s3", "azure"):
                key = src_uri.key
                idx = key.find(f"sample={pattern_folder}")
                root_key = key[: idx + len(f"sample={pattern_folder}")]
                uri_root = f"{src_uri.backend}://{src_uri.bucket}/{root_key}"
                src_pattern_root = uri_root
            else:
                # Local filesystem
                path_str = dataset.bronze.path_pattern
                idx = path_str.find(f"sample={pattern_folder}")
                root_path = path_str[: idx + len(f"sample={pattern_folder}")]
                src_pattern_root = str(Path(root_path).resolve())
        else:
            storage_cfg = cfg.get("storage", {})
            source_prefix = storage_cfg.get("source", {}).get("prefix", "source_samples/")
            # Default assumption: local mapping to sampledata/source_samples
            src_pattern_root = str(_local_prefix_for(source_prefix) / f"sample={pattern_folder}")

        # Resolve bronze root using dataset.bronze.output_bucket / output_prefix and pattern folder
        bronz_pattern_root = None
        if dataset.bronze.output_storage == "s3":
            bucket = dataset.bronze.output_bucket or ""
            prefix = dataset.bronze.output_prefix or ""
            prefix = prefix.rstrip("/")
            bronz_pattern_root = f"s3://{bucket}/{prefix}/sample={pattern_folder}" if prefix else f"s3://{bucket}/sample={pattern_folder}"
        else:
            # Local filesystem
            base = dataset.bronze.options.get("local_output_dir") or Path("sampledata") / "bronze_samples"
            bronz_pattern_root = str(Path(base) / f"sample={pattern_folder}")

        # The src_pattern_root may be a string path (local) or s3:// URI. Use StorageURI to determine.
        src_uri_obj = StorageURI.parse(str(src_pattern_root))
        if src_uri_obj.backend == "local":
            from pathlib import Path as LocalPath

            src_root_path = LocalPath(src_uri_obj.key)
            if not src_root_path.exists():
                continue
        else:
            # s3/azure: check if path exists by creating filesystem; fallback to local mirror
            src_fs = create_filesystem(src_uri_obj, env_config)
            try:
                _ = src_fs.ls(src_uri_obj.to_fsspec_path(env_config), detail=False)
            except Exception:
                fallback_local_src = REPO_ROOT / "sampledata" / "source_samples" / f"sample={pattern_folder}"
                if fallback_local_src.exists():
                    src_uri_obj = StorageURI.parse(str(fallback_local_src))
                    src_fs = None
                else:
                    continue
        bronz_uri_obj = StorageURI.parse(str(bronz_pattern_root))
        if bronz_uri_obj.backend == "local":
            from pathlib import Path as LocalPath

            bronz_root_path = LocalPath(bronz_uri_obj.key)
            if not bronz_root_path.exists():
                missing.append(str(bronz_root_path))
                continue
        else:
            bronz_fs = create_filesystem(bronz_uri_obj, env_config)
            try:
                _ = bronz_fs.ls(bronz_uri_obj.to_fsspec_path(env_config), detail=False)
            except Exception:
                # S3 not accessible; fallback to local sampledata mirror if it exists
                fallback_local = REPO_ROOT / "sampledata" / "bronze_samples" / f"sample={pattern_folder}"
                if fallback_local.exists():
                    bronz_uri_obj = StorageURI.parse(str(fallback_local))
                    bronz_fs = None
                else:
                    missing.append(str(bronz_uri_obj.to_fsspec_path(env_config)))
                    continue

        # Iterate source files using the appropriate filesystem API
        # Build list of candidate source files
        src_candidate_files: list[str] = []
        if src_uri_obj.backend == "local":
            for f in Path(src_uri_obj.key).rglob("**/*"):
                if f.is_file():
                    src_candidate_files.append(str(f))
        else:
            src_fs = create_filesystem(src_uri_obj, env_config)
            src_root_fsspec = src_uri_obj.to_fsspec_path(env_config)
            try:
                found = src_fs.find(src_root_fsspec)
                for k in found:
                    if not k.endswith("/"):
                        src_candidate_files.append(k)
            except Exception:
                src_candidate_files = []

        sample_files = _choose_sample(src_candidate_files, max_samples=10)

        for src_file in sample_files:
            if src_uri_obj.backend == "local":
                src_file_path = Path(src_file)
                rel = src_file_path.relative_to(Path(src_uri_obj.key))
                bronze_file_path = None
                if bronz_uri_obj.backend == "local":
                    bronze_file_path = Path(bronz_uri_obj.key) / rel
                else:
                    bronze_file_path = bronz_uri_obj.to_fsspec_path(env_config) + "/" + str(rel).replace('\\\\','/')
                if bronz_uri_obj.backend == "local":
                    if not (bronze_file_path.exists()):
                        missing.append(str(bronze_file_path))
                        continue
                    try:
                        with src_file_path.open("rb") as s, open(bronze_file_path, "rb") as b:
                            if s.read() != b.read():
                                mismatched.append(str(rel))
                    except Exception as exc:
                        mismatched.append(f"{rel} (error reading: {exc})")
                else:
                    bronze_fs = bronz_fs
                    bronze_rel_path = bronze_file_path
                    try:
                        with src_file.open("rb") as s, bronze_fs.open(bronze_rel_path, "rb") as b:
                            if s.read() != b.read():
                                mismatched.append(str(rel))
                    except Exception as exc:
                        mismatched.append(f"{rel} (error reading: {exc})")
        else:
            src_fs = create_filesystem(src_uri_obj, env_config)
            src_root_fsspec = src_uri_obj.to_fsspec_path(env_config)
            for file_info in sample_files:
                # fsspec filesystem returns a list of keys; filter by extension
                if file_info.endswith("/"):
                    continue
                # file_info is the absolute fsspec path, compute relative key
                if file_info.startswith(src_root_fsspec.rstrip("/")):
                    rel_key = file_info[len(src_root_fsspec.rstrip("/")) :].lstrip("/")
                else:
                    rel_key = Path(file_info).name
                rel = Path(rel_key)
                bronze_target = None
                if bronz_uri_obj.backend == "local":
                    bronze_target = Path(bronz_uri_obj.key) / rel
                else:
                    bronze_target = bronz_uri_obj.to_fsspec_path(env_config) + "/" + str(rel).replace('\\\\','/')
                if bronz_uri_obj.backend == "local":
                    if not bronze_target.exists():
                        missing.append(str(bronze_target))
                        continue
                    try:
                        with src_fs.open(file_info, "rb") as s, open(bronze_target, "rb") as b:
                            if s.read() != b.read():
                                mismatched.append(str(rel))
                    except Exception as exc:
                        mismatched.append(f"{rel} (error reading: {exc})")
                else:
                    bronze_fs = bronz_fs
                    try:
                        with src_fs.open(file_info, "rb") as s, bronze_fs.open(bronze_target, "rb") as b:
                            if s.read() != b.read():
                                mismatched.append(str(rel))
                    except Exception as exc:
                        mismatched.append(f"{rel} (error reading: {exc})")
            # end of s3 branch; all sample comparisons performed above

    assert not missing, f"Missing files in bronze mirror: {missing}"
    assert not mismatched, f"Files mismatch between source and bronze mirror: {mismatched}"


@pytest.mark.integration
def test_bronze_partitions_have_valid_checksums_and_metadata() -> None:
    """For every Bronze partition that has checksum manifest, run verification and
    validate that `_metadata.json` record_count equals the number of rows found in CSV/Parquet artifacts."""
    _assert_sampledata_present()

    issues = []
    # Walk Bronze root(s) for partitions (a partition is a dir with `_metadata.json`)
    config_dir = REPO_ROOT / "docs" / "examples" / "configs" / "patterns"
    bronze_roots = set()
    for cfg_file in config_dir.glob("pattern_*.yaml"):
        cfg = __import__("yaml").safe_load(cfg_file.read_text(encoding="utf-8"))
        bronze_prefix = cfg.get("storage", {}).get("bronze", {}).get("prefix", "bronze_samples/")
        bronze_roots.add(_local_prefix_for(bronze_prefix))
    for bronze_root in bronze_roots:
        all_metadata_paths = list(bronze_root.rglob("_metadata.json"))
        if not all_metadata_paths:
            continue
        sampled_metadata = _choose_sample([str(p) for p in all_metadata_paths], max_samples=10)
        for metadata_path_str in sampled_metadata:
            metadata_path = Path(metadata_path_str)
            partition_dir = metadata_path.parent
        # Verify checksum manifest
        try:
            verify_checksum_manifest(partition_dir)
        except Exception as exc:
            issues.append(f"Checksum verification failed for {partition_dir}: {exc}")

        # Now verify metadata record_count: sum rows across CSV/Parquet files in partition
        try:
            md = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception as exc:
            issues.append(f"Failed to parse _metadata.json for {partition_dir}: {exc}")
            continue
        expected_count = md.get("record_count")
        if expected_count is None:
            issues.append(f"_metadata.json for {partition_dir} missing record_count")
            continue

        # Count rows across CSVs & Parquets in the partition
        actual_count = 0
        for file in partition_dir.glob("*.*"):
            if file.suffix.lower() == ".csv":
                try:
                    import csv

                    with file.open("r", encoding="utf-8") as fh:
                        reader = csv.reader(fh)
                        rows = list(reader)
                        # csv includes header
                        cnt = max(len(rows) - 1, 0) if rows else 0
                        actual_count += cnt
                except Exception:
                    # Fallback: approximate by reading lines (less strict)
                    actual_count += sum(1 for _ in file.open("r", encoding="utf-8")) - 1
            elif file.suffix.lower() == ".parquet":
                try:
                    import pandas as pd

                    df = pd.read_parquet(file)
                    actual_count += len(df)
                except Exception:
                    # If parquet reading fails, skip counting but note inability
                    issues.append(f"Failed to read parquet file {file} for counting")

        if expected_count != actual_count:
            issues.append(
                f"Record count mismatch for {partition_dir}: metadata={expected_count}, actual_rows={actual_count}"
            )

    assert not issues, "\n".join(issues)
