#!/usr/bin/env python
"""
Verify bronze_samples S3 data correctness.

Usage:
    python scripts/verify_bronze_samples_s3.py
    python scripts/verify_bronze_samples_s3.py --pattern pattern5_hybrid_cdc_cumulative
    python scripts/verify_bronze_samples_s3.py --run-date 2025-11-24
    python scripts/verify_bronze_samples_s3.py --check metadata-leak
    python scripts/verify_bronze_samples_s3.py --output-report bronze_verification_report.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import boto3  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402

from botocore.exceptions import ClientError  # noqa: E402

# Pattern definitions
PATTERN_DEFINITIONS = {
    "pattern1_full_events": {
        "name": "Full Events",
        "load_pattern": "full",
        "config": "docs/examples/configs/patterns/pattern_full.yaml",
    },
    "pattern2_cdc_events": {
        "name": "CDC Events",
        "load_pattern": "cdc",
        "config": "docs/examples/configs/patterns/pattern_cdc.yaml",
        "has_reference_mode": True,
    },
    "pattern3_scd_state": {
        "name": "SCD State",
        "load_pattern": "full",
        "config": "docs/examples/configs/patterns/pattern_current_history.yaml",
    },
    "pattern4_hybrid_cdc_point": {
        "name": "Hybrid CDC Point",
        "load_pattern": "cdc",
        "config": "docs/examples/configs/patterns/pattern_hybrid_cdc_point.yaml",
        "has_reference_mode": True,
        "has_delta_subdir": True,
    },
    "pattern5_hybrid_cdc_cumulative": {
        "name": "Hybrid CDC Cumulative",
        "load_pattern": "cdc",
        "config": "docs/examples/configs/patterns/pattern_hybrid_cdc_cumulative.yaml",
        "has_reference_mode": True,
        "has_delta_subdir": True,
    },
    "pattern6_hybrid_incremental_point": {
        "name": "Hybrid Incremental Point",
        "load_pattern": "cdc",
        "config": "docs/examples/configs/patterns/pattern_hybrid_incremental_point.yaml",
        "has_delta_subdir": True,
    },
    "pattern7_hybrid_incremental_cumulative": {
        "name": "Hybrid Incremental Cumulative",
        "load_pattern": "cdc",
        "config": "docs/examples/configs/patterns/pattern_hybrid_incremental_cumulative.yaml",
    },
}


@dataclass
class VerificationIssue:
    pattern: str
    run_date: str
    severity: str  # CRITICAL, WARNING, INFO
    issue_type: str
    details: str
    file: Optional[str] = None


@dataclass
class BronzeVerificationResult:
    critical_issues: List[VerificationIssue] = field(default_factory=list)
    warnings: List[VerificationIssue] = field(default_factory=list)
    info: List[VerificationIssue] = field(default_factory=list)
    pattern_results: Dict[str, Any] = field(default_factory=dict)

    def add_critical(
        self,
        pattern: str,
        run_date: str,
        issue_type: str,
        details: str,
        file: Optional[str] = None,
    ):
        self.critical_issues.append(
            VerificationIssue(pattern, run_date, "CRITICAL", issue_type, details, file)
        )

    def add_warning(
        self,
        pattern: str,
        run_date: str,
        issue_type: str,
        details: str,
        file: Optional[str] = None,
    ):
        self.warnings.append(
            VerificationIssue(pattern, run_date, "WARNING", issue_type, details, file)
        )

    def add_info(
        self,
        pattern: str,
        run_date: str,
        issue_type: str,
        details: str,
        file: Optional[str] = None,
    ):
        self.info.append(
            VerificationIssue(pattern, run_date, "INFO", issue_type, details, file)
        )

    def report(self) -> bool:
        """Generate verification report. Returns True if no critical issues."""
        print(f"\n{'=' * 80}")
        print("BRONZE SAMPLES VERIFICATION REPORT")
        print(f"{'=' * 80}\n")

        print(f"CRITICAL ISSUES: {len(self.critical_issues)}")
        if self.critical_issues:
            for issue in self.critical_issues:
                print(f"  [{issue.pattern}][{issue.run_date}] {issue.issue_type}")
                print(f"    Details: {issue.details}")
                if issue.file:
                    print(f"    File: {issue.file}")
                print()

        print(f"\nWARNINGS: {len(self.warnings)}")
        if self.warnings:
            for warning in self.warnings[:10]:  # Show first 10
                print(
                    f"  [{warning.pattern}][{warning.run_date}] {warning.issue_type}: {warning.details}"
                )
            if len(self.warnings) > 10:
                print(f"  ... and {len(self.warnings) - 10} more warnings")

        print(f"\nINFO: {len(self.info)}")
        if self.info:
            for info_item in self.info[:5]:  # Show first 5
                print(
                    f"  [{info_item.pattern}][{info_item.run_date}] {info_item.issue_type}: {info_item.details}"
                )
            if len(self.info) > 5:
                print(f"  ... and {len(self.info) - 5} more info items")

        print(f"\n{'=' * 80}")
        if len(self.critical_issues) == 0:
            print("[PASS] VERIFICATION PASSED - No critical issues found")
        else:
            print("[FAIL] VERIFICATION FAILED - Critical issues found")
        print(f"{'=' * 80}\n")

        return len(self.critical_issues) == 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary for JSON export."""
        return {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "critical_count": len(self.critical_issues),
                "warning_count": len(self.warnings),
                "info_count": len(self.info),
                "passed": len(self.critical_issues) == 0,
            },
            "critical_issues": [
                {
                    "pattern": i.pattern,
                    "run_date": i.run_date,
                    "issue_type": i.issue_type,
                    "details": i.details,
                    "file": i.file,
                }
                for i in self.critical_issues
            ],
            "warnings": [
                {
                    "pattern": i.pattern,
                    "run_date": i.run_date,
                    "issue_type": i.issue_type,
                    "details": i.details,
                    "file": i.file,
                }
                for i in self.warnings
            ],
            "info": [
                {
                    "pattern": i.pattern,
                    "run_date": i.run_date,
                    "issue_type": i.issue_type,
                    "details": i.details,
                    "file": i.file,
                }
                for i in self.info
            ],
            "pattern_results": self.pattern_results,
        }


class BronzeS3Verifier:
    def __init__(self, bucket: str = "mdf", endpoint_url: str = "http://localhost:9000"):
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.client = self._build_s3_client()
        self.results = BronzeVerificationResult()

    def _build_s3_client(self):
        """Build S3 client using MinIO credentials."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin123",
            region_name="us-east-1",
        )

    def list_bronze_partitions(self, pattern: Optional[str] = None) -> List[str]:
        """List all dt= partitions for a pattern or all patterns."""
        prefix = "bronze_samples/system=retail_demo/table=orders/"
        if pattern:
            prefix += f"pattern={pattern}/"

        partitions = set()
        paginator = self.client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if "/dt=" in key:
                        # Extract partition path up to and including dt=YYYY-MM-DD
                        parts = key.split("/")
                        dt_idx = next(
                            (i for i, p in enumerate(parts) if p.startswith("dt=")),
                            None,
                        )
                        if dt_idx is not None:
                            partition_path = "/".join(parts[: dt_idx + 1]) + "/"
                            partitions.add(partition_path)
        except ClientError as e:
            print(f"Error listing S3 objects: {e}")
            return []

        return sorted(partitions)

    def verify_partition_files(
        self, partition_prefix: str, pattern_name: str
    ) -> Dict[str, Any]:
        """Verify files exist and are readable in a partition."""
        result = {
            "partition": partition_prefix,
            "parquet_files": [],
            "metadata_file": None,
            "checksums_file": None,
            "issues": [],
        }

        # List all files in partition
        paginator = self.client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=partition_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Only consider files within this partition (not subdirectories beyond dt=)
                    relative_key = key[len(partition_prefix) :]
                    if key.endswith(".parquet") and not key.endswith("/_metadata"):
                        result["parquet_files"].append(key)
                    elif key.endswith("_metadata.json"):
                        result["metadata_file"] = key
                    elif key.endswith("_checksums.json"):
                        result["checksums_file"] = key
        except ClientError as e:
            result["issues"].append(f"Error listing partition files: {e}")
            return result

        # Check for required files
        if not result["parquet_files"]:
            result["issues"].append("No parquet files found")
        if not result["metadata_file"]:
            result["issues"].append("Missing _metadata.json")
        if not result["checksums_file"]:
            result["issues"].append("Missing _checksums.json")

        return result

    def check_metadata_corruption(
        self, parquet_key: str, pattern_name: str
    ) -> Optional[Dict]:
        """
        CRITICAL CHECK: Detect metadata-as-data corruption.

        Returns issue dict if corruption found, None otherwise.
        """
        # Create temp file and close it before download (Windows requirement)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_path = tmp.name
        tmp.close()

        try:
            self.client.download_file(self.bucket, parquet_key, tmp_path)
        except ClientError as e:
            import os
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return {
                "file": parquet_key,
                "issue": f"Failed to download file: {e}",
            }

        try:
            df = pd.read_parquet(tmp_path)

            # Check for dict objects in data
            metadata_keys = {"role", "delta_mode", "reference_run_date", "delta_patterns"}

            for col in df.columns:
                # Sample first 100 rows to check for corruption
                sample_size = min(100, len(df))
                for idx in range(sample_size):
                    try:
                        value = df[col].iloc[idx]
                    except (IndexError, KeyError):
                        continue

                    # Check if it's a dict object
                    if isinstance(value, dict):
                        if any(k in value for k in metadata_keys):
                            return {
                                "file": parquet_key,
                                "row": idx,
                                "column": col,
                                "value": str(value)[:200],
                                "type": str(type(value)),
                                "issue": "Metadata dict found in data row",
                            }

                    # Check for stringified metadata dicts
                    if isinstance(value, str) and value.startswith("{"):
                        if any(k in value for k in metadata_keys):
                            return {
                                "file": parquet_key,
                                "row": idx,
                                "column": col,
                                "value": value[:200],
                                "issue": "Stringified metadata dict found in data",
                            }

            return None  # No corruption detected

        except Exception as e:
            return {
                "file": parquet_key,
                "issue": f"Error reading parquet file: {e}",
            }
        finally:
            import os

            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def verify_metadata_json(
        self, metadata_key: str, expected_pattern: str, expected_date: str
    ) -> Dict[str, Any]:
        """Verify _metadata.json contents."""
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=metadata_key)
            metadata = json.loads(obj["Body"].read())
        except ClientError as e:
            return {"metadata": {}, "issues": [f"Failed to read metadata: {e}"]}
        except json.JSONDecodeError as e:
            return {"metadata": {}, "issues": [f"Invalid JSON in metadata: {e}"]}

        issues = []

        # Check required fields
        required = [
            "timestamp",
            "record_count",
            "chunk_count",
            "run_date",
            "system",
            "table",
            "load_pattern",
        ]
        for field in required:
            if field not in metadata:
                issues.append(f"Missing required field: {field}")

        # Validate values
        if metadata.get("system") != "retail_demo":
            issues.append(f"Wrong system: {metadata.get('system')}")
        if metadata.get("table") != "orders":
            issues.append(f"Wrong table: {metadata.get('table')}")
        if metadata.get("run_date") != expected_date:
            issues.append(
                f"run_date mismatch: {metadata.get('run_date')} != {expected_date}"
            )

        # Check data types
        if not isinstance(metadata.get("record_count"), int):
            issues.append("record_count should be int")
        elif metadata.get("record_count", 0) < 0:
            issues.append("record_count should be >= 0")

        if not isinstance(metadata.get("chunk_count"), int):
            issues.append("chunk_count should be int")
        elif metadata.get("chunk_count", 0) < 1:
            issues.append("chunk_count should be >= 1")

        return {"metadata": metadata, "issues": issues}

    def verify_checksums(
        self, checksums_key: str, partition_prefix: str
    ) -> Dict[str, Any]:
        """Verify _checksums.json and validate file hashes (sampled)."""
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=checksums_key)
            manifest = json.loads(obj["Body"].read())
        except ClientError as e:
            return {"issues": [f"Failed to read checksums: {e}"]}
        except json.JSONDecodeError as e:
            return {"issues": [f"Invalid JSON in checksums: {e}"]}

        issues = []

        if "files" not in manifest:
            issues.append("Missing 'files' array in checksum manifest")
            return {"issues": issues}

        # Sample up to 3 files to verify checksums (full verification can be slow)
        files_to_check = manifest["files"][:3] if len(manifest["files"]) > 3 else manifest["files"]

        for file_entry in files_to_check:
            file_name = file_entry.get("path")
            if not file_name:
                issues.append("File entry missing 'path'")
                continue

            file_key = partition_prefix + file_name

            # Verify file exists
            try:
                file_obj = self.client.get_object(Bucket=self.bucket, Key=file_key)
            except ClientError as e:
                issues.append(f"File not found: {file_name} - {e}")
                continue

            # Verify size
            actual_size = file_obj["ContentLength"]
            expected_size = file_entry.get("size_bytes")
            if actual_size != expected_size:
                issues.append(
                    f"Size mismatch for {file_name}: {actual_size} != {expected_size}"
                )

            # Verify checksum (first file only to save time)
            if file_entry == files_to_check[0]:
                hasher = hashlib.sha256()
                for chunk in file_obj["Body"].iter_chunks(chunk_size=1024 * 1024):
                    hasher.update(chunk)
                actual_hash = hasher.hexdigest()
                expected_hash = file_entry.get("sha256")
                if actual_hash != expected_hash:
                    issues.append(f"Checksum mismatch for {file_name}")

        return {"issues": issues, "files_checked": len(files_to_check)}

    def get_source_row_count(self, pattern: str, run_date: str) -> Optional[int]:
        """Get row count from source_samples for comparison."""
        # Construct source path
        source_prefix = f"source_samples/sample={pattern}/system=retail_demo/table=orders/dt={run_date}/"

        try:
            # List files in source partition
            paginator = self.client.get_paginator("list_objects_v2")
            source_files = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=source_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith((".parquet", ".csv")) and not key.endswith("_"):
                        source_files.append(key)

            if not source_files:
                return None

            # Read first source file to get row count
            total_rows = 0
            for source_key in source_files[:1]:  # Check first file only for speed
                tmp = tempfile.NamedTemporaryFile(delete=False)
                tmp_path = tmp.name
                tmp.close()

                try:
                    self.client.download_file(self.bucket, source_key, tmp_path)
                    if source_key.endswith(".parquet"):
                        df = pd.read_parquet(tmp_path)
                    else:  # CSV
                        df = pd.read_csv(tmp_path)
                    total_rows += len(df)
                finally:
                    import os

                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass

            return total_rows

        except Exception as e:
            print(f"Error reading source file for {pattern}/{run_date}: {e}")
            return None

    def verify_pattern(
        self,
        pattern_name: str,
        run_date: Optional[str] = None,
        check_type: str = "all",
    ):
        """Verify all partitions for a pattern."""
        pattern_def = PATTERN_DEFINITIONS.get(pattern_name)
        if not pattern_def:
            print(f"Unknown pattern: {pattern_name}")
            return

        partitions = self.list_bronze_partitions(pattern_name)

        if run_date:
            partitions = [p for p in partitions if f"/dt={run_date}/" in p]

        print(f"\nVerifying pattern: {pattern_name} ({pattern_def['name']})")
        print(f"  Found {len(partitions)} partition(s)")

        if not partitions:
            self.results.add_warning(
                pattern_name, "N/A", "no_partitions", "No bronze partitions found"
            )
            return

        for partition_prefix in partitions:
            # Extract run_date from partition path
            date_match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", partition_prefix)
            partition_date = date_match.group(1) if date_match else "unknown"

            print(f"\n  Checking partition: dt={partition_date}")

            # Always get files list for later checks
            files_result = self.verify_partition_files(partition_prefix, pattern_name)

            # 1. Verify files exist
            if check_type in ["all", "files"]:
                if files_result["issues"]:
                    for issue in files_result["issues"]:
                        self.results.add_critical(
                            pattern_name, partition_date, "missing_files", issue
                        )
                else:
                    print(
                        f"    [OK] Files: {len(files_result['parquet_files'])} parquet, metadata, checksums"
                    )

            # 2. CRITICAL: Check for metadata corruption
            if check_type in ["all", "metadata-leak"]:
                if files_result["parquet_files"]:
                    # Check first parquet file (corruption usually affects all)
                    parquet_key = files_result["parquet_files"][0]
                    corruption = self.check_metadata_corruption(
                        parquet_key, pattern_name
                    )
                    if corruption:
                        self.results.add_critical(
                            pattern_name,
                            partition_date,
                            "metadata_corruption",
                            f"Row: {corruption.get('row', 'N/A')}, "
                            f"Column: {corruption.get('column', 'N/A')}, "
                            f"Issue: {corruption['issue']}",
                            file=corruption["file"],
                        )
                        print(f"    [FAIL] CRITICAL: Metadata corruption detected!")
                    else:
                        print(f"    [OK] No metadata corruption detected")

            # 3. Verify metadata.json
            if check_type in ["all", "metadata"]:
                if files_result.get("metadata_file"):
                    metadata_result = self.verify_metadata_json(
                        files_result["metadata_file"], pattern_name, partition_date
                    )
                    if metadata_result["issues"]:
                        for issue in metadata_result["issues"]:
                            self.results.add_warning(
                                pattern_name, partition_date, "metadata_invalid", issue
                            )
                    else:
                        print(f"    [OK] Metadata valid")

            # 4. Verify checksums (sampled)
            if check_type in ["all", "checksums"]:
                if files_result.get("checksums_file"):
                    checksum_result = self.verify_checksums(
                        files_result["checksums_file"], partition_prefix
                    )
                    if checksum_result.get("issues"):
                        for issue in checksum_result["issues"]:
                            self.results.add_warning(
                                pattern_name, partition_date, "checksum_failure", issue
                            )
                    else:
                        checked = checksum_result.get("files_checked", 0)
                        print(f"    [OK] Checksums valid (sampled {checked} files)")

    def verify_all_patterns(self, check_type: str = "all"):
        """Verify all pattern folders."""
        for pattern_name in PATTERN_DEFINITIONS.keys():
            self.verify_pattern(pattern_name, check_type=check_type)

    def generate_report(self, output_file: Optional[str] = None) -> bool:
        """Generate verification report. Returns True if no critical issues."""
        success = self.results.report()

        if output_file:
            with open(output_file, "w") as f:
                json.dump(self.results.to_dict(), f, indent=2)
            print(f"\nDetailed report written to: {output_file}")

        return success


def main():
    parser = argparse.ArgumentParser(description="Verify bronze_samples S3 data")
    parser.add_argument("--pattern", help="Verify specific pattern only")
    parser.add_argument(
        "--run-date", help="Verify specific run date only (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--check",
        choices=["all", "metadata-leak", "files", "checksums", "metadata"],
        default="all",
        help="Type of check to perform",
    )
    parser.add_argument(
        "--output-report", help="Write detailed JSON report to file"
    )
    parser.add_argument(
        "--bucket", default="mdf", help="S3 bucket name (default: mdf)"
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:9000",
        help="S3 endpoint URL (default: http://localhost:9000)",
    )

    args = parser.parse_args()

    # Create verifier
    print(f"Connecting to S3: {args.endpoint}, bucket: {args.bucket}")
    verifier = BronzeS3Verifier(bucket=args.bucket, endpoint_url=args.endpoint)

    # Run verification
    if args.pattern:
        verifier.verify_pattern(args.pattern, args.run_date, args.check)
    else:
        verifier.verify_all_patterns(args.check)

    # Generate report
    success = verifier.generate_report(args.output_report)

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
