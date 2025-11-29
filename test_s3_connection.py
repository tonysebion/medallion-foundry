#!/usr/bin/env python3
"""Test script to verify S3/MinIO connection and configuration.

This script tests:
1. Environment config loading
2. S3 connection to MinIO
3. File listing from source_samples
4. URI parsing and bucket resolution
"""

from pathlib import Path
from core.config.environment import EnvironmentConfig
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem


def test_environment_config():
    """Test loading the dev environment configuration."""
    print("=" * 60)
    print("TEST 1: Loading Environment Configuration")
    print("=" * 60)

    env_file = Path("environments/dev.yaml")

    if not env_file.exists():
        print(f"[ERROR] Environment config not found: {env_file}")
        return None

    env_config = EnvironmentConfig.from_yaml(env_file)

    print(f"[OK] Loaded environment: {env_config.name}")
    print(f"   S3 Endpoint: {env_config.s3.endpoint_url}")
    print(f"   Access Key: {env_config.s3.access_key_id}")
    print(f"   Region: {env_config.s3.region}")
    print("   Buckets:")
    for name, bucket in env_config.s3.buckets.items():
        print(f"     - {name} -> {bucket}")
    print()

    return env_config


def test_uri_parsing(env_config):
    """Test URI parsing and bucket resolution."""
    print("=" * 60)
    print("TEST 2: URI Parsing and Bucket Resolution")
    print("=" * 60)

    test_uris = [
        "s3://source_data/source_samples",
        "s3://bronze_data/bronze_samples",
        "s3://mdf/source_samples",
        "./local/path/file.csv",
    ]

    for uri_str in test_uris:
        uri = StorageURI.parse(uri_str)
        fsspec_path = uri.to_fsspec_path(env_config)

        print(f"Original:  {uri_str}")
        print(f"  Backend: {uri.backend}")
        print(f"  Bucket:  {uri.bucket}")
        print(f"  Key:     {uri.key}")
        print(f"  Resolved: {fsspec_path}")
        print()


def test_s3_connection(env_config):
    """Test actual S3 connection to MinIO."""
    print("=" * 60)
    print("TEST 3: S3 Connection to MinIO")
    print("=" * 60)

    try:
        # Parse S3 URI
        uri = StorageURI.parse("s3://source_data/source_samples")
        print(f"Testing URI: {uri.original}")
        print(f"Resolved to: {uri.to_fsspec_path(env_config)}")
        print()

        # Create filesystem
        print("Creating S3 filesystem...")
        fs = create_filesystem(uri, env_config)
        print(f"[OK] Filesystem created: {type(fs).__name__}")
        print()

        # List root of bucket
        print("Listing bucket root (mdf/)...")
        try:
            root_items = fs.ls("mdf", detail=False)
            print(f"[OK] Found {len(root_items)} items in bucket root:")
            for item in root_items[:10]:
                print(f"   - {item}")
            print()
        except Exception as e:
            print(f"[ERROR] Failed to list bucket root: {e}")
            print()

        # List source_samples directory
        print("Listing source_samples directory...")
        try:
            source_items = fs.ls("mdf/source_samples", detail=False)
            print(f"[OK] Found {len(source_items)} items in source_samples:")
            for item in source_items[:10]:
                print(f"   - {item}")
            print()
        except Exception as e:
            print(f"[ERROR] Failed to list source_samples: {e}")
            print()

        # Try to list a specific pattern directory
        print("Listing pattern1_full_events directory...")
        try:
            pattern_path = "mdf/source_samples/sample=pattern1_full_events"
            pattern_items = fs.ls(pattern_path, detail=False)
            print(f"[OK] Found {len(pattern_items)} items:")
            for item in pattern_items[:10]:
                print(f"   - {item}")
            print()
        except Exception as e:
            print(f"[ERROR] Failed to list pattern directory: {e}")
            print()

        return True

    except Exception as e:
        print(f"[ERROR] Failed to connect to S3: {e}")
        print()
        import traceback

        traceback.print_exc()
        return False


def test_file_reading(env_config):
    """Test reading a file from S3."""
    print("=" * 60)
    print("TEST 4: Reading File from S3")
    print("=" * 60)

    try:
        # Find a parquet or CSV file to read
        uri = StorageURI.parse("s3://source_data/source_samples")
        fs = create_filesystem(uri, env_config)

        # Look for parquet files first, then CSV as fallback
        print("Searching for parquet/CSV files...")
        pattern_path = "mdf/source_samples/sample=pattern1_full_events"

        try:
            items = fs.ls(pattern_path, detail=False)

            # Recursively find parquet files first, then CSV
            parquet_file = None
            csv_file = None

            for item in items:
                if item.endswith(".parquet"):
                    parquet_file = item
                    break
                elif item.endswith(".csv"):
                    csv_file = item

                # Check subdirectories
                try:
                    subitems = fs.ls(item, detail=False)
                    for subitem in subitems:
                        if subitem.endswith(".parquet"):
                            parquet_file = subitem
                            break
                        elif subitem.endswith(".csv") and not csv_file:
                            csv_file = subitem
                except:
                    pass

                if parquet_file:
                    break

            # Prefer parquet, fallback to CSV
            if parquet_file:
                print(f"[OK] Found parquet file: {parquet_file}")
                print(f"   Attempting to read first few rows...")

                try:
                    import pandas as pd
                    import io

                    # Read parquet file into memory buffer for pandas
                    with fs.open(parquet_file, "rb") as f:
                        buffer = io.BytesIO(f.read())
                        df = pd.read_parquet(buffer)
                        print(f"   Shape: {df.shape[0]} rows, {df.shape[1]} columns")
                        print(f"   Columns: {', '.join(df.columns[:5])}")
                        if len(df.columns) > 5:
                            print(f"            ... and {len(df.columns) - 5} more")
                        print(f"   First 3 rows:")
                        print(df.head(3).to_string(max_colwidth=30))
                    print()
                except ImportError:
                    print("[WARNING] pandas not available, skipping parquet read test")
                    print()

            elif csv_file:
                print(f"[OK] Found CSV file (no parquet available): {csv_file}")
                print(f"   Attempting to read first few lines...")

                with fs.open(csv_file, "r") as f:
                    lines = [f.readline() for _ in range(5)]
                    print(f"   First 5 lines:")
                    for i, line in enumerate(lines, 1):
                        print(f"     {i}: {line.strip()[:80]}")
                print()
            else:
                print("[WARNING] No parquet or CSV files found in expected location")
                print()

        except Exception as e:
            print(f"[ERROR] Error searching for files: {e}")
            print()

    except Exception as e:
        print(f"[ERROR] Failed to read file: {e}")
        import traceback

        traceback.print_exc()
        print()


def main():
    """Run all tests."""
    print()
    print("=" * 60)
    print(" " * 10 + "S3/MinIO Connection Test Suite")
    print("=" * 60)
    print()

    # Test 1: Load environment config
    env_config = test_environment_config()
    if not env_config:
        print("❌ Cannot proceed without environment config")
        return

    # Test 2: URI parsing
    test_uri_parsing(env_config)

    # Test 3: S3 connection
    connection_ok = test_s3_connection(env_config)

    # Test 4: File reading (only if connection works)
    if connection_ok:
        test_file_reading(env_config)

    print("=" * 60)
    print("Test suite completed!")
    print("=" * 60)
    print()

    if connection_ok:
        print("[OK] All critical tests passed!")
        print()
        print("Next steps:")
        print("1. Try running a pattern with S3 storage")
        print("2. Check the S3_SETUP_GUIDE.md for usage examples")
    else:
        print("[ERROR] Connection failed. Please check:")
        print("1. MinIO is running on port 9000")
        print("2. Credentials in environments/dev.yaml are correct")
        print("3. The 'mdf' bucket exists in MinIO")


if __name__ == "__main__":
    main()
