"""Test script to verify S3 config parsing and file reading."""

from pathlib import Path
import sys

from core.config.loader import load_config_with_env

# Unused imports removed to satisfy flake8
from core.storage.uri import StorageURI
from core.storage.filesystem import create_filesystem


def test_config_loading(config_path: Path):
    """Test that the new storage config format loads correctly."""
    print("=" * 80)
    print("STEP 1: Config Loading - Testing New Storage Section")
    print("=" * 80)

    dataset, env_config = load_config_with_env(config_path)

    print("\n[OK] Config loaded successfully")
    print(f"Dataset: {dataset.system}.{dataset.entity}")
    print(f"Environment: {dataset.environment}")
    print()
    print("Storage Configuration:")
    print(f"  Source storage: {dataset.bronze.source_storage}")
    print(f"  Source path: {dataset.bronze.path_pattern}")
    print()
    print(f"  Bronze output storage: {dataset.bronze.output_storage}")
    print(f"  Bronze output bucket: {dataset.bronze.output_bucket}")
    print(f"  Bronze output prefix: {dataset.bronze.output_prefix}")
    print()
    print(f"  Silver input storage: {dataset.silver.input_storage}")
    print(f"  Silver output storage: {dataset.silver.output_storage}")
    print(f"  Silver output bucket: {dataset.silver.output_bucket}")
    print(f"  Silver output prefix: {dataset.silver.output_prefix}")

    return dataset, env_config


def test_s3_source_reading(dataset, env_config):
    """Test reading source files from S3."""
    print("\n" + "=" * 80)
    print("STEP 2: Source Reading - Reading CSV from S3")
    print("=" * 80)

    print("\nSource path: {}".format(dataset.bronze.path_pattern))

    try:
        # Parse the S3 URI
        uri = StorageURI.parse(dataset.bronze.path_pattern)
        print("Parsed URI:")
        print(f"  Backend: {uri.backend}")
        print(f"  Bucket: {uri.bucket}")
        print(f"  Key: {uri.key}")

        # Create filesystem
        fs = create_filesystem(uri, env_config)
        fsspec_path = uri.to_fsspec_path(env_config)
        print("\nResolved path: {}".format(fsspec_path))

        # Try to read the file
        print("\nAttempting to read file...")
        with fs.open(fsspec_path, "r") as f:
            # Read first 5 lines
            lines = [f.readline() for _ in range(5)]

        print("[OK] Successfully read {} lines from S3".format(len(lines)))
        print("\nFirst 3 lines:")
        for i, line in enumerate(lines[:3], 1):
            print(f"  {i}: {line.strip()[:100]}")

        return True

    except Exception as e:
        print(f"[ERROR] Failed to read from S3: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_s3_write_simulation(dataset, env_config):
    """Test S3 write path construction (simulation only)."""
    print("\n" + "=" * 80)
    print("STEP 3: Write Path Construction - S3 Output Paths")
    print("=" * 80)

    # Bronze output path
    bronze_bucket = env_config.s3.get_bucket(dataset.bronze.output_bucket)
    bronze_prefix = dataset.bronze.output_prefix
    bronze_path = f"s3://{bronze_bucket}/{bronze_prefix}system={dataset.system}/entity={dataset.entity}/run_date=2025-11-13/"

    print("\nBronze output would be written to:")
    print(f"  {bronze_path}")

    # Silver output path
    silver_bucket = env_config.s3.get_bucket(dataset.silver.output_bucket)
    silver_prefix = dataset.silver.output_prefix
    silver_path = f"s3://{silver_bucket}/{silver_prefix}system={dataset.system}/entity={dataset.entity}/event_date=2025-11-13/"

    print("\nSilver output would be written to:")
    print(f"  {silver_path}")

    print("\n[OK] Output path construction successful")
    return True


def main():
    """Test S3 configuration and connectivity."""
    config_path = Path("docs/examples/configs/patterns/pattern_full.yaml")

    print("\n" + "=" * 80)
    print("S3 Configuration Test: New Storage Section Format")
    print("=" * 80)
    print(f"Config: {config_path}")

    # Step 1: Load config
    dataset, env_config = test_config_loading(config_path)

    if not dataset or not env_config:
        print("\n[ERROR] Config loading failed")
        return 1

    # Step 2: Test reading from S3
    read_ok = test_s3_source_reading(dataset, env_config)

    if not read_ok:
        print("\n[ERROR] S3 source reading failed")
        return 1

    # Step 3: Test write path construction
    write_ok = test_s3_write_simulation(dataset, env_config)

    if write_ok:
        print("\n" + "=" * 80)
        print("[OK] All S3 configuration tests PASSED!")
        print("=" * 80)
        print("\nThe new storage section is working correctly:")
        print("  ✓ Config parsing")
        print("  ✓ S3 bucket resolution (bucket names from environment)")
        print("  ✓ Source file reading from S3")
        print("  ✓ Output path construction for Bronze and Silver")
        print("\nNext step: Run full Bronze->Silver pipeline")
        return 0
    else:
        print("\n[ERROR] Test FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
