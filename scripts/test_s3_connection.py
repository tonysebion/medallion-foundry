#!/usr/bin/env python
"""Simple S3 connection test script.

This script tests S3 connectivity using the project's S3Storage class.
Edit the configuration values below and run:

    python scripts/test_s3_connection.py

"""

from __future__ import annotations

import json
import sys
from datetime import datetime

# ============ EDIT THESE VALUES ============
S3_ENDPOINT_URL = "https://objects.nutanix.local:443"  # Your S3-compatible endpoint
S3_ACCESS_KEY = "your-access-key-here"
S3_SECRET_KEY = "your-secret-key-here"
S3_BUCKET = "your-bucket-name"
S3_REGION = "us-east-1"  # Usually us-east-1 for Nutanix/MinIO
# ===========================================


def main() -> int:
    """Run S3 connection tests."""
    # Import here so config validation happens before import errors
    from pipelines.lib.storage.s3 import S3Storage

    print("S3 Connection Test Script")
    print("=" * 50)
    print(f"Endpoint: {S3_ENDPOINT_URL}")
    print(f"Bucket:   {S3_BUCKET}")
    print(f"Region:   {S3_REGION}")

    # Validate configuration
    if S3_ACCESS_KEY == "your-access-key-here":
        print("\nERROR: Please edit the configuration values at the top of this script.")
        return 1

    # Create test path with timestamp to avoid collisions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_file = f"hello_world_{timestamp}.json"
    test_path = f"s3://{S3_BUCKET}/test/{test_file}"

    print(f"\nTest path: {test_path}")
    print()

    # Initialize S3Storage with Nutanix-compatible settings
    storage = S3Storage(
        f"s3://{S3_BUCKET}/test/",
        endpoint_url=S3_ENDPOINT_URL,
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        region=S3_REGION,
        signature_version="s3v4",  # Required for most S3-compatible storage
        addressing_style="path",  # Required for Nutanix/MinIO
    )

    # Test data
    test_data = {
        "message": "Hello, S3!",
        "timestamp": datetime.now().isoformat(),
        "source": "test_s3_connection.py",
    }
    test_bytes = json.dumps(test_data, indent=2).encode("utf-8")

    passed = 0
    failed = 0

    # Test 1: Write
    print("[1/4] Writing test file...", end=" ", flush=True)
    try:
        result = storage.write_bytes(test_file, test_bytes)
        if result.success:
            print(f"OK ({result.bytes_written} bytes written)")
            passed += 1
        else:
            print(f"FAILED: {result.error}")
            failed += 1
    except Exception as e:
        print(f"FAILED: {e}")
        failed += 1

    # Test 2: Exists
    print("[2/4] Verifying file exists...", end=" ", flush=True)
    try:
        if storage.exists(test_file):
            print("OK")
            passed += 1
        else:
            print("FAILED: File not found")
            failed += 1
    except Exception as e:
        print(f"FAILED: {e}")
        failed += 1

    # Test 3: Read
    print("[3/4] Reading file back...", end=" ", flush=True)
    try:
        read_bytes = storage.read_bytes(test_file)
        if read_bytes == test_bytes:
            print("OK (content matches)")
            passed += 1
        else:
            print("FAILED: Content mismatch")
            failed += 1
    except Exception as e:
        print(f"FAILED: {e}")
        failed += 1

    # Test 4: List
    print("[4/4] Listing files...", end=" ", flush=True)
    try:
        files = storage.list_files()
        matching = [f for f in files if test_file in f.path]
        if matching:
            print(f"OK ({len(files)} file(s) found)")
            passed += 1
        else:
            print(f"FAILED: Test file not in list (found {len(files)} files)")
            failed += 1
    except Exception as e:
        print(f"FAILED: {e}")
        failed += 1

    # Cleanup
    print("\nCleaning up test file...", end=" ", flush=True)
    try:
        if storage.delete(test_file):
            print("OK")
        else:
            print("FAILED (non-critical)")
    except Exception as e:
        print(f"FAILED: {e} (non-critical)")

    # Summary
    print()
    if failed == 0:
        print(f"All {passed} tests passed! S3 configuration is working.")
        return 0
    else:
        print(f"Results: {passed} passed, {failed} failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
