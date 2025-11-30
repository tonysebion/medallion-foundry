"""Manual S3/MinIO connection script for developer usage.

This file replaces the older `tests/test_s3_connection.py` wrapper; it is not
discovered by pytest because it doesn't start with `test_`. Run interactively:

    python tests/manual_s3_connection.py

It delegates to `scripts/check_s3_connection.py` for the actual implementation.
"""

if __name__ == "__main__":
    from scripts.check_s3_connection import main
    import sys

    sys.exit(main())

