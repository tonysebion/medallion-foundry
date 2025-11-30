"""Manual S3 pipeline config check for developer usage.

This file replaces the older `tests/test_s3_pipeline.py` wrapper; it is not
discovered by pytest because it doesn't start with `test_`. Run interactively:

    python tests/manual_s3_pipeline.py

It delegates to `scripts/check_s3_pipeline.py` for the actual implementation.
"""

if __name__ == "__main__":
    from scripts.check_s3_pipeline import main
    import sys

    sys.exit(main())

