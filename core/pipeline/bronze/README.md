Bronze data helpers
====================

This package contains reusable Bronze routines:

* `io.py` (also re-exported via `core/io.py`) chunks records, writes CSV/Parquet slices, writes batch metadata, and maintains checksum manifests.

Use these helpers whenever you need to manipulate Bronze partitions, generate manifests, or measure chunking behavior.
