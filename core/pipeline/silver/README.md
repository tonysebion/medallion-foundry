Silver artifact helpers
=======================

This package groups the Silver-layer logic:

* `artifacts.py` handles normalization, deduplication, artifact planning, and dataset writing for each Silver model.
* `models.py` maintains the `SilverModel` enum and profile aliases.
* `stream.py` implements the streaming promotion path that mirror `silver_stream.py`.

Put future Silver-specific helpers (quality guards, lineage writers, etc.) here so the CLI layer (`silver_extract.py`) remains focused on orchestration.
