Runner orchestration
====================

This package organizes Bronze execution helpers:

* `chunks.py` manages chunk writers, storage plans, and parallel execution so the writer assembly is isolated.
* `job.py` wires extractor discovery, chunk processing, metadata emission, and cleanup into `ExtractJob`.
* The package exports `run_extract` so CLI entry points can stay thin wrappers over this shared flow.

Add new runner helpers here (e.g., telemetry hooks or new chunk strategies) so the CLI scripts remain nearly identity functions.
