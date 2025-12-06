Storage plugin framework
========================

The `core.storage` package exposes the plugin-driven storage abstractions:

* `backend.py` creates storage backend instances via the plugin manager.
* `policy.py` validates storage metadata and enforces `--storage-scope` rules.
* `registry.py` / `plugin_manager.py` hold the plugin registry and factory helpers.
* `plugin_factories.py` registers the built-in S3/local/Azure factories, and `plugins/` houses their implementations.

Drop new providers into `core/storage/plugins/`, register them via `core/storage/plugin_factories.py`, and keep any metadata helpers here so the rest of the stack can remain agnostic.
