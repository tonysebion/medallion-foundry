## Legacy Streaming & Resume (retired)

This framework previously supported a `silver_extract.py --stream --resume` mode that processed Bronze files chunk-by-chunk while keeping checkpoints to skip already-processed portions on rerun. The new `SilverProcessor` unifies Bronze/Silver handling and writes chunked outputs through the standard dataset writer, so there is no longer a separate streaming mode.

If you used to rely on `--stream`/`--resume`:

1. Just re-run `bronze_extract.py --config <config> --date <date>` to rewrite Bronze safely.
2. Run `silver_extract.py --config <config> --date <date>` (or `scripts/run_intent_config.py`) to promote the same data; the idempotent metadata/checksum flow replaces checkpoint resume.
3. Review `docs/framework/reference/intent-lifecycle.md` for the updated Bronze→Silver path and `docs/framework/manifesto-playbook.md` for how that flow matches the Modern Data Pipeline Manifesto.

The old streaming helper has been removed from the codebase; this doc is kept for historical reference only.
