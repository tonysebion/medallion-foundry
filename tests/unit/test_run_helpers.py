"""Tests for the shared pipeline run helpers (now in io.py)."""

import logging

from pipelines.lib.io import maybe_dry_run, maybe_skip_if_exists


def test_maybe_skip_if_exists_without_flag():
    """Should not call exists_fn when skip flag is false."""
    called = False

    def exists_fn():
        nonlocal called
        called = True
        return True

    result = maybe_skip_if_exists(
        skip_if_exists=False,
        exists_fn=exists_fn,
        target="/tmp/output",
        logger=logging.getLogger("test"),
    )

    assert result is None
    assert called is False


def test_maybe_skip_if_exists_returns_skip_result():
    """Should return skip metadata when data exists."""
    runtime = logging.getLogger("test")
    called = False

    def exists_fn():
        nonlocal called
        called = True
        return True

    result = maybe_skip_if_exists(
        skip_if_exists=True,
        exists_fn=exists_fn,
        target="/tmp/output",
        logger=runtime,
        context="tests.orders",
    )

    assert called is True
    assert result == {"skipped": True, "reason": "already_exists", "target": "/tmp/output"}


def test_maybe_dry_run_returns_extra_fields():
    """Should log dry-run result with extra metadata."""
    result = maybe_dry_run(
        dry_run=True,
        logger=logging.getLogger("test"),
        message="[DRY RUN] %s",
        message_args=("noop",),
        target="/tmp/output",
        extra={"rows": 100},
    )

    assert result["dry_run"] is True
    assert result["target"] == "/tmp/output"
    assert result["rows"] == 100
