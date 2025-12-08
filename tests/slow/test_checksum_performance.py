"""Slow performance checks for checksum verification."""

import time
from pathlib import Path

import pytest

from core.infrastructure.io.storage.checksum import (
    verify_checksum_manifest_with_result,
    write_checksum_manifest,
)


class TestChecksumPerformance:
    """Performance tests for checksum verification."""

    @pytest.mark.slow
    def test_verification_completes_reasonably(self, tmp_path: Path) -> None:
        """Verify checksum verification completes in reasonable time."""
        files = []
        for i in range(20):
            f = tmp_path / f"chunk_{i:04d}.parquet"
            f.write_bytes(b"x" * 100_000)
            files.append(f)

        write_checksum_manifest(tmp_path, files, "snapshot")

        start = time.perf_counter()
        result = verify_checksum_manifest_with_result(tmp_path)
        verification_time = time.perf_counter() - start

        assert result.valid
        assert len(result.verified_files) == 20
        assert verification_time < 2.0, (
            f"Verification took {verification_time:.2f}s, expected < 2.0s"
        )
        assert result.verification_time_ms > 0
