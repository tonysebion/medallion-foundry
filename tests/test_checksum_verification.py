"""Tests for checksum verification and quarantine functionality.

Story #12: Chunk File Metadata Integrity Verification
"""

import json
import time
from pathlib import Path

import pytest

from core.io.storage.checksum import (
    ChecksumVerificationResult,
    compute_file_sha256,
    verify_checksum_manifest_with_result,
    write_checksum_manifest,
)
from core.io.storage.quarantine import (
    QuarantineConfig,
    QuarantineResult,
    quarantine_corrupted_files,
)


class TestChecksumVerificationResult:
    """Tests for ChecksumVerificationResult dataclass."""

    def test_valid_result_properties(self) -> None:
        """Test properties of a valid result."""
        result = ChecksumVerificationResult(
            valid=True,
            verified_files=["chunk_0001.parquet", "chunk_0002.parquet"],
            missing_files=[],
            mismatched_files=[],
            manifest={"load_pattern": "snapshot"},
            verification_time_ms=42.5,
        )
        assert result.valid
        assert len(result.verified_files) == 2
        assert result.verification_time_ms == 42.5
        assert "VALID" in str(result)

    def test_invalid_result_properties(self) -> None:
        """Test properties of an invalid result."""
        result = ChecksumVerificationResult(
            valid=False,
            verified_files=["chunk_0001.parquet"],
            missing_files=["chunk_0002.parquet"],
            mismatched_files=["chunk_0003.parquet"],
            manifest={},
            verification_time_ms=100.0,
        )
        assert not result.valid
        assert len(result.missing_files) == 1
        assert len(result.mismatched_files) == 1
        assert "INVALID" in str(result)

    def test_to_dict_roundtrip(self) -> None:
        """Test serialization roundtrip."""
        original = ChecksumVerificationResult(
            valid=True,
            verified_files=["file1.parquet", "file2.parquet"],
            missing_files=[],
            mismatched_files=[],
            manifest={"load_pattern": "snapshot"},
            verification_time_ms=55.5,
        )
        data = original.to_dict()
        restored = ChecksumVerificationResult.from_dict(data)

        assert restored.valid == original.valid
        assert restored.verified_files == original.verified_files
        assert restored.verification_time_ms == original.verification_time_ms


class TestVerifyChecksumManifestWithResult:
    """Tests for verify_checksum_manifest_with_result function."""

    def test_verify_valid_manifest(self, tmp_path: Path) -> None:
        """Test verification passes for valid files."""
        # Create test files
        file1 = tmp_path / "chunk_0001.parquet"
        file2 = tmp_path / "chunk_0002.parquet"
        file1.write_bytes(b"test data for chunk 1")
        file2.write_bytes(b"test data for chunk 2")

        # Write manifest
        write_checksum_manifest(tmp_path, [file1, file2], "snapshot")

        # Verify
        result = verify_checksum_manifest_with_result(tmp_path)

        assert result.valid
        assert len(result.verified_files) == 2
        assert "chunk_0001.parquet" in result.verified_files
        assert "chunk_0002.parquet" in result.verified_files
        assert not result.mismatched_files
        assert not result.missing_files
        assert result.verification_time_ms > 0
        assert result.manifest.get("load_pattern") == "snapshot"

    def test_verify_corrupted_file(self, tmp_path: Path) -> None:
        """Test verification fails for corrupted files."""
        file_path = tmp_path / "data.parquet"
        file_path.write_bytes(b"original data")

        # Write manifest with original hash
        write_checksum_manifest(tmp_path, [file_path], "snapshot")

        # Corrupt the file
        file_path.write_bytes(b"corrupted data - different content")

        # Verify
        result = verify_checksum_manifest_with_result(tmp_path)

        assert not result.valid
        assert len(result.mismatched_files) == 1
        assert "data.parquet" in result.mismatched_files
        assert not result.verified_files

    def test_verify_missing_file(self, tmp_path: Path) -> None:
        """Test verification fails for missing files."""
        file_path = tmp_path / "data.parquet"
        file_path.write_bytes(b"test data")

        # Write manifest
        write_checksum_manifest(tmp_path, [file_path], "snapshot")

        # Delete the file
        file_path.unlink()

        # Verify
        result = verify_checksum_manifest_with_result(tmp_path)

        assert not result.valid
        assert len(result.missing_files) == 1
        assert "data.parquet" in result.missing_files

    def test_verify_missing_manifest(self, tmp_path: Path) -> None:
        """Test verification returns invalid when manifest is missing."""
        result = verify_checksum_manifest_with_result(tmp_path)

        assert not result.valid
        assert "_checksums.json" in result.missing_files

    def test_verify_malformed_manifest(self, tmp_path: Path) -> None:
        """Test verification handles malformed JSON gracefully."""
        manifest_path = tmp_path / "_checksums.json"
        manifest_path.write_text("{ invalid json }")

        result = verify_checksum_manifest_with_result(tmp_path)

        assert not result.valid

    def test_verify_pattern_mismatch(self, tmp_path: Path) -> None:
        """Test verification fails when load pattern doesn't match."""
        file_path = tmp_path / "data.parquet"
        file_path.write_bytes(b"test data")

        write_checksum_manifest(tmp_path, [file_path], "snapshot")

        result = verify_checksum_manifest_with_result(
            tmp_path, expected_pattern="incremental_append"
        )

        assert not result.valid

    def test_verify_empty_manifest(self, tmp_path: Path) -> None:
        """Test verification handles empty files list."""
        manifest = {
            "timestamp": "2025-01-01T00:00:00Z",
            "load_pattern": "snapshot",
            "files": [],
        }
        manifest_path = tmp_path / "_checksums.json"
        manifest_path.write_text(json.dumps(manifest))

        result = verify_checksum_manifest_with_result(tmp_path)

        # Empty manifest should be invalid (no files to verify)
        assert not result.valid


class TestQuarantineConfig:
    """Tests for QuarantineConfig dataclass."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = QuarantineConfig()
        assert config.enabled
        assert config.quarantine_dir == "_quarantine"
        assert config.write_manifest

    def test_from_dict(self) -> None:
        """Test creation from dictionary."""
        data = {
            "enabled": False,
            "quarantine_dir": "bad_files",
            "write_manifest": False,
        }
        config = QuarantineConfig.from_dict(data)

        assert not config.enabled
        assert config.quarantine_dir == "bad_files"
        assert not config.write_manifest

    def test_from_dict_none(self) -> None:
        """Test from_dict with None returns defaults."""
        config = QuarantineConfig.from_dict(None)
        assert config.enabled
        assert config.quarantine_dir == "_quarantine"


class TestQuarantineCorruptedFiles:
    """Tests for quarantine_corrupted_files function."""

    def test_quarantine_single_file(self, tmp_path: Path) -> None:
        """Test quarantining a single file."""
        bad_file = tmp_path / "bad.parquet"
        bad_file.write_bytes(b"corrupted data")

        result = quarantine_corrupted_files(
            tmp_path,
            ["bad.parquet"],
            reason="checksum_mismatch",
        )

        assert result.count == 1
        assert not bad_file.exists()
        assert (tmp_path / "_quarantine" / "bad.parquet").exists()
        assert result.reason == "checksum_mismatch"

    def test_quarantine_multiple_files(self, tmp_path: Path) -> None:
        """Test quarantining multiple files."""
        files = []
        for i in range(3):
            f = tmp_path / f"bad_{i}.parquet"
            f.write_bytes(f"bad data {i}".encode())
            files.append(f"bad_{i}.parquet")

        result = quarantine_corrupted_files(tmp_path, files)

        assert result.count == 3
        for filename in files:
            assert not (tmp_path / filename).exists()
            assert (tmp_path / "_quarantine" / filename).exists()

    def test_quarantine_disabled(self, tmp_path: Path) -> None:
        """Test quarantine when disabled."""
        bad_file = tmp_path / "bad.parquet"
        bad_file.write_bytes(b"bad data")

        config = QuarantineConfig(enabled=False)
        result = quarantine_corrupted_files(
            tmp_path, ["bad.parquet"], config=config
        )

        assert result.count == 0
        assert bad_file.exists()  # File should still exist
        assert "bad.parquet" in result.failed_files

    def test_quarantine_nonexistent_file(self, tmp_path: Path) -> None:
        """Test quarantining a file that doesn't exist."""
        result = quarantine_corrupted_files(
            tmp_path, ["nonexistent.parquet"]
        )

        assert result.count == 0
        assert "nonexistent.parquet" in result.failed_files

    def test_quarantine_writes_manifest(self, tmp_path: Path) -> None:
        """Test that quarantine writes a manifest file."""
        bad_file = tmp_path / "bad.parquet"
        bad_file.write_bytes(b"bad data")

        quarantine_corrupted_files(
            tmp_path, ["bad.parquet"], reason="test_reason"
        )

        manifest_path = tmp_path / "_quarantine" / "_quarantine_manifest.json"
        assert manifest_path.exists()

        with manifest_path.open() as f:
            manifest = json.load(f)
        assert manifest["reason"] == "test_reason"
        assert len(manifest["files"]) == 1

    def test_quarantine_empty_list(self, tmp_path: Path) -> None:
        """Test quarantining with empty file list."""
        result = quarantine_corrupted_files(tmp_path, [])

        assert result.count == 0
        assert not result.failed_files

    def test_quarantine_result_to_dict(self, tmp_path: Path) -> None:
        """Test QuarantineResult serialization."""
        bad_file = tmp_path / "bad.parquet"
        bad_file.write_bytes(b"bad data")

        result = quarantine_corrupted_files(
            tmp_path, ["bad.parquet"], reason="test"
        )
        data = result.to_dict()

        assert data["reason"] == "test"
        assert len(data["quarantined_files"]) == 1
        assert data["quarantine_path"] is not None


class TestChecksumPerformance:
    """Performance tests for checksum verification."""

    @pytest.mark.slow
    def test_verification_completes_reasonably(self, tmp_path: Path) -> None:
        """Verify checksum verification completes in reasonable time.

        This test creates multiple files and verifies that checksum
        verification completes within acceptable bounds. SHA256 hashing
        does add computational overhead beyond just reading files.
        """
        # Create 20 files of 100KB each (2MB total)
        files = []
        for i in range(20):
            f = tmp_path / f"chunk_{i:04d}.parquet"
            f.write_bytes(b"x" * 100_000)
            files.append(f)

        # Write checksum manifest
        write_checksum_manifest(tmp_path, files, "snapshot")

        # Measure verification time
        start = time.perf_counter()
        result = verify_checksum_manifest_with_result(tmp_path)
        verification_time = time.perf_counter() - start

        assert result.valid
        assert len(result.verified_files) == 20

        # Verification of 2MB should complete in under 2 seconds
        # (very generous for any reasonable system)
        assert verification_time < 2.0, (
            f"Verification took {verification_time:.2f}s, expected < 2.0s"
        )

        # Verify timing is tracked
        assert result.verification_time_ms > 0


class TestIntegration:
    """Integration tests combining checksum verification and quarantine."""

    def test_verify_and_quarantine_workflow(self, tmp_path: Path) -> None:
        """Test the complete verify -> quarantine workflow."""
        # Create good and bad files
        good_file = tmp_path / "good.parquet"
        bad_file = tmp_path / "bad.parquet"
        good_file.write_bytes(b"good data")
        bad_file.write_bytes(b"original bad data")

        # Write manifest
        write_checksum_manifest(tmp_path, [good_file, bad_file], "snapshot")

        # Corrupt the bad file
        bad_file.write_bytes(b"corrupted!")

        # Verify
        result = verify_checksum_manifest_with_result(tmp_path)
        assert not result.valid
        assert "good.parquet" in result.verified_files
        assert "bad.parquet" in result.mismatched_files

        # Quarantine corrupted files
        quarantine_result = quarantine_corrupted_files(
            tmp_path,
            result.mismatched_files,
            reason="checksum_mismatch",
        )

        assert quarantine_result.count == 1
        assert not bad_file.exists()
        assert good_file.exists()
        assert (tmp_path / "_quarantine" / "bad.parquet").exists()
