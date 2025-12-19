"""Tests for the pipelines checksum helpers."""

from pathlib import Path


from pipelines.lib.checksum import (
    ChecksumManifest,
    ChecksumVerificationResult,
    compute_file_sha256,
    verify_checksum_manifest,
    write_checksum_manifest,
)


class TestChecksumResult:
    def test_to_dict_contains_metadata(self):
        manifest = ChecksumManifest(
            timestamp="2025-01-01T00:00:00Z",
            files=[{"path": "data.parquet", "size_bytes": 10, "sha256": "abc"}],
            row_count=10,
            entity_kind="bronze",
        )
        result = ChecksumVerificationResult(
            valid=True,
            verified_files=["data.parquet"],
            missing_files=[],
            mismatched_files=[],
            manifest=manifest,
            verification_time_ms=12.3,
        )

        assert result.valid is True
        assert result.verification_time_ms == 12.3
        assert result.manifest.row_count == 10


def test_compute_file_sha256(tmp_path: Path):
    file = tmp_path / "data.parquet"
    file.write_bytes(b"hello")
    h = compute_file_sha256(file)
    assert len(h) == 64
    assert isinstance(h, str)


class TestChecksumManifestVerification:
    def test_valid_manifest(self, tmp_path: Path):
        file1 = tmp_path / "chunk_0001.parquet"
        file2 = tmp_path / "chunk_0002.parquet"
        file1.write_bytes(b"a")
        file2.write_bytes(b"b")

        manifest_path = write_checksum_manifest(tmp_path, [file1, file2], row_count=2)
        assert manifest_path.exists()

        result = verify_checksum_manifest(tmp_path)
        assert result.valid
        assert len(result.verified_files) == 2

    def test_missing_file(self, tmp_path: Path):
        file = tmp_path / "data.parquet"
        file.write_bytes(b"x")
        write_checksum_manifest(tmp_path, [file], row_count=1)
        file.unlink()

        result = verify_checksum_manifest(tmp_path)
        assert not result.valid
        assert "data.parquet" in result.missing_files

    def test_mismatched_file(self, tmp_path: Path):
        file = tmp_path / "data.parquet"
        file.write_bytes(b"original")
        write_checksum_manifest(tmp_path, [file], row_count=1)
        file.write_bytes(b"changed")

        result = verify_checksum_manifest(tmp_path)
        assert not result.valid
        assert "data.parquet" in result.mismatched_files

    def test_missing_manifest(self, tmp_path: Path):
        result = verify_checksum_manifest(tmp_path)
        assert not result.valid
        assert "_checksums.json" in result.missing_files

    def test_malformed_manifest(self, tmp_path: Path):
        manifest_path = tmp_path / "_checksums.json"
        manifest_path.write_text("{ invalid }")
        result = verify_checksum_manifest(tmp_path)
        assert not result.valid
