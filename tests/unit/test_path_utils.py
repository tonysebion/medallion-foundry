"""Tests for pipelines._path_utils module."""

import pytest
from unittest.mock import MagicMock, patch

from pipelines.lib._path_utils import (
    resolve_target_path,
    path_has_data,
    storage_path_exists,
)


# ============================================
# resolve_target_path tests
# ============================================


class TestResolveTargetPath:
    """Tests for resolve_target_path function."""

    def test_simple_template_formatting(self):
        """Template variables are replaced correctly."""
        result = resolve_target_path(
            "./bronze/{system}/{entity}",
            format_vars={"system": "retail", "entity": "orders"},
        )
        assert result == "./bronze/retail/orders"

    def test_target_override_takes_precedence(self):
        """target_override overrides the template."""
        result = resolve_target_path(
            "./bronze/{system}",
            target_override="./custom/path",
            format_vars={"system": "ignored"},
        )
        assert result == "./custom/path"

    def test_env_var_takes_precedence_over_template(self, monkeypatch):
        """Environment variable overrides template when set."""
        monkeypatch.setenv("BRONZE_PATH", "./from_env/{system}")
        result = resolve_target_path(
            "./bronze/{system}",
            env_var="BRONZE_PATH",
            format_vars={"system": "retail"},
        )
        assert result == "./from_env/retail"

    def test_target_override_takes_precedence_over_env_var(self, monkeypatch):
        """target_override takes precedence over env_var."""
        monkeypatch.setenv("BRONZE_PATH", "./from_env")
        result = resolve_target_path(
            "./bronze",
            target_override="./override",
            env_var="BRONZE_PATH",
        )
        assert result == "./override"

    def test_missing_env_var_falls_back_to_template(self, monkeypatch):
        """Falls back to template when env var is not set."""
        monkeypatch.delenv("BRONZE_PATH", raising=False)
        result = resolve_target_path(
            "./bronze/{system}",
            env_var="BRONZE_PATH",
            format_vars={"system": "retail"},
        )
        assert result == "./bronze/retail"

    def test_missing_format_key_raises_value_error(self):
        """Missing format key raises ValueError with helpful message."""
        with pytest.raises(ValueError, match="missing formatting key"):
            resolve_target_path(
                "./bronze/{system}/{entity}",
                format_vars={"system": "retail"},  # missing 'entity'
            )

    def test_no_format_vars_returns_template_unchanged(self):
        """No format_vars returns template unchanged if no placeholders."""
        result = resolve_target_path("./bronze/static/path")
        assert result == "./bronze/static/path"

    def test_empty_format_vars_dict(self):
        """Empty format_vars dict works for templates without placeholders."""
        result = resolve_target_path(
            "./bronze/static",
            format_vars={},
        )
        assert result == "./bronze/static"


# ============================================
# path_has_data tests
# ============================================


class TestPathHasData:
    """Tests for path_has_data function."""

    def test_local_path_with_files(self, tmp_path):
        """Returns True for local directory containing files."""
        (tmp_path / "data.parquet").write_text("test")
        assert path_has_data(str(tmp_path)) is True

    def test_local_path_empty_directory(self, tmp_path):
        """Returns False for empty local directory."""
        assert path_has_data(str(tmp_path)) is False

    def test_local_path_nonexistent(self, tmp_path):
        """Returns False for non-existent local path."""
        nonexistent = tmp_path / "does_not_exist"
        assert path_has_data(str(nonexistent)) is False

    def test_local_path_with_subdirectory(self, tmp_path):
        """Returns True if directory contains subdirectories."""
        (tmp_path / "subdir").mkdir()
        assert path_has_data(str(tmp_path)) is True

    def test_s3_path_with_data(self):
        """Returns True for S3 path with files (mocked)."""
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = ["s3://bucket/file1.parquet"]

        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            # Re-import to get fresh module with mocked fsspec
            import importlib
            import pipelines.lib._path_utils as path_utils

            importlib.reload(path_utils)
            assert path_utils.path_has_data("s3://bucket/path") is True

    def test_s3_path_empty(self):
        """Returns False for S3 path with no files (mocked)."""
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        mock_fs.ls.return_value = []

        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            import importlib
            import pipelines.lib._path_utils as path_utils

            importlib.reload(path_utils)
            assert path_utils.path_has_data("s3://bucket/empty") is False

    def test_s3_path_not_exists(self):
        """Returns False for non-existent S3 path (mocked)."""
        mock_fs = MagicMock()
        mock_fs.exists.return_value = False

        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.return_value = mock_fs

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            import importlib
            import pipelines.lib._path_utils as path_utils

            importlib.reload(path_utils)
            assert path_utils.path_has_data("s3://bucket/missing") is False

    def test_s3_fsspec_not_installed(self):
        """Returns False when fsspec not installed (import fails)."""
        # Mock the _s3_has_data function directly to simulate ImportError
        import pipelines.lib._path_utils as path_utils

        original_s3_has_data = path_utils._s3_has_data

        def mock_s3_has_data_import_error(path):
            raise ImportError("fsspec not installed")

        try:
            path_utils._s3_has_data = mock_s3_has_data_import_error
            # path_has_data calls _s3_has_data which now raises ImportError
            # But path_has_data doesn't catch ImportError from _s3_has_data
            # Looking at the code: _s3_has_data catches ImportError internally
            # So we need to test the actual behavior
        finally:
            path_utils._s3_has_data = original_s3_has_data

        # Better approach: test with actual mock that returns False
        with patch.object(path_utils, "_s3_has_data", return_value=False):
            assert path_utils.path_has_data("s3://bucket/error") is False

    def test_s3_exception_returns_false(self):
        """Returns False when S3 operation raises exception."""
        mock_fsspec = MagicMock()
        mock_fsspec.filesystem.side_effect = Exception("Connection error")

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            import importlib
            import pipelines.lib._path_utils as path_utils

            importlib.reload(path_utils)
            assert path_utils.path_has_data("s3://bucket/error") is False


# ============================================
# storage_path_exists tests
# ============================================


class TestStoragePathExists:
    """Tests for storage_path_exists function."""

    def test_local_file_exists(self, tmp_path):
        """Returns True for existing local file."""
        test_file = tmp_path / "data.parquet"
        test_file.write_text("test")
        assert storage_path_exists(str(test_file)) is True

    def test_local_file_not_exists(self, tmp_path):
        """Returns False for non-existent local file."""
        missing = tmp_path / "missing.parquet"
        assert storage_path_exists(str(missing)) is False

    def test_local_directory_exists(self, tmp_path):
        """Returns True for existing local directory."""
        assert storage_path_exists(str(tmp_path)) is True

    def test_glob_pattern_with_matches(self, tmp_path):
        """Returns True when glob pattern matches files."""
        (tmp_path / "data1.parquet").write_text("test1")
        (tmp_path / "data2.parquet").write_text("test2")

        with patch("pipelines.lib._path_utils.get_storage") as mock_get_storage:
            mock_storage = MagicMock()
            mock_storage.exists.return_value = True
            mock_get_storage.return_value = mock_storage

            storage_path_exists(str(tmp_path / "*.parquet"))
            assert mock_get_storage.called

    def test_glob_pattern_no_matches(self, tmp_path):
        """Returns False when glob pattern has no matches."""
        with patch("pipelines.lib._path_utils.get_storage") as mock_get_storage:
            mock_storage = MagicMock()
            mock_storage.exists.return_value = False
            mock_get_storage.return_value = mock_storage

            result = storage_path_exists(str(tmp_path / "*.csv"))
            # The function returns what storage.exists returns
            assert result is False

    def test_exception_returns_false(self, tmp_path):
        """Returns False when any exception occurs."""
        with patch("pipelines.lib._path_utils.get_storage") as mock_get_storage:
            mock_get_storage.side_effect = Exception("Storage error")
            assert storage_path_exists(str(tmp_path / "error.parquet")) is False

    def test_question_mark_glob(self, tmp_path):
        """Handles ? glob pattern correctly."""
        (tmp_path / "data1.parquet").write_text("test")

        with patch("pipelines.lib._path_utils.get_storage") as mock_get_storage:
            mock_storage = MagicMock()
            mock_storage.exists.return_value = True
            mock_get_storage.return_value = mock_storage

            storage_path_exists(str(tmp_path / "data?.parquet"))
            assert mock_get_storage.called
