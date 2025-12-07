"""Tests for parallel orchestration module."""

import threading
import time
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from core.domain.services.processing.chunk_processor import ChunkProcessor
from core.orchestration.parallel import run_parallel_extracts, _safe_run_extract
from core.infrastructure.runtime.context import RunContext
from core.foundation.primitives.patterns import LoadPattern


class TestRunParallelExtracts:
    """Tests for run_parallel_extracts function."""

    def _make_context(self, name: str) -> RunContext:
        """Create a RunContext for testing."""
        return RunContext(
            cfg={"source": {"system": "test", "table": "test_table"}},
            run_date=date(2024, 1, 15),
            relative_path=f"system=test/table={name}",
            local_output_dir=Path("/tmp/bronze"),
            bronze_path=Path(f"/tmp/bronze/system=test/table={name}"),
            source_system="test",
            source_table=name,
            dataset_id=f"test.{name}",
            config_name=name,
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id=f"test-run-{name}",
        )

    @patch("core.orchestration.parallel.run_extract")
    def test_single_successful_extraction(self, mock_run_extract):
        """Test single successful extraction."""
        mock_run_extract.return_value = 0

        contexts = [self._make_context("config1")]
        results = run_parallel_extracts(contexts, max_workers=1)

        assert len(results) == 1
        config_name, status, error = results[0]
        assert config_name == "config1"
        assert status == 0
        assert error is None

    @patch("core.orchestration.parallel.run_extract")
    def test_multiple_successful_extractions(self, mock_run_extract):
        """Test multiple successful extractions."""
        mock_run_extract.return_value = 0

        contexts = [
            self._make_context("config1"),
            self._make_context("config2"),
            self._make_context("config3"),
        ]
        results = run_parallel_extracts(contexts, max_workers=2)

        assert len(results) == 3
        successful = sum(1 for _, status, _ in results if status == 0)
        assert successful == 3

    @patch("core.orchestration.parallel.run_extract")
    def test_single_failed_extraction(self, mock_run_extract):
        """Test single failed extraction."""
        mock_run_extract.side_effect = ValueError("Test error")

        contexts = [self._make_context("config1")]
        results = run_parallel_extracts(contexts, max_workers=1)

        assert len(results) == 1
        config_name, status, error = results[0]
        assert config_name == "config1"
        assert status == -1
        assert isinstance(error, ValueError)

    @patch("core.orchestration.parallel.run_extract")
    def test_mixed_success_and_failure(self, mock_run_extract):
        """Test mixed success and failure results."""
        def mock_extract(context):
            if context.config_name == "config2":
                raise RuntimeError("Config2 failed")
            return 0

        mock_run_extract.side_effect = mock_extract

        contexts = [
            self._make_context("config1"),
            self._make_context("config2"),
            self._make_context("config3"),
        ]
        results = run_parallel_extracts(contexts, max_workers=2)

        assert len(results) == 3

        # Find results by config name
        results_by_name = {name: (status, error) for name, status, error in results}

        assert results_by_name["config1"][0] == 0
        assert results_by_name["config2"][0] == -1
        assert results_by_name["config3"][0] == 0

    @patch("core.orchestration.parallel.run_extract")
    def test_empty_contexts_list(self, mock_run_extract):
        """Test with empty contexts list."""
        results = run_parallel_extracts([], max_workers=2)
        assert results == []
        mock_run_extract.assert_not_called()

    @patch("core.orchestration.parallel.run_extract")
    def test_max_workers_zero_normalized_to_one(self, mock_run_extract):
        """Test that max_workers <= 0 is normalized to 1."""
        mock_run_extract.return_value = 0

        contexts = [self._make_context("config1")]
        results = run_parallel_extracts(contexts, max_workers=0)

        assert len(results) == 1
        assert results[0][1] == 0

    @patch("core.orchestration.parallel.run_extract")
    def test_max_workers_negative_normalized_to_one(self, mock_run_extract):
        """Test that negative max_workers is normalized to 1."""
        mock_run_extract.return_value = 0

        contexts = [self._make_context("config1")]
        results = run_parallel_extracts(contexts, max_workers=-5)

        assert len(results) == 1
        assert results[0][1] == 0

    @patch("core.orchestration.parallel.run_extract")
    def test_nonzero_return_status(self, mock_run_extract):
        """Test extraction with non-zero return status (not an exception)."""
        mock_run_extract.return_value = 1  # Non-zero status

        contexts = [self._make_context("config1")]
        results = run_parallel_extracts(contexts, max_workers=1)

        assert len(results) == 1
        config_name, status, error = results[0]
        assert config_name == "config1"
        assert status == 1  # Returns actual status, not -1
        assert error is None


class TestSafeRunExtract:
    """Tests for _safe_run_extract helper function."""

    def _make_context(self, name: str) -> RunContext:
        """Create a RunContext for testing."""
        return RunContext(
            cfg={"source": {"system": "test", "table": "test_table"}},
            run_date=date(2024, 1, 15),
            relative_path=f"system=test/table={name}",
            local_output_dir=Path("/tmp/bronze"),
            bronze_path=Path(f"/tmp/bronze/system=test/table={name}"),
            source_system="test",
            source_table=name,
            dataset_id=f"test.{name}",
            config_name=name,
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id=f"test-run-{name}",
        )

    @patch("core.orchestration.parallel.run_extract")
    def test_successful_extraction(self, mock_run_extract):
        """Test successful extraction returns status 0."""
        mock_run_extract.return_value = 0
        context = self._make_context("test")

        status, error = _safe_run_extract(context)

        assert status == 0
        assert error is None

    @patch("core.orchestration.parallel.run_extract")
    def test_failed_extraction_returns_error(self, mock_run_extract):
        """Test failed extraction returns -1 and error."""
        mock_run_extract.side_effect = ValueError("Test error")
        context = self._make_context("test")

        status, error = _safe_run_extract(context)

        assert status == -1
        assert isinstance(error, ValueError)
        assert str(error) == "Test error"

    @patch("core.orchestration.parallel.run_extract")
    def test_exception_type_preserved(self, mock_run_extract):
        """Test that exception type is preserved."""
        class CustomError(Exception):
            pass

        mock_run_extract.side_effect = CustomError("Custom error")
        context = self._make_context("test")

        status, error = _safe_run_extract(context)

        assert status == -1
        assert isinstance(error, CustomError)


class TestParallelExtractsLogging:
    """Tests to verify logging behavior."""

    def _make_context(self, name: str) -> RunContext:
        """Create a RunContext for testing."""
        return RunContext(
            cfg={"source": {"system": "test", "table": "test_table"}},
            run_date=date(2024, 1, 15),
            relative_path=f"system=test/table={name}",
            local_output_dir=Path("/tmp/bronze"),
            bronze_path=Path(f"/tmp/bronze/system=test/table={name}"),
            source_system="test",
            source_table=name,
            dataset_id=f"test.{name}",
            config_name=name,
            load_pattern=LoadPattern.SNAPSHOT,
            env_config=None,
            run_id=f"test-run-{name}",
        )

    @patch("core.orchestration.parallel.run_extract")
    @patch("core.orchestration.parallel.logger")
    def test_logs_start_message(self, mock_logger, mock_run_extract):
        """Test that start message is logged."""
        mock_run_extract.return_value = 0

        contexts = [self._make_context("config1")]
        run_parallel_extracts(contexts, max_workers=2)

        # Check that info was called at least once
        assert mock_logger.info.called

    @patch("core.orchestration.parallel.run_extract")
    @patch("core.orchestration.parallel.logger")
    def test_logs_completion_summary(self, mock_logger, mock_run_extract):
        """Test that completion summary is logged."""
        mock_run_extract.return_value = 0

        contexts = [self._make_context("config1"), self._make_context("config2")]
        run_parallel_extracts(contexts, max_workers=2)

        # Check that summary was logged
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        assert any("complete" in call.lower() for call in info_calls)


# =============================================================================
# Concurrent Chunk Processing Tests (#4)
# =============================================================================


class TestChunkProcessorConcurrency:
    """Tests for ChunkProcessor with parallel_workers > 1."""

    def _make_mock_writer(self, delay: float = 0.0, fail_on_index: int | None = None):
        """Create a mock ChunkWriter that tracks calls."""
        written_chunks = []
        write_lock = threading.Lock()

        class TrackingWriter:
            def write(self, chunk_index: int, chunk):
                if fail_on_index is not None and chunk_index == fail_on_index:
                    raise ValueError(f"Simulated failure on chunk {chunk_index}")

                if delay > 0:
                    time.sleep(delay)

                with write_lock:
                    written_chunks.append((chunk_index, chunk, threading.current_thread().name))

                return [Path(f"/tmp/chunk_{chunk_index}.parquet")]

        return TrackingWriter(), written_chunks

    def test_sequential_processing_with_single_worker(self):
        """Test that single worker processes chunks sequentially."""
        writer, written_chunks = self._make_mock_writer()
        processor = ChunkProcessor(writer, parallel_workers=1)

        chunks = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
        result = processor.process(chunks)

        assert len(result) == 3
        assert len(written_chunks) == 3
        # Verify sequential order (chunks should be in order 1, 2, 3)
        indices = [idx for idx, _, _ in written_chunks]
        assert indices == [1, 2, 3]

    def test_parallel_processing_with_multiple_workers(self):
        """Test that multiple workers process chunks in parallel."""
        writer, written_chunks = self._make_mock_writer(delay=0.05)
        processor = ChunkProcessor(writer, parallel_workers=3)

        chunks = [[{"id": i}] for i in range(6)]
        start = time.monotonic()
        result = processor.process(chunks)
        elapsed = time.monotonic() - start

        assert len(result) == 6
        assert len(written_chunks) == 6

        # Parallel execution should be faster than sequential (6 * 0.05 = 0.3s sequential)
        # With 3 workers, should complete in roughly 2 * 0.05 = 0.1s
        assert elapsed < 0.25  # Allow some overhead

    def test_no_double_writes(self):
        """Verify chunks aren't written twice under concurrency."""
        writer, written_chunks = self._make_mock_writer(delay=0.01)
        processor = ChunkProcessor(writer, parallel_workers=4)

        chunks = [[{"id": i}] for i in range(10)]
        processor.process(chunks)

        # Each chunk should be written exactly once
        indices = [idx for idx, _, _ in written_chunks]
        assert len(indices) == 10
        assert sorted(indices) == list(range(1, 11))  # Indices start at 1

    def test_chunk_index_isolation(self):
        """Verify parallel workers receive correct chunk indices."""
        writer, written_chunks = self._make_mock_writer()
        processor = ChunkProcessor(writer, parallel_workers=3)

        chunks = [
            [{"id": "a"}],
            [{"id": "b"}],
            [{"id": "c"}],
            [{"id": "d"}],
        ]
        processor.process(chunks)

        # Verify each chunk index maps to correct data
        for idx, chunk, _ in written_chunks:
            expected_id = chr(ord('a') + idx - 1)  # idx 1 -> 'a', idx 2 -> 'b', etc.
            assert chunk[0]["id"] == expected_id

    def test_error_propagation_in_parallel_mode(self):
        """Test that errors in parallel execution are properly propagated."""
        writer, _ = self._make_mock_writer(fail_on_index=2)
        processor = ChunkProcessor(writer, parallel_workers=2)

        chunks = [[{"id": i}] for i in range(4)]

        with pytest.raises(ValueError, match="Simulated failure on chunk 2"):
            processor.process(chunks)

    def test_empty_chunks_list(self):
        """Test processing empty chunks list."""
        writer, written_chunks = self._make_mock_writer()
        processor = ChunkProcessor(writer, parallel_workers=4)

        result = processor.process([])

        assert result == []
        assert len(written_chunks) == 0

    def test_single_chunk_uses_sequential(self):
        """Test that single chunk doesn't use parallel execution."""
        writer, written_chunks = self._make_mock_writer()
        processor = ChunkProcessor(writer, parallel_workers=4)

        chunks = [[{"id": 1}]]
        result = processor.process(chunks)

        assert len(result) == 1
        assert len(written_chunks) == 1

    def test_worker_count_normalization(self):
        """Test that worker count is normalized to at least 1."""
        writer, _ = self._make_mock_writer()

        # Test zero workers
        processor_zero = ChunkProcessor(writer, parallel_workers=0)
        assert processor_zero.parallel_workers == 1

        # Test negative workers
        processor_neg = ChunkProcessor(writer, parallel_workers=-5)
        assert processor_neg.parallel_workers == 1

    def test_varying_worker_counts(self):
        """Test that different worker counts all complete successfully."""
        for worker_count in [1, 2, 4, 8]:
            writer, written_chunks = self._make_mock_writer()
            processor = ChunkProcessor(writer, parallel_workers=worker_count)

            chunks = [[{"id": i}] for i in range(8)]
            result = processor.process(chunks)

            assert len(result) == 8
            written_chunks.clear()  # Reset for next iteration
