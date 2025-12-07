"""Tests for HTTP connection pooling.

Story #9: Connection Pooling for HTTP Extractors
"""

from unittest.mock import patch, MagicMock
from unittest.mock import AsyncMock
import pytest

from core.infrastructure.io.http.session import (
    HttpPoolConfig,
    ConnectionMetrics,
    AsyncApiClient,
    HTTPX_AVAILABLE,
)
from core.domain.adapters.extractors.api_extractor import (
    SyncPoolConfig,
    _create_pooled_session,
)


class TestHttpPoolConfig:
    """Tests for HttpPoolConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = HttpPoolConfig()
        assert config.max_connections == 100
        assert config.max_keepalive_connections == 20
        assert config.keepalive_expiry == 30.0
        assert config.http2 is False

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = HttpPoolConfig(
            max_connections=50,
            max_keepalive_connections=10,
            keepalive_expiry=60.0,
            http2=True,
        )
        assert config.max_connections == 50
        assert config.max_keepalive_connections == 10
        assert config.keepalive_expiry == 60.0
        assert config.http2 is True

    def test_from_dict(self) -> None:
        """Test creation from dictionary."""
        data = {
            "max_connections": 200,
            "max_keepalive_connections": 40,
            "keepalive_expiry": 45.0,
            "http2": True,
        }
        config = HttpPoolConfig.from_dict(data)

        assert config.max_connections == 200
        assert config.max_keepalive_connections == 40
        assert config.keepalive_expiry == 45.0
        assert config.http2 is True

    def test_from_dict_none(self) -> None:
        """Test from_dict with None returns defaults."""
        config = HttpPoolConfig.from_dict(None)
        assert config.max_connections == 100
        assert config.max_keepalive_connections == 20

    def test_from_dict_partial(self) -> None:
        """Test from_dict with partial data uses defaults for missing."""
        data = {"max_connections": 150}
        config = HttpPoolConfig.from_dict(data)

        assert config.max_connections == 150
        assert config.max_keepalive_connections == 20  # default
        assert config.keepalive_expiry == 30.0  # default
        assert config.http2 is False  # default


class TestConnectionMetrics:
    """Tests for ConnectionMetrics dataclass."""

    def test_default_values(self) -> None:
        """Test default metric values."""
        metrics = ConnectionMetrics()
        assert metrics.requests_made == 0
        assert metrics.connections_created == 0
        assert metrics.connections_reused == 0

    def test_reuse_ratio_zero_requests(self) -> None:
        """Test reuse ratio with zero requests."""
        metrics = ConnectionMetrics()
        assert metrics.reuse_ratio == 0.0

    def test_reuse_ratio_calculation(self) -> None:
        """Test reuse ratio calculation."""
        metrics = ConnectionMetrics(
            requests_made=10,
            connections_created=1,
            connections_reused=9,
        )
        assert metrics.reuse_ratio == 0.9

    def test_reuse_ratio_no_reuse(self) -> None:
        """Test reuse ratio with no reuse."""
        metrics = ConnectionMetrics(
            requests_made=5,
            connections_created=5,
            connections_reused=0,
        )
        assert metrics.reuse_ratio == 0.0


class TestSyncPoolConfig:
    """Tests for SyncPoolConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = SyncPoolConfig()
        assert config.pool_connections == 10
        assert config.pool_maxsize == 10
        assert config.pool_block is False
        assert config.max_retries == 0

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = SyncPoolConfig(
            pool_connections=20,
            pool_maxsize=30,
            pool_block=True,
            max_retries=3,
        )
        assert config.pool_connections == 20
        assert config.pool_maxsize == 30
        assert config.pool_block is True
        assert config.max_retries == 3

    def test_from_dict(self) -> None:
        """Test creation from dictionary."""
        data = {
            "pool_connections": 15,
            "pool_maxsize": 25,
            "pool_block": True,
            "max_retries": 2,
        }
        config = SyncPoolConfig.from_dict(data)

        assert config.pool_connections == 15
        assert config.pool_maxsize == 25
        assert config.pool_block is True
        assert config.max_retries == 2

    def test_from_dict_none(self) -> None:
        """Test from_dict with None returns defaults."""
        config = SyncPoolConfig.from_dict(None)
        assert config.pool_connections == 10
        assert config.pool_maxsize == 10

    def test_from_dict_partial(self) -> None:
        """Test from_dict with partial data uses defaults for missing."""
        data = {"pool_connections": 5}
        config = SyncPoolConfig.from_dict(data)

        assert config.pool_connections == 5
        assert config.pool_maxsize == 10  # default


class TestCreatePooledSession:
    """Tests for _create_pooled_session function."""

    def test_creates_session(self) -> None:
        """Test session is created."""
        session = _create_pooled_session()
        assert session is not None
        session.close()

    def test_mounts_adapters(self) -> None:
        """Test adapters are mounted for http and https."""
        session = _create_pooled_session()

        # Check that adapters are mounted
        assert "https://" in session.adapters
        assert "http://" in session.adapters
        session.close()

    def test_uses_custom_config(self) -> None:
        """Test custom pool config is applied."""
        config = SyncPoolConfig(
            pool_connections=5,
            pool_maxsize=15,
            pool_block=True,
            max_retries=2,
        )
        session = _create_pooled_session(config)

        # Get the adapter to verify config
        adapter = session.get_adapter("https://example.com")
        # HTTPAdapter stores values as private attributes
        assert adapter._pool_connections == 5
        assert adapter._pool_maxsize == 15
        assert adapter._pool_block is True
        session.close()

    def test_default_config(self) -> None:
        """Test default pool config values."""
        session = _create_pooled_session()

        adapter = session.get_adapter("https://example.com")
        # HTTPAdapter stores values as private attributes
        assert adapter._pool_connections == 10
        assert adapter._pool_maxsize == 10
        session.close()


@pytest.mark.skipif(not HTTPX_AVAILABLE, reason="httpx not installed")
class TestAsyncApiClientPooling:
    """Tests for AsyncApiClient connection pooling."""

    def test_init_with_pool_config(self) -> None:
        """Test client initialization with pool config."""
        pool_config = HttpPoolConfig(max_connections=50)
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={"Authorization": "Bearer token"},
            pool_config=pool_config,
        )
        assert client._pool_config.max_connections == 50

    def test_init_default_pool_config(self) -> None:
        """Test client initialization with default pool config."""
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
        )
        assert client._pool_config.max_connections == 100  # default

    def test_metrics_initialized(self) -> None:
        """Test metrics are initialized."""
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
        )
        assert client.metrics.requests_made == 0
        assert client.metrics.connections_created == 0

    @pytest.mark.asyncio
    async def test_client_reuse_across_requests(self) -> None:
        """Test that client is reused across multiple requests."""
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_response.raise_for_status = MagicMock()

        # Mock httpx AsyncClient to avoid real HTTP calls while still running _get_client
        with patch(
            "core.infrastructure.io.http.session.httpx.AsyncClient", new_callable=AsyncMock
        ) as mock_async_client:
            mock_httpx_client = MagicMock()
            mock_httpx_client.get = AsyncMock(return_value=mock_response)
            mock_httpx_client.aclose = AsyncMock()
            mock_async_client.return_value = mock_httpx_client

            # Note: We're testing the _get_client method behavior
            # In a real scenario, making 3 requests would reuse the client
            await client._get_client()
            await client._get_client()
            await client._get_client()

            # First call creates, subsequent calls reuse
            assert client._metrics.connections_created == 1
            assert client._metrics.connections_reused == 2

    @pytest.mark.asyncio
    async def test_context_manager_closes_client(self) -> None:
        """Test context manager properly closes client."""
        async with AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
        ) as client:
            # Create client
            with patch(
                "core.infrastructure.io.http.session.httpx.AsyncClient", new_callable=AsyncMock
            ) as mock_async_client:
                mock_httpx_client = MagicMock()
                mock_httpx_client.aclose = AsyncMock()
                mock_async_client.return_value = mock_httpx_client
                await client._get_client()
                client._client = mock_httpx_client

        # After context, client should be None
        assert client._client is None
        mock_httpx_client.aclose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_without_client(self) -> None:
        """Test close works when client was never created."""
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
        )
        # Should not raise
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_pool_limits_applied(self) -> None:
        """Test that pool limits are applied to httpx client."""
        pool_config = HttpPoolConfig(
            max_connections=42,
            max_keepalive_connections=12,
            keepalive_expiry=15.0,
        )
        client = AsyncApiClient(
            base_url="https://api.example.com",
            headers={},
            pool_config=pool_config,
        )

        # Mock httpx to verify limits are passed
        with patch("core.infrastructure.io.http.session.httpx") as mock_httpx:
            mock_limits = MagicMock()
            mock_httpx.Limits = MagicMock(return_value=mock_limits)
            mock_async_client = MagicMock()
            mock_httpx.AsyncClient = MagicMock(return_value=mock_async_client)

            await client._get_client()

            # Verify Limits was called with our config
            mock_httpx.Limits.assert_called_once_with(
                max_connections=42,
                max_keepalive_connections=12,
                keepalive_expiry=15.0,
            )

            # Verify AsyncClient was created with limits
            mock_httpx.AsyncClient.assert_called_once()
            call_kwargs = mock_httpx.AsyncClient.call_args[1]
            assert call_kwargs["limits"] == mock_limits


class TestApiExtractorPoolingIntegration:
    """Integration tests for API extractor pooling configuration."""

    def test_sync_pool_config_in_api_cfg(self) -> None:
        """Test sync pool config is read from api_cfg."""
        api_cfg = {
            "sync_pool": {
                "pool_connections": 25,
                "pool_maxsize": 50,
            }
        }
        config = SyncPoolConfig.from_dict(api_cfg.get("sync_pool"))
        assert config.pool_connections == 25
        assert config.pool_maxsize == 50

    def test_http_pool_config_in_api_cfg(self) -> None:
        """Test http pool config is read from api_cfg."""
        api_cfg = {
            "http_pool": {
                "max_connections": 200,
                "max_keepalive_connections": 50,
                "http2": True,
            }
        }
        config = HttpPoolConfig.from_dict(api_cfg.get("http_pool"))
        assert config.max_connections == 200
        assert config.max_keepalive_connections == 50
        assert config.http2 is True
