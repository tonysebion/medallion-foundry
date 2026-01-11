"""Unit tests for authentication configuration and header construction.

These tests validate the authentication logic that was formerly in auth.py
and is now integrated into api.py.

Tests cover:
- AuthConfig validation
- Header construction for all auth types (bearer, api_key, basic)
- Environment variable expansion in credentials
- Edge cases: empty credentials, missing fields
"""

import os
import pytest
from unittest.mock import patch

from pipelines.lib.api import (
    AuthConfig,
    AuthType,
    build_auth_headers,
    build_auth_headers_from_dict,
)


# ============================================================================
# AuthConfig Tests
# ============================================================================


class TestAuthConfigValidation:
    """Tests for AuthConfig dataclass validation."""

    def test_none_auth_requires_no_credentials(self):
        """NONE auth type should not require any credentials."""
        config = AuthConfig(auth_type=AuthType.NONE)
        assert config.auth_type == AuthType.NONE

    def test_bearer_requires_token(self):
        """Bearer auth should require token."""
        with pytest.raises(ValueError, match="Bearer authentication requires 'token'"):
            AuthConfig(auth_type=AuthType.BEARER)

    def test_bearer_with_token_valid(self):
        """Bearer auth with token should be valid."""
        config = AuthConfig(auth_type=AuthType.BEARER, token="my-token")
        assert config.token == "my-token"

    def test_api_key_requires_key(self):
        """API key auth should require api_key."""
        with pytest.raises(
            ValueError, match="API key authentication requires 'api_key'"
        ):
            AuthConfig(auth_type=AuthType.API_KEY)

    def test_api_key_with_key_valid(self):
        """API key auth with key should be valid."""
        config = AuthConfig(auth_type=AuthType.API_KEY, api_key="secret-key")
        assert config.api_key == "secret-key"

    def test_api_key_default_header(self):
        """API key should default to X-API-Key header."""
        config = AuthConfig(auth_type=AuthType.API_KEY, api_key="key")
        assert config.api_key_header == "X-API-Key"

    def test_api_key_custom_header(self):
        """API key should support custom header name."""
        config = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="key",
            api_key_header="Authorization-Key",
        )
        assert config.api_key_header == "Authorization-Key"

    def test_basic_requires_username_and_password(self):
        """Basic auth should require both username and password."""
        with pytest.raises(ValueError, match="Basic authentication requires both"):
            AuthConfig(auth_type=AuthType.BASIC, username="user")

        with pytest.raises(ValueError, match="Basic authentication requires both"):
            AuthConfig(auth_type=AuthType.BASIC, password="pass")

    def test_basic_with_credentials_valid(self):
        """Basic auth with username and password should be valid."""
        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="user",
            password="pass",
        )
        assert config.username == "user"
        assert config.password == "pass"


# ============================================================================
# build_auth_headers Tests - No Auth
# ============================================================================


class TestBuildAuthHeadersNoAuth:
    """Tests for build_auth_headers with no authentication."""

    def test_none_auth_returns_accept_header_only(self):
        """No auth should return only Accept header."""
        config = AuthConfig(auth_type=AuthType.NONE)
        headers, auth_tuple = build_auth_headers(config)

        assert headers == {"Accept": "application/json"}
        assert auth_tuple is None

    def test_none_config_returns_accept_header_only(self):
        """None config should return only Accept header."""
        headers, auth_tuple = build_auth_headers(None)

        assert headers == {"Accept": "application/json"}
        assert auth_tuple is None

    def test_extra_headers_added(self):
        """Extra headers should be included."""
        config = AuthConfig(auth_type=AuthType.NONE)
        headers, auth_tuple = build_auth_headers(
            config,
            extra_headers={"Content-Type": "application/json", "X-Custom": "value"},
        )

        assert headers["Accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Custom"] == "value"


# ============================================================================
# build_auth_headers Tests - Bearer Token
# ============================================================================


class TestBuildAuthHeadersBearer:
    """Tests for build_auth_headers with bearer token auth."""

    def test_bearer_adds_authorization_header(self):
        """Bearer auth should add Authorization header."""
        config = AuthConfig(auth_type=AuthType.BEARER, token="my-secret-token")
        headers, auth_tuple = build_auth_headers(config)

        assert headers["Authorization"] == "Bearer my-secret-token"
        assert auth_tuple is None

    def test_bearer_with_env_var_expansion(self):
        """Bearer token should expand environment variables."""
        with patch.dict(os.environ, {"API_TOKEN": "env-token-value"}):
            config = AuthConfig(auth_type=AuthType.BEARER, token="${API_TOKEN}")
            headers, auth_tuple = build_auth_headers(config)

            assert headers["Authorization"] == "Bearer env-token-value"

    def test_bearer_with_missing_env_var_raises(self):
        """Bearer with missing env var should raise KeyError."""
        # Ensure the env var doesn't exist
        env = os.environ.copy()
        env.pop("MISSING_TOKEN", None)
        with patch.dict(os.environ, env, clear=True):
            config = AuthConfig(auth_type=AuthType.BEARER, token="${MISSING_TOKEN}")
            with pytest.raises(KeyError, match="Environment variable not set"):
                build_auth_headers(config)

    def test_bearer_empty_token_raises(self):
        """Bearer with empty token (after expansion) should raise."""
        with patch.dict(os.environ, {"EMPTY_TOKEN": ""}):
            config = AuthConfig(auth_type=AuthType.BEARER, token="${EMPTY_TOKEN}")
            with pytest.raises(ValueError, match="empty string"):
                build_auth_headers(config)


# ============================================================================
# build_auth_headers Tests - API Key
# ============================================================================


class TestBuildAuthHeadersApiKey:
    """Tests for build_auth_headers with API key auth."""

    def test_api_key_default_header(self):
        """API key should use X-API-Key header by default."""
        config = AuthConfig(auth_type=AuthType.API_KEY, api_key="my-api-key")
        headers, auth_tuple = build_auth_headers(config)

        assert headers["X-API-Key"] == "my-api-key"
        assert auth_tuple is None

    def test_api_key_custom_header(self):
        """API key should support custom header name."""
        config = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="my-api-key",
            api_key_header="X-Custom-Key",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert headers["X-Custom-Key"] == "my-api-key"
        assert "X-API-Key" not in headers

    def test_api_key_with_env_var(self):
        """API key should expand environment variables."""
        with patch.dict(os.environ, {"MY_API_KEY": "expanded-key"}):
            config = AuthConfig(auth_type=AuthType.API_KEY, api_key="${MY_API_KEY}")
            headers, auth_tuple = build_auth_headers(config)

            assert headers["X-API-Key"] == "expanded-key"

    def test_api_key_empty_raises(self):
        """Empty API key (after expansion) should raise."""
        with patch.dict(os.environ, {"EMPTY_KEY": ""}):
            config = AuthConfig(auth_type=AuthType.API_KEY, api_key="${EMPTY_KEY}")
            with pytest.raises(ValueError, match="empty string"):
                build_auth_headers(config)


# ============================================================================
# build_auth_headers Tests - Basic Auth
# ============================================================================


class TestBuildAuthHeadersBasic:
    """Tests for build_auth_headers with basic auth."""

    def test_basic_returns_auth_tuple(self):
        """Basic auth should return auth tuple (not header)."""
        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="user",
            password="pass",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert auth_tuple == ("user", "pass")
        assert "Authorization" not in headers  # Basic auth uses tuple, not header

    def test_basic_with_env_vars(self):
        """Basic auth should expand environment variables."""
        with patch.dict(os.environ, {"DB_USER": "admin", "DB_PASS": "secret"}):
            config = AuthConfig(
                auth_type=AuthType.BASIC,
                username="${DB_USER}",
                password="${DB_PASS}",
            )
            headers, auth_tuple = build_auth_headers(config)

            assert auth_tuple == ("admin", "secret")

    def test_basic_empty_username_raises(self):
        """Empty username should raise ValueError."""
        with patch.dict(os.environ, {"EMPTY_USER": "", "VALID_PASS": "secret"}):
            config = AuthConfig(
                auth_type=AuthType.BASIC,
                username="${EMPTY_USER}",
                password="${VALID_PASS}",
            )
            with pytest.raises(ValueError, match="empty string"):
                build_auth_headers(config)

    def test_basic_empty_password_raises(self):
        """Empty password should raise ValueError."""
        with patch.dict(os.environ, {"VALID_USER": "admin", "EMPTY_PASS": ""}):
            config = AuthConfig(
                auth_type=AuthType.BASIC,
                username="${VALID_USER}",
                password="${EMPTY_PASS}",
            )
            with pytest.raises(ValueError, match="empty string"):
                build_auth_headers(config)


# ============================================================================
# build_auth_headers Tests - Extra Headers
# ============================================================================


class TestBuildAuthHeadersExtraHeaders:
    """Tests for extra headers handling."""

    def test_extra_headers_with_bearer(self):
        """Extra headers should work alongside bearer auth."""
        config = AuthConfig(auth_type=AuthType.BEARER, token="token")
        headers, _ = build_auth_headers(
            config,
            extra_headers={"X-Request-ID": "123", "X-Tenant": "acme"},
        )

        assert headers["Authorization"] == "Bearer token"
        assert headers["X-Request-ID"] == "123"
        assert headers["X-Tenant"] == "acme"

    def test_extra_headers_env_var_expansion(self):
        """Extra headers should expand environment variables."""
        with patch.dict(os.environ, {"TENANT_ID": "org-123"}):
            config = AuthConfig(auth_type=AuthType.NONE)
            headers, _ = build_auth_headers(
                config,
                extra_headers={"X-Tenant": "${TENANT_ID}"},
            )

            assert headers["X-Tenant"] == "org-123"

    def test_extra_headers_missing_env_var_not_strict(self):
        """Missing env vars in extra headers should not raise (not strict)."""
        config = AuthConfig(auth_type=AuthType.NONE)
        headers, _ = build_auth_headers(
            config,
            extra_headers={"X-Custom": "${MISSING_VAR}"},
        )
        # Should not raise - just keeps the placeholder or empty
        assert "X-Custom" in headers


# ============================================================================
# build_auth_headers_from_dict Tests
# ============================================================================


class TestBuildAuthHeadersFromDict:
    """Tests for build_auth_headers_from_dict helper."""

    def test_empty_dict_returns_no_auth(self):
        """Empty dict should return no auth."""
        headers, auth_tuple = build_auth_headers_from_dict({})

        assert headers == {"Accept": "application/json"}
        assert auth_tuple is None

    def test_bearer_from_dict(self):
        """Should build bearer auth from dict."""
        headers, auth_tuple = build_auth_headers_from_dict(
            {
                "auth_type": "bearer",
                "token": "my-token",
            }
        )

        assert headers["Authorization"] == "Bearer my-token"
        assert auth_tuple is None

    def test_api_key_from_dict(self):
        """Should build API key auth from dict."""
        headers, auth_tuple = build_auth_headers_from_dict(
            {
                "auth_type": "api_key",
                "api_key": "secret-key",
            }
        )

        assert headers["X-API-Key"] == "secret-key"
        assert auth_tuple is None

    def test_api_key_custom_header_from_dict(self):
        """Should build API key with custom header from dict."""
        headers, auth_tuple = build_auth_headers_from_dict(
            {
                "auth_type": "api_key",
                "api_key": "secret-key",
                "api_key_header": "Authorization-Token",
            }
        )

        assert headers["Authorization-Token"] == "secret-key"
        assert "X-API-Key" not in headers

    def test_basic_from_dict(self):
        """Should build basic auth from dict."""
        headers, auth_tuple = build_auth_headers_from_dict(
            {
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
            }
        )

        assert auth_tuple == ("user", "pass")

    def test_unsupported_auth_type_raises(self):
        """Unsupported auth type should raise."""
        with pytest.raises(ValueError, match="Unsupported auth_type"):
            build_auth_headers_from_dict(
                {
                    "auth_type": "oauth2",  # Not supported
                }
            )

    def test_case_insensitive_auth_type(self):
        """Auth type should be case insensitive."""
        headers, _ = build_auth_headers_from_dict(
            {
                "auth_type": "BEARER",
                "token": "token",
            }
        )

        assert headers["Authorization"] == "Bearer token"

    def test_extra_headers_from_dict(self):
        """Extra headers dict should be processed."""
        headers, _ = build_auth_headers_from_dict(
            {
                "auth_type": "none",
                "headers": {"X-Custom": "value", "X-Another": "another"},
            }
        )

        assert headers["X-Custom"] == "value"
        assert headers["X-Another"] == "another"

    def test_headers_non_dict_ignored(self):
        """Non-dict headers should be ignored."""
        headers, _ = build_auth_headers_from_dict(
            {
                "auth_type": "none",
                "headers": "not-a-dict",
            }
        )

        # Should not raise, just ignore invalid headers
        assert "not-a-dict" not in headers


# ============================================================================
# AuthType Enum Tests
# ============================================================================


class TestAuthTypeEnum:
    """Tests for AuthType enum."""

    def test_auth_type_values(self):
        """AuthType should have expected values."""
        assert AuthType.NONE.value == "none"
        assert AuthType.BEARER.value == "bearer"
        assert AuthType.API_KEY.value == "api_key"
        assert AuthType.BASIC.value == "basic"

    def test_auth_type_from_string(self):
        """Should create AuthType from string."""
        assert AuthType("none") == AuthType.NONE
        assert AuthType("bearer") == AuthType.BEARER
        assert AuthType("api_key") == AuthType.API_KEY
        assert AuthType("basic") == AuthType.BASIC

    def test_invalid_auth_type_raises(self):
        """Invalid auth type string should raise."""
        with pytest.raises(ValueError):
            AuthType("oauth2")


# ============================================================================
# Edge Cases and Security Tests
# ============================================================================


class TestAuthEdgeCases:
    """Edge case and security-related tests."""

    def test_token_with_special_characters(self):
        """Token with special characters should work."""
        config = AuthConfig(
            auth_type=AuthType.BEARER,
            token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkw",
        )
        headers, _ = build_auth_headers(config)

        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" in headers["Authorization"]

    def test_api_key_with_equals_sign(self):
        """API key with equals sign should work (common in base64)."""
        config = AuthConfig(
            auth_type=AuthType.API_KEY,
            api_key="abc123==",
        )
        headers, _ = build_auth_headers(config)

        assert headers["X-API-Key"] == "abc123=="

    def test_username_with_colon(self):
        """Username with colon should work (edge case for basic auth)."""
        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="user:name",
            password="pass",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert auth_tuple == ("user:name", "pass")

    def test_password_with_special_chars(self):
        """Password with special characters should work."""
        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="user",
            password="p@ss:w0rd!#$%",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert auth_tuple == ("user", "p@ss:w0rd!#$%")

    def test_unicode_in_credentials(self):
        """Unicode characters in credentials should work."""
        config = AuthConfig(
            auth_type=AuthType.BASIC,
            username="用户",
            password="密码",
        )
        headers, auth_tuple = build_auth_headers(config)

        assert auth_tuple == ("用户", "密码")

    def test_long_token(self):
        """Very long token should work."""
        long_token = "a" * 1000
        config = AuthConfig(auth_type=AuthType.BEARER, token=long_token)
        headers, _ = build_auth_headers(config)

        assert headers["Authorization"] == f"Bearer {long_token}"


class TestAuthWithEnvVarPartialExpansion:
    """Tests for partial environment variable expansion."""

    def test_mixed_literal_and_env_var(self):
        """Token with mixed literal and env var should expand correctly."""
        with patch.dict(os.environ, {"TOKEN_SUFFIX": "123"}):
            config = AuthConfig(
                auth_type=AuthType.BEARER,
                token="prefix-${TOKEN_SUFFIX}",
            )
            headers, _ = build_auth_headers(config)

            assert headers["Authorization"] == "Bearer prefix-123"

    def test_multiple_env_vars_in_token(self):
        """Multiple env vars in single token should all expand."""
        with patch.dict(os.environ, {"PART1": "abc", "PART2": "xyz"}):
            config = AuthConfig(
                auth_type=AuthType.BEARER,
                token="${PART1}-${PART2}",
            )
            headers, _ = build_auth_headers(config)

            assert headers["Authorization"] == "Bearer abc-xyz"
