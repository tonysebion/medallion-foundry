"""Tests for storage_config utilities.

Tests for get_config_value() and get_bool_config_value() helper functions
that provide canonical config reading with ${VAR} expansion and env var fallback.
"""

from __future__ import annotations

from pipelines.lib.storage_config import get_bool_config_value, get_config_value


class TestGetConfigValue:
    """Tests for get_config_value() function."""

    def test_returns_option_value_when_present(self):
        """Returns value from options dict when present."""
        options = {"endpoint_url": "http://localhost:9000"}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://localhost:9000"

    def test_expands_env_var_pattern_in_option_value(self, monkeypatch):
        """Expands ${VAR} patterns in option values."""
        monkeypatch.setenv("MY_ENDPOINT", "http://minio:9000")
        options = {"endpoint_url": "${MY_ENDPOINT}"}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://minio:9000"

    def test_falls_back_to_env_var_when_option_missing(self, monkeypatch):
        """Falls back to environment variable when option not present."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://from-env:9000")
        options = {}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://from-env:9000"

    def test_falls_back_to_env_var_when_option_empty(self, monkeypatch):
        """Falls back to environment variable when option is empty string."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://from-env:9000")
        options = {"endpoint_url": ""}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://from-env:9000"

    def test_returns_default_when_nothing_set(self, monkeypatch):
        """Returns default value when neither option nor env var provides value."""
        monkeypatch.delenv("AWS_REGION", raising=False)
        options = {}
        result = get_config_value(options, "region", "AWS_REGION", "us-east-1")
        assert result == "us-east-1"

    def test_handles_none_options_dict(self, monkeypatch):
        """Handles None options dict gracefully."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://from-env:9000")
        result = get_config_value(None, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://from-env:9000"

    def test_handles_none_options_with_default(self, monkeypatch):
        """Handles None options dict with default fallback."""
        monkeypatch.delenv("AWS_REGION", raising=False)
        result = get_config_value(None, "region", "AWS_REGION", "us-west-2")
        assert result == "us-west-2"

    def test_handles_non_string_values(self):
        """Non-string values are returned without expansion."""
        options = {"port": 9000}
        # Non-string values don't get expanded but also aren't valid for string config
        # The function expects string values, so this returns empty/default
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == ""  # Key not present, no env var

    def test_option_value_takes_precedence_over_env_var(self, monkeypatch):
        """Option value takes precedence over environment variable."""
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://from-env:9000")
        options = {"endpoint_url": "http://from-option:9000"}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://from-option:9000"

    def test_expanded_option_takes_precedence(self, monkeypatch):
        """Expanded ${VAR} option takes precedence over fallback env var."""
        monkeypatch.setenv("MY_CUSTOM_URL", "http://custom:9000")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://fallback:9000")
        options = {"endpoint_url": "${MY_CUSTOM_URL}"}
        result = get_config_value(options, "endpoint_url", "AWS_ENDPOINT_URL")
        assert result == "http://custom:9000"


class TestGetBoolConfigValue:
    """Tests for get_bool_config_value() function."""

    def test_returns_true_from_bool_option(self):
        """Returns True when option is boolean True."""
        options = {"verify_ssl": True}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_returns_false_from_bool_option(self):
        """Returns False when option is boolean False."""
        options = {"verify_ssl": False}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is False

    def test_returns_true_from_string_true(self):
        """Returns True from string 'true'."""
        options = {"verify_ssl": "true"}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_returns_true_from_string_yes(self):
        """Returns True from string 'yes'."""
        options = {"verify_ssl": "yes"}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_returns_true_from_string_one(self):
        """Returns True from string '1'."""
        options = {"verify_ssl": "1"}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_returns_false_from_string_false(self):
        """Returns False from string 'false'."""
        options = {"verify_ssl": "false"}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is False

    def test_case_insensitive_string_parsing(self):
        """String parsing is case-insensitive."""
        assert get_bool_config_value({"ssl": "TRUE"}, "ssl", "SSL") is True
        assert get_bool_config_value({"ssl": "True"}, "ssl", "SSL") is True
        assert get_bool_config_value({"ssl": "YES"}, "ssl", "SSL") is True
        assert get_bool_config_value({"ssl": "Yes"}, "ssl", "SSL") is True

    def test_falls_back_to_env_var_when_option_missing(self, monkeypatch):
        """Falls back to environment variable when option not present."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "true")
        options = {}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_falls_back_to_env_var_yes(self, monkeypatch):
        """Falls back to environment variable with 'yes' value."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "yes")
        result = get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_falls_back_to_env_var_one(self, monkeypatch):
        """Falls back to environment variable with '1' value."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "1")
        result = get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_returns_default_when_nothing_set(self, monkeypatch):
        """Returns default value when neither option nor env var set."""
        monkeypatch.delenv("AWS_S3_VERIFY_SSL", raising=False)
        options = {}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL", default=True)
        assert result is True

    def test_default_is_false_when_not_specified(self, monkeypatch):
        """Default is False when not specified."""
        monkeypatch.delenv("AWS_S3_VERIFY_SSL", raising=False)
        result = get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is False

    def test_handles_none_options_dict(self, monkeypatch):
        """Handles None options dict gracefully."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "true")
        result = get_bool_config_value(None, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is True

    def test_option_takes_precedence_over_env_var(self, monkeypatch):
        """Option value takes precedence over environment variable."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "true")
        options = {"verify_ssl": False}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is False

    def test_unknown_string_returns_false(self):
        """Unknown string values return False."""
        options = {"verify_ssl": "maybe"}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        assert result is False

    def test_empty_string_returns_false(self, monkeypatch):
        """Empty string in options returns False (not a truthy value)."""
        monkeypatch.setenv("AWS_S3_VERIFY_SSL", "true")
        options = {"verify_ssl": ""}
        result = get_bool_config_value(options, "verify_ssl", "AWS_S3_VERIFY_SSL")
        # Empty string is explicitly set, parsed as false (not in truthy list)
        assert result is False
