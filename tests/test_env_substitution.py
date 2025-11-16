"""Tests for environment variable substitution in configs."""

import os
import pytest
from core.config.env_substitution import substitute_env_vars, apply_env_substitution


def test_simple_substitution():
    """Test basic ${VAR} substitution."""
    os.environ["TEST_VAR"] = "hello"
    result = substitute_env_vars("${TEST_VAR} world")
    assert result == "hello world"


def test_substitution_with_default():
    """Test ${VAR:default} syntax."""
    # Unset variable should use default
    result = substitute_env_vars("${NONEXISTENT:default_value}")
    assert result == "default_value"

    # Set variable should override default
    os.environ["SET_VAR"] = "actual"
    result = substitute_env_vars("${SET_VAR:default}")
    assert result == "actual"


def test_missing_var_raises():
    """Test that missing variable without default raises."""
    with pytest.raises(
        ValueError, match="Environment variable 'MISSING_VAR' is not set"
    ):
        substitute_env_vars("${MISSING_VAR}")


def test_dict_substitution():
    """Test substitution in nested dictionaries."""
    os.environ["HOST"] = "localhost"
    os.environ["PORT"] = "5432"

    config = {
        "database": {"host": "${HOST}", "port": "${PORT}", "name": "${DB_NAME:testdb}"}
    }

    result = substitute_env_vars(config)
    assert result["database"]["host"] == "localhost"
    assert result["database"]["port"] == "5432"
    assert result["database"]["name"] == "testdb"


def test_list_substitution():
    """Test substitution in lists."""
    os.environ["ITEM1"] = "first"
    os.environ["ITEM2"] = "second"

    config = ["${ITEM1}", "${ITEM2}", "${ITEM3:third}"]
    result = substitute_env_vars(config)

    assert result == ["first", "second", "third"]


def test_nested_structure():
    """Test substitution in deeply nested structures."""
    os.environ["API_KEY"] = "secret123"
    os.environ["BASE_URL"] = "https://api.example.com"

    config = {
        "sources": [
            {
                "name": "api1",
                "config": {"url": "${BASE_URL}/v1", "auth": {"key": "${API_KEY}"}},
            }
        ]
    }

    result = substitute_env_vars(config)
    assert result["sources"][0]["config"]["url"] == "https://api.example.com/v1"
    assert result["sources"][0]["config"]["auth"]["key"] == "secret123"


def test_no_substitution_needed():
    """Test that values without ${} are unchanged."""
    config = {"simple": "value", "number": 42, "bool": True, "none": None}

    result = substitute_env_vars(config)
    assert result == config


def test_partial_substitution():
    """Test substitution mixed with literals."""
    os.environ["BUCKET"] = "my-bucket"
    os.environ["ENV"] = "prod"

    path = "s3://${BUCKET}/${ENV}/data"
    result = substitute_env_vars(path)
    assert result == "s3://my-bucket/prod/data"


def test_multiple_substitutions_same_string():
    """Test multiple ${} in same string."""
    os.environ["USER"] = "admin"
    os.environ["HOST"] = "db.example.com"

    conn_str = "postgresql://${USER}@${HOST}:5432/mydb"
    result = substitute_env_vars(conn_str)
    assert result == "postgresql://admin@db.example.com:5432/mydb"


def test_empty_default():
    """Test that empty default is allowed."""
    result = substitute_env_vars("${UNSET:}")
    assert result == ""


def test_apply_env_substitution():
    """Test top-level apply function."""
    os.environ["TEST_PLATFORM"] = "test-platform"

    config = {"platform": {"bronze": {"s3_bucket": "${TEST_PLATFORM}-bronze"}}}

    result = apply_env_substitution(config)
    assert result["platform"]["bronze"]["s3_bucket"] == "test-platform-bronze"
