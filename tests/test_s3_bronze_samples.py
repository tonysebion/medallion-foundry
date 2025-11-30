"""Smoke test for the Bronze samples S3 location described in environments/dev.yaml."""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from core.config.environment import EnvironmentConfig

ENV_PATH = Path("environments") / "dev.yaml"


@pytest.mark.skipif(not ENV_PATH.exists(), reason="dev environment config missing")
def test_dev_s3_bronze_samples_exist() -> None:
    env_config = EnvironmentConfig.from_yaml(ENV_PATH)
    if not env_config.s3:
        pytest.skip("Environment 'dev' does not define S3 storage.")

    client_kwargs: dict[str, str] = {}
    if env_config.s3.endpoint_url:
        client_kwargs["endpoint_url"] = env_config.s3.endpoint_url
    if env_config.s3.region:
        client_kwargs["region_name"] = env_config.s3.region

    client = boto3.client(
        "s3",
        aws_access_key_id=env_config.s3.access_key_id,
        aws_secret_access_key=env_config.s3.secret_access_key,
        **client_kwargs,
    )

    bucket = env_config.s3.get_bucket("bronze_data")
    prefix = "bronze_samples/"

    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    except EndpointConnectionError as exc:
        pytest.skip(f"Cannot reach configured S3 endpoint {env_config.s3.endpoint_url}: {exc}")
    except ClientError as exc:
        pytest.fail(f"Failed to list {prefix} in bucket {bucket}: {exc}")

    assert response.get("KeyCount", 0) > 0, (
        f"No Bronze artifacts found under s3://{bucket}/{prefix}; "
        "run the Bronze generator to populate the bucket before generating Silver."
    )
