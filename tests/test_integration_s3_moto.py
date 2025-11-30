import os
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_SMOKE") != "1", reason="Set RUN_SMOKE=1 to run smoke tests"
)
def test_s3_roundtrip_with_moto():
    try:
        from moto import mock_aws
        import boto3
    except Exception as exc:
        pytest.skip(f"moto/boto3 not available: {exc}")

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "bronze-foundry-smoke"
        s3.create_bucket(Bucket=bucket)

        key = "smoke/hello.txt"
        body = b"hello world"
        s3.put_object(Bucket=bucket, Key=key, Body=body)

        res = s3.get_object(Bucket=bucket, Key=key)
        data = res["Body"].read()
        assert data == body
