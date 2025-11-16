import os
import requests
import pandas as pd
import pytest


pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    os.environ.get("RUN_SMOKE") != "1", reason="Set RUN_SMOKE=1 to run smoke tests"
)
def test_public_api_jsonplaceholder_smoke():
    resp = requests.get("https://jsonplaceholder.typicode.com/posts", timeout=15)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) > 0


@pytest.mark.skipif(
    os.environ.get("RUN_SMOKE") != "1", reason="Set RUN_SMOKE=1 to run smoke tests"
)
def test_local_parquet_roundtrip(tmp_path):
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    out = tmp_path / "sample.parquet"
    df.to_parquet(out)

    assert out.exists() and out.stat().st_size > 0
    df2 = pd.read_parquet(out)
    assert len(df2) == 3
    assert list(df2["name"]) == ["a", "b", "c"]
