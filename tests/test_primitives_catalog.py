from __future__ import annotations

from pathlib import Path
from typing import Iterable
from urllib import error

import pytest

from core.om.client import ColumnSchema, OpenMetadataClient, TableSchema
from core.primitives.catalog import hooks, tracing, webhooks, yaml_generator


class StubOpenMetadataClient:
    def __init__(self, schema: TableSchema | None = None, tables: Iterable[TableSchema] | None = None):
        self._schema = schema
        self._tables = list(tables or [])
        self.requested: list[tuple] = []

    def get_table_schema(self, fully_qualified_name: str) -> TableSchema | None:
        self.requested.append(("schema", fully_qualified_name))
        return self._schema

    def get_database_tables(self, database: str, schema: str | None = None) -> list[TableSchema]:
        self.requested.append(("tables", database, schema))
        return self._tables


class StubResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class StubRequest:
    def __init__(self, succeed: bool = True):
        self.succeed = succeed
        self.calls: list[tuple[str, float]] = []

    def __call__(self, req, timeout: float = 5.0):
        self.calls.append((req.full_url, timeout))
        if not self.succeed:
            raise error.URLError("boom")
        return StubResponse()


def test_column_schema_to_dict_includes_optional_fields() -> None:
    schema = ColumnSchema(
        name="event_ts",
        data_type="timestamp",
        description="Event time",
        nullable=False,
        is_primary_key=True,
        precision=6,
        scale=3,
    )
    payload = schema.to_dict()

    assert payload["name"] == "event_ts"
    assert payload["type"] == "timestamp"
    assert payload["description"] == "Event time"
    assert payload["nullable"] is False
    assert payload["primary_key"] is True
    assert payload["precision"] == 6
    assert payload["scale"] == 3


def test_table_schema_primary_keys() -> None:
    columns = [
        ColumnSchema(name="id", data_type="integer", is_primary_key=True),
        ColumnSchema(name="name", data_type="string"),
    ]
    table = TableSchema(
        fully_qualified_name="db.schema.table",
        name="table",
        database="db",
        schema="schema",
        columns=columns,
    )

    assert table.primary_keys == ["id"]


def test_openmetadata_client_defaults_trim_url() -> None:
    client = OpenMetadataClient(base_url="http://example.com/api/")
    assert client.base_url == "http://example.com/api"
    assert client.connect() is True
    assert client.get_table_schema("anything") is None
    assert client.get_database_tables("db") == []
    assert client.get_lineage("db.table") == []
    assert client.create_or_update_table(
        table_schema=TableSchema("db.schema.t", "t", "db", "schema"),
    ) is True
    assert client.add_lineage("source", "target") is True
    assert client.report_quality_metrics("db.t", {"passed": 1}) is True
    assert client.search_tables("query") == []


def test_hooks_toggle() -> None:
    hooks.set_om_client(None)
    assert hooks.get_om_client() is None
    assert hooks.is_om_enabled() is False

    stub = object()
    hooks.set_om_client(stub)
    assert hooks.get_om_client() is stub
    assert hooks.is_om_enabled() is True

    hooks.set_om_client(None)
    assert hooks.is_om_enabled() is False


def test_report_quality_rule_results_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("INFO")
    hooks.report_quality_rule_results(
        "ds",
        [
            {"id": "rule1", "passed": True},
            {"id": "rule2", "passed": False},
        ],
    )

    assert any("quality_rule_results" in rec.message for rec in caplog.records)
    assert any("rules_passed" in rec.getMessage() for rec in caplog.records)


def test_fire_webhooks_success_and_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    stub = StubRequest(succeed=True)
    monkeypatch.setattr(webhooks.request, "urlopen", stub)

    webhooks.fire_webhooks(["https://example.com/hook"], {"foo": "bar"})
    assert stub.calls

    stub = StubRequest(succeed=False)
    monkeypatch.setattr(webhooks.request, "urlopen", stub)
    caplog.set_level("WARNING")
    webhooks.fire_webhooks(["https://example.com/hook"], {"foo": "bar"})
    assert any("Webhook POST failed" in rec.getMessage() for rec in caplog.records)


def test_trace_span_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRONZE_TRACING", "1")
    with tracing.trace_span("test"):
        pass


def test_generate_yaml_minimal_skeleton(tmp_path: Path) -> None:
    generated = yaml_generator.generate_yaml_skeleton(
        table_fqn="db.schema.table",
        om_client=StubOpenMetadataClient(schema=None),
        output_path=tmp_path / "skeleton.yaml",
    )

    assert generated["dataset_id"].startswith("db")
    assert "bronze" in generated and "silver" in generated
    assert (tmp_path / "skeleton.yaml").exists()


def test_generate_yaml_from_schema(tmp_path: Path) -> None:
    columns = [
        ColumnSchema(name="id", data_type="integer", is_primary_key=True),
        ColumnSchema(name="updated_at", data_type="timestamp"),
        ColumnSchema(name="payload", data_type="string"),
    ]
    table_schema = TableSchema(
        fully_qualified_name="db.schema.table",
        name="table",
        database="db",
        schema="schema",
        columns=columns,
        tags=["domain:payment"],
        description="payments table",
    )
    client = StubOpenMetadataClient(schema=table_schema)

    generated = yaml_generator.generate_yaml_skeleton(
        table_fqn="db.schema.table",
        om_client=client,
        output_path=tmp_path / "full.yaml",
        default_entity_kind="state",
    )

    assert generated["domain"] == "payment"
    assert generated["bronze"]["watermark_column"] == "updated_at"
    assert generated["schema"]["primary_keys"] == ["id"]
    assert "change_ts_column" in generated["silver"]
    assert (tmp_path / "full.yaml").exists()


def test_generate_multi_table_skeletons(tmp_path: Path) -> None:
    columns = [ColumnSchema(name="id", data_type="integer", is_primary_key=True)]
    table_schema = TableSchema(
        fully_qualified_name="db.schema.table",
        name="table",
        database="db",
        schema="schema",
        columns=columns,
    )
    client = StubOpenMetadataClient(tables=[table_schema])

    configs = yaml_generator.generate_multi_table_skeletons(
        database="db",
        schema="schema",
        om_client=client,
        output_dir=tmp_path,
    )

    assert len(configs) == 1
    assert (tmp_path / "table.yaml").exists()
