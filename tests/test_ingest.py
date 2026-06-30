from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest
from starlette.testclient import TestClient

from scrooge.ingest import (
    BadPayload,
    IngestConfig,
    authorized,
    build_app,
    insert_records,
    map_record,
    parse_body,
)

_SCHEMA = (Path(__file__).parent.parent / "schema.sql").read_text()
_CFG = IngestConfig(token="super_secret")

_FULL_RECORD = {
    "log": "hello world\n",
    "stream": "stdout",
    "date": 1735689600.0,  # 2025-01-01T00:00:00Z
    "level": "info",
    "kubernetes": {
        "pod_name": "p-1",
        "host": "node-1",
        "container_name": "c",
        "labels": {"app.kubernetes.io/name": "svc"},
    },
}


def _row(record: dict[str, Any], **over: Any) -> dict[str, Any]:
    cfg = replace(_CFG, **over) if over else _CFG
    return map_record(record, cfg)


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (b'[{"a": 1}, {"b": 2}]', [{"a": 1}, {"b": 2}]),
        (b'{"a": 1}', [{"a": 1}]),
        (b'{"a": 1}\n{"b": 2}\n', [{"a": 1}, {"b": 2}]),
        (b'{"a": 1}\n\n  \n{"b": 2}', [{"a": 1}, {"b": 2}]),
        (b"", []),
        (b"   \n  ", []),
    ],
    ids=[
        "array",
        "single-object",
        "ndjson",
        "ndjson-blank-lines",
        "empty",
        "whitespace",
    ],
)
def test_parse_body(body: bytes, expected: list[dict[str, Any]]) -> None:
    assert parse_body(body) == expected


@pytest.mark.parametrize(
    "body",
    [b"{not json", b"[1, 2]", b"5", b'{"a": 1}\nnot json'],
    ids=["malformed", "array-of-scalars", "bare-scalar", "ndjson-bad-line"],
)
def test_parse_body_rejects(body: bytes) -> None:
    with pytest.raises(BadPayload):
        parse_body(body)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Bearer super_secret", True),
        ("Bearer wrong", False),
        ("", False),
        # Non-ASCII must return False, not raise (Starlette latin-1-decodes header bytes).
        ("Bearer café", False),
    ],
    ids=["match", "mismatch", "empty", "non-ascii"],
)
def test_authorized(header: str, expected: bool) -> None:
    assert authorized(header, b"Bearer super_secret") is expected


def test_map_record_full() -> None:
    row = _row(_FULL_RECORD)
    assert row["capture_time"] == datetime(2025, 1, 1)  # naive UTC
    assert row["service"] == "svc"
    assert row["pod"] == "p-1"
    assert row["node"] == "node-1"
    assert row["stream"] == "stdout"
    assert row["level"] == "info"
    assert row["message"] == "hello world"  # trailing newline stripped
    assert json.loads(row["fields"]) == _FULL_RECORD  # full record preserved


@pytest.mark.parametrize(
    ("kubernetes", "service_label_key", "expected"),
    [
        (
            {"labels": {"app.kubernetes.io/name": "svc"}},
            "app.kubernetes.io/name",
            "svc",
        ),
        ({"container_name": "c"}, "app.kubernetes.io/name", "c"),
        ({}, "app.kubernetes.io/name", "unknown"),
        ({"labels": {"app": "viceroy"}}, "app", "viceroy"),
    ],
    ids=["from-label", "fallback-container", "fallback-unknown", "custom-label-key"],
)
def test_map_record_service(
    kubernetes: dict[str, Any], service_label_key: str, expected: str
) -> None:
    record = {"log": "x", "stream": "stdout", "kubernetes": kubernetes}
    assert _row(record, service_label_key=service_label_key)["service"] == expected


@pytest.mark.parametrize(
    ("date_value", "expected"),
    [
        (1735689600.0, datetime(2025, 1, 1)),
        (1735689600, datetime(2025, 1, 1)),
        ("2025-01-01T00:00:00+00:00", datetime(2025, 1, 1)),
        ("2025-01-01T00:00:00Z", datetime(2025, 1, 1)),
        # Offset input is converted to UTC, then stored naive.
        ("2025-01-01T00:00:00-07:00", datetime(2025, 1, 1, 7, 0, 0)),
    ],
    ids=["epoch-float", "epoch-int", "iso", "iso-zulu", "iso-offset"],
)
def test_map_record_capture_time(date_value: Any, expected: datetime) -> None:
    # All results are naive UTC (no tzinfo) so DuckDB won't shift them on insert.
    got = _row({"date": date_value})["capture_time"]
    assert got == expected and got.tzinfo is None


@pytest.mark.parametrize(
    "date_value",
    [None, "not-a-date", {"x": 1}],
    ids=["missing", "garbage", "wrong-type"],
)
def test_map_record_capture_time_fallback(date_value: Any) -> None:
    # Unparseable timestamps fall back to a naive UTC receive time, not an error.
    ct = _row({"date": date_value} if date_value is not None else {})["capture_time"]
    assert isinstance(ct, datetime) and ct.tzinfo is None


def test_map_record_defaults_for_missing_fields() -> None:
    row = _row({})  # nothing present
    assert row["service"] == "unknown"
    assert row["pod"] is None and row["node"] is None
    assert row["stream"] == "" and row["level"] == "" and row["message"] == ""


def test_insert_records_round_trips() -> None:
    con = duckdb.connect(":memory:")
    con.execute(_SCHEMA)
    records = [
        map_record(_FULL_RECORD, _CFG),
        map_record({"log": "two", "stream": "stderr"}, _CFG),
    ]
    assert insert_records(con, threading.Lock(), records) == 2
    got = con.execute(
        "SELECT service, message, fields->>'stream' FROM logs ORDER BY message"
    ).fetchall()
    assert got == [("svc", "hello world", "stdout"), ("unknown", "two", "stderr")]
    # capture_time stored as naive UTC, unshifted by the host timezone.
    ct = con.execute("SELECT capture_time FROM logs WHERE service = 'svc'").fetchone()
    assert ct is not None and ct[0] == datetime(2025, 1, 1)


@pytest.fixture
def client_and_con() -> tuple[TestClient, duckdb.DuckDBPyConnection]:
    con = duckdb.connect(":memory:")
    con.execute(_SCHEMA)
    app = build_app(con, threading.Lock(), _CFG)
    return TestClient(app), con


def _count(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute("SELECT count(*) FROM logs").fetchone()
    return int(row[0]) if row else 0


_GOOD_AUTH = {"Authorization": "Bearer super_secret"}
_ARRAY_BODY = json.dumps([_FULL_RECORD, {"log": "b", "stream": "stderr"}]).encode()
_NDJSON_BODY = (
    json.dumps({"log": "a", "stream": "stdout"})
    + "\n"
    + json.dumps({"log": "b", "stream": "stderr"})
).encode()


@pytest.mark.parametrize(
    ("headers", "body", "status", "count"),
    [
        ({}, b'[{"log":"x","stream":"stdout"}]', 401, 0),
        ({"Authorization": "Bearer wrong"}, b'[{"log":"x","stream":"stdout"}]', 401, 0),
        (_GOOD_AUTH, _ARRAY_BODY, 204, 2),
        (_GOOD_AUTH, _NDJSON_BODY, 204, 2),
        (_GOOD_AUTH, b"{not json", 400, 0),
        (_GOOD_AUTH, b"", 204, 0),
    ],
    ids=["no-auth", "bad-token", "json-array", "ndjson", "bad-body", "empty-body"],
)
def test_endpoint_post(
    client_and_con: tuple[TestClient, duckdb.DuckDBPyConnection],
    headers: dict[str, str],
    body: bytes,
    status: int,
    count: int,
) -> None:
    client, con = client_and_con
    resp = client.post("/logs", content=body, headers=headers)
    assert resp.status_code == status
    assert _count(con) == count


def test_healthz_needs_no_auth(
    client_and_con: tuple[TestClient, duckdb.DuckDBPyConnection],
) -> None:
    client, _con = client_and_con
    resp = client.get("/healthz")
    assert resp.status_code == 200 and resp.text == "ok"
