"""Interoperability: daffy (Quack) and HTTP REST ingest land in one consistent table."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime

import duckdb
import pytest

from conftest import ScroogeServer

from daffy.config import Config
from daffy.schema import LogRecord
from daffy.shipper import Shipper
from daffy.store import LogStore

pytestmark = pytest.mark.integration

Reader = Callable[[ScroogeServer], duckdb.DuckDBPyConnection]
Poster = Callable[..., tuple[int, str]]


def _records(n: int, *, service: str, message: str = "hello") -> list[LogRecord]:
    return [
        LogRecord(
            capture_time=datetime(2026, 6, 22, 0, 0, 0, i),
            service=service,
            stream="stdout",
            message=f"{message}-{i}",
        )
        for i in range(n)
    ]


def _ship(server: ScroogeServer, records: list[LogRecord]) -> int:
    """Ship records into a live scrooge using daffy's real Shipper over Quack."""
    store = LogStore(":memory:")
    store.insert_many(records)
    config = Config(
        service="shipper",
        local_db=":memory:",
        pod=None,
        node=None,
        scrooge_uri=server.quack_uri,
        scrooge_token=server.quack_token,
        flush_rows=10_000,
        flush_interval=5.0,
        max_buffer_rows=100_000,
    )
    try:
        return Shipper(config, store).flush()
    finally:
        store.close()


def test_daffy_quack_library(scrooge: ScroogeServer, quack_reader: Reader) -> None:
    shipped = _ship(scrooge, _records(50, service="svc-a"))
    assert shipped == 50

    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 50

    row = conn.execute(
        "SELECT capture_time, service, stream, message FROM remote.logs "
        "WHERE message = 'hello-7'"
    ).fetchone()
    assert row == (datetime(2026, 6, 22, 0, 0, 0, 7), "svc-a", "stdout", "hello-7")


def test_daffy_quack_cli(scrooge: ScroogeServer, quack_reader: Reader) -> None:
    # A stdout JSON line (level pulled from the object) and a stderr warn-token line,
    # exercising daffy's full wrapper → parse → ship path as a real subprocess.
    script = 'echo \'{"level":"error","msg":"jsonline"}\'; echo \'warn: broke\' 1>&2'
    env = os.environ.copy()
    for key in (
        "SERVICE_NAME",
        "SCROOGE_URI",
        "SCROOGE_TOKEN",
        "POD_NAME",
        "NODE_NAME",
    ):
        env.pop(key, None)
    # Invoke daffy's console entrypoint from the same venv running the tests (installed via
    # the `integration` group), so no separate daffy checkout is needed.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "daffy.cli",
            "--service",
            "cli-svc",
            "--scrooge-uri",
            scrooge.quack_uri,
            "--scrooge-token",
            scrooge.quack_token,
            "--",
            "sh",
            "-c",
            script,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    assert result.returncode == 0, result.stderr

    conn = quack_reader(scrooge)
    rows = conn.execute(
        "SELECT stream, level, message, fields FROM remote.logs WHERE service = 'cli-svc'"
    ).fetchall()
    by_message = {
        message: (stream, level, fields) for stream, level, message, fields in rows
    }

    stream, level, fields = by_message['{"level":"error","msg":"jsonline"}']
    assert stream == "stdout"
    assert level == "error"
    assert json.loads(fields)["msg"] == "jsonline"

    stream, level, _ = by_message["warn: broke"]
    assert stream == "stderr"
    assert level == "warn"


def test_http_rest_fluentbit_shape(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    record = {
        "log": "hello world\n",
        "stream": "stdout",
        "date": 1_750_000_000,
        "kubernetes": {
            "labels": {"app.kubernetes.io/name": "web"},
            "pod_name": "web-abc",
            "host": "node-1",
            "container_name": "web",
        },
    }
    status, _ = post_logs(scrooge, [record])
    assert status == 204

    conn = quack_reader(scrooge)
    service, pod, node, stream, message, fields = conn.execute(
        "SELECT service, pod, node, stream, message, fields FROM remote.logs"
    ).fetchone()
    assert (service, pod, node, stream) == ("web", "web-abc", "node-1", "stdout")
    assert message == "hello world"  # trailing newline stripped
    assert json.loads(fields)["log"] == "hello world\n"  # full record preserved


def test_cross_encoder_one_table(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    assert _ship(scrooge, _records(10, service="svc-quack")) == 10

    http_records = [
        {
            "log": f"http-{i}",
            "stream": "stdout",
            "date": 1_750_000_100 + i,
            "kubernetes": {"labels": {"app.kubernetes.io/name": "svc-http"}},
        }
        for i in range(10)
    ]
    status, _ = post_logs(scrooge, http_records)
    assert status == 204

    conn = quack_reader(scrooge)
    (total,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert total == 20

    services = {
        row[0]
        for row in conn.execute("SELECT DISTINCT service FROM remote.logs").fetchall()
    }
    assert services == {"svc-quack", "svc-http"}

    # Both encoders write naive-UTC timestamps, so a single ordering spans all 20 rows.
    ordered = conn.execute(
        "SELECT service FROM remote.logs ORDER BY capture_time"
    ).fetchall()
    assert len(ordered) == 20


def test_all_logs_view_matches_without_archive(
    scrooge: ScroogeServer, quack_reader: Reader
) -> None:
    _ship(scrooge, _records(5, service="svc-view"))
    conn = quack_reader(scrooge)
    (logs_count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    (all_count,) = conn.execute("SELECT count(*) FROM remote.all_logs").fetchone()
    assert logs_count == all_count == 5
