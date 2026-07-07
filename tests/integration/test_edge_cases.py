"""Ingestion, concurrency, and lifecycle edge cases against a live scrooge."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from _daffy import daffy_config, make_records, ship_records
from conftest import ScroogeServer, ServerFactory

from daffy.shipper import Shipper
from daffy.store import LogStore

pytestmark = pytest.mark.integration

Reader = Callable[[ScroogeServer], duckdb.DuckDBPyConnection]
Poster = Callable[..., tuple[int, str]]


@pytest.mark.parametrize(
    "message",
    [
        "héllo 世界 🚀 café",  # multibyte unicode
        "x" * 100_000,  # very long line
        "line1\nline2\nline3",  # embedded newlines (only trailing is stripped)
        "tab\there\r carriage",  # control characters
    ],
)
def test_message_payloads_round_trip(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster, message: str
) -> None:
    status, _ = post_logs(scrooge, [{"log": message}])
    assert status == 204
    conn = quack_reader(scrooge)
    (stored,) = conn.execute("SELECT message FROM remote.logs").fetchone()
    assert stored == message


def test_nested_fields_preserved(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    record = {
        "log": "nested",
        "context": {"a": [1, 2, {"deep": True}], "b": {"c": "d"}},
        "tags": ["x", "y"],
    }
    status, _ = post_logs(scrooge, [record])
    assert status == 204
    conn = quack_reader(scrooge)
    (fields,) = conn.execute("SELECT fields FROM remote.logs").fetchone()
    decoded = json.loads(fields)
    assert decoded["context"] == {"a": [1, 2, {"deep": True}], "b": {"c": "d"}}
    assert decoded["tags"] == ["x", "y"]


@pytest.mark.parametrize("service", ["svc/with/slash", "svc*star", "svc?q", ""])
def test_special_service_names(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster, service: str
) -> None:
    # An empty label falls through to container_name; set it too so `service` is the label.
    record = {
        "log": "s",
        "kubernetes": {"labels": {"app.kubernetes.io/name": service}},
    }
    status, _ = post_logs(scrooge, [record])
    assert status == 204
    conn = quack_reader(scrooge)
    (stored,) = conn.execute("SELECT service FROM remote.logs").fetchone()
    # An empty-string label is falsy, so map_record falls back to "unknown".
    assert stored == (service or "unknown")


def test_duplicate_rows_not_deduped(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    record = {"log": "dup", "date": 1_750_000_000}
    assert post_logs(scrooge, [record])[0] == 204
    assert post_logs(scrooge, [record])[0] == 204
    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 2


def test_concurrent_mixed_ingest(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    per_worker = 100
    quack_workers = 4
    http_workers = 4

    def ship_batch(worker: int) -> None:
        records = make_records(
            per_worker, service=f"quack-{worker}", message=f"q-{worker}"
        )
        assert ship_records(scrooge, records) == per_worker

    def post_batch(worker: int) -> None:
        records = [
            {"log": f"h-{worker}-{i}", "date": 1_750_000_000 + i}
            for i in range(per_worker)
        ]
        assert post_logs(scrooge, records)[0] == 204

    with ThreadPoolExecutor(max_workers=quack_workers + http_workers) as pool:
        futures = [pool.submit(ship_batch, w) for w in range(quack_workers)]
        futures += [pool.submit(post_batch, w) for w in range(http_workers)]
        for future in futures:
            future.result()

    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == (quack_workers + http_workers) * per_worker


def test_restart_durability(
    scrooge_factory: ServerFactory,
    quack_reader: Reader,
    post_logs: Poster,
    stop_server: Callable[[ScroogeServer], None],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    db_path = tmp_path_factory.mktemp("durable") / "scrooge.duckdb"
    first = scrooge_factory(db_path=db_path)
    records = [{"log": f"persist-{i}", "date": 1_750_000_000 + i} for i in range(200)]
    assert post_logs(first, records)[0] == 204
    stop_server(first)

    second = scrooge_factory(db_path=db_path)
    conn = quack_reader(second)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 200


def test_daffy_reconnect_after_downtime(
    scrooge_factory: ServerFactory,
    quack_reader: Reader,
    stop_server: Callable[[ScroogeServer], None],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    from conftest import _free_port  # reuse the same host:port across the restart

    db_path = tmp_path_factory.mktemp("reconnect") / "scrooge.duckdb"
    quack_port = _free_port()
    server = scrooge_factory(db_path=db_path, quack_port=quack_port)

    store = LogStore(":memory:")
    shipper = Shipper(daffy_config(server), store)

    store.insert_many(make_records(20, service="recon", message="pre"))
    assert shipper.flush() == 20  # server up: delivered

    stop_server(server)
    store.insert_many(make_records(15, service="recon", message="post"))
    assert shipper.flush() == 0  # server down: retained, nothing shipped
    assert store.count() == 15

    restarted = scrooge_factory(db_path=db_path, quack_port=quack_port)
    shipped = 0
    for _ in range(20):
        shipped = shipper.flush()
        if shipped:
            break
        time.sleep(0.25)
    assert shipped == 15  # buffered rows delivered after reconnect
    store.close()

    conn = quack_reader(restarted)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 35
