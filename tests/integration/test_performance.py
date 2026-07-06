"""Performance signals: ingest throughput, mixed load, and query latency.

These measure and print numbers, asserting only generous sanity bounds — the goal is to
catch gross regressions and hangs, not to benchmark a specific machine. Run with `-s` to
see the reported rates and latencies.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import duckdb
import pytest

from conftest import ScroogeServer

from daffy.config import Config
from daffy.schema import LogRecord
from daffy.shipper import Shipper
from daffy.store import LogStore

pytestmark = [pytest.mark.integration, pytest.mark.slow]

Reader = Callable[[ScroogeServer], duckdb.DuckDBPyConnection]
Poster = Callable[..., tuple[int, str]]

# Generous ceilings: real hardware clears these by orders of magnitude; tripping one means
# something is hung or pathologically slow, not merely a slow CI box.
MAX_INGEST_SECONDS = 120.0
MAX_QUERY_SECONDS = 15.0


def _config(server: ScroogeServer) -> Config:
    return Config(
        service="perf",
        local_db=":memory:",
        pod=None,
        node=None,
        scrooge_uri=server.quack_uri,
        scrooge_token=server.quack_token,
        flush_rows=1_000_000,
        flush_interval=60.0,
        max_buffer_rows=2_000_000,
    )


def _records(count: int, *, service: str = "perf") -> list[LogRecord]:
    return [
        LogRecord(
            capture_time=datetime(2026, 6, 22, 0, 0, 0, i % 1_000_000),
            service=service,
            stream="stdout",
            message=f"perf-{i}",
        )
        for i in range(count)
    ]


def _post_bulk(
    post_logs: Poster, server: ScroogeServer, total: int, batch: int
) -> None:
    for start in range(0, total, batch):
        records = [
            {"log": f"perf-{i}", "date": 1_750_000_000 + i}
            for i in range(start, min(start + batch, total))
        ]
        status, _ = post_logs(server, records)
        assert status == 204


def test_quack_ingest_throughput(scrooge: ScroogeServer, quack_reader: Reader) -> None:
    total = 20_000
    store = LogStore(":memory:")
    store.insert_many(_records(total))
    shipper = Shipper(_config(scrooge), store)

    started = time.perf_counter()
    shipped = shipper.flush()
    elapsed = time.perf_counter() - started
    store.close()

    assert shipped == total
    assert elapsed < MAX_INGEST_SECONDS
    print(
        f"\nQuack ingest: {total} rows in {elapsed:.2f}s = {total / elapsed:,.0f} rows/s"
    )

    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == total


def test_http_ingest_throughput(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    total = 20_000
    started = time.perf_counter()
    _post_bulk(post_logs, scrooge, total, batch=1_000)
    elapsed = time.perf_counter() - started

    assert elapsed < MAX_INGEST_SECONDS
    print(
        f"\nHTTP ingest: {total} rows in {elapsed:.2f}s = {total / elapsed:,.0f} rows/s"
    )

    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == total


def test_mixed_concurrent_throughput(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    per_worker = 1_000
    quack_workers = 4
    http_workers = 4
    total = (quack_workers + http_workers) * per_worker

    def ship(worker: int) -> None:
        store = LogStore(":memory:")
        store.insert_many(_records(per_worker, service=f"q-{worker}"))
        try:
            assert Shipper(_config(scrooge), store).flush() == per_worker
        finally:
            store.close()

    def post(worker: int) -> None:
        records = [
            {"log": f"h-{worker}-{i}", "date": 1_750_000_000 + i}
            for i in range(per_worker)
        ]
        assert post_logs(scrooge, records)[0] == 204

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=quack_workers + http_workers) as pool:
        futures = [pool.submit(ship, w) for w in range(quack_workers)]
        futures += [pool.submit(post, w) for w in range(http_workers)]
        for future in futures:
            future.result()
    elapsed = time.perf_counter() - started

    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == total  # no lost rows under concurrent mixed load
    assert elapsed < MAX_INGEST_SECONDS
    print(
        f"\nMixed load: {total} rows ({quack_workers} Quack + {http_workers} HTTP workers) "
        f"in {elapsed:.2f}s = {total / elapsed:,.0f} rows/s"
    )


def test_query_latency_on_populated_table(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    total = 50_000
    _post_bulk(post_logs, scrooge, total, batch=2_500)
    conn = quack_reader(scrooge)

    def timed(label: str, sql: str) -> None:
        started = time.perf_counter()
        conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - started
        print(f"\n{label}: {elapsed * 1000:.1f}ms over {total} rows")
        assert elapsed < MAX_QUERY_SECONDS

    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == total
    timed("count(*)", "SELECT count(*) FROM remote.logs")
    timed("filtered", "SELECT count(*) FROM remote.logs WHERE message LIKE 'perf-1%'")
    timed(
        "aggregation",
        "SELECT service, count(*) FROM remote.logs GROUP BY service ORDER BY 2 DESC",
    )
