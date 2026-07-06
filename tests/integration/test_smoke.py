"""Harness sanity: a spawned scrooge serves probes, metrics, and Quack."""

from __future__ import annotations

import urllib.request
from collections.abc import Callable

import duckdb
import pytest

from conftest import ScroogeServer

pytestmark = pytest.mark.integration


def _get(server: ScroogeServer, path: str) -> tuple[int, str]:
    with urllib.request.urlopen(server.http_url(path), timeout=10.0) as resp:
        return resp.status, resp.read().decode("utf-8")


def test_probes_up(scrooge: ScroogeServer) -> None:
    health_status, health_body = _get(scrooge, "/healthz")
    assert health_status == 200
    assert health_body == "ok"

    ready_status, ready_body = _get(scrooge, "/readyz")
    assert ready_status == 200
    assert ready_body == "ok"


def test_metrics_exposed(scrooge: ScroogeServer) -> None:
    status, body = _get(scrooge, "/metrics")
    assert status == 200
    assert "scrooge_ingest_requests_total" in body


def test_quack_reachable(
    scrooge: ScroogeServer,
    quack_reader: Callable[[ScroogeServer], duckdb.DuckDBPyConnection],
) -> None:
    conn = quack_reader(scrooge)
    (one,) = conn.execute("SELECT 1").fetchone()
    assert one == 1
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 0
