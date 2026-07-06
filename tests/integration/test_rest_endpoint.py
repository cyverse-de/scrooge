"""Edge cases for the HTTP REST ingest endpoint against a live scrooge."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from conftest import ScroogeServer, ServerFactory, metric_value

pytestmark = pytest.mark.integration

Reader = Callable[[ScroogeServer], duckdb.DuckDBPyConnection]
Poster = Callable[..., tuple[int, str]]

_MINIMAL = [{"log": "x", "kubernetes": {"labels": {"app.kubernetes.io/name": "svc"}}}]


@pytest.mark.parametrize(
    ("token_kind", "expected"),
    [
        ("valid", 204),
        ("missing", 401),
        ("wrong", 401),
        ("nonascii", 401),  # non-ASCII bearer must be a clean 401, never a 500
    ],
)
def test_auth(
    scrooge: ScroogeServer, post_logs: Poster, token_kind: str, expected: int
) -> None:
    token: object = {
        "valid": scrooge.ingest_token,
        "missing": None,
        "wrong": "wrong-token",
        "nonascii": "nöt-thé-tökén",
    }[token_kind]
    status, _ = post_logs(scrooge, _MINIMAL, token=token)
    assert status == expected


@pytest.mark.parametrize(
    "body",
    [
        "{bad json",
        "[1, 2, 3]",  # array of scalars
        "5",  # bare scalar
        '"just a string"',
    ],
)
def test_malformed_body_400(
    scrooge: ScroogeServer, post_logs: Poster, body: str
) -> None:
    status, _ = post_logs(scrooge, body)
    assert status == 400


@pytest.mark.parametrize(
    ("body", "expected_rows"),
    [
        ('[{"log": "a"}, {"log": "b"}]', 2),  # JSON array
        ('{"log": "a"}', 1),  # single object
        ('{"log": "a"}\n{"log": "b"}\n\n', 2),  # ndjson, blank line skipped
        ("", 0),  # empty body
        ("   \n  ", 0),  # whitespace-only body
    ],
)
def test_body_formats(
    scrooge: ScroogeServer,
    quack_reader: Reader,
    post_logs: Poster,
    body: str,
    expected_rows: int,
) -> None:
    status, _ = post_logs(scrooge, body)
    assert status == 204
    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == expected_rows


def _naive_utc_from_epoch(value: float) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC).replace(tzinfo=None)


@pytest.mark.parametrize(
    ("date_value", "expected"),
    [
        (1_750_000_000, _naive_utc_from_epoch(1_750_000_000)),
        (1_750_000_000.5, _naive_utc_from_epoch(1_750_000_000.5)),
        ("2026-06-22T01:02:03Z", datetime(2026, 6, 22, 1, 2, 3)),
        ("2026-06-22T01:02:03+02:00", datetime(2026, 6, 21, 23, 2, 3)),
    ],
)
def test_timestamp_exact(
    scrooge: ScroogeServer,
    quack_reader: Reader,
    post_logs: Poster,
    date_value: object,
    expected: datetime,
) -> None:
    status, _ = post_logs(scrooge, [{"log": "t", "date": date_value}])
    assert status == 204
    conn = quack_reader(scrooge)
    (captured,) = conn.execute("SELECT capture_time FROM remote.logs").fetchone()
    assert captured == expected


@pytest.mark.parametrize("date_value", ["not-a-time", True, None])
def test_timestamp_falls_back_to_receive_time(
    scrooge: ScroogeServer,
    quack_reader: Reader,
    post_logs: Poster,
    date_value: object,
) -> None:
    before = datetime.now(UTC).replace(tzinfo=None)
    status, _ = post_logs(scrooge, [{"log": "t", "date": date_value}])
    assert status == 204
    after = datetime.now(UTC).replace(tzinfo=None)
    conn = quack_reader(scrooge)
    (captured,) = conn.execute("SELECT capture_time FROM remote.logs").fetchone()
    assert before - timedelta(seconds=5) <= captured <= after + timedelta(seconds=5)


@pytest.mark.parametrize(
    ("kubernetes", "expected_service"),
    [
        ({"labels": {"app.kubernetes.io/name": "web"}, "container_name": "c"}, "web"),
        ({"container_name": "sidecar"}, "sidecar"),  # no label → container name
        ({}, "unknown"),  # neither → NOT NULL fallback
    ],
)
def test_service_resolution(
    scrooge: ScroogeServer,
    quack_reader: Reader,
    post_logs: Poster,
    kubernetes: dict[str, object],
    expected_service: str,
) -> None:
    status, _ = post_logs(scrooge, [{"log": "s", "kubernetes": kubernetes}])
    assert status == 204
    conn = quack_reader(scrooge)
    (service,) = conn.execute("SELECT service FROM remote.logs").fetchone()
    assert service == expected_service


@pytest.mark.parametrize(
    ("record", "expected_message"),
    [
        ({"stream": "stdout"}, ""),  # missing message key → NOT NULL empty
        ({"log": 123}, "123"),  # non-string coerced via str()
        ({"log": "trailing\n"}, "trailing"),  # single trailing newline stripped
    ],
)
def test_message_handling(
    scrooge: ScroogeServer,
    quack_reader: Reader,
    post_logs: Poster,
    record: dict[str, object],
    expected_message: str,
) -> None:
    status, _ = post_logs(scrooge, [record])
    assert status == 204
    conn = quack_reader(scrooge)
    (message,) = conn.execute("SELECT message FROM remote.logs").fetchone()
    assert message == expected_message


def test_large_single_post(
    scrooge: ScroogeServer, quack_reader: Reader, post_logs: Poster
) -> None:
    records = [{"log": f"line-{i}", "date": 1_750_000_000 + i} for i in range(5_000)]
    status, _ = post_logs(scrooge, records)
    assert status == 204
    conn = quack_reader(scrooge)
    (count,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
    assert count == 5_000


def test_ingest_route_off_without_token(
    scrooge_factory: ServerFactory, post_logs: Poster
) -> None:
    server = scrooge_factory(ingest_token=None)
    # Probes and metrics still serve in a Quack-only deployment...
    status, _ = post_logs(server, _MINIMAL, token=None)
    assert status == 404  # ...but the ingest route isn't registered.


def test_metrics_reflect_outcomes(
    scrooge: ScroogeServer,
    post_logs: Poster,
    get_metrics: Callable[[ScroogeServer], str],
) -> None:
    before = get_metrics(scrooge)

    ok_status, _ = post_logs(
        scrooge,
        [{"log": "m", "kubernetes": {"labels": {"app.kubernetes.io/name": "m"}}}],
    )
    empty_status, _ = post_logs(scrooge, "")
    unauth_status, _ = post_logs(scrooge, _MINIMAL, token="nope")
    bad_status, _ = post_logs(scrooge, "5")
    assert (ok_status, empty_status, unauth_status, bad_status) == (204, 204, 401, 400)

    after = get_metrics(scrooge)
    name = "scrooge_ingest_requests_total"
    for outcome in ("ok", "empty", "unauthorized", "bad_payload"):
        delta = metric_value(after, name, {"outcome": outcome}) - metric_value(
            before, name, {"outcome": outcome}
        )
        assert delta == 1, outcome
    rows_delta = metric_value(
        after, "scrooge_ingest_rows_total", {"service": "m"}
    ) - metric_value(before, "scrooge_ingest_rows_total", {"service": "m"})
    assert rows_delta == 1
