"""HTTP ingest endpoint for Fluent Bit's `http` output.

Fluent Bit can't speak the Quack protocol, so this exposes a small Starlette app (run by
uvicorn in a dedicated thread) that accepts log records over HTTP and inserts them into the
same `logs` table that daffy/Quack and retention feed. Records map onto the canonical
columns; the full original record is preserved in `fields`.

The parsing and mapping helpers are pure functions so they can be tested without a server
or a database.
"""

from __future__ import annotations

import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import uvicorn
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from scrooge.retention import COLUMNS

logger = logging.getLogger("scrooge.ingest")

DEFAULT_INGEST_HOST = "0.0.0.0"
DEFAULT_INGEST_PORT = 9595
DEFAULT_INGEST_PATH = "/logs"
DEFAULT_SERVICE_LABEL_KEY = "app.kubernetes.io/name"
DEFAULT_MESSAGE_KEY = "log"
DEFAULT_DATE_KEY = "date"

# fields is the only JSON column; the rest bind as their natural types.
_PLACEHOLDERS = ", ".join("?::JSON" if c == "fields" else "?" for c in COLUMNS)
_INSERT_SQL = f"INSERT INTO logs ({', '.join(COLUMNS)}) VALUES ({_PLACEHOLDERS})"


class BadPayload(ValueError):
    """Raised when a request body can't be parsed into log records."""


class IngestError(RuntimeError):
    """Raised when the ingest HTTP server fails to start (e.g. the port is in use)."""


@dataclass(frozen=True)
class IngestConfig:
    """Configuration for the HTTP ingest endpoint.

    The endpoint is enabled only when `token` is set. `service_label_key`, `message_key`,
    and `date_key` select where each record's service identity, message, and timestamp come
    from; their defaults match the Kubernetes filter + Fluent Bit `http` output conventions.
    """

    token: str
    host: str = DEFAULT_INGEST_HOST
    port: int = DEFAULT_INGEST_PORT
    path: str = DEFAULT_INGEST_PATH
    service_label_key: str = DEFAULT_SERVICE_LABEL_KEY
    message_key: str = DEFAULT_MESSAGE_KEY
    date_key: str = DEFAULT_DATE_KEY


def parse_body(body: bytes) -> list[dict[str, Any]]:
    """Parse a request body into a list of records.

    Accepts a JSON array (`format json`), a single JSON object, or newline-delimited JSON
    (`format json_lines`). Raises `BadPayload` if the body isn't one of those or holds a
    non-object record.
    """
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return []
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        records: list[Any] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise BadPayload(f"invalid JSON line: {exc}") from exc
        return _as_records(records)
    if isinstance(doc, list):
        return _as_records(doc)
    if isinstance(doc, dict):
        return [doc]
    raise BadPayload(
        "body must be a JSON object, array of objects, or newline-delimited"
    )


def _as_records(items: list[Any]) -> list[dict[str, Any]]:
    if not all(isinstance(item, dict) for item in items):
        raise BadPayload("every record must be a JSON object")
    return items


def _utc_naive(dt: datetime) -> datetime:
    """Normalize to a naive UTC datetime (tz-aware values are converted, then dropped)."""
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def _parse_time(value: Any) -> datetime:
    """Parse a Fluent Bit timestamp into a naive UTC datetime; fall back to receive time.

    `capture_time` is a naive DuckDB `TIMESTAMP`. Storing naive UTC keeps HTTP-ingested
    rows consistent with each other — and with daffy/Quack rows — regardless of the host
    timezone; a tz-aware datetime would be shifted to local time on insert (and a naive
    one would not, so the two would disagree).
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    if isinstance(value, bool):
        return now
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=UTC).replace(tzinfo=None)
        except (OverflowError, OSError, ValueError):
            return now
    if isinstance(value, str):
        try:
            return _utc_naive(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            try:
                return datetime.fromtimestamp(float(value), tz=UTC).replace(tzinfo=None)
            except (OverflowError, OSError, ValueError):
                return now
    return now


def map_record(record: dict[str, Any], cfg: IngestConfig) -> dict[str, Any]:
    """Map a Fluent Bit record onto a `logs` row, keyed by column name.

    `service` comes from the configured pod label, falling back to the container name and
    then `"unknown"` (the column is NOT NULL). The full original record is kept in `fields`.
    Returning a dict keyed by column (rather than a positional tuple) keeps the mapping in
    sync with `COLUMNS` by name — the insert orders by `COLUMNS`, so reordering it can't
    silently misbind values.
    """
    k8s = record.get("kubernetes")
    k8s = k8s if isinstance(k8s, dict) else {}
    labels = k8s.get("labels")
    labels = labels if isinstance(labels, dict) else {}

    service = (
        labels.get(cfg.service_label_key) or k8s.get("container_name") or "unknown"
    )
    message = record.get(cfg.message_key)
    message = "" if message is None else str(message).rstrip("\n")

    return {
        "capture_time": _parse_time(record.get(cfg.date_key)),
        "service": service,
        "pod": k8s.get("pod_name"),
        "node": k8s.get("host"),
        "stream": record.get("stream") or "",  # NOT NULL
        "level": record.get("level") or "",
        "message": message,
        "fields": json.dumps(record, default=str),
    }


def insert_records(
    conn: duckdb.DuckDBPyConnection,
    lock: threading.Lock,
    records: list[dict[str, Any]],
) -> int:
    """Insert mapped rows under `lock` (the ingest connection is single-use). Returns count.

    Rows are ordered by `COLUMNS` so the bind order always matches `_INSERT_SQL`.
    """
    if not records:
        return 0
    rows = [tuple(rec[c] for c in COLUMNS) for rec in records]
    with lock:
        conn.executemany(_INSERT_SQL, rows)
    return len(records)


def authorized(header_value: str, expected: bytes) -> bool:
    """Constant-time bearer-token check.

    Compares as bytes: `hmac.compare_digest` raises `TypeError` on a non-ASCII `str`
    (Starlette latin-1-decodes raw header bytes), which would otherwise surface as a 500 —
    and an endless Fluent Bit retry — instead of a clean 401.
    """
    return hmac.compare_digest(header_value.encode("utf-8"), expected)


def build_app(
    conn: duckdb.DuckDBPyConnection,
    lock: threading.Lock,
    cfg: IngestConfig,
) -> Starlette:
    """Build the Starlette app: `POST <path>` ingests, `GET /healthz` is an unauthed probe."""
    expected = f"Bearer {cfg.token}".encode()

    async def ingest(request: Request) -> Response:
        if not authorized(request.headers.get("authorization", ""), expected):
            return Response(status_code=401)
        body = await request.body()
        try:
            records = parse_body(body)
            mapped = [map_record(r, cfg) for r in records]
        except ValueError as exc:  # BadPayload and any mapping error
            return PlainTextResponse(f"bad payload: {exc}", status_code=400)
        if not mapped:
            return Response(status_code=204)
        try:
            await run_in_threadpool(insert_records, conn, lock, mapped)
        except duckdb.Error as exc:
            logger.warning(
                "ingest insert failed (%d rows); Fluent Bit will retry. Probable cause: "
                "a DuckDB error or write conflict with a concurrent sweep/append. (%s)",
                len(mapped),
                exc,
            )
            return PlainTextResponse("insert failed", status_code=500)
        return Response(status_code=204)

    async def health(_request: Request) -> Response:
        # Liveness only: confirms the HTTP server is up. Intentionally does not probe the
        # DB — a DB-dependent liveness check on the embedded DuckDB would risk restart
        # loops on a transient hiccup. Wire a readiness probe separately if depooling is
        # needed.
        return PlainTextResponse("ok")

    return Starlette(
        routes=[
            Route(cfg.path, ingest, methods=["POST"]),
            Route("/healthz", health, methods=["GET"]),
        ]
    )


def serve_in_thread(
    app: Starlette, cfg: IngestConfig
) -> tuple[uvicorn.Server, threading.Thread]:
    """Start uvicorn in a daemon thread, waiting until it is actually serving.

    uvicorn skips installing its own signal handlers when not on the main thread, so this
    leaves scrooge's SIGINT/SIGTERM handling intact. We block until `server.started` so a
    bind failure (e.g. the port is in use) raises `IngestError` here instead of dying
    silently in the thread while the caller believes the endpoint is up.
    """
    config = uvicorn.Config(app, host=cfg.host, port=cfg.port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="scrooge-ingest", daemon=True)
    thread.start()

    waited = 0.0
    while not server.started:
        if not thread.is_alive():
            raise IngestError(
                f"ingest server failed to start on {cfg.host}:{cfg.port} "
                "(port already in use?)"
            )
        if waited >= 5.0:
            server.should_exit = True
            raise IngestError(
                f"ingest server did not start within 5s on {cfg.host}:{cfg.port}"
            )
        time.sleep(0.05)
        waited += 0.05

    logger.info(
        "ingest endpoint serving on http://%s:%d%s", cfg.host, cfg.port, cfg.path
    )
    return server, thread
