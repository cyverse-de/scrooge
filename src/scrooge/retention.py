"""Roll older logs out of the live DuckDB into per-service, per-day Parquet files.

When a service's live row count exceeds the threshold, the oldest log-days are exported
(oldest first) and deleted until the service is back under the threshold. The most recent
day is kept live when possible so recent logs stay queryable without touching Parquet.

The archive lives wherever ``storage_dir`` points: an injected fsspec filesystem handles
directory listing/creation, and DuckDB's ``COPY``/``read_parquet`` do the IO through the
same backend. In production that backend is iRODS (``irods://``); tests drive it with a
local filesystem. ``storage_dir`` is a plain URL string, never a ``pathlib.Path`` — Path
would collapse ``irods://`` to ``irods:/``.
"""

from __future__ import annotations

import posixpath
from datetime import date

import duckdb
from fsspec.spec import AbstractFileSystem

# Column order used by every SELECT against the logs table; mirrors the schema.sql DDL.
COLUMNS: tuple[str, ...] = (
    "capture_time",
    "service",
    "pod",
    "node",
    "stream",
    "level",
    "message",
    "fields",
)

_SELECT_COLS = ", ".join(COLUMNS)


def sql_literal(value: str) -> str:
    """Quote a string as a SQL literal, for the few spots a bind parameter won't work.

    DuckDB's ``COPY ... TO '<path>'`` target and ``read_parquet('<glob>')`` pattern must
    be literals, not parameters. Single quotes are doubled per SQL string-literal rules.
    """
    return "'" + value.replace("'", "''") + "'"


def service_dir(storage_dir: str, service: str) -> str:
    """Return the archive directory URL for one service (lower-cased)."""
    return f"{storage_dir.rstrip('/')}/{service.lower()}"


def _next_sequence(
    fs: AbstractFileSystem, directory: str, day: str, service_lower: str
) -> int:
    suffix = f"_{service_lower}.parquet"
    highest = 0
    for path in fs.glob(f"{directory}/{day}-*{suffix}"):
        name = posixpath.basename(str(path))
        seq_part = name[len(day) + 1 : -len(suffix)]
        if seq_part.isdigit():
            highest = max(highest, int(seq_part))
    return highest + 1


def export_day(
    conn: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    storage_dir: str,
    service: str,
    day: date,
) -> str:
    """Export one service's logs for one day to a new Parquet file and return its URL."""
    directory = service_dir(storage_dir, service)
    fs.makedirs(directory, exist_ok=True)
    day_str = day.isoformat()
    service_lower = service.lower()
    seq = _next_sequence(fs, directory, day_str, service_lower)
    out_url = f"{directory}/{day_str}-{seq:03d}_{service_lower}.parquet"

    predicate = (
        f"service = {sql_literal(service)} "
        f"AND capture_time::date = DATE {sql_literal(day_str)}"
    )
    conn.execute(
        f"COPY (SELECT {_SELECT_COLS} FROM logs WHERE {predicate} ORDER BY capture_time) "
        f"TO {sql_literal(out_url)} (FORMAT PARQUET)"
    )
    conn.execute(f"DELETE FROM logs WHERE {predicate}")
    return out_url


def _service_count(conn: duckdb.DuckDBPyConnection, service: str) -> int:
    row = conn.execute(
        "SELECT count(*) FROM logs WHERE service = ?", [service]
    ).fetchone()
    return int(row[0]) if row else 0


def _days_for_service(conn: duckdb.DuckDBPyConnection, service: str) -> list[date]:
    rows = conn.execute(
        "SELECT DISTINCT capture_time::date AS d FROM logs WHERE service = ? ORDER BY d",
        [service],
    ).fetchall()
    return [r[0] for r in rows]


def sweep_once(
    conn: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    storage_dir: str,
    threshold: int,
) -> list[str]:
    """Export and delete oldest log-days for every over-threshold service.

    Returns the URLs of the Parquet files written.
    """
    services = [
        r[0]
        for r in conn.execute(
            "SELECT service FROM logs GROUP BY service HAVING count(*) > ?",
            [threshold],
        ).fetchall()
    ]

    written: list[str] = []
    for service in services:
        while _service_count(conn, service) > threshold:
            days = _days_for_service(conn, service)
            if not days:
                break
            written.append(export_day(conn, fs, storage_dir, service, days[0]))
    return written


def refresh_view(
    conn: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    storage_dir: str | None,
) -> None:
    """(Re)create the ``all_logs`` view spanning live rows and the Parquet archive.

    With no archive configured or no Parquet present, the view is just the live table.
    """
    if storage_dir:
        pattern = f"{storage_dir.rstrip('/')}/*/*.parquet"
        if fs.glob(pattern):
            conn.execute(
                f"CREATE OR REPLACE VIEW all_logs AS "
                f"SELECT {_SELECT_COLS} FROM logs UNION ALL "
                f"SELECT {_SELECT_COLS} FROM read_parquet("
                f"{sql_literal(pattern)}, union_by_name => true)"
            )
            return
    conn.execute(f"CREATE OR REPLACE VIEW all_logs AS SELECT {_SELECT_COLS} FROM logs")
