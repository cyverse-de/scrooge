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

import logging
import posixpath
from datetime import date

import duckdb
from fsspec.spec import AbstractFileSystem

from scrooge import metrics

logger = logging.getLogger("scrooge.retention")

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

# Journal of exports whose COPY may have happened but whose DELETE has not committed.
# A surviving row after a crash names a Parquet file that duplicates still-live rows.
_PENDING_DDL = (
    "CREATE TABLE IF NOT EXISTS pending_exports ("
    "out_url VARCHAR NOT NULL PRIMARY KEY, "
    "service VARCHAR NOT NULL, "
    "day DATE NOT NULL)"
)


def _ensure_pending_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_PENDING_DDL)


def _pending_count(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute("SELECT count(*) FROM pending_exports").fetchone()
    return int(row[0]) if row else 0


def _rollback_quietly(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.execute("ROLLBACK")
    except Exception:
        pass  # no transaction was open


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
    """Next per-day sequence number, found by listing the directory and matching names.

    Listing (not globbing) avoids treating glob metacharacters in a service name
    (`*`, `?`, `[`) as patterns — which would silently fail to match existing files and
    reset the sequence to 1, overwriting an already-archived file.
    """
    prefix = f"{day}-"
    suffix = f"_{service_lower}.parquet"
    highest = 0
    try:
        entries = fs.ls(directory, detail=False)
    except FileNotFoundError:
        return 1
    for path in entries:
        name = posixpath.basename(str(path))
        if name.startswith(prefix) and name.endswith(suffix):
            seq_part = name[len(prefix) : -len(suffix)]
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
    """Export one service's logs for one day to a new Parquet file and return its URL.

    The Parquet write can't join a DuckDB transaction, so a journal closes the gap: an
    intent marker is committed to `pending_exports` before the COPY, and the row DELETE
    plus marker removal then commit atomically. On any error the just-written file is
    removed — its rows are still live, so it would double-count in `all_logs` — and a
    hard kill leaves the marker for `reconcile_pending` to clean up. Rows are never lost.
    """
    directory = service_dir(storage_dir, service)
    fs.makedirs(directory, exist_ok=True)
    day_str = day.isoformat()
    service_lower = service.lower()
    seq = _next_sequence(fs, directory, day_str, service_lower)
    out_url = f"{directory}/{day_str}-{seq:03d}_{service_lower}.parquet"

    # COPY's inner predicate must be a literal (COPY is not parameterizable); it must
    # select exactly the rows the parameterized DELETE below removes.
    predicate = (
        f"service = {sql_literal(service)} "
        f"AND capture_time::date = DATE {sql_literal(day_str)}"
    )
    _ensure_pending_table(conn)
    conn.execute(
        "INSERT OR REPLACE INTO pending_exports (out_url, service, day) "
        "VALUES (?, ?, ?)",
        [out_url, service, day],
    )
    try:
        conn.execute(
            f"COPY (SELECT {_SELECT_COLS} FROM logs WHERE {predicate} "
            f"ORDER BY capture_time) TO {sql_literal(out_url)} (FORMAT PARQUET)"
        )
        conn.execute("BEGIN")
        conn.execute(
            "DELETE FROM logs WHERE service = ? AND capture_time::date = ?",
            [service, day],
        )
        conn.execute("DELETE FROM pending_exports WHERE out_url = ?", [out_url])
        conn.execute("COMMIT")
    except Exception:
        _rollback_quietly(conn)
        removed = True
        try:
            if fs.exists(out_url):
                fs.rm(out_url)
        except Exception as cleanup_exc:
            removed = False
            logger.warning(
                "failed to remove orphan parquet %s after export error: %s",
                out_url,
                cleanup_exc,
            )
        # The marker must outlive any file that might still exist, so reconcile_pending
        # can retry the removal; clear it only once the orphan is confirmed gone.
        if removed:
            try:
                conn.execute("DELETE FROM pending_exports WHERE out_url = ?", [out_url])
            except Exception as marker_exc:
                logger.warning(
                    "could not clear pending_exports marker for %s after export "
                    "failure; startup or the next sweep will reconcile it. (%s)",
                    out_url,
                    marker_exc,
                )
        raise
    metrics.ARCHIVE_FILES.labels(service).inc()
    try:
        size = fs.size(out_url)
    except Exception as exc:
        logger.debug("could not size archived parquet %s: %s", out_url, exc)
    else:
        if size:
            metrics.ARCHIVE_BYTES.labels(service).inc(size)
    return out_url


def reconcile_pending(
    conn: duckdb.DuckDBPyConnection, fs: AbstractFileSystem
) -> list[str]:
    """Clean up after interrupted exports; returns the markers cleared.

    A surviving `pending_exports` row means an export's COPY may have run but its DELETE
    never committed: the rows are still live, so any file at `out_url` is an orphan whose
    rows would appear twice in `all_logs`. Remove the file, then clear the marker — in
    that order, so a failed removal keeps the marker and the next reconcile retries.
    """
    _ensure_pending_table(conn)
    cleared: list[str] = []
    for (out_url,) in conn.execute("SELECT out_url FROM pending_exports").fetchall():
        try:
            if fs.exists(out_url):
                logger.warning(
                    "removing orphaned parquet %s left by an interrupted export "
                    "(probable cause: process killed between COPY and DELETE); "
                    "its rows are still live and will be re-archived",
                    out_url,
                )
                fs.rm(out_url)
            conn.execute("DELETE FROM pending_exports WHERE out_url = ?", [out_url])
            cleared.append(out_url)
        except Exception as exc:
            logger.warning(
                "could not reconcile pending export %s; keeping its marker (sweeps "
                "pause until it clears). Probable cause: the archive (iRODS) is "
                "unreachable. (%s)",
                out_url,
                exc,
            )
    return cleared


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

    Returns the URLs of the Parquet files written. Interrupted exports are reconciled
    first; if a marker can't be cleared (archive unreachable), the sweep is skipped —
    a COPY would fail the same way, and exporting past an unresolved marker could
    stack a second sequence file on top of an orphan.
    """
    _ensure_pending_table(conn)
    if _pending_count(conn):
        reconcile_pending(conn, fs)
        if _pending_count(conn):
            logger.warning(
                "skipping retention sweep: unresolved pending-export markers remain "
                "after reconcile; will retry next interval"
            )
            return []

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


def _logs_table_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'logs' LIMIT 1"
    ).fetchone()
    return row is not None


def refresh_view(
    conn: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    storage_dir: str | None,
) -> None:
    """(Re)create the ``all_logs`` view spanning live rows and the Parquet archive.

    With no archive configured or no Parquet present, the view is just the live table.
    No-op when there is no ``logs`` table (e.g. a pre-existing database, or a custom
    schema that doesn't define it) — building the view would otherwise raise.
    """
    if not _logs_table_exists(conn):
        logger.warning("no `logs` table present; leaving `all_logs` view uncreated")
        return
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
