"""Lifecycle supervisor for a DuckDB database served over the Quack protocol.

The supervisor locates the database file, runs first-time setup when the file is created
fresh, runs the always-on boot script that starts the Quack server, then — when an
archive is configured — sweeps log retention on an interval until signalled, before
shutting down cleanly. Schema and serving logic live in SQL; retention (rolling older
logs out to Parquet) is the one piece of data logic the supervisor drives, in Python.
"""

from __future__ import annotations

import gc
import logging
import os
import signal
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import duckdb
import fsspec
import uvicorn
from fsspec.spec import AbstractFileSystem

from scrooge import ingest, retention

logger = logging.getLogger("scrooge")

DEFAULT_BOOT_SQL = "startup.sql"
DEFAULT_SCHEMA_SQL = "schema.sql"
DEFAULT_RETENTION_ROWS = 100_000
DEFAULT_SWEEP_INTERVAL = 10.0
MIN_TOKEN_LENGTH = 4


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """Resolved runtime configuration.

    `database` is the DuckDB file to open. `boot_sql` runs on every start (it installs
    Quack and calls `quack_serve`). `schema_sql`, when set, runs only when the database
    is created fresh; an explicit empty string disables it (`None`). `storage_dir`, when
    set, is the archive root (a URL — `irods://` in production); retention sweeps roll
    over-threshold services' oldest days into it, and when unset archival is disabled.
    `retention_rows` is the per-service live-row threshold; `sweep_interval` is the
    seconds between sweeps. Both are validated positive. The `ingest_*` fields configure
    the HTTP ingest endpoint (non-secret settings only); the endpoint runs only when an
    ingest token is supplied separately to `run()`.
    """

    database: Path
    boot_sql: Path
    schema_sql: Path | None
    storage_dir: str | None
    retention_rows: int
    sweep_interval: float
    ingest_host: str
    ingest_port: int
    ingest_path: str
    ingest_service_label_key: str


def _resolve_int(
    flag: int | None, env: Mapping[str, str], env_name: str, default: int
) -> int:
    if flag is not None:
        return flag
    raw = env.get(env_name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{env_name} must be an integer: {raw!r}") from exc


def _resolve_float(
    flag: float | None, env: Mapping[str, str], env_name: str, default: float
) -> float:
    if flag is not None:
        return flag
    raw = env.get(env_name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{env_name} must be a number: {raw!r}") from exc


def resolve_config(
    *,
    database: str | None,
    schema_sql: str | None,
    boot_sql: str | None,
    storage_dir: str | None = None,
    retention_rows: int | None = None,
    sweep_interval: float | None = None,
    ingest_host: str | None = None,
    ingest_port: int | None = None,
    ingest_path: str | None = None,
    ingest_service_label_key: str | None = None,
    env: Mapping[str, str],
) -> Config:
    """Merge CLI flags with environment fallbacks into a validated Config.

    Flags take precedence over the environment. The database path is required; the boot
    script defaults to `startup.sql` and the schema script to `schema.sql` (both in the
    working directory). An explicitly empty schema (flag or env) disables schema setup.
    The archive `storage_dir` has no default — archival stays off until one is
    configured — and must be an `irods://` URL or a bare iRODS path.
    """
    db = database or env.get("DUCKDB_DATABASE")
    if not db:
        raise ConfigError(
            "no database path provided; pass --database or set DUCKDB_DATABASE"
        )

    # Resolve the schema path, distinguishing "unset" (use the default) from an explicit
    # empty value (disable schema setup): `or` can't tell them apart.
    if schema_sql is not None:
        schema_raw = schema_sql
    elif env.get("DUCKDB_SCHEMA_SQL") is not None:
        schema_raw = env["DUCKDB_SCHEMA_SQL"]
    else:
        schema_raw = DEFAULT_SCHEMA_SQL

    boot = boot_sql or env.get("DUCKDB_BOOT_SQL") or DEFAULT_BOOT_SQL

    storage = storage_dir or env.get("SCROOGE_STORAGE_DIR") or None
    if storage:
        scheme = urlsplit(storage).scheme
        if scheme and scheme != "irods":
            raise ConfigError(
                "SCROOGE_STORAGE_DIR must be an irods:// URL or a bare iRODS path; "
                f"got scheme {scheme!r} in {storage!r}"
            )

    retention_rows_value = _resolve_int(
        retention_rows, env, "SCROOGE_RETENTION_ROWS", DEFAULT_RETENTION_ROWS
    )
    if retention_rows_value <= 0:
        raise ConfigError(
            f"SCROOGE_RETENTION_ROWS must be positive; got {retention_rows_value}"
        )
    sweep_interval_value = _resolve_float(
        sweep_interval, env, "SCROOGE_SWEEP_INTERVAL", DEFAULT_SWEEP_INTERVAL
    )
    if sweep_interval_value <= 0:
        raise ConfigError(
            f"SCROOGE_SWEEP_INTERVAL must be positive; got {sweep_interval_value}"
        )

    ingest_port_value = _resolve_int(
        ingest_port, env, "SCROOGE_INGEST_PORT", ingest.DEFAULT_INGEST_PORT
    )
    if not 1 <= ingest_port_value <= 65535:
        raise ConfigError(
            f"SCROOGE_INGEST_PORT must be between 1 and 65535; got {ingest_port_value}"
        )

    return Config(
        database=Path(db),
        boot_sql=Path(boot),
        schema_sql=Path(schema_raw) if schema_raw else None,
        storage_dir=storage,
        retention_rows=retention_rows_value,
        sweep_interval=sweep_interval_value,
        ingest_host=ingest_host
        or env.get("SCROOGE_INGEST_HOST")
        or ingest.DEFAULT_INGEST_HOST,
        ingest_port=ingest_port_value,
        ingest_path=ingest_path
        or env.get("SCROOGE_INGEST_PATH")
        or ingest.DEFAULT_INGEST_PATH,
        ingest_service_label_key=ingest_service_label_key
        or env.get("SCROOGE_INGEST_SERVICE_LABEL_KEY")
        or ingest.DEFAULT_SERVICE_LABEL_KEY,
    )


def resolve_token(env: Mapping[str, str]) -> str:
    """Read and validate the Quack auth token from the environment.

    Kept out of Config so the secret never lands in a logged dataclass. Quack requires a
    token of at least 4 characters (and the embedded library has no `getenv`, so the
    boot script reads it via `getvariable`); we fail fast rather than start a server
    nobody can authenticate against.
    """
    token = env.get("QUACK_TOKEN", "")
    if len(token) < MIN_TOKEN_LENGTH:
        raise ConfigError(
            f"QUACK_TOKEN must be set and at least {MIN_TOKEN_LENGTH} characters long"
        )
    return token


def resolve_ingest_token(env: Mapping[str, str]) -> str | None:
    """Read the HTTP ingest token from the environment; `None` disables the endpoint.

    Env-only, like `resolve_token`, so the secret never lands in a logged dataclass. When
    set it must be at least `MIN_TOKEN_LENGTH` characters — a too-short value is a
    misconfiguration we fail fast on rather than expose a weakly-guarded endpoint.
    """
    token = env.get("SCROOGE_INGEST_TOKEN")
    if not token:
        return None
    if len(token) < MIN_TOKEN_LENGTH:
        raise ConfigError(
            f"SCROOGE_INGEST_TOKEN must be at least {MIN_TOKEN_LENGTH} characters long"
        )
    return token


def should_run_schema(*, existed: bool, schema_sql: Path | None) -> bool:
    """Run the schema SQL only when one is configured and the DB is freshly created."""
    return schema_sql is not None and not existed


def _read_sql(path: Path) -> str:
    try:
        return path.read_text()
    except OSError as exc:
        raise ConfigError(f"could not read SQL file {path}: {exc}") from exc


def _apply_schema(con: duckdb.DuckDBPyConnection, schema_sql: Path) -> None:
    """Apply schema SQL on a fresh database, tolerating a missing *default* schema file.

    The default `schema.sql` is best-effort: if it isn't present (e.g. not shipped in the
    image) the server still starts, just without a `logs` table. A schema configured
    explicitly must exist — a missing one is a hard error so misconfiguration fails fast.
    """
    if schema_sql.exists():
        logger.info("fresh database; applying schema %s", schema_sql)
        con.execute(_read_sql(schema_sql))
    elif str(schema_sql) == DEFAULT_SCHEMA_SQL:
        logger.warning(
            "fresh database but default schema %s not found; skipping schema setup "
            "(no `logs` table will be created)",
            schema_sql,
        )
    else:
        raise ConfigError(f"configured schema SQL not found: {schema_sql}")


def _serve(con: duckdb.DuckDBPyConnection, boot_sql: Path) -> None:
    """Run the boot script and log what `quack_serve` reported (listen URI + token).

    The boot script ends in `CALL quack_serve(...)`, whose result row carries the
    listen URI and auth token (auto-generated when QUACK_TOKEN is unset or too short),
    so we surface it — clients cannot connect without the token.
    """
    try:
        result = con.execute(_read_sql(boot_sql))
    except duckdb.Error as exc:
        raise ConfigError(
            f"boot script {boot_sql} failed: {exc}. This usually means the Quack "
            "extension could not be installed/loaded, a boot statement "
            "(e.g. quack_identify/quack_serve) is unsupported by the installed quack "
            "version, or the listen address is already in use."
        ) from exc
    if result.description is None:
        logger.warning("boot script produced no result; is quack_serve being called?")
        return
    columns = [d[0] for d in result.description]
    for row in result.fetchall():
        info = dict(zip(columns, row))
        if "auth_token" in info:
            info["auth_token"] = "***redacted***"
        logger.info("quack serving: %s", info)


def _sweep(
    con: duckdb.DuckDBPyConnection, fs: AbstractFileSystem, config: Config
) -> None:
    """Run one retention sweep and refresh the `all_logs` view if anything was archived.

    Never raises: a sweep is best-effort maintenance and must not take down the live Quack
    ingestion endpoint, so any failure (archive unreachable, DuckDB error during
    COPY/DELETE) is logged and swallowed for the next interval to retry.
    """
    if not config.storage_dir:
        return
    try:
        written = retention.sweep_once(
            con, fs, config.storage_dir, config.retention_rows
        )
        if written:
            retention.refresh_view(con, fs, config.storage_dir)
            logger.info(
                "rolled %d parquet file(s) to %s", len(written), config.storage_dir
            )
    except Exception as exc:
        logger.warning(
            "retention sweep failed; will retry next interval. Probable cause: the "
            "archive (iRODS) is unreachable or a DuckDB error occurred during "
            "COPY/DELETE. (%s)",
            exc,
        )


def _sweep_until_signal(
    con: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    config: Config,
    stop: threading.Event,
) -> None:
    """Sweep on each interval tick until signalled.

    Runs on the main thread so the signal handlers install. `con` here is a dedicated
    sweep connection, distinct from the one Quack serves on, so no single connection
    object is shared across threads; cross-connection concurrency is left to DuckDB's MVCC.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    logger.info("ready; waiting for shutdown signal (SIGINT/SIGTERM)")
    while not stop.wait(config.sweep_interval):
        _sweep(con, fs, config)


def _shutdown(
    con: duckdb.DuckDBPyConnection,
    sweep_con: duckdb.DuckDBPyConnection | None,
    ingest_con: duckdb.DuckDBPyConnection | None,
    fs: AbstractFileSystem | None,
) -> None:
    """Checkpoint and close the database connections, then release the iRODS session pool.

    Order matters. Closing the connections releases DuckDB's `read_parquet` file handles,
    but those handles sit in reference cycles, so refcounting alone won't finalize them —
    a `gc.collect()` runs their close while the iRODS session is still alive. Only then do
    we close the session. Skipping this lets the handles finalize at interpreter exit
    against a torn-down connection pool, producing noisy `NetworkException` tracebacks.
    """
    logger.info("shutting down; checkpointing database")
    try:
        con.execute("CHECKPOINT")
    except duckdb.Error as exc:
        logger.warning("checkpoint failed during shutdown: %s", exc)
    if sweep_con is not None:
        sweep_con.close()
    if ingest_con is not None:
        ingest_con.close()
    con.close()
    gc.collect()
    close = getattr(fs, "close", None)
    if callable(close):
        try:
            close()
        except Exception as exc:
            logger.warning("iRODS filesystem close failed during shutdown: %s", exc)


def _irods_storage_options(env: Mapping[str, str]) -> dict[str, Any]:
    """Build iRODS connection options for ducktape from the environment.

    With IRODS_HOST set, connect explicitly (ducktape reads the password from
    IRODS_PASSWORD, keeping the secret out of process args and logs); otherwise return
    no options so ducktape falls back to the standard iRODS environment file.
    """
    host = env.get("IRODS_HOST")
    if not host:
        return {}
    options: dict[str, Any] = {"host": host}
    if env.get("IRODS_PORT"):
        options["port"] = int(env["IRODS_PORT"])
    if env.get("IRODS_USER"):
        options["user"] = env["IRODS_USER"]
    if env.get("IRODS_ZONE"):
        options["zone"] = env["IRODS_ZONE"]
    return options


def _register_filesystems(
    con: duckdb.DuckDBPyConnection, env: Mapping[str, str]
) -> AbstractFileSystem:
    """Register the ducktape iRODS backend so SQL can read/write `irods://` paths.

    The filesystem is lazy: no iRODS connection is opened until an `irods://` path is
    used, so registering is harmless even when iRODS is never queried. Returns the
    filesystem so retention sweeps can reuse it for directory listing/creation.
    """
    options = _irods_storage_options(env)
    fs = fsspec.filesystem("irods", **options)
    con.register_filesystem(fs)
    source = "explicit credentials" if options else "iRODS environment file"
    logger.info("registered iRODS filesystem (irods://) using %s", source)
    return fs


def _start_ingest(
    con: duckdb.DuckDBPyConnection, config: Config, token: str
) -> tuple[duckdb.DuckDBPyConnection, uvicorn.Server, threading.Thread]:
    """Build the ingest app on a dedicated connection and start it in a thread."""
    cfg = ingest.IngestConfig(
        token=token,
        host=config.ingest_host,
        port=config.ingest_port,
        path=config.ingest_path,
        service_label_key=config.ingest_service_label_key,
    )
    ingest_con = con.cursor()
    app = ingest.build_app(ingest_con, threading.Lock(), cfg)
    try:
        server, thread = ingest.serve_in_thread(app, cfg)
    except ingest.IngestError as exc:
        ingest_con.close()
        raise ConfigError(str(exc)) from exc
    return ingest_con, server, thread


def run(
    config: Config,
    *,
    token: str,
    ingest_token: str | None = None,
    env: Mapping[str, str] | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Open the database, run setup, start the Quack server, and block until signalled.

    Must run on the main thread so the signal handlers can be installed. When
    `ingest_token` is set, an HTTP ingest endpoint runs in a background thread alongside
    the Quack server and the sweep loop.
    """
    env = env if env is not None else os.environ
    stop = stop or threading.Event()
    existed = config.database.exists()
    logger.info(
        "opening database %s (%s)",
        config.database,
        "existing" if existed else "creating",
    )
    con = duckdb.connect(str(config.database))
    fs: AbstractFileSystem | None = None
    sweep_con: duckdb.DuckDBPyConnection | None = None
    ingest_con: duckdb.DuckDBPyConnection | None = None
    ingest_server: uvicorn.Server | None = None
    ingest_thread: threading.Thread | None = None
    try:
        con.execute("SET VARIABLE quack_token = ?", [token])
        fs = _register_filesystems(con, env)
        if should_run_schema(existed=existed, schema_sql=config.schema_sql):
            assert config.schema_sql is not None
            _apply_schema(con, config.schema_sql)
        _serve(con, config.boot_sql)
        # Sweeps and HTTP ingest each run on their own connection (sharing the same
        # database instance and the inherited iRODS filesystem) so the main thread and the
        # ingest thread never issue queries on `con` concurrently with Quack's own use of
        # it. DuckDB's MVCC arbitrates the connections; a sweep that loses a write-write
        # race with a concurrent append/insert just raises and is retried (see _sweep).
        sweep_con = con.cursor()
        if ingest_token:
            ingest_con, ingest_server, ingest_thread = _start_ingest(
                con, config, ingest_token
            )
        else:
            logger.info("HTTP ingest disabled (set SCROOGE_INGEST_TOKEN to enable)")
        # Reconcile interrupted exports before the view is built so an orphaned parquet
        # never enters `all_logs`, even transiently; sweep_once re-checks as a backstop.
        if config.storage_dir:
            try:
                retention.reconcile_pending(sweep_con, fs)
            except Exception as exc:
                logger.warning(
                    "startup export reconcile failed; sweeps stay paused until it "
                    "succeeds. Probable cause: the archive (iRODS) is unreachable. (%s)",
                    exc,
                )
        try:
            retention.refresh_view(sweep_con, fs, config.storage_dir)
        except Exception as exc:
            logger.warning("initial all_logs view refresh failed: %s", exc)
        _sweep_until_signal(sweep_con, fs, config, stop)
        _sweep(sweep_con, fs, config)
    finally:
        if ingest_server is not None:
            ingest_server.should_exit = True
            if ingest_thread is not None:
                ingest_thread.join(timeout=10.0)
                if ingest_thread.is_alive():
                    # An in-flight insert is still using ingest_con; closing it now could
                    # crash that worker. Leave it for process exit rather than race it.
                    logger.warning(
                        "ingest server did not stop in time; leaving its connection open"
                    )
                    ingest_con = None
        _shutdown(con, sweep_con, ingest_con, fs)
