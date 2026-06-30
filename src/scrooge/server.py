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

import duckdb
import fsspec
from fsspec.spec import AbstractFileSystem

from scrooge import retention

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
    is created fresh. `storage_dir`, when set, is the archive root (a URL — `irods://` in
    production); retention sweeps roll over-threshold services' oldest days into it, and
    when unset archival is disabled. `retention_rows` is the per-service live-row
    threshold; `sweep_interval` is the seconds between sweeps.
    """

    database: Path
    boot_sql: Path
    schema_sql: Path | None
    storage_dir: str | None
    retention_rows: int
    sweep_interval: float


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
    env: Mapping[str, str],
) -> Config:
    """Merge CLI flags with environment fallbacks into a validated Config.

    Flags take precedence over the environment. The database path is required; the boot
    script defaults to `startup.sql` and the schema script to `schema.sql` (both in the
    working directory). The archive `storage_dir` has no default — archival stays off
    until one is configured.
    """
    db = database or env.get("DUCKDB_DATABASE")
    if not db:
        raise ConfigError(
            "no database path provided; pass --database or set DUCKDB_DATABASE"
        )
    schema = schema_sql or env.get("DUCKDB_SCHEMA_SQL") or DEFAULT_SCHEMA_SQL
    boot = boot_sql or env.get("DUCKDB_BOOT_SQL") or DEFAULT_BOOT_SQL
    storage = storage_dir or env.get("SCROOGE_STORAGE_DIR")
    return Config(
        database=Path(db),
        boot_sql=Path(boot),
        schema_sql=Path(schema),
        storage_dir=storage or None,
        retention_rows=_resolve_int(
            retention_rows, env, "SCROOGE_RETENTION_ROWS", DEFAULT_RETENTION_ROWS
        ),
        sweep_interval=_resolve_float(
            sweep_interval, env, "SCROOGE_SWEEP_INTERVAL", DEFAULT_SWEEP_INTERVAL
        ),
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


def should_run_schema(*, existed: bool, schema_sql: Path | None) -> bool:
    """Run the schema SQL only when one is configured and the DB is freshly created."""
    return schema_sql is not None and not existed


def _read_sql(path: Path) -> str:
    try:
        return path.read_text()
    except OSError as exc:
        raise ConfigError(f"could not read SQL file {path}: {exc}") from exc


def _serve(con: duckdb.DuckDBPyConnection, boot_sql: Path) -> None:
    """Run the boot script and log what `quack_serve` reported (listen URI + token).

    The boot script ends in `CALL quack_serve(...)`, whose result row carries the
    listen URI and auth token (auto-generated when QUACK_TOKEN is unset or too short),
    so we surface it — clients cannot connect without the token.
    """
    result = con.execute(_read_sql(boot_sql))
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
    """Run one retention sweep and refresh the `all_logs` view if anything was archived."""
    if not config.storage_dir:
        return
    written = retention.sweep_once(con, fs, config.storage_dir, config.retention_rows)
    if written:
        retention.refresh_view(con, fs, config.storage_dir)
        logger.info("rolled %d parquet file(s) to %s", len(written), config.storage_dir)


def _sweep_until_signal(
    con: duckdb.DuckDBPyConnection,
    fs: AbstractFileSystem,
    config: Config,
    stop: threading.Event,
) -> None:
    """Sweep on each interval tick until signalled. Runs on the main thread (so the signal
    handlers install) and is the only Python caller on the connection, so no lock is
    needed between sweeps and Quack's own appends.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    logger.info("ready; waiting for shutdown signal (SIGINT/SIGTERM)")
    while not stop.wait(config.sweep_interval):
        _sweep(con, fs, config)


def _shutdown(con: duckdb.DuckDBPyConnection, fs: AbstractFileSystem | None) -> None:
    """Checkpoint and close the database, then release the iRODS session pool.

    Order matters. Closing the connection releases DuckDB's `read_parquet` file handles,
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


def run(
    config: Config,
    *,
    token: str,
    env: Mapping[str, str] | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Open the database, run setup, start the Quack server, and block until signalled.

    Must run on the main thread so the signal handlers can be installed.
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
    try:
        con.execute("SET VARIABLE quack_token = ?", [token])
        fs = _register_filesystems(con, env)
        if should_run_schema(existed=existed, schema_sql=config.schema_sql):
            assert config.schema_sql is not None
            logger.info("fresh database; applying schema %s", config.schema_sql)
            con.execute(_read_sql(config.schema_sql))
        _serve(con, config.boot_sql)
        retention.refresh_view(con, fs, config.storage_dir)
        _sweep_until_signal(con, fs, config, stop)
        _sweep(con, fs, config)
    finally:
        _shutdown(con, fs)
