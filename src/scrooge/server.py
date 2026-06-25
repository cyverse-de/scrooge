"""Lifecycle supervisor for a DuckDB database served over the Quack protocol.

The supervisor does only lifecycle work: locate the database file, run first-time
setup when the file is created fresh, run the always-on boot script that starts the
Quack server, then block until signalled and shut down cleanly. All data logic lives
in SQL.
"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import duckdb

logger = logging.getLogger("scrooge")

DEFAULT_BOOT_SQL = "startup.sql"
MIN_TOKEN_LENGTH = 4


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """Resolved runtime configuration.

    `database` is the DuckDB file to open. `boot_sql` runs on every start (it installs
    Quack and calls `quack_serve`). `schema_sql`, when set, runs only when the database
    is created fresh.
    """

    database: Path
    boot_sql: Path
    schema_sql: Path | None


def resolve_config(
    *,
    database: str | None,
    schema_sql: str | None,
    boot_sql: str | None,
    env: Mapping[str, str],
) -> Config:
    """Merge CLI flags with environment fallbacks into a validated Config.

    Flags take precedence over the environment. The database path is required; the boot
    script defaults to `startup.sql` in the working directory.
    """
    db = database or env.get("DUCKDB_DATABASE")
    if not db:
        raise ConfigError(
            "no database path provided; pass --database or set DUCKDB_DATABASE"
        )
    schema = schema_sql or env.get("DUCKDB_SCHEMA_SQL")
    boot = boot_sql or env.get("DUCKDB_BOOT_SQL") or DEFAULT_BOOT_SQL
    return Config(
        database=Path(db),
        boot_sql=Path(boot),
        schema_sql=Path(schema) if schema else None,
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


def _wait_for_signal(stop: threading.Event) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    logger.info("ready; waiting for shutdown signal (SIGINT/SIGTERM)")
    stop.wait()


def _shutdown(con: duckdb.DuckDBPyConnection) -> None:
    logger.info("shutting down; checkpointing database")
    try:
        con.execute("CHECKPOINT")
    except duckdb.Error as exc:
        logger.warning("checkpoint failed during shutdown: %s", exc)
    con.close()


def run(config: Config, *, token: str, stop: threading.Event | None = None) -> None:
    """Open the database, run setup, start the Quack server, and block until signalled.

    Must run on the main thread so the signal handlers can be installed.
    """
    stop = stop or threading.Event()
    existed = config.database.exists()
    logger.info(
        "opening database %s (%s)",
        config.database,
        "existing" if existed else "creating",
    )
    con = duckdb.connect(str(config.database))
    try:
        con.execute("SET VARIABLE quack_token = ?", [token])
        if should_run_schema(existed=existed, schema_sql=config.schema_sql):
            assert config.schema_sql is not None
            logger.info("fresh database; applying schema %s", config.schema_sql)
            con.execute(_read_sql(config.schema_sql))
        _serve(con, config.boot_sql)
        _wait_for_signal(stop)
    finally:
        _shutdown(con)
