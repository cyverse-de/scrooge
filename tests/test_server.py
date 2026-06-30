from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scrooge.server import (
    Config,
    ConfigError,
    _irods_storage_options,
    _register_filesystems,
    resolve_config,
    resolve_token,
    should_run_schema,
)


@pytest.mark.parametrize(
    (
        "database",
        "schema_sql",
        "boot_sql",
        "storage_dir",
        "retention_rows",
        "sweep_interval",
        "env",
        "expected",
    ),
    [
        (
            "db.duckdb",
            None,
            None,
            None,
            None,
            None,
            {},
            Config(
                Path("db.duckdb"),
                Path("startup.sql"),
                Path("schema.sql"),
                None,
                100_000,
                10.0,
            ),
        ),
        (
            "db.duckdb",
            "schema.sql",
            "boot.sql",
            "irods://zone/archive",
            5,
            2.5,
            {},
            Config(
                Path("db.duckdb"),
                Path("boot.sql"),
                Path("schema.sql"),
                "irods://zone/archive",
                5,
                2.5,
            ),
        ),
        (
            None,
            None,
            None,
            None,
            None,
            None,
            {
                "DUCKDB_DATABASE": "env.duckdb",
                "DUCKDB_SCHEMA_SQL": "env-schema.sql",
                "DUCKDB_BOOT_SQL": "env-boot.sql",
                "SCROOGE_STORAGE_DIR": "irods://zone/env-archive",
                "SCROOGE_RETENTION_ROWS": "42",
                "SCROOGE_SWEEP_INTERVAL": "1.5",
            },
            Config(
                Path("env.duckdb"),
                Path("env-boot.sql"),
                Path("env-schema.sql"),
                "irods://zone/env-archive",
                42,
                1.5,
            ),
        ),
        (
            "flag.duckdb",
            None,
            None,
            None,
            None,
            None,
            {"DUCKDB_DATABASE": "env.duckdb", "SCROOGE_RETENTION_ROWS": "7"},
            Config(
                Path("flag.duckdb"),
                Path("startup.sql"),
                Path("schema.sql"),
                None,
                7,
                10.0,
            ),
        ),
    ],
    ids=["defaults", "all-flags", "all-env", "flag-overrides-env"],
)
def test_resolve_config(
    database: str | None,
    schema_sql: str | None,
    boot_sql: str | None,
    storage_dir: str | None,
    retention_rows: int | None,
    sweep_interval: float | None,
    env: dict[str, str],
    expected: Config,
) -> None:
    assert (
        resolve_config(
            database=database,
            schema_sql=schema_sql,
            boot_sql=boot_sql,
            storage_dir=storage_dir,
            retention_rows=retention_rows,
            sweep_interval=sweep_interval,
            env=env,
        )
        == expected
    )


def test_resolve_config_requires_database() -> None:
    with pytest.raises(ConfigError, match="no database path"):
        resolve_config(database=None, schema_sql=None, boot_sql=None, env={})


@pytest.mark.parametrize(
    ("env", "match"),
    [
        ({"DUCKDB_DATABASE": "db", "SCROOGE_RETENTION_ROWS": "lots"}, "integer"),
        ({"DUCKDB_DATABASE": "db", "SCROOGE_SWEEP_INTERVAL": "soon"}, "number"),
    ],
    ids=["bad-retention-rows", "bad-sweep-interval"],
)
def test_resolve_config_rejects_bad_numbers(env: dict[str, str], match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        resolve_config(database=None, schema_sql=None, boot_sql=None, env=env)


def test_resolve_token_returns_valid_token() -> None:
    assert resolve_token({"QUACK_TOKEN": "super_secret"}) == "super_secret"


@pytest.mark.parametrize(
    "env",
    [{}, {"QUACK_TOKEN": ""}, {"QUACK_TOKEN": "abc"}],
    ids=["missing", "empty", "too-short"],
)
def test_resolve_token_rejects_invalid(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match="QUACK_TOKEN"):
        resolve_token(env)


@pytest.mark.parametrize(
    ("existed", "schema_sql", "expected"),
    [
        (False, Path("schema.sql"), True),
        (True, Path("schema.sql"), False),
        (False, None, False),
        (True, None, False),
    ],
    ids=[
        "fresh-with-schema",
        "existing-with-schema",
        "fresh-no-schema",
        "existing-no-schema",
    ],
)
def test_should_run_schema(
    existed: bool, schema_sql: Path | None, expected: bool
) -> None:
    assert should_run_schema(existed=existed, schema_sql=schema_sql) is expected


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, {}),
        ({"IRODS_HOST": "irods.example.org"}, {"host": "irods.example.org"}),
        (
            {
                "IRODS_HOST": "irods.example.org",
                "IRODS_PORT": "1247",
                "IRODS_USER": "rods",
                "IRODS_ZONE": "tempZone",
            },
            {
                "host": "irods.example.org",
                "port": 1247,
                "user": "rods",
                "zone": "tempZone",
            },
        ),
    ],
    ids=["env-file-mode", "host-only", "explicit"],
)
def test_irods_storage_options(env: dict[str, str], expected: dict[str, Any]) -> None:
    assert _irods_storage_options(env) == expected


class _RecordingConnection:
    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_filesystem(self, filesystem: Any) -> None:
        self.registered.append(filesystem)


def test_register_filesystems_registers_irods() -> None:
    con = _RecordingConnection()
    _register_filesystems(con, {})  # type: ignore[arg-type]
    assert len(con.registered) == 1
    assert con.registered[0].protocol == "irods"
