from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from scrooge.server import (
    DEFAULT_SCHEMA_SQL,
    Config,
    ConfigError,
    _apply_schema,
    _irods_storage_options,
    _register_filesystems,
    _resolve_bool,
    resolve_config,
    resolve_ingest_token,
    resolve_token,
    should_run_schema,
)


def _cfg(**overrides: Any) -> Config:
    """A Config with all-default fields, overridable per test case."""
    base: dict[str, Any] = {
        "database": Path("db.duckdb"),
        "boot_sql": Path("startup.sql"),
        "schema_sql": Path("schema.sql"),
        "storage_dir": None,
        "retention_rows": 100_000,
        "sweep_interval": 10.0,
        "ingest_host": "0.0.0.0",
        "ingest_port": 9595,
        "ingest_path": "/logs",
        "ingest_service_label_key": "app.kubernetes.io/name",
        "ingest_access_log": False,
    }
    base.update(overrides)
    return Config(**base)


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
        ("db.duckdb", None, None, None, None, None, {}, _cfg()),
        (
            "db.duckdb",
            "schema.sql",
            "boot.sql",
            "irods://zone/archive",
            5,
            2.5,
            {},
            _cfg(
                boot_sql=Path("boot.sql"),
                storage_dir="irods://zone/archive",
                retention_rows=5,
                sweep_interval=2.5,
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
                "SCROOGE_INGEST_HOST": "127.0.0.1",
                "SCROOGE_INGEST_PORT": "8080",
                "SCROOGE_INGEST_PATH": "/ingest",
                "SCROOGE_INGEST_SERVICE_LABEL_KEY": "app",
                "SCROOGE_INGEST_ACCESS_LOG": "true",
            },
            _cfg(
                database=Path("env.duckdb"),
                boot_sql=Path("env-boot.sql"),
                schema_sql=Path("env-schema.sql"),
                storage_dir="irods://zone/env-archive",
                retention_rows=42,
                sweep_interval=1.5,
                ingest_host="127.0.0.1",
                ingest_port=8080,
                ingest_path="/ingest",
                ingest_service_label_key="app",
                ingest_access_log=True,
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
            _cfg(database=Path("flag.duckdb"), retention_rows=7),
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
    "env",
    [
        {"DUCKDB_DATABASE": "db", "SCROOGE_INGEST_PORT": "0"},
        {"DUCKDB_DATABASE": "db", "SCROOGE_INGEST_PORT": "70000"},
    ],
    ids=["zero", "too-high"],
)
def test_resolve_config_rejects_bad_ingest_port(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match="INGEST_PORT must be between"):
        resolve_config(database=None, schema_sql=None, boot_sql=None, env=env)


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({}, None),
        ({"SCROOGE_INGEST_TOKEN": ""}, None),
        ({"SCROOGE_INGEST_TOKEN": "super_secret"}, "super_secret"),
    ],
    ids=["unset", "empty", "set"],
)
def test_resolve_ingest_token(env: dict[str, str], expected: str | None) -> None:
    assert resolve_ingest_token(env) == expected


def test_resolve_ingest_token_rejects_short() -> None:
    with pytest.raises(ConfigError, match="SCROOGE_INGEST_TOKEN"):
        resolve_ingest_token({"SCROOGE_INGEST_TOKEN": "abc"})


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


@pytest.mark.parametrize(
    ("schema_sql", "env", "expected"),
    [
        (None, {}, Path("schema.sql")),
        ("", {}, None),
        (None, {"DUCKDB_SCHEMA_SQL": ""}, None),
        ("custom.sql", {}, Path("custom.sql")),
    ],
    ids=["unset-default", "flag-empty-disables", "env-empty-disables", "explicit"],
)
def test_resolve_config_schema_disable(
    schema_sql: str | None, env: dict[str, str], expected: Path | None
) -> None:
    cfg = resolve_config(
        database="db.duckdb", schema_sql=schema_sql, boot_sql=None, env=env
    )
    assert cfg.schema_sql == expected


@pytest.mark.parametrize(
    "env",
    [
        {"DUCKDB_DATABASE": "db", "SCROOGE_RETENTION_ROWS": "0"},
        {"DUCKDB_DATABASE": "db", "SCROOGE_RETENTION_ROWS": "-1"},
    ],
    ids=["zero", "negative"],
)
def test_resolve_config_rejects_nonpositive_retention(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match="RETENTION_ROWS must be positive"):
        resolve_config(database=None, schema_sql=None, boot_sql=None, env=env)


@pytest.mark.parametrize(
    "env",
    [
        {"DUCKDB_DATABASE": "db", "SCROOGE_SWEEP_INTERVAL": "0"},
        {"DUCKDB_DATABASE": "db", "SCROOGE_SWEEP_INTERVAL": "-2.5"},
    ],
    ids=["zero", "negative"],
)
def test_resolve_config_rejects_nonpositive_interval(env: dict[str, str]) -> None:
    with pytest.raises(ConfigError, match="SWEEP_INTERVAL must be positive"):
        resolve_config(database=None, schema_sql=None, boot_sql=None, env=env)


@pytest.mark.parametrize(
    ("env", "default", "expected"),
    [
        ({}, False, False),
        ({}, True, True),
        ({"FLAG": "true"}, False, True),
        ({"FLAG": "1"}, False, True),
        ({"FLAG": "YES"}, False, True),
        ({"FLAG": "on"}, False, True),
        ({"FLAG": "false"}, True, False),
        ({"FLAG": "0"}, True, False),
        ({"FLAG": "No"}, True, False),
        ({"FLAG": "off"}, True, False),
    ],
    ids=[
        "unset-default-false",
        "unset-default-true",
        "true",
        "one",
        "yes-upper",
        "on",
        "false",
        "zero",
        "no-mixed",
        "off",
    ],
)
def test_resolve_bool(env: dict[str, str], default: bool, expected: bool) -> None:
    assert _resolve_bool(env, "FLAG", default) is expected


@pytest.mark.parametrize(
    "raw",
    ["", "maybe", "2"],
    ids=["explicit-empty", "garbage", "numeric"],
)
def test_resolve_bool_rejects_invalid(raw: str) -> None:
    # Explicitly empty is not "unset": it must fail fast, not silently become the default.
    with pytest.raises(ConfigError, match="FLAG must be a boolean"):
        _resolve_bool({"FLAG": raw}, "FLAG", False)


@pytest.mark.parametrize(
    "storage", ["file:///tmp/x", "s3://bucket/x"], ids=["file", "s3"]
)
def test_resolve_config_rejects_non_irods_storage(storage: str) -> None:
    with pytest.raises(ConfigError, match="irods"):
        resolve_config(
            database="db", schema_sql=None, boot_sql=None, storage_dir=storage, env={}
        )


@pytest.mark.parametrize(
    "storage",
    ["irods:///zone/home/x", "irods://zone/x", "/zone/home/x"],
    ids=["irods-triple-slash", "irods-double-slash", "bare-path"],
)
def test_resolve_config_accepts_irods_storage(storage: str) -> None:
    cfg = resolve_config(
        database="db", schema_sql=None, boot_sql=None, storage_dir=storage, env={}
    )
    assert cfg.storage_dir == storage


def test_apply_schema_runs_existing_file(tmp_path: Path) -> None:
    schema = tmp_path / "s.sql"
    schema.write_text("CREATE TABLE t (x INTEGER);")
    con = duckdb.connect(":memory:")
    _apply_schema(con, schema)
    count = con.execute("SELECT count(*) FROM t").fetchone()
    assert count is not None and count[0] == 0


def test_apply_schema_skips_missing_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)  # no schema.sql here
    con = duckdb.connect(":memory:")
    _apply_schema(con, Path(DEFAULT_SCHEMA_SQL))  # missing default -> skip, no raise
    tables = con.execute("SELECT count(*) FROM information_schema.tables").fetchone()
    assert tables is not None and tables[0] == 0


def test_apply_schema_errors_on_missing_explicit(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    with pytest.raises(ConfigError, match="not found"):
        _apply_schema(con, tmp_path / "explicit-missing.sql")


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
