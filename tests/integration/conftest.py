"""Fixtures for scrooge integration tests.

These tests spawn a real ``scrooge`` process (``uv run scrooge``) and drive it over both
ingest paths: the Quack protocol (using the real ``daffy`` code) and the HTTP endpoint.
Data is read back over Quack, since the DuckDB file is held open by the scrooge process
and can only be reached through its Quack server.

``daffy`` is provided by the ``integration`` dependency group; run these with
``uv run --group integration pytest -m integration``. The default (hermetic) sync omits
that group, so the test modules — which import daffy directly — are skipped from collection
below when it isn't installed (and they're marked ``integration`` and deselected by default
regardless).
"""

from __future__ import annotations

import importlib.util
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

# The integration test modules import daffy directly. When the `integration` group isn't
# installed (e.g. the hermetic unit-test CI job), skip collecting them so a plain `pytest`
# run stays green.
if importlib.util.find_spec("daffy") is None:
    collect_ignore_glob = ["test_*.py"]

QUACK_TOKEN = "integration-quack-token"
INGEST_TOKEN = "integration-ingest-token"
STARTUP_TIMEOUT = 60.0

# Mirrors startup.sql but with a templated host:port so each server gets its own Quack
# listener (the real startup.sql hardcodes 0.0.0.0:9494, which can't be parameterized by
# env). `token = getvariable(...)` matches startup.sql's exact call form.
_BOOT_SQL = """\
INSTALL quack;
LOAD quack;
CALL quack_identify(name => 'scrooge');
CALL quack_serve('quack:127.0.0.1:{port}', allow_other_hostname => true, token = getvariable('quack_token'));
"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _load_quack(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.execute("LOAD quack")
    except duckdb.Error:
        conn.execute("INSTALL quack")
        conn.execute("LOAD quack")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(slots=True)
class ScroogeServer:
    """A live scrooge process under test."""

    proc: subprocess.Popen[bytes]
    http_base: str
    quack_uri: str
    quack_token: str
    ingest_token: str | None
    db_path: Path
    boot_sql: Path
    quack_port: int
    http_port: int

    def http_url(self, path: str) -> str:
        return f"{self.http_base}{path}"

    def attach(self, conn: duckdb.DuckDBPyConnection, *, name: str = "remote") -> None:
        """ATTACH this server's Quack endpoint onto an existing client connection."""
        _load_quack(conn)
        try:
            conn.execute(f"DETACH {name}")
        except duckdb.Error:
            pass
        conn.execute(
            f"ATTACH {_sql_literal(self.quack_uri)} AS {name} "
            f"(TOKEN {_sql_literal(self.quack_token)})"
        )


def _readyz(http_base: str) -> bool:
    try:
        with urllib.request.urlopen(f"{http_base}/readyz", timeout=2.0) as resp:
            return resp.status == 200
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return False


def _quack_ready(server: ScroogeServer) -> bool:
    conn = duckdb.connect(":memory:")
    try:
        server.attach(conn)
        conn.execute("SELECT 1 FROM remote.logs LIMIT 1").fetchall()
        return True
    except duckdb.Error:
        return False
    finally:
        conn.close()


def _wait_ready(server: ScroogeServer) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if server.proc.poll() is not None:
            raise RuntimeError(
                f"scrooge exited early with code {server.proc.returncode} during startup"
            )
        if _readyz(server.http_base) and _quack_ready(server):
            return
        time.sleep(0.25)
    server.proc.kill()
    raise RuntimeError(f"scrooge did not become ready within {STARTUP_TIMEOUT}s")


ServerFactory = Callable[..., ScroogeServer]


@pytest.fixture
def scrooge_factory(tmp_path: Path) -> Iterator[ServerFactory]:
    """Factory that launches configurable scrooge processes and tears them all down.

    Keyword args mirror the environment knobs: ``ingest_token`` (None disables the HTTP
    ingest route), ``storage_dir`` (enables archival), ``db_path`` (reuse a file across a
    restart), and ``extra_env`` for anything else (retention, sweep interval, iRODS creds).
    """
    servers: list[ScroogeServer] = []
    counter = 0

    def _launch(
        *,
        ingest_token: str | None = INGEST_TOKEN,
        storage_dir: str | None = None,
        db_path: Path | None = None,
        quack_port: int | None = None,
        http_port: int | None = None,
        extra_env: Mapping[str, str] | None = None,
    ) -> ScroogeServer:
        nonlocal counter
        counter += 1
        quack_port = quack_port if quack_port is not None else _free_port()
        http_port = http_port if http_port is not None else _free_port()
        boot_sql = tmp_path / f"boot-{counter}.sql"
        boot_sql.write_text(_BOOT_SQL.format(port=quack_port))
        db = db_path if db_path is not None else tmp_path / f"scrooge-{counter}.duckdb"

        env = os.environ.copy()
        env.update(
            {
                "DUCKDB_DATABASE": str(db),
                "DUCKDB_BOOT_SQL": str(boot_sql),
                "QUACK_TOKEN": QUACK_TOKEN,
                "SCROOGE_INGEST_HOST": "127.0.0.1",
                "SCROOGE_INGEST_PORT": str(http_port),
                "SCROOGE_INGEST_PATH": "/logs",
            }
        )
        if ingest_token is not None:
            env["SCROOGE_INGEST_TOKEN"] = ingest_token
        else:
            env.pop("SCROOGE_INGEST_TOKEN", None)
        if storage_dir is not None:
            env["SCROOGE_STORAGE_DIR"] = storage_dir
        else:
            env.pop("SCROOGE_STORAGE_DIR", None)
        if extra_env:
            env.update(extra_env)

        proc = subprocess.Popen(["uv", "run", "scrooge"], cwd=REPO_ROOT, env=env)
        server = ScroogeServer(
            proc=proc,
            http_base=f"http://127.0.0.1:{http_port}",
            quack_uri=f"quack:127.0.0.1:{quack_port}",
            quack_token=QUACK_TOKEN,
            ingest_token=ingest_token,
            db_path=db,
            boot_sql=boot_sql,
            quack_port=quack_port,
            http_port=http_port,
        )
        servers.append(server)
        _wait_ready(server)
        return server

    yield _launch

    for server in servers:
        _stop(server)


def _stop(server: ScroogeServer, *, timeout: float = 15.0) -> None:
    """Terminate a scrooge process (SIGTERM → clean checkpointed shutdown)."""
    if server.proc.poll() is not None:
        return
    server.proc.terminate()
    try:
        server.proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        server.proc.kill()
        server.proc.wait(timeout=timeout)


@pytest.fixture
def scrooge(scrooge_factory: ServerFactory) -> ScroogeServer:
    """A single scrooge process with HTTP ingest enabled and archival off."""
    return scrooge_factory()


@pytest.fixture
def stop_server() -> Callable[[ScroogeServer], None]:
    """Stop a running server mid-test (for restart/reconnect scenarios)."""
    return _stop


@pytest.fixture
def quack_reader() -> Iterator[Callable[[ScroogeServer], duckdb.DuckDBPyConnection]]:
    """Open read-only Quack client connections against a server; close them on teardown."""
    conns: list[duckdb.DuckDBPyConnection] = []

    def _open(server: ScroogeServer) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:")
        server.attach(conn)
        conns.append(conn)
        return conn

    yield _open

    for conn in conns:
        conn.close()


@pytest.fixture
def post_logs() -> Callable[..., tuple[int, str]]:
    """POST a body to a server's ingest endpoint; return (status, text).

    ``body`` may be bytes/str (sent verbatim) or any JSON-serializable value (encoded).
    ``token`` defaults to the server's ingest token; pass a value or None to override auth.
    """
    _unset = object()

    def _post(
        server: ScroogeServer,
        body: object,
        *,
        token: object = _unset,
        path: str = "/logs",
    ) -> tuple[int, str]:
        if isinstance(body, bytes):
            data = body
        elif isinstance(body, str):
            data = body.encode("utf-8")
        else:
            data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        bearer = server.ingest_token if token is _unset else token
        if bearer is not None:
            headers["Authorization"] = f"Bearer {bearer}"
        req = urllib.request.Request(
            server.http_url(path), data=data, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=30.0) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", "replace")

    return _post


@pytest.fixture
def get_metrics() -> Callable[[ScroogeServer], str]:
    """Scrape a server's /metrics endpoint as text."""

    def _get(server: ScroogeServer) -> str:
        with urllib.request.urlopen(server.http_url("/metrics"), timeout=10.0) as resp:
            return resp.read().decode("utf-8")

    return _get


def metric_value(
    text: str, name: str, labels: Mapping[str, str] | None = None
) -> float:
    """Extract a single Prometheus sample value from /metrics text (0.0 if absent)."""
    if labels:
        rendered = ",".join(f'{k}="{v}"' for k, v in labels.items())
        needle = f"{name}{{{rendered}}}"
    else:
        needle = name
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        key, _, value = line.partition(" ")
        if key == needle:
            return float(value)
    return 0.0
