"""Opt-in end-to-end archival test against real iRODS.

Skipped unless ``SCROOGE_IT_IRODS`` is set (needs iRODS creds in the environment, e.g.
``set -a; source .env.test``). Drives a live scrooge with a small retention threshold so
its sweep loop rolls the oldest day out to Parquet in iRODS, then verifies the live table
stays bounded while ``all_logs`` still spans the archive. Fed by both encoders — HTTP for
the older day, daffy/Quack for the newer.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime

import duckdb
import fsspec
import pytest

from _daffy import make_records, ship_records
from conftest import ScroogeServer, ServerFactory

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("SCROOGE_IT_IRODS"),
        reason="set SCROOGE_IT_IRODS (with iRODS creds) to run the archival test",
    ),
]

Reader = Callable[[ScroogeServer], duckdb.DuckDBPyConnection]
Poster = Callable[..., tuple[int, str]]

RETENTION = 500
# OLD_DAY is tz-aware because .timestamp() below needs true UTC; NEW_DAY is the naive base
# for daffy capture_times and must be a later day so the old day is the one that rolls off.
OLD_DAY = datetime(2026, 6, 20, tzinfo=UTC)
NEW_DAY = datetime(2026, 6, 22)
OLD_ROWS = 800
NEW_ROWS = 300
SERVICE = "arch-svc"


def _irods_options() -> dict[str, object]:
    options: dict[str, object] = {"host": os.environ["IRODS_HOST"]}
    if os.environ.get("IRODS_PORT"):
        options["port"] = int(os.environ["IRODS_PORT"])
    if os.environ.get("IRODS_USER"):
        options["user"] = os.environ["IRODS_USER"]
    if os.environ.get("IRODS_ZONE"):
        options["zone"] = os.environ["IRODS_ZONE"]
    return options


def _ship_new_day(server: ScroogeServer) -> None:
    records = make_records(NEW_ROWS, service=SERVICE, message="new", base=NEW_DAY)
    assert ship_records(server, records) == NEW_ROWS


def _post_old_day(server: ScroogeServer, post_logs: Poster) -> None:
    epoch = OLD_DAY.timestamp()
    records = [
        {
            "log": f"old-{i}",
            "date": epoch + i,
            "kubernetes": {"labels": {"app.kubernetes.io/name": SERVICE}},
        }
        for i in range(OLD_ROWS)
    ]
    assert post_logs(server, records)[0] == 204


def test_archival_to_irods(
    scrooge_factory: ServerFactory,
    quack_reader: Reader,
    post_logs: Poster,
    get_metrics: Callable[[ScroogeServer], str],
    stop_server: Callable[[ScroogeServer], None],
) -> None:
    zone = os.environ.get("IRODS_ZONE", "cyverse")
    user = os.environ["IRODS_USER"]
    storage_dir = (
        f"irods:///{zone}/home/{user}/scrooge-it-{os.getpid()}-{int(time.time())}"
    )

    server = scrooge_factory(
        storage_dir=storage_dir,
        extra_env={
            "SCROOGE_RETENTION_ROWS": str(RETENTION),
            "SCROOGE_SWEEP_INTERVAL": "1.0",
        },
    )
    try:
        _post_old_day(server, post_logs)
        _ship_new_day(server)
        conn = quack_reader(server)

        # Wait for a sweep to archive the older day and rebuild the view. A query can land
        # mid-sweep — while refresh_view swaps the all_logs view or a parquet is still being
        # written — and raise; treat that as "not settled yet" and keep polling.
        deadline = time.monotonic() + 45.0
        live = all_rows = -1
        while time.monotonic() < deadline:
            try:
                (live,) = conn.execute("SELECT count(*) FROM remote.logs").fetchone()
                (all_rows,) = conn.execute(
                    "SELECT count(*) FROM remote.all_logs"
                ).fetchone()
            except duckdb.Error:
                time.sleep(1.0)
                continue
            if live <= RETENTION and all_rows == OLD_ROWS + NEW_ROWS:
                break
            time.sleep(1.0)

        assert all_rows == OLD_ROWS + NEW_ROWS  # archive unioned back into all_logs
        assert live == NEW_ROWS  # older day rolled off; newer day stays live
        assert live <= RETENTION

        (pending,) = conn.execute(
            "SELECT count(*) FROM remote.pending_exports"
        ).fetchone()
        assert pending == 0  # journal cleared after a clean sweep

        metrics = get_metrics(server)
        assert f'scrooge_archive_files_total{{service="{SERVICE}"}}' in metrics

        fs = fsspec.filesystem("irods", **_irods_options())
        parquet = [
            p
            for p in fs.ls(f"{storage_dir}/{SERVICE}", detail=False)
            if str(p).endswith(".parquet")
        ]
        assert parquet, "expected at least one archived parquet file"
    finally:
        stop_server(server)
        # Remove the collection via the iRODS session directly: fs.rm() unlinks object by
        # object, and unlink here trips a server policy rule (CUT_ACTION_PROCESSED_ERR)
        # even though it succeeds — a recursive collection remove sidesteps that noise.
        try:
            fs = fsspec.filesystem("irods", **_irods_options())
            logical = storage_dir.removeprefix("irods://")
            fs.session.collections.remove(logical, recurse=True, force=True)
        except Exception as exc:  # best-effort cleanup; don't mask a test failure
            print(f"\niRODS cleanup of {storage_dir} failed: {exc!r}")
