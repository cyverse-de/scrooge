from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import fsspec
import pytest

from scrooge.retention import export_day, refresh_view, service_dir, sweep_once

_SCHEMA = (Path(__file__).parent.parent / "schema.sql").read_text()


def _seeded(
    con_path: str, seed: dict[str, list[tuple[str, int]]]
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB with the real schema and insert `count` rows per (service, day)."""
    con = duckdb.connect(con_path)
    con.execute(_SCHEMA)
    for service, days in seed.items():
        for day, count in days:
            con.execute(
                "INSERT INTO logs "
                "SELECT CAST(? AS TIMESTAMP), ?, NULL, NULL, 'stdout', 'info', 'msg', NULL "
                f"FROM range({int(count)})",
                [f"{day} 00:00:00", service],
            )
    return con


def _live_total(con: duckdb.DuckDBPyConnection) -> int:
    row = con.execute("SELECT count(*) FROM logs").fetchone()
    return int(row[0]) if row else 0


@pytest.mark.parametrize(
    ("seed", "threshold", "expected_files", "expected_live_total"),
    [
        # one service over threshold: evict oldest days until back under it.
        ({"svc": [("2026-06-01", 3), ("2026-06-02", 3), ("2026-06-03", 3)]}, 5, 2, 3),
        # one service under threshold: nothing archived.
        ({"svc": [("2026-06-01", 3)]}, 5, 0, 3),
        # mixed: svc-a over (one day evicted), svc-b under (untouched).
        (
            {
                "svc-a": [("2026-06-01", 4), ("2026-06-02", 4)],
                "svc-b": [("2026-06-01", 2)],
            },
            5,
            1,
            6,
        ),
    ],
    ids=["evict-oldest", "under-threshold", "mixed-services"],
)
def test_sweep_once(
    tmp_path: Path,
    seed: dict[str, list[tuple[str, int]]],
    threshold: int,
    expected_files: int,
    expected_live_total: int,
) -> None:
    con = _seeded(":memory:", seed)
    fs = fsspec.filesystem("file")
    written = sweep_once(con, fs, str(tmp_path), threshold)
    assert len(written) == expected_files
    for url in written:
        assert fs.exists(url)
    assert _live_total(con) == expected_live_total


def test_export_day_increments_sequence(tmp_path: Path) -> None:
    fs = fsspec.filesystem("file")
    con = _seeded(":memory:", {"svc": [("2026-06-01", 2)]})
    first = export_day(con, fs, str(tmp_path), "svc", date(2026, 6, 1))
    # Re-seed the same day so a second export rolls a second file.
    con.execute(
        "INSERT INTO logs SELECT TIMESTAMP '2026-06-01 00:00:00', 'svc', "
        "NULL, NULL, 'stdout', 'info', 'msg', NULL FROM range(2)"
    )
    second = export_day(con, fs, str(tmp_path), "svc", date(2026, 6, 1))
    assert first.endswith("2026-06-01-001_svc.parquet")
    assert second.endswith("2026-06-01-002_svc.parquet")
    assert service_dir(str(tmp_path), "svc") == f"{tmp_path}/svc"


def test_refresh_view_unions_live_and_archive(tmp_path: Path) -> None:
    con = _seeded(
        ":memory:", {"svc": [("2026-06-01", 3), ("2026-06-02", 3), ("2026-06-03", 3)]}
    )
    fs = fsspec.filesystem("file")
    written = sweep_once(con, fs, str(tmp_path), 5)
    assert written  # archived something
    refresh_view(con, fs, str(tmp_path))
    total = con.execute("SELECT count(*) FROM all_logs").fetchone()
    assert total is not None and total[0] == 9


def test_refresh_view_live_only_when_no_archive(tmp_path: Path) -> None:
    con = _seeded(":memory:", {"svc": [("2026-06-01", 4)]})
    fs = fsspec.filesystem("file")
    refresh_view(con, fs, str(tmp_path))
    total = con.execute("SELECT count(*) FROM all_logs").fetchone()
    assert total is not None and total[0] == 4


def test_refresh_view_skips_without_logs_table(tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")  # no `logs` table
    fs = fsspec.filesystem("file")
    refresh_view(con, fs, str(tmp_path))  # must not raise
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'all_logs'"
    ).fetchone()
    assert row is not None and row[0] == 0


def test_export_day_handles_glob_metacharacters_in_service(tmp_path: Path) -> None:
    fs = fsspec.filesystem("file")
    con = _seeded(":memory:", {"svc[1]": [("2026-06-01", 2)]})
    first = export_day(con, fs, str(tmp_path), "svc[1]", date(2026, 6, 1))
    con.execute(
        "INSERT INTO logs SELECT CAST('2026-06-01 00:00:00' AS TIMESTAMP), 'svc[1]', "
        "NULL, NULL, 'stdout', 'info', 'msg', NULL FROM range(2)"
    )
    second = export_day(con, fs, str(tmp_path), "svc[1]", date(2026, 6, 1))
    # Sequence advances rather than resetting to 001 (which would overwrite `first`).
    assert first.endswith("2026-06-01-001_svc[1].parquet")
    assert second.endswith("2026-06-01-002_svc[1].parquet")
    assert fs.exists(first) and fs.exists(second)


class _FailDeleteConnection:
    """Delegates to a real connection but raises on the DELETE in export_day."""

    def __init__(self, real: duckdb.DuckDBPyConnection) -> None:
        self._real = real

    def execute(self, sql: str, *args: object, **kwargs: object) -> object:
        if "DELETE FROM" in sql.upper():
            raise RuntimeError("simulated delete failure")
        return self._real.execute(sql, *args, **kwargs)


def test_export_day_removes_orphan_on_delete_failure(tmp_path: Path) -> None:
    fs = fsspec.filesystem("file")
    real = _seeded(":memory:", {"svc": [("2026-06-01", 2)]})
    con = _FailDeleteConnection(real)
    with pytest.raises(RuntimeError, match="simulated delete failure"):
        export_day(con, fs, str(tmp_path), "svc", date(2026, 6, 1))  # type: ignore[arg-type]
    # COPY wrote a file, but the failed DELETE triggered cleanup, leaving no orphan.
    assert fs.glob(f"{tmp_path}/svc/*.parquet") == []
