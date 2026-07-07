"""Shared daffy-backed helpers for the integration tests.

Kept out of ``conftest.py`` — which must import cleanly without daffy and is type-checked in
the hermetic env — because daffy comes from the ``integration`` dependency group and is only
present when these tests actually run. Excluded from pyright for the same reason. Imported
only by the ``test_*`` modules, which are skipped from collection when daffy is absent.
"""

from __future__ import annotations

from datetime import datetime

from conftest import ScroogeServer

from daffy.config import Config
from daffy.schema import LogRecord
from daffy.shipper import Shipper
from daffy.store import LogStore

_DEFAULT_DAY = datetime(2026, 6, 22)


def daffy_config(server: ScroogeServer, **overrides: object) -> Config:
    """Build a daffy Config aimed at ``server``'s Quack endpoint (flush() ships everything)."""
    fields: dict[str, object] = {
        "service": "integration",
        "local_db": ":memory:",
        "pod": None,
        "node": None,
        "scrooge_uri": server.quack_uri,
        "scrooge_token": server.quack_token,
        "flush_rows": 1_000_000,
        "flush_interval": 60.0,
        "max_buffer_rows": 1_000_000,
    }
    fields.update(overrides)
    return Config(**fields)  # type: ignore[arg-type]


def make_records(
    n: int, *, service: str, message: str = "hello", base: datetime = _DEFAULT_DAY
) -> list[LogRecord]:
    """Build ``n`` stdout LogRecords with distinct microsecond capture times on one day."""
    return [
        LogRecord(
            capture_time=base.replace(microsecond=i % 1_000_000),
            service=service,
            stream="stdout",
            message=f"{message}-{i}",
        )
        for i in range(n)
    ]


def ship_records(
    server: ScroogeServer, records: list[LogRecord], **config_overrides: object
) -> int:
    """Ship ``records`` into ``server`` over Quack via daffy's Shipper; return rows shipped."""
    store = LogStore(":memory:")
    store.insert_many(records)
    try:
        return Shipper(daffy_config(server, **config_overrides), store).flush()
    finally:
        store.close()
