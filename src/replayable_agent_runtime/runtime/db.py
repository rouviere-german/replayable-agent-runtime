"""SQLite engines in WAL mode with explicit transaction control.

pysqlite's implicit transaction handling is disabled (``isolation_level = None``) and every
SQLAlchemy transaction emits its own ``BEGIN``. Writers use ``BEGIN IMMEDIATE``, which takes the
database write lock up front, so read-then-write sequences (claim a lease, append after checking
the fencing token) cannot interleave with another writer. Readers use a deferred ``BEGIN`` on a
separate engine so they never queue behind the write lock.

``synchronous=NORMAL`` (the default) in WAL mode syncs at checkpoints rather than on every commit:
a committed transaction survives a killed process, but the last commits can be lost on power loss
or an OS crash. ``FULL`` syncs every commit. See docs/adr/0003-sqlite-wal-synchronous-normal.md.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import Connection

__all__ = ["Clock", "SynchronousMode", "make_engine", "now_us", "sqlite_url"]

Clock = Callable[[], float]
SynchronousMode = Literal["NORMAL", "FULL"]

_SYNCHRONOUS_PRAGMAS: dict[str, str] = {
    "NORMAL": "PRAGMA synchronous=NORMAL",
    "FULL": "PRAGMA synchronous=FULL",
}


def now_us(clock: Clock = time.time) -> int:
    """The clock reading in integer microseconds (what the journal stores)."""
    return round(clock() * 1_000_000)


def sqlite_url(path: Path) -> str:
    return f"sqlite+pysqlite:///{path.resolve().as_posix()}"


def make_engine(
    path: Path, *, synchronous: SynchronousMode = "NORMAL", immediate: bool = True
) -> Engine:
    if synchronous not in _SYNCHRONOUS_PRAGMAS:
        raise ValueError("synchronous must be NORMAL or FULL")
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        sqlite_url(path),
        connect_args={"check_same_thread": False, "timeout": 30.0},
    )
    pragma = _SYNCHRONOUS_PRAGMAS[synchronous]
    begin_sql = "BEGIN IMMEDIATE" if immediate else "BEGIN"

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_connection: Any, _record: Any) -> None:
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute(pragma)
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()

    @event.listens_for(engine, "begin")
    def _on_begin(connection: Connection) -> None:
        connection.exec_driver_sql(begin_sql)

    return engine
