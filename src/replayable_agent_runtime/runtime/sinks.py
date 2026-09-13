"""Bundled fake external systems with fencing-aware writes: a ticket store and a notifier.

They live in their own SQLite database so they behave like services outside the journal: writes to
them are not part of any journal transaction, which is exactly the crash window durable execution
has to handle.

Both sinks remember the highest fencing token they have seen per run and refuse lower tokens, so a
worker that was paused while holding a lease that has since been reclaimed cannot apply a late
write (Kleppmann, "How to do distributed locking", 2016).

- :class:`TicketStore` is also idempotent: every write carries the step's ``run_id:seq`` key and a
  repeated key returns the originally stored result without writing again. Effects on it are
  effectively once.
- :class:`Notifier` models a channel with no idempotency support (webhooks, e-mail): a repeated
  send is delivered again. It is at-least-once in the crash window between delivery and journal
  commit, and a stale-token send is still refused.

``enforce_fencing=False`` exists only for the control experiment that shows the zombie test can
fail.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.engine import Connection

from replayable_agent_runtime.agentic.errors import StaleFencingTokenError, ToolPolicyError
from replayable_agent_runtime.core.canonical import canonical_json
from replayable_agent_runtime.runtime.db import Clock, SynchronousMode, make_engine, now_us

__all__ = ["LedgerEntry", "Notifier", "ProviderMeter", "SinkStore", "TicketStore"]

_metadata = MetaData()

fences = Table(
    "sink_fences",
    _metadata,
    Column("sink", String(32), primary_key=True),
    Column("scope", String(64), primary_key=True),
    Column("highest_token", Integer, nullable=False),
)
tickets = Table(
    "tickets",
    _metadata,
    Column("ticket_pk", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("number", Integer, nullable=False),
    Column("title", Text, nullable=False),
    Column("body", Text, nullable=False),
    Column("status", String(32), nullable=False),
    UniqueConstraint("run_id", "number"),
)
applied_writes = Table(
    "applied_writes",
    _metadata,
    Column("write_id", Integer, primary_key=True, autoincrement=True),
    Column("key", String(96), nullable=False, unique=True),
    Column("sink", String(32), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("op", String(32), nullable=False),
    Column("args_json", Text, nullable=False),
    Column("result_json", Text, nullable=False),
    Column("token", Integer, nullable=False),
    Column("applied_at_us", BigInteger, nullable=False),
)
notifications = Table(
    "notifications",
    _metadata,
    Column("delivery_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("key", String(96), nullable=False),
    Column("channel", String(64), nullable=False),
    Column("message", Text, nullable=False),
    Column("token", Integer, nullable=False),
    Column("delivered_at_us", BigInteger, nullable=False),
)
rejections = Table(
    "fencing_rejections",
    _metadata,
    Column("rejection_id", Integer, primary_key=True, autoincrement=True),
    Column("sink", String(32), nullable=False),
    Column("run_id", String(64), nullable=False),
    Column("key", String(96), nullable=False),
    Column("token", Integer, nullable=False),
    Column("highest_token", Integer, nullable=False),
    Column("at_us", BigInteger, nullable=False),
)
provider_calls = Table(
    "provider_calls",
    _metadata,
    Column("call_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("key", String(96), nullable=False),
    Column("request_sha256", String(64), nullable=False),
    Column("at_us", BigInteger, nullable=False),
)


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """One applied external effect, normalized for comparison across runs (key -> seq)."""

    sink: str
    op: str
    seq: int
    args: str
    result: str


def _seq_of(key: str) -> int:
    return int(key.rsplit(":", 1)[1])


class SinkStore:
    """The database behind the fake external services (not the journal)."""

    def __init__(
        self, path: Path, *, synchronous: SynchronousMode = "NORMAL", clock: Clock = time.time
    ) -> None:
        self.path = path
        self._clock = clock
        self.engine = make_engine(path, synchronous=synchronous, immediate=True)
        self._reader = make_engine(path, synchronous=synchronous, immediate=False)
        try:
            _metadata.create_all(self.engine)
        except Exception:
            # A failed open must not keep the database files open in this process.
            self.close()
            raise

    def now_us(self) -> int:
        return now_us(self._clock)

    def close(self) -> None:
        self.engine.dispose()
        self._reader.dispose()

    @contextmanager
    def read(self) -> Iterator[Connection]:
        with self._reader.connect() as conn:
            yield conn

    def fenced_write(
        self,
        *,
        sink: str,
        run_id: str,
        key: str,
        token: int,
        enforce: bool,
    ) -> _FencedTxn:
        return _FencedTxn(self, sink=sink, run_id=run_id, key=key, token=token, enforce=enforce)

    def record_rejection(
        self, *, sink: str, run_id: str, key: str, token: int, highest: int
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(rejections).values(
                    sink=sink,
                    run_id=run_id,
                    key=key,
                    token=token,
                    highest_token=highest,
                    at_us=self.now_us(),
                )
            )

    def rejections(self, run_id: str | None = None) -> list[dict[str, Any]]:
        query = select(rejections).order_by(rejections.c.rejection_id)
        if run_id is not None:
            query = query.where(rejections.c.run_id == run_id)
        with self.read() as conn:
            return [dict(row._mapping) for row in conn.execute(query)]

    def ledger(self, run_id: str) -> list[LedgerEntry]:
        """Every applied effect for a run, in application order."""
        with self.read() as conn:
            writes = conn.execute(
                select(applied_writes)
                .where(applied_writes.c.run_id == run_id)
                .order_by(applied_writes.c.write_id)
            ).all()
            sends = conn.execute(
                select(notifications)
                .where(notifications.c.run_id == run_id)
                .order_by(notifications.c.delivery_id)
            ).all()
        entries = [
            LedgerEntry("tickets", row.op, _seq_of(row.key), row.args_json, row.result_json)
            for row in writes
        ]
        entries += [
            LedgerEntry(
                "notifier",
                "send",
                _seq_of(row.key),
                canonical_json({"channel": row.channel, "message": row.message}),
                "",
            )
            for row in sends
        ]
        return entries


class _FencedTxn:
    """Check-and-raise the fencing token, then write, in one ``BEGIN IMMEDIATE`` transaction.

    A stale token aborts the transaction and the rejection is recorded in a separate one, so the
    evidence survives the rollback.
    """

    def __init__(
        self, store: SinkStore, *, sink: str, run_id: str, key: str, token: int, enforce: bool
    ) -> None:
        self._store = store
        self._sink = sink
        self._run_id = run_id
        self._key = key
        self._token = token
        self._enforce = enforce

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        stale: tuple[int, int] | None = None
        with self._store.engine.begin() as conn:
            highest = conn.execute(
                select(fences.c.highest_token).where(
                    fences.c.sink == self._sink, fences.c.scope == self._run_id
                )
            ).scalar()
            if self._enforce and highest is not None and self._token < highest:
                stale = (self._token, int(highest))
            else:
                if highest is None:
                    conn.execute(
                        insert(fences).values(
                            sink=self._sink, scope=self._run_id, highest_token=self._token
                        )
                    )
                elif self._token > highest:
                    conn.execute(
                        update(fences)
                        .where(fences.c.sink == self._sink, fences.c.scope == self._run_id)
                        .values(highest_token=self._token)
                    )
                yield conn
        if stale is not None:
            self._store.record_rejection(
                sink=self._sink,
                run_id=self._run_id,
                key=self._key,
                token=stale[0],
                highest=stale[1],
            )
            raise StaleFencingTokenError(f"{self._sink}:{self._run_id}", stale[0], stale[1])


class TicketStore:
    """Fencing-aware, idempotent ticket service. Ticket ids are ``T-<n>`` within a run."""

    sink = "tickets"

    def __init__(self, store: SinkStore, *, enforce_fencing: bool = True) -> None:
        self._store = store
        self.enforce_fencing = enforce_fencing

    def _apply(
        self,
        *,
        run_id: str,
        key: str,
        token: int,
        op: str,
        args: dict[str, JsonValue],
        write: Any,
    ) -> dict[str, JsonValue]:
        txn = self._store.fenced_write(
            sink=self.sink, run_id=run_id, key=key, token=token, enforce=self.enforce_fencing
        )
        result: dict[str, JsonValue] = {}
        with txn.begin() as conn:
            existing = conn.execute(
                select(applied_writes.c.result_json).where(applied_writes.c.key == key)
            ).scalar()
            if existing is not None:
                # Like an idempotent HTTP API: a repeated key gets the original response, so a
                # re-executed step observes exactly what the first execution did.
                result = json.loads(existing)
                return result
            result = write(conn)
            conn.execute(
                insert(applied_writes).values(
                    key=key,
                    sink=self.sink,
                    run_id=run_id,
                    op=op,
                    args_json=canonical_json(args),
                    result_json=canonical_json(result),
                    token=token,
                    applied_at_us=self._store.now_us(),
                )
            )
        return result

    def create(
        self, *, run_id: str, key: str, token: int, title: str, body: str
    ) -> dict[str, JsonValue]:
        def write(conn: Connection) -> dict[str, JsonValue]:
            number = (
                conn.execute(
                    select(func.count()).select_from(tickets).where(tickets.c.run_id == run_id)
                ).scalar_one()
                + 1
            )
            conn.execute(
                insert(tickets).values(
                    run_id=run_id, number=number, title=title, body=body, status="open"
                )
            )
            return {"ticket_id": f"T-{number}", "status": "open"}

        return self._apply(
            run_id=run_id,
            key=key,
            token=token,
            op="create",
            args={"title": title, "body": body},
            write=write,
        )

    def _set_status(
        self, *, run_id: str, key: str, token: int, ticket_id: str, status: str, op: str
    ) -> dict[str, JsonValue]:
        number = int(ticket_id.removeprefix("T-")) if ticket_id.startswith("T-") else -1

        def write(conn: Connection) -> dict[str, JsonValue]:
            result = conn.execute(
                update(tickets)
                .where(tickets.c.run_id == run_id, tickets.c.number == number)
                .values(status=status)
            )
            if result.rowcount != 1:
                raise ToolPolicyError("ticket does not exist in this run's project")
            return {"ticket_id": ticket_id, "status": status}

        return self._apply(
            run_id=run_id,
            key=key,
            token=token,
            op=op,
            args={"ticket_id": ticket_id, "status": status},
            write=write,
        )

    def update_status(
        self, *, run_id: str, key: str, token: int, ticket_id: str, status: str
    ) -> dict[str, JsonValue]:
        return self._set_status(
            run_id=run_id, key=key, token=token, ticket_id=ticket_id, status=status, op="update"
        )

    def close(self, *, run_id: str, key: str, token: int, ticket_id: str) -> dict[str, JsonValue]:
        return self._set_status(
            run_id=run_id, key=key, token=token, ticket_id=ticket_id, status="closed", op="close"
        )

    def tickets(self, run_id: str) -> list[dict[str, Any]]:
        with self._store.read() as conn:
            rows = conn.execute(
                select(tickets.c.number, tickets.c.title, tickets.c.status)
                .where(tickets.c.run_id == run_id)
                .order_by(tickets.c.number)
            )
            return [
                {"ticket_id": f"T-{row.number}", "title": row.title, "status": row.status}
                for row in rows
            ]


class Notifier:
    """Fencing-aware notifier without idempotency keys (every accepted send is delivered)."""

    sink = "notifier"

    def __init__(self, store: SinkStore, *, enforce_fencing: bool = True) -> None:
        self._store = store
        self.enforce_fencing = enforce_fencing

    def send(
        self, *, run_id: str, key: str, token: int, channel: str, message: str
    ) -> dict[str, JsonValue]:
        txn = self._store.fenced_write(
            sink=self.sink, run_id=run_id, key=key, token=token, enforce=self.enforce_fencing
        )
        with txn.begin() as conn:
            inserted = conn.execute(
                insert(notifications)
                .values(
                    run_id=run_id,
                    key=key,
                    channel=channel,
                    message=message,
                    token=token,
                    delivered_at_us=self._store.now_us(),
                )
                .returning(notifications.c.delivery_id)
            ).first()
        if inserted is None:  # pragma: no cover - RETURNING always yields the inserted row
            raise RuntimeError("notification insert returned no row")
        return {"delivered": True, "channel": channel}

    def deliveries(self, run_id: str) -> list[dict[str, Any]]:
        with self._store.read() as conn:
            rows = conn.execute(
                select(notifications)
                .where(notifications.c.run_id == run_id)
                .order_by(notifications.c.delivery_id)
            )
            return [dict(row._mapping) for row in rows]


class ProviderMeter:
    """The fake provider's billing meter, shared across processes through the sink database.

    Every provider call is recorded with the key of the journal step it served, so recovery tests
    can count calls that were paid twice for the same step.
    """

    def __init__(self, store: SinkStore) -> None:
        self._store = store

    def record(self, *, run_id: str, key: str, request_sha256: str) -> None:
        with self._store.engine.begin() as conn:
            conn.execute(
                insert(provider_calls).values(
                    run_id=run_id,
                    key=key,
                    request_sha256=request_sha256,
                    at_us=self._store.now_us(),
                )
            )

    def calls(self, run_id: str | None = None) -> int:
        query = select(func.count()).select_from(provider_calls)
        if run_id is not None:
            query = query.where(provider_calls.c.run_id == run_id)
        with self._store.read() as conn:
            return int(conn.execute(query).scalar_one())

    def duplicates(self, run_id: str | None = None) -> dict[str, int]:
        """Step keys the provider was called for more than once."""
        query = (
            select(provider_calls.c.key, func.count())
            .group_by(provider_calls.c.key)
            .having(func.count() > 1)
        )
        if run_id is not None:
            query = query.where(provider_calls.c.run_id == run_id)
        with self._store.read() as conn:
            return {row[0]: int(row[1]) for row in conn.execute(query)}
