"""The append-only, hash-chained SQLite journal, run leases, inbox (signals, approvals) and outbox.

Invariants the rest of the runtime relies on:

- Events of a run form a gapless sequence 0..head_seq, each hash-chained to its predecessor, and
  ``runs.head_seq`` / ``runs.head_hash`` are updated in the same transaction as every append.
- Only the current lease holder can append: every append runs under ``BEGIN IMMEDIATE`` and is
  conditional on ``owner`` and ``fencing`` still matching, so a paused worker whose lease was
  reclaimed is refused (``FencedOutError``) instead of forking the history.
- Claims are atomic: ``UPDATE runs SET owner=?, fencing=fencing+1, lease_expires_at_us=? WHERE
  run_id=? AND lease_expires_at_us < now RETURNING fencing`` inside ``BEGIN IMMEDIATE``, so fencing
  tokens increase monotonically per run.
- External inputs (signals, approval decisions, cancellation) bump ``runs.inbox_version``; a worker
  suspending a run compares the version it started with, so an input that arrives during an
  activation wakes the run instead of being lost.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from pydantic import JsonValue
from sqlalchemy import case, func, insert, select, update
from sqlalchemy.engine import Connection, Row
from sqlalchemy.exc import IntegrityError

from replayable_agent_runtime.agentic.approval import (
    ApprovalRequest,
    ApprovalStatus,
    SubjectKind,
    check_decision,
)
from replayable_agent_runtime.agentic.audit import (
    GENESIS_HASH,
    VerifyResult,
    event_hash,
    export_lines,
    payload_digest,
    verify_chain,
)
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.errors import ApprovalError, ApprovalIntegrityError
from replayable_agent_runtime.core.canonical import canonical_json
from replayable_agent_runtime.runtime.bindings import ApprovalColumns, RunColumns, binding_error
from replayable_agent_runtime.runtime.db import Clock, SynchronousMode, make_engine, now_us
from replayable_agent_runtime.runtime.errors import (
    FencedOutError,
    HistoryLimitExceededError,
    RunNotFoundError,
    RunStateError,
)
from replayable_agent_runtime.runtime.events import (
    RUN_LEVEL,
    TERMINAL_EVENTS,
    EventRecord,
    EventType,
    NewEvent,
    validate_payload,
)
from replayable_agent_runtime.runtime.schema import (
    SCHEMA_REVISION,
    approvals,
    events,
    outbox,
    runs,
    signals,
)
from replayable_agent_runtime.runtime.telemetry import RuntimeMetrics

__all__ = [
    "RUN_ID_PATTERN",
    "AppendHook",
    "Claim",
    "Journal",
    "Lease",
    "OutboxRow",
    "RunRow",
    "RunStatus",
]

RUN_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
_RUN_ID = re.compile(RUN_ID_PATTERN)
_MIGRATIONS = Path(__file__).resolve().parent / "migrations"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


RUNNABLE_STATUSES = (RunStatus.PENDING.value, RunStatus.RUNNING.value)
TERMINAL_STATUSES = frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED})

AppendPhase = Literal["before_commit", "after_commit"]
AppendHook = Callable[[AppendPhase, Sequence[EventRecord]], None]
EventBuilder = Callable[[Connection], Sequence[NewEvent] | None]
TxnHook = Callable[[Connection, Sequence[EventRecord]], None]


@dataclass(frozen=True, slots=True)
class Lease:
    run_id: str
    owner: str
    fencing: int


@dataclass(frozen=True, slots=True)
class RunRow:
    run_id: str
    workflow: str
    status: RunStatus
    input: JsonValue
    input_json: str
    started_by: str
    created_at_us: int
    updated_at_us: int
    owner: str | None
    fencing: int
    lease_expires_at_us: int
    wake_at_us: int | None
    waiting_on: dict[str, Any] | None
    cancel_requested: bool
    inbox_version: int
    head_seq: int
    head_hash: str
    result: JsonValue
    error: dict[str, Any] | None
    blocked_reason: str | None

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def summary(self) -> dict[str, JsonValue]:
        return {
            "run_id": self.run_id,
            "workflow": self.workflow,
            "status": self.status.value,
            "started_by": self.started_by,
            "head_seq": self.head_seq,
            "fencing": self.fencing,
            "waiting_on": self.waiting_on,
            "cancel_requested": self.cancel_requested,
            "result": self.result,
            "error": self.error,
            "blocked_reason": self.blocked_reason,
        }


@dataclass(frozen=True, slots=True)
class Claim:
    lease: Lease
    run: RunRow
    reclaimed: bool


@dataclass(frozen=True, slots=True)
class OutboxRow:
    key: str
    run_id: str
    seq: int
    tool: str
    call_hash: str
    effect: str
    status: str
    fencing: int


def _loads(text: str | None) -> Any:
    return json.loads(text) if text is not None else None


def _run_from_row(row: Row[Any]) -> RunRow:
    return RunRow(
        run_id=row.run_id,
        workflow=row.workflow,
        status=RunStatus(row.status),
        input=json.loads(row.input_json),
        input_json=row.input_json,
        started_by=row.started_by,
        created_at_us=row.created_at_us,
        updated_at_us=row.updated_at_us,
        owner=row.owner,
        fencing=row.fencing,
        lease_expires_at_us=row.lease_expires_at_us,
        wake_at_us=row.wake_at_us,
        waiting_on=_loads(row.waiting_on),
        cancel_requested=bool(row.cancel_requested),
        inbox_version=row.inbox_version,
        head_seq=row.head_seq,
        head_hash=row.head_hash,
        result=_loads(row.result_json),
        error=_loads(row.error_json),
        blocked_reason=row.blocked_reason,
    )


def _event_from_row(row: Row[Any]) -> EventRecord:
    return EventRecord(
        run_id=row.run_id,
        seq=row.seq,
        command=row.command,
        type=row.type,
        fingerprint=row.fingerprint,
        payload=row.payload,
        payload_sha256=row.payload_sha256,
        recorded_at_us=row.recorded_at_us,
        prev_hash=row.prev_hash,
        hash=row.hash,
    )


def _approval_from_row(row: Row[Any]) -> ApprovalRequest:
    return ApprovalRequest(
        approval_id=row.approval_id,
        run_id=row.run_id,
        seq=row.seq,
        command=row.command,
        kind=SubjectKind(row.subject_kind),
        name=row.name,
        args=json.loads(row.args_json),
        call_hash=row.call_hash,
        requested_by=row.requested_by,
        status=ApprovalStatus(row.status),
        decided_by=row.decided_by,
        reason=row.reason,
    )


def _run_columns(row: Row[Any]) -> RunColumns:
    return RunColumns(workflow=row.workflow, input_json=row.input_json, started_by=row.started_by)


def _approval_columns(row: Row[Any]) -> ApprovalColumns:
    return ApprovalColumns(
        approval_id=row.approval_id,
        run_id=row.run_id,
        seq=row.seq,
        command=row.command,
        subject_kind=row.subject_kind,
        name=row.name,
        args_json=row.args_json,
        call_hash=row.call_hash,
        requested_by=row.requested_by,
        status=row.status,
        decided_by=row.decided_by,
    )


def _event_values(record: EventRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "seq": record.seq,
        "command": record.command,
        "type": record.type,
        "fingerprint": record.fingerprint,
        "payload": record.payload,
        "payload_sha256": record.payload_sha256,
        "recorded_at_us": record.recorded_at_us,
        "prev_hash": record.prev_hash,
        "hash": record.hash,
    }


def build_record(
    run_id: str, seq: int, prev_hash: str, new: NewEvent, recorded_at_us: int
) -> EventRecord:
    """Validate a new event's payload and compute its chained hash."""
    validate_payload(new.type, new.payload)
    text = canonical_json(new.payload)
    digest = payload_digest(text)
    return EventRecord(
        run_id=run_id,
        seq=seq,
        command=new.command,
        type=new.type.value,
        fingerprint=new.fingerprint,
        payload=text,
        payload_sha256=digest,
        recorded_at_us=recorded_at_us,
        prev_hash=prev_hash,
        hash=event_hash(
            run_id=run_id,
            seq=seq,
            command=new.command,
            type=new.type.value,
            fingerprint=new.fingerprint,
            payload_sha256=digest,
            recorded_at_us=recorded_at_us,
            prev_hash=prev_hash,
        ),
    )


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID.fullmatch(run_id):
        raise ValueError(f"run_id must match {RUN_ID_PATTERN}")
    return run_id


class Journal:
    """Journal database access. Thread-safe; share one instance per process."""

    def __init__(
        self,
        path: Path,
        *,
        synchronous: SynchronousMode = "NORMAL",
        max_history_events: int = 10_000,
        compensation_reserve_events: int = 256,
        clock: Clock = time.time,
        metrics: RuntimeMetrics | None = None,
        migrate: bool = True,
    ) -> None:
        if max_history_events < 2:
            raise ValueError("max_history_events must be at least 2")
        if compensation_reserve_events < 0:
            raise ValueError("compensation_reserve_events must not be negative")
        self._path = path
        self._clock = clock
        self._metrics = metrics
        self.max_history_events = max_history_events
        self.compensation_reserve_events = compensation_reserve_events
        self.synchronous = synchronous
        self._writer = make_engine(path, synchronous=synchronous, immediate=True)
        self._reader = make_engine(path, synchronous=synchronous, immediate=False)
        self.append_hooks: list[AppendHook] = []
        if migrate:
            self.migrate()

    @property
    def path(self) -> Path:
        return self._path

    def now_us(self) -> int:
        return now_us(self._clock)

    # ------------------------------------------------------------------ lifecycle

    def migrate(self) -> None:
        """Bring the schema to head with Alembic (skipped when already at head)."""
        with self._reader.connect() as conn:
            has_version = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='alembic_version'"
            ).first()
            current = (
                conn.exec_driver_sql("SELECT version_num FROM alembic_version").scalar()
                if has_version
                else None
            )
        if current == SCHEMA_REVISION:
            return
        config = AlembicConfig()
        config.set_main_option("script_location", str(_MIGRATIONS))
        with self._writer.begin() as conn:
            config.attributes["connection"] = conn
            alembic_command.upgrade(config, "head")

    def close(self) -> None:
        self._writer.dispose()
        self._reader.dispose()

    def ping(self) -> bool:
        with self._reader.connect() as conn:
            return conn.exec_driver_sql("SELECT 1").scalar() == 1

    # ------------------------------------------------------------------ runs

    def create_run(
        self,
        *,
        run_id: str,
        workflow: str,
        input_value: JsonValue,
        started_by: str,
        budget: BudgetLimits,
    ) -> RunRow:
        """Insert a run and its ``RunStarted`` event (seq 0) atomically."""
        validate_run_id(run_id)
        now = self.now_us()
        record = build_record(
            run_id,
            0,
            GENESIS_HASH,
            NewEvent(
                EventType.RUN_STARTED,
                {
                    "workflow": workflow,
                    "input": input_value,
                    "started_by": started_by,
                    "budget": budget.model_dump(mode="json"),
                    "max_history_events": self.max_history_events,
                },
            ),
            now,
        )
        try:
            with self._writer.begin() as conn:
                conn.execute(
                    insert(runs).values(
                        run_id=run_id,
                        workflow=workflow,
                        status=RunStatus.PENDING.value,
                        input_json=canonical_json(input_value),
                        started_by=started_by,
                        created_at_us=now,
                        updated_at_us=now,
                        owner=None,
                        fencing=0,
                        lease_expires_at_us=0,
                        cancel_requested=False,
                        inbox_version=0,
                        head_seq=0,
                        head_hash=record.hash,
                    )
                )
                conn.execute(insert(events).values(**_event_values(record)))
        except IntegrityError as exc:
            raise RunStateError(f"run {run_id} already exists") from exc
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> RunRow:
        with self._reader.connect() as conn:
            row = conn.execute(select(runs).where(runs.c.run_id == run_id)).first()
        if row is None:
            raise RunNotFoundError(run_id)
        return _run_from_row(row)

    def list_runs(self, *, status: RunStatus | None = None, limit: int = 100) -> list[RunRow]:
        query = select(runs).order_by(runs.c.created_at_us, runs.c.run_id).limit(limit)
        if status is not None:
            query = query.where(runs.c.status == status.value)
        with self._reader.connect() as conn:
            return [_run_from_row(row) for row in conn.execute(query)]

    def load_history(self, run_id: str) -> list[EventRecord]:
        with self._reader.connect() as conn:
            return self._history(conn, run_id)

    @staticmethod
    def _history(conn: Connection, run_id: str) -> list[EventRecord]:
        rows = conn.execute(select(events).where(events.c.run_id == run_id).order_by(events.c.seq))
        return [_event_from_row(row) for row in rows]

    @staticmethod
    def _approval_rows(conn: Connection, run_id: str) -> list[ApprovalColumns]:
        rows = conn.execute(
            select(approvals).where(approvals.c.run_id == run_id).order_by(approvals.c.seq)
        )
        return [_approval_columns(row) for row in rows]

    # ------------------------------------------------------------------ leases

    def claim(self, owner: str, lease_seconds: float, *, run_id: str | None = None) -> Claim | None:
        """Atomically take the lease of one runnable run whose lease has expired."""
        now = self.now_us()
        expires = now + round(lease_seconds * 1_000_000)
        with self._writer.begin() as conn:
            query = select(runs.c.run_id, runs.c.owner).where(
                runs.c.status.in_(RUNNABLE_STATUSES), runs.c.lease_expires_at_us < now
            )
            if run_id is not None:
                query = query.where(runs.c.run_id == run_id)
            candidate = conn.execute(
                query.order_by(runs.c.updated_at_us, runs.c.run_id).limit(1)
            ).first()
            if candidate is None:
                return None
            claimed = conn.execute(
                update(runs)
                .where(runs.c.run_id == candidate.run_id, runs.c.lease_expires_at_us < now)
                .values(
                    owner=owner,
                    fencing=runs.c.fencing + 1,
                    lease_expires_at_us=expires,
                    status=RunStatus.RUNNING.value,
                    updated_at_us=now,
                )
                .returning(runs.c.fencing)
            ).first()
            if claimed is None:  # pragma: no cover - BEGIN IMMEDIATE makes this unreachable
                return None
        reclaimed = candidate.owner is not None
        if reclaimed and self._metrics is not None:
            self._metrics.lease_reclaims_total.inc()
        lease = Lease(run_id=candidate.run_id, owner=owner, fencing=int(claimed.fencing))
        return Claim(lease=lease, run=self.get_run(candidate.run_id), reclaimed=reclaimed)

    def renew(self, lease: Lease, lease_seconds: float) -> bool:
        expires = self.now_us() + round(lease_seconds * 1_000_000)
        with self._writer.begin() as conn:
            result = conn.execute(
                update(runs)
                .where(
                    runs.c.run_id == lease.run_id,
                    runs.c.owner == lease.owner,
                    runs.c.fencing == lease.fencing,
                )
                .values(lease_expires_at_us=expires)
            )
        return result.rowcount == 1

    def wake_due(self) -> int:
        """Make waiting runs whose timer is due runnable again. Returns how many woke."""
        now = self.now_us()
        with self._writer.begin() as conn:
            result = conn.execute(
                update(runs)
                .where(
                    runs.c.status == RunStatus.WAITING.value,
                    runs.c.wake_at_us.is_not(None),
                    runs.c.wake_at_us <= now,
                )
                .values(status=RunStatus.PENDING.value, updated_at_us=now)
            )
        return int(result.rowcount)

    def _fenced(self, lease: Lease) -> Any:
        return (
            (runs.c.run_id == lease.run_id)
            & (runs.c.owner == lease.owner)
            & (runs.c.fencing == lease.fencing)
        )

    def _stale(self, where: str) -> None:
        if self._metrics is not None:
            self._metrics.stale_fencing_rejections_total.labels(where).inc()

    def suspend(
        self,
        lease: Lease,
        *,
        waiting_on: Mapping[str, JsonValue],
        wake_at_us: int | None,
        seen_inbox_version: int,
    ) -> RunStatus:
        """Release the lease and park the run; inputs that arrived meanwhile make it runnable."""
        now = self.now_us()
        due = wake_at_us is not None and wake_at_us <= now
        with self._writer.begin() as conn:
            status = case(
                (runs.c.inbox_version != seen_inbox_version, RunStatus.PENDING.value),
                else_=RunStatus.PENDING.value if due else RunStatus.WAITING.value,
            )
            result = conn.execute(
                update(runs)
                .where(self._fenced(lease))
                .values(
                    status=status,
                    waiting_on=canonical_json(dict(waiting_on)),
                    wake_at_us=wake_at_us,
                    owner=None,
                    lease_expires_at_us=0,
                    updated_at_us=now,
                )
                .returning(runs.c.status)
            ).first()
        if result is None:
            self._stale("journal")
            raise FencedOutError(f"lease on {lease.run_id} was lost before suspending")
        return RunStatus(result.status)

    def block(self, lease: Lease, reason: str) -> None:
        """Park a run that must not be replayed until an operator intervenes."""
        with self._writer.begin() as conn:
            result = conn.execute(
                update(runs)
                .where(self._fenced(lease))
                .values(
                    status=RunStatus.BLOCKED.value,
                    blocked_reason=reason[:2000],
                    owner=None,
                    lease_expires_at_us=0,
                    updated_at_us=self.now_us(),
                )
            )
        if result.rowcount != 1:
            self._stale("journal")
            raise FencedOutError(f"lease on {lease.run_id} was lost before blocking")

    def retry_blocked(self, run_id: str) -> RunRow:
        """Make a blocked run runnable again (after deploying fixed workflow code)."""
        with self._writer.begin() as conn:
            result = conn.execute(
                update(runs)
                .where(runs.c.run_id == run_id, runs.c.status == RunStatus.BLOCKED.value)
                .values(
                    status=RunStatus.PENDING.value,
                    blocked_reason=None,
                    updated_at_us=self.now_us(),
                )
            )
        if result.rowcount != 1:
            run = self.get_run(run_id)
            raise RunStateError(f"run {run_id} is {run.status.value}, not blocked")
        return self.get_run(run_id)

    # ------------------------------------------------------------------ appends

    def append(
        self,
        lease: Lease,
        new_events: Sequence[NewEvent] | EventBuilder,
        *,
        within: TxnHook | None = None,
        run_update: Mapping[str, Any] | None = None,
        following: int = 0,
        reserve: bool = False,
    ) -> list[EventRecord]:
        """Append events for the lease holder in one fenced transaction.

        ``new_events`` may be a builder called inside the transaction (returning ``None`` appends
        nothing). ``within`` runs extra statements (outbox, inbox) in the same transaction after
        the events are inserted; ``run_update`` sets extra ``runs`` columns.

        History cap: the append fails with ``HistoryLimitExceededError`` if the history would then
        hold more than ``max_history_events`` events *plus* ``following`` more (the first event of
        a step passes ``following=1``, so a step never starts unless its completion fits).
        ``reserve=True`` (compensation commands) may use ``compensation_reserve_events`` more, so a
        run that hit the cap can still roll back. Terminal events always fit.
        """
        started = time.perf_counter()
        records: list[EventRecord] = []
        with self._writer.begin() as conn:
            head = conn.execute(
                select(runs.c.owner, runs.c.fencing, runs.c.head_seq, runs.c.head_hash).where(
                    runs.c.run_id == lease.run_id
                )
            ).first()
            if head is None:
                raise RunNotFoundError(lease.run_id)
            if head.owner != lease.owner or head.fencing != lease.fencing:
                self._stale("journal")
                raise FencedOutError(
                    f"lease on {lease.run_id} (fencing {lease.fencing}) was superseded"
                )
            built = new_events if isinstance(new_events, Sequence) else new_events(conn)
            if not built:
                return []
            final_seq = head.head_seq + len(built)
            limit = self.max_history_events + (self.compensation_reserve_events if reserve else 0)
            if final_seq + 1 + following > limit and not all(
                item.type in TERMINAL_EVENTS for item in built
            ):
                raise HistoryLimitExceededError(lease.run_id, self.max_history_events)
            now = self.now_us()
            seq, prev = head.head_seq, head.head_hash
            for item in built:
                seq += 1
                record = build_record(lease.run_id, seq, prev, item, now)
                records.append(record)
                prev = record.hash
            result = conn.execute(
                update(runs)
                .where(self._fenced(lease) & (runs.c.head_seq == head.head_seq))
                .values(head_seq=seq, head_hash=prev, updated_at_us=now, **(run_update or {}))
            )
            if result.rowcount != 1:  # pragma: no cover - guarded by BEGIN IMMEDIATE above
                raise FencedOutError(f"concurrent append on {lease.run_id}")
            conn.execute(insert(events), [_event_values(record) for record in records])
            if within is not None:
                within(conn, records)
            for hook in self.append_hooks:
                hook("before_commit", records)
        if self._metrics is not None:
            self._metrics.journal_append_latency.observe(time.perf_counter() - started)
        for hook in self.append_hooks:
            hook("after_commit", records)
        return records

    def finish(
        self, lease: Lease, event: NewEvent, *, result: JsonValue = None, error: JsonValue = None
    ) -> EventRecord:
        """Append a terminal event and close the run in the same transaction."""
        status = {
            EventType.RUN_COMPLETED: RunStatus.COMPLETED,
            EventType.RUN_FAILED: RunStatus.FAILED,
            EventType.RUN_CANCELLED: RunStatus.CANCELLED,
        }[event.type]
        records = self.append(
            lease,
            [event],
            run_update={
                "status": status.value,
                "result_json": canonical_json(result) if result is not None else None,
                "error_json": canonical_json(error) if error is not None else None,
                "owner": None,
                "lease_expires_at_us": 0,
                "waiting_on": None,
                "wake_at_us": None,
            },
        )
        return records[0]

    # ------------------------------------------------------------------ signals

    def send_signal(self, run_id: str, name: str, payload: JsonValue, *, sent_by: str) -> int:
        now = self.now_us()
        with self._writer.begin() as conn:
            run = conn.execute(select(runs.c.status).where(runs.c.run_id == run_id)).first()
            if run is None:
                raise RunNotFoundError(run_id)
            if RunStatus(run.status) in TERMINAL_STATUSES:
                raise RunStateError(f"run {run_id} is {run.status}; signals are not accepted")
            inserted = conn.execute(
                insert(signals)
                .values(
                    run_id=run_id,
                    name=name,
                    payload_json=canonical_json(payload),
                    sent_by=sent_by,
                    created_at_us=now,
                )
                .returning(signals.c.signal_id)
            ).first()
            self._bump_inbox(conn, run_id, now)
        if inserted is None:  # pragma: no cover - RETURNING always yields the inserted row
            raise RuntimeError("signal insert returned no row")
        return int(inserted.signal_id)

    def signal_count(self, run_id: str) -> int:
        """How many signals were ever sent to a run (consumed or not)."""
        with self._reader.connect() as conn:
            count = conn.execute(
                select(func.count()).select_from(signals).where(signals.c.run_id == run_id)
            ).scalar_one()
        return int(count)

    def _bump_inbox(self, conn: Connection, run_id: str, now: int) -> None:
        conn.execute(
            update(runs)
            .where(runs.c.run_id == run_id)
            .values(
                inbox_version=runs.c.inbox_version + 1,
                status=case(
                    (runs.c.status == RunStatus.WAITING.value, RunStatus.PENDING.value),
                    else_=runs.c.status,
                ),
                updated_at_us=now,
            )
        )

    def consume_signal(
        self, lease: Lease, *, command: int, fingerprint: str, name: str
    ) -> EventRecord | None:
        """Append ``SignalReceived`` for the oldest unconsumed signal ``name``, if there is one."""
        chosen: dict[str, int] = {}

        def build(conn: Connection) -> Sequence[NewEvent] | None:
            row = conn.execute(
                select(signals)
                .where(
                    signals.c.run_id == lease.run_id,
                    signals.c.name == name,
                    signals.c.consumed_seq.is_(None),
                )
                .order_by(signals.c.signal_id)
                .limit(1)
            ).first()
            if row is None:
                return None
            chosen["signal_id"] = row.signal_id
            return [
                NewEvent(
                    EventType.SIGNAL_RECEIVED,
                    {
                        "signal_id": row.signal_id,
                        "name": name,
                        "payload": json.loads(row.payload_json),
                        "sent_by": row.sent_by,
                    },
                    command,
                    fingerprint,
                )
            ]

        def mark(conn: Connection, records: Sequence[EventRecord]) -> None:
            conn.execute(
                update(signals)
                .where(signals.c.signal_id == chosen["signal_id"])
                .values(consumed_seq=records[0].seq)
            )

        records = self.append(lease, build, within=mark)
        return records[0] if records else None

    # ------------------------------------------------------------------ approvals

    def request_approval(
        self,
        lease: Lease,
        *,
        command: int,
        fingerprint: str,
        kind: SubjectKind,
        name: str,
        args: Mapping[str, JsonValue],
        call_hash: str,
        requested_by: str,
    ) -> EventRecord:
        approval_id = f"{lease.run_id}:{command}"
        event = NewEvent(
            EventType.APPROVAL_REQUESTED,
            {
                "approval_id": approval_id,
                "subject_kind": kind.value,
                "name": name,
                "args": dict(args),
                "call_hash": call_hash,
                "requested_by": requested_by,
            },
            command,
            fingerprint,
        )

        def insert_row(conn: Connection, records: Sequence[EventRecord]) -> None:
            conn.execute(
                insert(approvals).values(
                    approval_id=approval_id,
                    run_id=lease.run_id,
                    seq=records[0].seq,
                    command=command,
                    subject_kind=kind.value,
                    name=name,
                    args_json=canonical_json(dict(args)),
                    call_hash=call_hash,
                    requested_by=requested_by,
                    status=ApprovalStatus.PENDING.value,
                    created_at_us=records[0].recorded_at_us,
                )
            )

        # following=1: the decision must fit too, or nobody should be asked to approve.
        return self.append(lease, [event], within=insert_row, following=1)[0]

    def get_approval(self, approval_id: str) -> ApprovalRequest:
        with self._reader.connect() as conn:
            row = conn.execute(
                select(approvals).where(approvals.c.approval_id == approval_id)
            ).first()
        if row is None:
            raise ApprovalError(f"unknown approval {approval_id}")
        return _approval_from_row(row)

    def list_approvals(
        self,
        *,
        run_id: str | None = None,
        status: ApprovalStatus | None = None,
        limit: int = 100,
    ) -> list[ApprovalRequest]:
        query = select(approvals).order_by(approvals.c.created_at_us).limit(limit)
        if run_id is not None:
            query = query.where(approvals.c.run_id == run_id)
        if status is not None:
            query = query.where(approvals.c.status == status.value)
        with self._reader.connect() as conn:
            return [_approval_from_row(row) for row in conn.execute(query)]

    def decide_approval(
        self,
        approval_id: str,
        *,
        approver: str,
        approve: bool,
        presented_hash: str,
        reason: str | None = None,
    ) -> ApprovalRequest:
        """Record a decision after checking it against the verified chain, not mutable columns.

        Inside the write transaction the run's whole chain is verified against its head, the
        ``runs`` and ``approvals`` rows are checked against ``RunStarted`` and the
        ``ApprovalRequested`` events, and separation of duties uses the chained starter.
        """
        now = self.now_us()
        with self._writer.begin() as conn:
            row = conn.execute(
                select(approvals).where(approvals.c.approval_id == approval_id)
            ).first()
            if row is None:
                raise ApprovalError(f"unknown approval {approval_id}")
            request = _approval_from_row(row)
            run = conn.execute(select(runs).where(runs.c.run_id == request.run_id)).first()
            if run is None:
                raise RunNotFoundError(request.run_id)
            if RunStatus(run.status) in TERMINAL_STATUSES:
                raise ApprovalError(f"run {request.run_id} is {run.status}")
            history = self._history(conn, request.run_id)
            chain = verify_chain(
                history, run_id=request.run_id, expect_head=(run.head_seq, run.head_hash)
            )
            if not chain.ok:
                raise ApprovalIntegrityError(
                    f"journal of run {request.run_id} does not verify: {chain.error}"
                )
            bound = binding_error(
                history, _run_columns(run), self._approval_rows(conn, request.run_id)
            )
            if bound is not None:
                raise ApprovalIntegrityError(f"run {request.run_id}: {bound.message}")
            if approver == str(history[0].data()["started_by"]):
                raise ApprovalError(
                    "separation of duties: the run starter cannot approve its own run"
                )
            requested = next(record for record in history if record.seq == request.seq)
            check_decision(
                request,
                approver=approver,
                presented_hash=presented_hash,
                journal_call_hash=str(requested.data()["call_hash"]),
            )
            status = ApprovalStatus.APPROVED if approve else ApprovalStatus.DENIED
            conn.execute(
                update(approvals)
                .where(
                    approvals.c.approval_id == approval_id,
                    approvals.c.status == ApprovalStatus.PENDING.value,
                )
                .values(
                    status=status.value,
                    decided_by=approver,
                    decided_at_us=now,
                    reason=reason[:500] if reason else None,
                )
            )
            self._bump_inbox(conn, request.run_id, now)
        return self.get_approval(approval_id)

    # ------------------------------------------------------------------ cancellation

    def request_cancel(self, run_id: str) -> RunRow:
        now = self.now_us()
        with self._writer.begin() as conn:
            run = conn.execute(select(runs.c.status).where(runs.c.run_id == run_id)).first()
            if run is None:
                raise RunNotFoundError(run_id)
            status = RunStatus(run.status)
            if status in TERMINAL_STATUSES or status is RunStatus.BLOCKED:
                raise RunStateError(f"run {run_id} is {status.value} and cannot be cancelled")
            conn.execute(update(runs).where(runs.c.run_id == run_id).values(cancel_requested=True))
            self._bump_inbox(conn, run_id, now)
        return self.get_run(run_id)

    # ------------------------------------------------------------------ outbox

    @staticmethod
    def outbox_insert(
        conn: Connection,
        *,
        key: str,
        run_id: str,
        seq: int,
        tool: str,
        call_hash: str,
        effect: str,
        fencing: int,
        now: int,
    ) -> None:
        conn.execute(
            insert(outbox)
            .prefix_with("OR IGNORE")
            .values(
                key=key,
                run_id=run_id,
                seq=seq,
                tool=tool,
                call_hash=call_hash,
                effect=effect,
                status="pending",
                fencing=fencing,
                created_at_us=now,
            )
        )

    @staticmethod
    def outbox_finish(conn: Connection, *, key: str, status: str, fencing: int, now: int) -> None:
        conn.execute(
            update(outbox)
            .where(outbox.c.key == key)
            .values(status=status, fencing=fencing, completed_at_us=now)
        )

    def list_outbox(
        self, *, status: str | None = None, run_id: str | None = None
    ) -> list[OutboxRow]:
        query = select(outbox).order_by(outbox.c.created_at_us, outbox.c.key)
        if status is not None:
            query = query.where(outbox.c.status == status)
        if run_id is not None:
            query = query.where(outbox.c.run_id == run_id)
        with self._reader.connect() as conn:
            return [
                OutboxRow(
                    key=row.key,
                    run_id=row.run_id,
                    seq=row.seq,
                    tool=row.tool,
                    call_hash=row.call_hash,
                    effect=row.effect,
                    status=row.status,
                    fencing=row.fencing,
                )
                for row in conn.execute(query)
            ]

    # ------------------------------------------------------------------ verification and export

    def verify_run(self, run_id: str, *, expect_head: str | None = None) -> VerifyResult:
        """Verify the chain against the stored head, then the ``runs`` and ``approvals`` columns
        the chain binds (see :mod:`replayable_agent_runtime.runtime.bindings`)."""
        with self._reader.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.run_id == run_id)).first()
            if run is None:
                raise RunNotFoundError(run_id)
            history = self._history(conn, run_id)
            approval_rows = self._approval_rows(conn, run_id)
        result = verify_chain(history, run_id=run_id, expect_head=(run.head_seq, run.head_hash))
        if not result.ok:
            return result
        bound = binding_error(history, _run_columns(run), approval_rows)
        if bound is not None:
            return VerifyResult(False, result.entries, result.head, bound.message, bound.seq)
        if expect_head is not None and expect_head != result.head:
            return VerifyResult(
                False, result.entries, result.head, "head mismatch with the external anchor"
            )
        return result

    def verify_all(self) -> dict[str, VerifyResult]:
        """Verify every run, and report events whose run no longer exists."""
        results: dict[str, VerifyResult] = {}
        with self._reader.connect() as conn:
            run_ids = [row.run_id for row in conn.execute(select(runs.c.run_id))]
            orphans = conn.execute(
                select(events.c.run_id, func.count())
                .where(events.c.run_id.not_in(select(runs.c.run_id)))
                .group_by(events.c.run_id)
            ).all()
        for run_id in run_ids:
            results[run_id] = self.verify_run(run_id)
        for orphan in orphans:
            results[orphan.run_id] = VerifyResult(
                False, int(orphan[1]), GENESIS_HASH, "events without a run row (run deleted)"
            )
        return results

    def export_run(self, run_id: str, *, redact: bool) -> Iterator[str]:
        run = self.get_run(run_id)
        return export_lines(
            run_id, run.head_seq, run.head_hash, self.load_history(run_id), redact=redact
        )

    def run_level_events(self, run_id: str) -> list[EventRecord]:
        return [record for record in self.load_history(run_id) if record.command == RUN_LEVEL]
