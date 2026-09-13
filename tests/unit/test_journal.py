from __future__ import annotations

from pathlib import Path

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlalchemy import create_engine

from replayable_agent_runtime.agentic.approval import SubjectKind, subject_hash
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.errors import ApprovalError, ApprovalIntegrityError
from replayable_agent_runtime.runtime.chaos import FakeClock
from replayable_agent_runtime.runtime.db import make_engine, sqlite_url
from replayable_agent_runtime.runtime.errors import (
    FencedOutError,
    HistoryLimitExceededError,
    RunNotFoundError,
    RunStateError,
)
from replayable_agent_runtime.runtime.events import (
    CommandKind,
    EventType,
    NewEvent,
    command_fingerprint,
)
from replayable_agent_runtime.runtime.journal import Journal, Lease, RunStatus
from replayable_agent_runtime.runtime.schema import SCHEMA_REVISION, metadata
from replayable_agent_runtime.runtime.telemetry import create_runtime_metrics

pytestmark = pytest.mark.unit

BUDGET = BudgetLimits.of(steps=10, tokens=1_000, usd=1.0)
MIGRATIONS = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "replayable_agent_runtime"
    / "runtime"
    / "migrations"
)


def now_event(command: int) -> NewEvent:
    return NewEvent(
        EventType.STEP_COMPLETED,
        {"kind": "now", "name": "clock", "output": command},
        command,
        command_fingerprint(CommandKind.NOW, "clock", {}),
    )


def started(journal: Journal, run_id: str = "run-1", principal: str = "alice") -> None:
    journal.create_run(
        run_id=run_id, workflow="wf", input_value={"x": 1}, started_by=principal, budget=BUDGET
    )


def test_alembic_migration_matches_the_sqlalchemy_schema(journal: Journal) -> None:
    engine = create_engine(sqlite_url(journal.path))
    with engine.connect() as conn:
        assert compare_metadata(MigrationContext.configure(conn), metadata) == []
        assert conn.exec_driver_sql("SELECT version_num FROM alembic_version").scalar() == (
            SCHEMA_REVISION
        )
    engine.dispose()
    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS))
    assert ScriptDirectory.from_config(config).get_current_head() == SCHEMA_REVISION


def test_the_journal_uses_wal_mode(journal: Journal) -> None:
    engine = make_engine(journal.path)
    with engine.connect() as conn:
        assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
    engine.dispose()
    with pytest.raises(ValueError, match="synchronous"):
        make_engine(journal.path, synchronous="OFF")  # type: ignore[arg-type]


def test_reopening_a_journal_keeps_its_runs(tmp_path: Path) -> None:
    first = Journal(tmp_path / "j.db")
    started(first)
    first.close()
    reopened = Journal(tmp_path / "j.db")
    assert reopened.get_run("run-1").status is RunStatus.PENDING
    assert reopened.ping()
    reopened.close()
    with pytest.raises(ValueError, match="max_history_events"):
        Journal(tmp_path / "j.db", max_history_events=1)


def test_create_run_writes_run_started_and_anchors_the_head(journal: Journal) -> None:
    started(journal)
    run = journal.get_run("run-1")
    history = journal.load_history("run-1")
    assert [record.type for record in history] == ["RunStarted"]
    assert (run.head_seq, run.head_hash) == (0, history[0].hash)
    assert history[0].data()["budget"] == BUDGET.model_dump()
    assert journal.verify_run("run-1").ok
    assert run.summary()["status"] == "pending"
    with pytest.raises(RunStateError, match="already exists"):
        started(journal)
    with pytest.raises(ValueError, match="run_id"):
        started(journal, run_id="../etc")
    with pytest.raises(RunNotFoundError):
        journal.get_run("missing")


def test_claims_increment_fencing_monotonically_and_respect_leases(
    journal: Journal, clock: FakeClock
) -> None:
    metrics = create_runtime_metrics()
    fenced = Journal(journal.path, clock=clock, metrics=metrics)
    started(fenced)
    first = fenced.claim("w1", 10)
    assert first is not None
    assert (first.lease.fencing, first.reclaimed) == (1, False)
    assert fenced.claim("w2", 10) is None
    clock.advance(11)
    second = fenced.claim("w2", 10)
    assert second is not None
    assert (second.lease.fencing, second.reclaimed) == (2, True)
    assert metrics.lease_reclaims_total._value.get() == 1
    assert not fenced.renew(first.lease, 10)
    assert fenced.renew(second.lease, 10)
    fenced.close()


def test_appends_from_a_superseded_lease_are_fenced_out(journal: Journal, clock: FakeClock) -> None:
    started(journal)
    zombie = journal.claim("w1", 5)
    assert zombie is not None
    clock.advance(6)
    owner = journal.claim("w2", 5)
    assert owner is not None
    with pytest.raises(FencedOutError):
        journal.append(zombie.lease, [now_event(0)])
    records = journal.append(owner.lease, [now_event(0), now_event(1)])
    assert [record.seq for record in records] == [1, 2]
    assert records[1].prev_hash == records[0].hash
    with pytest.raises(FencedOutError):
        journal.append(Lease("run-1", "w2", owner.lease.fencing - 1), [now_event(2)])
    with pytest.raises(RunNotFoundError):
        journal.append(Lease("missing", "w2", 1), [now_event(2)])
    assert journal.verify_run("run-1").ok


def test_an_append_builder_returning_nothing_appends_nothing(journal: Journal) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    assert journal.append(claim.lease, lambda conn: None) == []
    assert journal.get_run("run-1").head_seq == 0


def test_history_limit_fails_loudly_but_still_records_the_terminal_event(
    tmp_path: Path, clock: FakeClock
) -> None:
    small = Journal(tmp_path / "small.db", max_history_events=3, clock=clock)
    started(small)
    claim = small.claim("w1", 5)
    assert claim is not None
    small.append(claim.lease, [now_event(0), now_event(1)])
    with pytest.raises(HistoryLimitExceededError, match="3 events"):
        small.append(claim.lease, [now_event(2)])
    small.finish(
        claim.lease,
        NewEvent(EventType.RUN_FAILED, {"error_type": "HistoryLimitExceededError", "message": "x"}),
        error={"error_type": "HistoryLimitExceededError"},
    )
    assert small.get_run("run-1").status is RunStatus.FAILED
    small.close()


def test_inputs_arriving_during_an_activation_wake_the_run_instead_of_being_lost(
    journal: Journal,
) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    journal.send_signal("run-1", "go", {"n": 1}, sent_by="bob")
    status = journal.suspend(
        claim.lease,
        waiting_on={"type": "signal", "name": "go"},
        wake_at_us=None,
        seen_inbox_version=claim.run.inbox_version,
    )
    assert status is RunStatus.PENDING

    again = journal.claim("w1", 5)
    assert again is not None
    parked = journal.suspend(
        again.lease,
        waiting_on={"type": "signal", "name": "go"},
        wake_at_us=None,
        seen_inbox_version=again.run.inbox_version,
    )
    assert parked is RunStatus.WAITING
    journal.send_signal("run-1", "go", {"n": 2}, sent_by="bob")
    assert journal.get_run("run-1").status is RunStatus.PENDING
    with pytest.raises(FencedOutError):
        journal.suspend(again.lease, waiting_on={}, wake_at_us=None, seen_inbox_version=0)


def test_timers_wake_waiting_runs_only_when_due(journal: Journal, clock: FakeClock) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    fire_at = journal.now_us() + 5_000_000
    journal.suspend(
        claim.lease,
        waiting_on={"type": "timer"},
        wake_at_us=fire_at,
        seen_inbox_version=claim.run.inbox_version,
    )
    assert journal.wake_due() == 0
    clock.advance(5)
    assert journal.wake_due() == 1
    assert journal.get_run("run-1").status is RunStatus.PENDING


def test_signals_to_missing_or_finished_runs_are_refused(journal: Journal) -> None:
    with pytest.raises(RunNotFoundError):
        journal.send_signal("missing", "go", None, sent_by="bob")
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    journal.finish(claim.lease, NewEvent(EventType.RUN_COMPLETED, {"result": 1}), result=1)
    with pytest.raises(RunStateError):
        journal.send_signal("run-1", "go", None, sent_by="bob")
    with pytest.raises(RunStateError):
        journal.request_cancel("run-1")


def test_blocked_runs_can_only_be_retried_explicitly(journal: Journal) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    journal.block(claim.lease, "nondeterminism")
    run = journal.get_run("run-1")
    assert (run.status, run.blocked_reason) == (RunStatus.BLOCKED, "nondeterminism")
    assert journal.claim("w1", 5) is None
    with pytest.raises(RunStateError):
        journal.request_cancel("run-1")
    assert journal.retry_blocked("run-1").status is RunStatus.PENDING
    with pytest.raises(RunStateError, match="not blocked"):
        journal.retry_blocked("run-1")
    with pytest.raises(FencedOutError):
        journal.block(claim.lease, "stale")


def _request_gate(journal: Journal, lease: Lease, details: dict[str, object]) -> str:
    call_hash = subject_hash(SubjectKind.GATE, "deploy", details)  # type: ignore[arg-type]
    journal.request_approval(
        lease,
        command=0,
        fingerprint=command_fingerprint(CommandKind.APPROVAL, "deploy", details),  # type: ignore[arg-type]
        kind=SubjectKind.GATE,
        name="deploy",
        args=details,  # type: ignore[arg-type]
        call_hash=call_hash,
        requested_by="alice",
    )
    return call_hash


def test_approval_decisions_enforce_separation_of_duties_and_the_pending_hash(
    journal: Journal,
) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    call_hash = _request_gate(journal, claim.lease, {"env": "prod"})
    approval_id = "run-1:0"
    with pytest.raises(ApprovalError, match="separation of duties"):
        journal.decide_approval(
            approval_id, approver="alice", approve=True, presented_hash=call_hash
        )
    with pytest.raises(ApprovalError, match="does not match"):
        journal.decide_approval(approval_id, approver="bob", approve=True, presented_hash="0" * 64)
    decided = journal.decide_approval(
        approval_id, approver="bob", approve=True, presented_hash=call_hash, reason="ok"
    )
    assert (decided.status.value, decided.decided_by) == ("approved", "bob")
    with pytest.raises(ApprovalError, match="not pending"):
        journal.decide_approval(approval_id, approver="bob", approve=True, presented_hash=call_hash)
    with pytest.raises(ApprovalError, match="unknown"):
        journal.decide_approval("run-1:9", approver="bob", approve=True, presented_hash=call_hash)
    assert [item.approval_id for item in journal.list_approvals(run_id="run-1")] == [approval_id]


def test_an_edited_approval_row_cannot_be_approved(journal: Journal) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    call_hash = _request_gate(journal, claim.lease, {"env": "staging"})
    engine = make_engine(journal.path)
    with engine.begin() as conn:
        conn.exec_driver_sql('UPDATE approvals SET args_json = \'{"env":"prod"}\'')
    with pytest.raises(ApprovalIntegrityError):
        journal.decide_approval("run-1:0", approver="bob", approve=True, presented_hash=call_hash)
    forged = subject_hash(SubjectKind.GATE, "deploy", {"env": "prod"})
    with engine.begin() as conn:
        conn.exec_driver_sql(f"UPDATE approvals SET call_hash = '{forged}'")  # noqa: S608
    with pytest.raises(ApprovalIntegrityError, match="journaled request"):
        journal.decide_approval("run-1:0", approver="bob", approve=True, presented_hash=forged)
    engine.dispose()


def test_separation_of_duties_uses_the_chained_starter_and_request(journal: Journal) -> None:
    """Regression: SoD read runs.started_by and approvals.requested_by, neither in the chain."""
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    call_hash = _request_gate(journal, claim.lease, {"env": "prod"})
    engine = make_engine(journal.path)
    with engine.begin() as conn:
        conn.exec_driver_sql("UPDATE runs SET started_by = 'mallory'")
        conn.exec_driver_sql("UPDATE approvals SET requested_by = 'mallory'")
    engine.dispose()
    with pytest.raises(ApprovalError):
        journal.decide_approval("run-1:0", approver="alice", approve=True, presented_hash=call_hash)
    assert journal.get_approval("run-1:0").status.value == "pending"
    result = journal.verify_run("run-1")
    assert not result.ok
    assert result.error is not None
    assert "RunStarted" in result.error


def test_verify_run_checks_approval_rows_against_their_chained_requests(journal: Journal) -> None:
    started(journal)
    claim = journal.claim("w1", 5)
    assert claim is not None
    _request_gate(journal, claim.lease, {"env": "prod"})
    assert journal.verify_run("run-1").ok
    engine = make_engine(journal.path)
    with engine.begin() as conn:
        conn.exec_driver_sql("UPDATE approvals SET requested_by = 'mallory'")
    result = journal.verify_run("run-1")
    assert not result.ok
    assert result.error is not None
    assert "approval run-1:0" in result.error
    with engine.begin() as conn:
        conn.exec_driver_sql("DELETE FROM approvals")
    engine.dispose()
    missing = journal.verify_run("run-1")
    assert not missing.ok
    assert missing.error is not None
    assert "no approval row" in missing.error


@settings(max_examples=25)
@given(st.lists(st.sampled_from(["claim", "expire", "append"]), min_size=1, max_size=25))
def test_fencing_tokens_strictly_increase_and_only_the_latest_lease_appends(
    tmp_path_factory: pytest.TempPathFactory, actions: list[str]
) -> None:
    clock = FakeClock(1_000.0)
    journal = Journal(tmp_path_factory.mktemp("fence") / "j.db", clock=clock)
    started(journal)
    leases: list[Lease] = []
    command = 0
    for action in actions:
        if action == "claim":
            claim = journal.claim(f"w{len(leases)}", 5)
            if claim is not None:
                assert not leases or claim.lease.fencing > leases[-1].fencing
                leases.append(claim.lease)
        elif action == "expire":
            clock.advance(6)
        else:
            for lease in leases[:-1]:
                with pytest.raises(FencedOutError):
                    journal.append(lease, [now_event(command)])
            if leases:
                journal.append(leases[-1], [now_event(command)])
                command += 1
    assert journal.verify_run("run-1").ok
    assert journal.get_run("run-1").head_seq == command
    journal.close()
