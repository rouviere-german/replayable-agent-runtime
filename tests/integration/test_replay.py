from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict, JsonValue

from conftest import Harness
from replayable_agent_runtime.agentic.errors import (
    ActivityFailedError,
    ProviderError,
    ProviderRefusalError,
)
from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest, LLMResponse
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.agentic.tools import Capability, ToolCallContext, ToolDefinition
from replayable_agent_runtime.runtime.chaos import (
    CrashInjector,
    CrashPoint,
    SimulatedCrash,
    echo_policy,
)
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.db import make_engine
from replayable_agent_runtime.runtime.engine import replay_check
from replayable_agent_runtime.runtime.errors import NonDeterminismError
from replayable_agent_runtime.runtime.events import EventType
from replayable_agent_runtime.runtime.journal import Journal, RunRow, RunStatus
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

pytestmark = pytest.mark.integration


class CountingProvider:
    """Counts real provider calls; optionally fails the first one."""

    name = "counting"

    def __init__(self, fail_first: ProviderError | None = None) -> None:
        self._inner = FakeProvider(echo_policy)
        self._fail = fail_first
        self.requests: list[str] = []

    def complete(self, request: LLMRequest) -> LLMResponse:
        self.requests.append(request.sha256())
        if self._fail is not None:
            error, self._fail = self._fail, None
            raise error
        return self._inner.complete(request)


def drive(harness: Harness, run_id: str, *, approve: bool = True, limit: int = 60) -> RunRow:
    for _ in range(limit):
        run = harness.journal.get_run(run_id)
        if run.terminal or run.status is RunStatus.BLOCKED:
            return run
        if run.status is RunStatus.WAITING:
            waiting = run.waiting_on or {}
            if waiting.get("type") == "approval" and approve:
                harness.approve_pending(run_id)
                continue
            if waiting.get("type") == "timer":
                harness.clock.advance_to(int(waiting["fire_at_us"]) / 1_000_000 + 0.001)
                harness.journal.wake_due()
                continue
            return run
        outcome = asyncio.run(harness.worker().run_once(run_id=run_id))
        if outcome is None:
            harness.clock.advance(31)
    raise AssertionError("run did not settle")


def types_of(journal: Journal, run_id: str) -> list[str]:
    return [record.type for record in journal.load_history(run_id)]


async def basic(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    started = await ctx.now()
    answer = await ctx.llm(LLMRequest(messages=(ChatMessage.user(f"hello {value}"),)))
    fact = await ctx.tool("kb_lookup", {"key": "cache"})
    token = await ctx.uuid()
    return {"answer": answer.text, "fact": fact, "token": token, "started": started}


def test_a_run_journals_every_command_and_strict_replay_calls_nothing(harness: Harness) -> None:
    provider = CountingProvider()
    harness.provider = provider
    harness.register("basic", basic, frozenset({"kb_lookup"}))
    run = drive(harness, harness.start("basic", "world"))
    assert run.status is RunStatus.COMPLETED
    assert types_of(harness.journal, "run-1") == [
        "RunStarted", "StepCompleted", "StepScheduled", "StepCompleted",
        "StepScheduled", "StepCompleted", "StepCompleted", "RunCompleted",
    ]  # fmt: skip
    assert len(provider.requests) == 1
    assert isinstance(run.result, dict)
    recorded_uuid = harness.journal.load_history("run-1")[6].data()["output"]
    assert run.result["token"] == recorded_uuid
    outcome = asyncio.run(replay_check(harness.runtime(), "run-1"))
    assert outcome.outcome == "completed"
    assert len(provider.requests) == 1
    assert harness.journal.verify_run("run-1").ok


@pytest.mark.parametrize("seq", range(1, 8))
def test_a_crash_after_any_commit_resumes_without_repeating_a_committed_call(
    harness: Harness, seq: int
) -> None:
    provider = CountingProvider()
    harness.provider = provider
    harness.register("basic", basic, frozenset({"kb_lookup"}))
    injector = CrashInjector([CrashPoint("after_commit", seq)])
    harness.hooks = injector
    harness.journal.append_hooks.append(injector.append_hook)
    run_id = harness.start("basic", "world")
    with pytest.raises(SimulatedCrash):
        asyncio.run(harness.worker().run_once(run_id=run_id))
    terminal = seq == 7  # the crash came right after RunCompleted committed
    expected = RunStatus.COMPLETED if terminal else RunStatus.RUNNING
    assert harness.journal.get_run(run_id).status is expected
    harness.clock.advance(31)
    run = drive(harness, run_id)
    assert run.status is RunStatus.COMPLETED
    assert len(provider.requests) == 1
    assert run.fencing == (1 if terminal else 2)
    assert harness.journal.verify_run(run_id).ok


def test_a_crash_inside_the_effect_window_re_executes_only_that_step(harness: Harness) -> None:
    provider = CountingProvider()
    harness.provider = provider
    harness.register("basic", basic, frozenset({"kb_lookup"}))
    injector = CrashInjector([CrashPoint("after_effect", 0)])
    harness.hooks = injector
    run_id = harness.start("basic", "world")
    with pytest.raises(SimulatedCrash):
        asyncio.run(harness.worker().run_once(run_id=run_id))
    harness.clock.advance(31)
    run = drive(harness, run_id)
    assert run.status is RunStatus.COMPLETED
    assert provider.requests == [provider.requests[0]] * 2
    history = harness.journal.load_history(run_id)
    llm_events = [r.type for r in history if r.command == 1]
    assert llm_events == ["StepScheduled", "StepCompleted"]
    assert types_of(harness.journal, run_id).count("StepScheduled") == 2


async def sleeper(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.sleep(10)
    return "woke"


def test_durable_timers_release_the_worker_and_fire_when_due(harness: Harness) -> None:
    harness.register("sleeper", sleeper)
    run_id = harness.start("sleeper")
    outcome = asyncio.run(harness.worker().run_once())
    assert outcome is not None
    assert outcome.outcome == "waiting"
    assert asyncio.run(harness.worker().run_once()) is None
    harness.clock.advance(10)
    assert asyncio.run(harness.worker().run_once()) is not None
    run = harness.journal.get_run(run_id)
    assert (run.status, run.result) == (RunStatus.COMPLETED, "woke")
    assert types_of(harness.journal, run_id)[1:3] == ["TimerScheduled", "TimerFired"]


async def negative_sleep(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.sleep(-1)
    return None


def test_invalid_timer_durations_fail_the_run(harness: Harness) -> None:
    harness.register("negative", negative_sleep)
    run = drive(harness, harness.start("negative"))
    assert run.status is RunStatus.FAILED
    assert run.error is not None
    assert run.error["error_type"] == "ValueError"


async def two_signals(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    first = await ctx.wait_signal("go")
    second = await ctx.wait_signal("go")
    return [first, second]


def test_signals_are_consumed_in_order_and_journaled(harness: Harness) -> None:
    harness.register("signals", two_signals)
    run_id = harness.start("signals")
    harness.control.signal(run_id, "go", {"n": 1}, principal="bob")
    assert drive(harness, run_id).status is RunStatus.WAITING
    harness.control.signal(run_id, "go", {"n": 2}, principal="carol")
    run = drive(harness, run_id)
    assert run.result == [{"n": 1}, {"n": 2}]
    received = [
        r.data() for r in harness.journal.load_history(run_id) if r.type == "SignalReceived"
    ]
    assert [item["sent_by"] for item in received] == ["bob", "carol"]


async def saga(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    for title in ("first", "second"):
        created = await ctx.tool("create_ticket", {"title": title, "body": "saga"})
        assert isinstance(created, dict)
        ctx.compensate("close_ticket", {"ticket_id": str(created["ticket_id"])})
    if value == "fail":
        raise RuntimeError("deploy failed for jane@example.com")
    await ctx.wait_signal("never")
    return "unreachable"


def test_failures_run_compensations_in_reverse_order_and_redact_errors(harness: Harness) -> None:
    harness.register("saga", saga, frozenset({"create_ticket", "close_ticket"}))
    run_id = harness.start("saga", "fail")
    run = drive(harness, run_id)
    assert run.status is RunStatus.FAILED
    assert run.error is not None
    assert run.error["error_type"] == "RuntimeError"
    assert "jane@example.com" not in str(run.error)
    ops = [(entry.op, entry.args) for entry in harness.sinks.ledger(run_id)]
    assert [op for op, _ in ops] == ["create", "create", "close", "close"]
    assert '"T-2"' in ops[2][1]
    assert '"T-1"' in ops[3][1]
    assert asyncio.run(replay_check(harness.runtime(), run_id)).outcome == "failed"


def test_cancellation_is_delivered_at_the_next_command_and_compensates(harness: Harness) -> None:
    harness.register("saga", saga, frozenset({"create_ticket", "close_ticket"}))
    run_id = harness.start("saga", "wait")
    assert drive(harness, run_id).status is RunStatus.WAITING
    assert harness.control.cancel(run_id).cancel_requested
    run = drive(harness, run_id)
    assert run.status is RunStatus.CANCELLED
    assert [e.op for e in harness.sinks.ledger(run_id)] == ["create", "create", "close", "close"]
    history = harness.journal.load_history(run_id)
    cancel = next(r for r in history if r.type == EventType.CANCEL_REQUESTED)
    assert cancel.command == 2
    assert history[-1].type == "RunCancelled"
    assert asyncio.run(replay_check(harness.runtime(), run_id)).outcome == "failed"


async def version_one(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    fact = await ctx.tool("kb_lookup", {"key": "v1"})
    await ctx.wait_signal("go")
    return fact


async def version_two(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    fact = await ctx.tool("kb_lookup", {"key": "v2"})
    await ctx.wait_signal("go")
    return fact


def test_nondeterministic_code_blocks_the_run_until_an_operator_retries(harness: Harness) -> None:
    harness.register("versioned", version_one, frozenset({"kb_lookup"}))
    run_id = harness.start("versioned")
    assert drive(harness, run_id).status is RunStatus.WAITING
    harness.workflows = WorkflowRegistry(
        [WorkflowDefinition("versioned", version_two, frozenset({"kb_lookup"}))]
    )
    harness.control.signal(run_id, "go", None, principal="bob")
    blocked = drive(harness, run_id)
    assert blocked.status is RunStatus.BLOCKED
    assert blocked.blocked_reason is not None
    assert "nondeterminism" in blocked.blocked_reason
    metrics = harness.journal_metrics()
    assert metrics.nondeterminism_errors_total._value.get() == 1
    with pytest.raises(NonDeterminismError):
        asyncio.run(replay_check(harness.runtime(), run_id))
    harness.workflows = WorkflowRegistry(
        [WorkflowDefinition("versioned", version_one, frozenset({"kb_lookup"}))]
    )
    harness.journal.retry_blocked(run_id)
    assert drive(harness, run_id).status is RunStatus.COMPLETED


async def ticket_then_gate(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    created = await ctx.tool("create_ticket", {"title": "deploy", "body": "rollout"})
    assert isinstance(created, dict)
    ctx.compensate("close_ticket", {"ticket_id": str(created["ticket_id"])})
    await ctx.tool("kb_lookup", {"key": "cache"})
    await ctx.await_approval("ship", {"env": "prod"})
    return "shipped"


async def ticket_then_raise(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    created = await ctx.tool("create_ticket", {"title": "deploy", "body": "rollout"})
    assert isinstance(created, dict)
    ctx.compensate("close_ticket", {"ticket_id": str(created["ticket_id"])})
    raise ValueError("validation added in v2")


async def ticket_then_raise_uncompensated(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.tool("create_ticket", {"title": "deploy", "body": "rollout"})
    raise ValueError("validation added in v2")


@pytest.mark.parametrize("inbox", ["approval", "cancel"])
@pytest.mark.parametrize("new_code", [ticket_then_raise_uncompensated, ticket_then_raise])
def test_new_code_that_raises_before_the_head_blocks_the_run_instead_of_failing_it(
    harness: Harness, inbox: str, new_code: Any
) -> None:
    """Regression: an exception during replay used to fail the run and run its compensations."""
    tools = frozenset({"create_ticket", "close_ticket", "kb_lookup"})
    harness.register("deploy", ticket_then_gate, tools)
    run_id = harness.start("deploy")
    assert asyncio.run(harness.worker().run_once(run_id=run_id)) is not None
    harness.approve_pending(run_id)
    parked = asyncio.run(harness.worker().run_once(run_id=run_id))
    assert parked is not None
    assert parked.outcome == "waiting"
    if inbox == "approval":
        harness.approve_pending(run_id)
    else:
        harness.control.cancel(run_id)
    before = types_of(harness.journal, run_id)
    assert before[-1] == "ApprovalRequested"

    harness.workflows = WorkflowRegistry([WorkflowDefinition("deploy", new_code, tools)])
    outcome = asyncio.run(harness.worker().run_once(run_id=run_id))
    assert outcome is not None
    assert outcome.outcome == "blocked"
    run = harness.journal.get_run(run_id)
    assert run.status is RunStatus.BLOCKED
    assert run.blocked_reason is not None
    assert "nondeterminism" in run.blocked_reason
    assert types_of(harness.journal, run_id) == before
    assert [entry.op for entry in harness.sinks.ledger(run_id)] == ["create"]
    assert harness.journal_metrics().nondeterminism_errors_total._value.get() == 1
    with pytest.raises(NonDeterminismError):
        asyncio.run(replay_check(harness.runtime(), run_id))


async def ticket_then_fail(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    created = await ctx.tool("create_ticket", {"title": "deploy", "body": "rollout"})
    assert isinstance(created, dict)
    ctx.compensate("close_ticket", {"ticket_id": str(created["ticket_id"])})
    raise RuntimeError("rollout failed")


@pytest.mark.parametrize("crash_seq", [5, 6])
def test_a_crash_during_compensation_still_replays_the_recorded_rollback(
    harness: Harness, crash_seq: int
) -> None:
    """Recorded compensations after a replayed failure are history, not divergence."""
    harness.register("failing", ticket_then_fail, frozenset({"create_ticket", "close_ticket"}))
    run_id = harness.start("failing")
    assert asyncio.run(harness.worker().run_once(run_id=run_id)) is not None
    harness.approve_pending(run_id)
    injector = CrashInjector([CrashPoint("after_commit", crash_seq)])
    harness.journal.append_hooks.append(injector.append_hook)
    with pytest.raises(SimulatedCrash):
        asyncio.run(harness.worker().run_once(run_id=run_id))
    harness.journal.append_hooks.clear()
    compensation = harness.journal.load_history(run_id)[5]
    assert compensation.data()["kind"] == "compensation"
    harness.clock.advance(31)
    run = drive(harness, run_id)
    assert run.status is RunStatus.FAILED
    assert run.error is not None
    assert (run.error["error_type"], run.error["compensations_failed"]) == ("RuntimeError", 0)
    assert [entry.op for entry in harness.sinks.ledger(run_id)] == ["create", "close"]
    assert asyncio.run(replay_check(harness.runtime(), run_id)).outcome == "failed"


async def patched_version(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    key = "v2" if ctx.patched("new-key") else "v1"
    fact = await ctx.tool("kb_lookup", {"key": key})
    await ctx.wait_signal("go")
    return fact


def test_patched_code_keeps_old_histories_and_new_runs_take_the_new_branch(
    harness: Harness,
) -> None:
    harness.register("versioned", version_one, frozenset({"kb_lookup"}))
    old = harness.start("versioned", run_id="old")
    assert drive(harness, old).status is RunStatus.WAITING
    harness.workflows = WorkflowRegistry(
        [WorkflowDefinition("versioned", patched_version, frozenset({"kb_lookup"}))]
    )
    harness.control.signal(old, "go", None, principal="bob")
    assert drive(harness, old).result == {"key": "v1", "value": "fact about v1"}
    new = harness.start("versioned", run_id="new")
    harness.control.signal(new, "go", None, principal="bob")
    assert drive(harness, new).result == {"key": "v2", "value": "fact about v2"}
    assert "PatchMarker" in types_of(harness.journal, new)
    assert "PatchMarker" not in types_of(harness.journal, old)
    for run_id in (old, new):
        assert asyncio.run(replay_check(harness.runtime(), run_id)).outcome == "completed"


async def looping(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    for index in range(50):
        await ctx.tool("kb_lookup", {"key": f"k{index}"})
    return "done"


def test_the_history_cap_fails_the_run_loudly(harness: Harness, tmp_path: Any) -> None:
    small = Journal(tmp_path / "small.db", max_history_events=12, clock=harness.clock)
    harness.journal = small
    harness.control = type(harness.control)(
        small, harness.workflows, default_budget=harness.control.default_budget
    )
    harness.register("looping", looping, frozenset({"kb_lookup"}))
    run = drive(harness, harness.start("looping"))
    assert run.status is RunStatus.FAILED
    assert run.error is not None
    assert run.error["error_type"] == "HistoryLimitExceededError"
    assert small.get_run("run-1").head_seq == 12
    small.close()


def test_a_tampered_journal_is_never_replayed(harness: Harness) -> None:
    harness.register("versioned", version_one, frozenset({"kb_lookup"}))
    run_id = harness.start("versioned")
    assert drive(harness, run_id).status is RunStatus.WAITING
    engine = make_engine(harness.journal.path)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "UPDATE events SET payload = replace(payload, 'v1', 'v9') WHERE seq = 2"
        )
    engine.dispose()
    harness.control.signal(run_id, "go", None, principal="bob")
    run = drive(harness, run_id)
    assert run.status is RunStatus.BLOCKED
    assert run.blocked_reason is not None
    assert "integrity" in run.blocked_reason


async def returns_a_set(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    return {1, 2}  # type: ignore[return-value]


def test_unknown_workflows_block_and_non_json_results_fail(harness: Harness) -> None:
    harness.register("known", returns_a_set)
    harness.start("known", run_id="non-json")
    assert drive(harness, "non-json").error == {
        "error_type": "InvalidResult",
        "message": "workflow result is not JSON",
    }
    harness.workflows = WorkflowRegistry()
    harness.control.workflows.register(WorkflowDefinition("ghost", returns_a_set))
    harness.control.start_run(workflow="ghost", input_value=None, principal="a", run_id="ghost")
    run = drive(harness, "ghost")
    assert run.status is RunStatus.BLOCKED
    assert run.blocked_reason == "workflow ghost is not registered"


async def tolerant(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    try:
        await ctx.llm(LLMRequest(messages=(ChatMessage.user("risky"),)))
    except ProviderRefusalError as exc:
        outcome = f"refused:{exc.category}"
    await ctx.wait_signal("go")
    return outcome


def test_model_failures_are_journaled_and_re_raised_with_the_same_type_on_replay(
    harness: Harness,
) -> None:
    provider = CountingProvider(fail_first=ProviderRefusalError("cyber"))
    harness.provider = provider
    harness.register("tolerant", tolerant)
    run_id = harness.start("tolerant")
    assert drive(harness, run_id).status is RunStatus.WAITING
    harness.control.signal(run_id, "go", None, principal="bob")
    run = drive(harness, run_id)
    assert (run.status, run.result) == (RunStatus.COMPLETED, "refused:cyber")
    assert len(provider.requests) == 1
    failed = next(r for r in harness.journal.load_history(run_id) if r.type == "StepFailed")
    assert failed.data()["error_type"] == "ProviderRefusalError"


class BoomArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")


def boom(args: BoomArgs, ctx: ToolCallContext) -> JsonValue:
    raise RuntimeError("database password=hunter2 at 10.0.0.5")


async def uses_boom(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    try:
        await ctx.tool("boom", {})
    except ActivityFailedError as exc:
        return str(exc)
    return "unreachable"


def test_tool_failures_are_journaled_without_internal_details(harness: Harness) -> None:
    harness.tools.register(
        ToolDefinition("boom", "Explodes.", BoomArgs, frozenset({Capability.READ_DATA}), boom)
    )
    harness.register("uses_boom", uses_boom, frozenset({"boom"}))
    run = drive(harness, harness.start("uses_boom"))
    assert run.result == "tool boom failed (RuntimeError)"
    history_text = "".join(record.payload for record in harness.journal.load_history("run-1"))
    assert "hunter2" not in history_text
