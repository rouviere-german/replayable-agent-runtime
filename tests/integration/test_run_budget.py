"""Budgets are persisted in the journal: crashes, resumes and looser runtimes cannot reset them."""

from __future__ import annotations

import asyncio
import random

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from pydantic import JsonValue

from conftest import Harness
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.base import (
    ChatMessage,
    LLMRequest,
    LLMResponse,
    Usage,
)
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.runtime.chaos import ChaosEnvironment, CrashPoint, Op, Program
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.journal import RunStatus

pytestmark = pytest.mark.integration


async def spender(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    assert isinstance(value, int)
    for index in range(value):
        await ctx.llm(LLMRequest(messages=(ChatMessage.user(f"step {index}"),)))
    return "done"


def test_the_step_budget_stops_the_run_and_the_rejection_is_journaled(harness: Harness) -> None:
    calls: list[str] = []

    def policy(request: LLMRequest, rng: random.Random, turn: int) -> LLMResponse:
        calls.append(request.sha256())
        return LLMResponse(model="fake", stop_reason="end_turn", text="ok")

    harness.provider = FakeProvider(policy)
    harness.register("spender", spender)
    harness.start("spender", 5, budget=BudgetLimits.of(steps=3, tokens=10**6, usd=1.0))
    asyncio.run(harness.worker().run_until_idle())
    run = harness.journal.get_run("run-1")
    assert run.status is RunStatus.FAILED
    assert run.error is not None
    assert run.error["error_type"] == "BudgetExceededError"
    assert len(calls) == 3
    rejected = [
        r.data() for r in harness.journal.load_history("run-1") if r.type == "CommandRejected"
    ]
    assert rejected[0]["details"] == {"dimension": "steps", "limit": 3, "attempted": 4}


def test_a_looser_runtime_cap_cannot_raise_recorded_limits_but_a_tighter_one_applies(
    harness: Harness,
) -> None:
    harness.register("spender", spender)
    harness.budget_cap = BudgetLimits.of(steps=1_000, tokens=10**9, usd=1_000.0)
    harness.start(
        "spender", 5, run_id="loose", budget=BudgetLimits.of(steps=2, tokens=10**6, usd=1)
    )
    harness.budget_cap = BudgetLimits.of(steps=1, tokens=10**9, usd=1_000.0)
    harness.start(
        "spender", 5, run_id="tight", budget=BudgetLimits.of(steps=4, tokens=10**6, usd=1)
    )
    harness.budget_cap = BudgetLimits.of(steps=1_000, tokens=10**9, usd=1_000.0)
    asyncio.run(harness.worker().run_once(run_id="loose"))
    harness.budget_cap = BudgetLimits.of(steps=1, tokens=10**9, usd=1_000.0)
    asyncio.run(harness.worker().run_once(run_id="tight"))
    for run_id, steps in (("loose", 2), ("tight", 1)):
        history = harness.journal.load_history(run_id)
        assert [r.type for r in history].count("StepScheduled") == steps
        assert history[-1].type == "RunFailed"


def test_token_and_usd_budgets_are_charged_from_recorded_usage(harness: Harness) -> None:
    def expensive(request: LLMRequest, rng: random.Random, turn: int) -> LLMResponse:
        return LLMResponse(
            model="claude-opus-5",
            stop_reason="end_turn",
            text="costly",
            usage=Usage(input_tokens=100_000, output_tokens=10_000),
        )

    harness.provider = FakeProvider(expensive)
    harness.register("spender", spender)
    harness.start(
        "spender", 3, run_id="usd", budget=BudgetLimits.of(steps=10, tokens=10**9, usd=0.5)
    )
    harness.start(
        "spender", 3, run_id="tokens", budget=BudgetLimits.of(steps=10, tokens=50_000, usd=9)
    )
    asyncio.run(harness.worker().run_until_idle())
    usd = [r.data() for r in harness.journal.load_history("usd") if r.type == "CommandRejected"]
    assert usd[0]["details"] == {"dimension": "usd_micros", "limit": 500_000, "attempted": 750_000}
    tokens = [
        r.data() for r in harness.journal.load_history("tokens") if r.type == "CommandRejected"
    ]
    assert tokens[0]["details"]["dimension"] == "tokens"
    completed = [r.data() for r in harness.journal.load_history("usd") if r.type == "StepCompleted"]
    assert completed[0]["cost_usd_micros"] == 750_000


@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    max_steps=st.integers(0, 9),
    crash_seqs=st.lists(st.integers(1, 30), min_size=0, max_size=4, unique=True),
    effect_crashes=st.lists(st.integers(0, 9), min_size=0, max_size=2, unique=True),
)
def test_budgets_are_never_reset_by_crashes_resumes_or_replays(
    tmp_path_factory: pytest.TempPathFactory,
    max_steps: int,
    crash_seqs: list[int],
    effect_crashes: list[int],
) -> None:
    program = Program(ops=tuple(Op(kind="llm", arg=index) for index in range(8)))
    budget = BudgetLimits.of(steps=max_steps, tokens=10**9, usd=1_000.0)
    points = [CrashPoint("after_commit", seq) for seq in crash_seqs]
    points += [CrashPoint("after_effect", ordinal) for ordinal in effect_crashes]
    env = ChaosEnvironment(tmp_path_factory.mktemp("budget"))
    try:
        reference = env.run(program, "reference", budget=budget)
        crashed = env.run(program, "crashed", points, budget=budget)
        history = env.journal.load_history("crashed")
    finally:
        env.close()
    scheduled = sum(1 for record in history if record.type == "StepScheduled")
    distinct_paid_steps = crashed.provider_calls - sum(
        count - 1 for count in crashed.duplicate_provider_seqs.values()
    )
    assert scheduled == min(8, max_steps)
    assert distinct_paid_steps == scheduled <= max_steps
    assert (crashed.status, crashed.error_type) == (reference.status, reference.error_type)
    if max_steps < 8:
        assert crashed.error_type == "BudgetExceededError"
