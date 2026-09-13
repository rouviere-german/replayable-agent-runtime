"""Crash equivalence: recovery by replay reaches the same result and external effects as a run
that never crashed. ``RAR_CRASH_EXAMPLES`` sets the example count (CI: 500)."""

from __future__ import annotations

import os

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from replayable_agent_runtime.runtime.chaos import (
    OP_KINDS,
    ChaosEnvironment,
    CrashPoint,
    Op,
    Program,
)

pytestmark = pytest.mark.integration

EXAMPLES = int(os.environ.get("RAR_CRASH_EXAMPLES", "60"))

programs = st.builds(
    Program,
    ops=st.lists(
        st.builds(Op, kind=st.sampled_from(OP_KINDS), arg=st.integers(0, 99)),
        min_size=1,
        max_size=8,
    ).map(tuple),
    compensate=st.booleans(),
    fail_at_end=st.booleans(),
)
any_crash_points = st.lists(
    st.builds(
        CrashPoint,
        kind=st.sampled_from(("before_commit", "after_commit", "after_effect")),
        at=st.integers(0, 45),
    ),
    min_size=1,
    max_size=3,
    unique=True,
)
commit_crash_points = st.lists(
    st.builds(CrashPoint, kind=st.just("after_commit"), at=st.integers(1, 45)),
    min_size=1,
    max_size=3,
    unique=True,
)


@settings(max_examples=EXAMPLES, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(program=programs, points=any_crash_points)
def test_recovery_after_random_crashes_is_equivalent_to_an_uncrashed_run(
    tmp_path_factory: pytest.TempPathFactory, program: Program, points: list[CrashPoint]
) -> None:
    env = ChaosEnvironment(tmp_path_factory.mktemp("crash"))
    try:
        reference = env.run(program, "reference")
        crashed = env.run(program, "crashed", points)
    finally:
        env.close()
    assert reference.status in {"completed", "failed"}
    assert reference.crashes == 0
    assert reference.duplicate_provider_seqs == {}
    assert reference.journal_ok
    assert crashed.journal_ok
    assert crashed.observations_stable
    if crashed.window_step_seqs:
        # Crashed between an external call and its commit: that step ran again (at-least-once).
        assert crashed.at_least_once_equivalent_to(reference)
        assert set(crashed.duplicate_provider_seqs) <= crashed.window_step_seqs
    else:
        assert crashed.equivalent_to(reference)
        assert crashed.duplicate_provider_seqs == {}


@settings(max_examples=EXAMPLES, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(program=programs, points=commit_crash_points)
def test_crashes_right_after_a_commit_never_pay_for_a_call_twice(
    tmp_path_factory: pytest.TempPathFactory, program: Program, points: list[CrashPoint]
) -> None:
    env = ChaosEnvironment(tmp_path_factory.mktemp("commit"))
    try:
        reference = env.run(program, "reference")
        crashed = env.run(program, "crashed", points)
    finally:
        env.close()
    assert crashed.window_step_seqs == frozenset()
    assert crashed.equivalent_to(reference)
    assert crashed.duplicate_provider_seqs == {}
    assert crashed.provider_calls == reference.provider_calls


def test_a_crash_plan_with_every_kind_fires_and_still_converges(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    program = Program(
        ops=(
            Op(kind="llm"),
            Op(kind="create_ticket"),
            Op(kind="notify"),
            Op(kind="update_ticket", arg=3),
            Op(kind="llm", arg=1),
        ),
        compensate=True,
        fail_at_end=True,
    )
    points = [
        CrashPoint("after_commit", 3),
        CrashPoint("before_commit", 8),
        CrashPoint("after_effect", 2),
    ]
    env = ChaosEnvironment(tmp_path_factory.mktemp("all-kinds"))
    try:
        reference = env.run(program, "reference")
        crashed = env.run(program, "crashed", points)
    finally:
        env.close()
    assert crashed.crashes == 3
    assert set(crashed.fired) == set(points)
    assert reference.status == "failed"
    assert reference.error_type == "RuntimeError"
    assert crashed.at_least_once_equivalent_to(reference)
    assert [entry.op for entry in reference.sink_ledger("tickets")] == ["create", "update", "close"]
