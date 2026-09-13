"""The chaos harness itself must not introduce differences between crashed and uncrashed runs."""

from __future__ import annotations

from pathlib import Path

import pytest

from replayable_agent_runtime.runtime.chaos import ChaosEnvironment, CrashPoint, Op, Program
from replayable_agent_runtime.runtime.journal import RunStatus

pytestmark = pytest.mark.integration


def test_signal_deliveries_are_numbered_from_the_journal_across_processes(tmp_path: Path) -> None:
    """A recovering process must continue the outside world's signal sequence.

    Regression: the delivery counter used to live in process memory, so a run killed after its
    first signal and resumed by a fresh process received a second ``delivery: 0``.
    """
    program = Program(ops=(Op(kind="signal", arg=0), Op(kind="llm"), Op(kind="signal", arg=3)))
    first = ChaosEnvironment(tmp_path / "shared")
    first.start(program, "run")
    first.drive("run", max_activations=1)
    run = first.journal.get_run("run")
    assert run.status is RunStatus.WAITING
    assert first.resolve_wait(run)
    first.drive("run", max_activations=1)
    first.close()

    # A different process (a fresh environment on the same files) resolves the second signal.
    second = ChaosEnvironment(tmp_path / "shared", clock_start=1_800_000_000.0)
    try:
        second.drive("run")
        recovered = second.journal.get_run("run")
    finally:
        second.close()

    reference_env = ChaosEnvironment(tmp_path / "reference")
    try:
        reference = reference_env.run(program, "run")
    finally:
        reference_env.close()
    assert recovered.status is RunStatus.COMPLETED
    assert recovered.result == reference.result
    assert isinstance(recovered.result, dict)
    outputs = recovered.result["outputs"]
    assert isinstance(outputs, list)
    assert [outputs[0], outputs[2]] == [
        {"name": "sig0", "delivery": 0},
        {"name": "sig0", "delivery": 1},
    ]


def test_crash_points_that_never_fire_leave_the_run_unchanged(tmp_path: Path) -> None:
    program = Program(ops=(Op(kind="llm"), Op(kind="read")))
    env = ChaosEnvironment(tmp_path)
    try:
        reference = env.run(program, "reference")
        crashed = env.run(program, "crashed", [CrashPoint("after_commit", 999)])
    finally:
        env.close()
    assert crashed.crashes == 0
    assert crashed.fired == ()
    assert crashed.equivalent_to(reference)
