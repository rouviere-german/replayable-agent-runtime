from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from pydantic import JsonValue

from conftest import Harness
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.zombie import run_zombie_scenario

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("sink", ["notifier", "tickets"])
def test_a_paused_zombie_worker_cannot_apply_a_stale_write(tmp_path: Path, sink: str) -> None:
    outcome = run_zombie_scenario(tmp_path, sink=sink, enforce_fencing=True)  # type: ignore[arg-type]
    assert outcome.run_status == "completed"
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.stale_rejections == 1
    assert outcome.duplicate_applications == 0
    assert sorted(outcome.applications_per_step.values()) == [1, 1]


def test_the_control_without_fencing_shows_the_zombie_duplicating_a_notification(
    tmp_path: Path,
) -> None:
    outcome = run_zombie_scenario(tmp_path, sink="notifier", enforce_fencing=False)
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == 1
    assert sorted(outcome.applications_per_step.values()) == [1, 2]
    # The journal still refuses the zombie's next append.
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.run_status == "completed"


def test_without_fencing_idempotency_keys_still_deduplicate_ticket_writes(tmp_path: Path) -> None:
    outcome = run_zombie_scenario(tmp_path, sink="tickets", enforce_fencing=False)
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == 0
    assert outcome.zombie_outcome == "lease_lost"


@pytest.mark.parametrize("sink", ["notifier", "tickets"])
def test_a_zombie_that_wakes_before_the_new_owner_writes_is_still_rejected(
    tmp_path: Path, sink: str
) -> None:
    """Regression: sinks only knew tokens they had been written with, so a zombie that woke after
    the reclaim but before the new owner re-executed the step applied its stale write (and the
    notifier delivered twice). The new owner now announces its token before replaying."""
    outcome = run_zombie_scenario(
        tmp_path,
        sink=sink,  # type: ignore[arg-type]
        enforce_fencing=True,
        order="zombie_wakes_before_owner_writes",
    )
    assert outcome.run_status == "completed"
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.stale_rejections == 1
    assert outcome.duplicate_applications == 0
    assert sorted(outcome.applications_per_step.values()) == [1, 1]


def test_without_fencing_a_zombie_waking_before_the_owner_writes_duplicates_it(
    tmp_path: Path,
) -> None:
    outcome = run_zombie_scenario(
        tmp_path,
        sink="notifier",
        enforce_fencing=False,
        order="zombie_wakes_before_owner_writes",
    )
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == 1
    assert outcome.zombie_outcome == "lease_lost"


@pytest.mark.parametrize(("sink", "duplicates"), [("notifier", 1), ("tickets", 0)])
def test_a_zombie_that_writes_before_the_new_owner_announces_its_token_is_at_least_once(
    tmp_path: Path, sink: str, duplicates: int
) -> None:
    """The window docs/SEMANTICS.md states: between the claim commit and the owner's announcement
    no sink can know the new token. A write completed there is like a crash inside the effect
    window: the step re-executes, so the non-idempotent notifier delivers twice and the idempotent
    ticket store deduplicates by key. The journal still fences the zombie out."""
    outcome = run_zombie_scenario(
        tmp_path,
        sink=sink,  # type: ignore[arg-type]
        enforce_fencing=True,
        order="zombie_wakes_right_after_the_claim",
    )
    assert outcome.run_status == "completed"
    assert outcome.zombie_outcome == "lease_lost"
    assert outcome.stale_rejections == 0
    assert outcome.duplicate_applications == duplicates


def test_every_activation_announces_its_fencing_token_to_the_sinks_before_replaying(
    harness: Harness,
) -> None:
    """The announcement is what closes the window between a reclaim and the new owner's first
    write: from the moment an activation starts, every fencing-aware sink refuses older tokens."""
    seen: list[tuple[str, int, str]] = []

    class RecordingFence:
        def announce(self, run_id: str, token: int) -> None:
            seen.append((run_id, token, "announce"))

    async def workflow(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
        seen.append((ctx.run_id, -1, "workflow"))
        await ctx.tool("kb_lookup", {"key": "k"})
        await ctx.wait_signal("go")
        return value

    harness.register("announce", workflow, frozenset({"kb_lookup"}))
    run_id = harness.start("announce", "v")
    runtime = harness.runtime()
    runtime.fences = (RecordingFence(), harness.sinks)
    worker = Worker(runtime, worker_id="a", heartbeat_seconds=None)
    first = asyncio.run(worker.run_once(run_id=run_id))
    assert first is not None
    assert first.outcome == "waiting"
    assert seen[:2] == [(run_id, 1, "announce"), (run_id, -1, "workflow")]
    assert harness.sinks.fence("tickets", run_id) == 1
    assert harness.sinks.fence("notifier", run_id) == 1

    harness.control.signal(run_id, "go", None, principal="alice")
    second = asyncio.run(
        Worker(runtime, worker_id="b", heartbeat_seconds=None).run_once(run_id=run_id)
    )
    assert second is not None
    assert second.outcome == "completed"
    announced = [item for item in seen if item[2] == "announce"]
    assert announced == [(run_id, 1, "announce"), (run_id, 2, "announce")]
    assert harness.sinks.fence("notifier", run_id) == 2
