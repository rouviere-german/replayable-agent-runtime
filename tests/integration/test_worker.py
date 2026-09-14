from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from pathlib import Path

import pytest
from pydantic import JsonValue

from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.runtime.builtin_tools import build_tool_registry
from replayable_agent_runtime.runtime.chaos import echo_policy
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.engine import Runtime
from replayable_agent_runtime.runtime.events import EventRecord
from replayable_agent_runtime.runtime.journal import Journal
from replayable_agent_runtime.runtime.sinks import SinkStore
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

pytestmark = pytest.mark.integration


async def _two_clock_reads(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.now()
    await ctx.now()
    return value


def test_lease_renewal_keeps_going_while_journal_io_blocks_the_event_loop(tmp_path: Path) -> None:
    """Journal appends are synchronous calls on the worker's event loop. A slow append (or a
    burst of them) must not starve lease renewal, or a healthy worker would lose its run to a
    reclaim. The heartbeat therefore runs on its own thread; this blocks the loop for 0.3 s
    with a 20 ms heartbeat and expects the lease expiry to keep moving meanwhile."""
    journal = Journal(tmp_path / "journal.db")  # real clock: renewals must move the expiry
    sinks = SinkStore(tmp_path / "sinks.db")
    try:
        workflows = WorkflowRegistry([WorkflowDefinition("clocks", _two_clock_reads, frozenset())])
        ControlPlane(
            workflows=workflows,
            journal=journal,
            default_budget=BudgetLimits.of(steps=10, tokens=10, usd=1.0),
        ).start_run(workflow="clocks", input_value="v", principal="alice", run_id="r")
        runtime = Runtime(
            journal=journal,
            workflows=workflows,
            tools=build_tool_registry(workspace=tmp_path / "ws", sinks=sinks),
            provider=FakeProvider(echo_policy),
        )
        expiries: list[int] = []

        def slow_append(phase: str, records: Sequence[EventRecord]) -> None:
            del records
            if phase != "after_commit" or expiries:
                return
            # Stall the loop thread right after the first commit, as a slow append would.
            for _ in range(6):
                time.sleep(0.05)
                expiries.append(journal.get_run("r").lease_expires_at_us)

        journal.append_hooks.append(slow_append)
        worker = Worker(runtime, worker_id="w", lease_seconds=5.0, heartbeat_seconds=0.02)
        outcome = asyncio.run(worker.run_once(run_id="r"))
        assert outcome is not None
        assert outcome.outcome == "completed"
        assert len(expiries) == 6
        assert expiries[-1] > expiries[0], "the lease was not renewed while the loop was blocked"
        assert worker.health.last_lease_tick > 0
        # The run was released cleanly: no owner and no lease left behind.
        run = journal.get_run("r")
        assert (run.owner, run.lease_expires_at_us) == (None, 0)
    finally:
        journal.close()
        sinks.close()
