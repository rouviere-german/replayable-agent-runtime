"""Zombie-worker scenario: a paused worker wakes up after its lease was reclaimed.

A worker executes an approved write and freezes inside the sink call (a stop-the-world pause, or
``docker pause``). Its lease expires, a second worker reclaims the run with a higher fencing token,
re-executes the in-flight step and finishes the run. Then the frozen worker resumes and delivers
its stale write.

- With fencing enforced, the sink refuses the stale token (recorded as a rejection) and the zombie
  stops with ``lease_lost``; every step is applied exactly once.
- The control disables fencing at the sinks. The notifier (no idempotency keys) then delivers the
  zombie's write a second time, which proves the scenario can fail; the ticket store still
  deduplicates by idempotency key. Either way the journal itself refuses the zombie's next append.
"""

from __future__ import annotations

import asyncio
import threading
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import JsonValue

from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.agentic.tools import ToolCallContext, ToolDefinition
from replayable_agent_runtime.runtime.builtin_tools import build_tool_registry
from replayable_agent_runtime.runtime.chaos import FakeClock, echo_policy
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.engine import ActivationOutcome, Runtime
from replayable_agent_runtime.runtime.journal import Journal, RunStatus
from replayable_agent_runtime.runtime.sinks import SinkStore
from replayable_agent_runtime.runtime.telemetry import create_runtime_metrics
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

__all__ = ["SinkName", "ZombieOutcome", "run_zombie_scenario"]

SinkName = Literal["notifier", "tickets"]
_TIMEOUT_SECONDS = 30.0
_TOOLS = frozenset({"paused_notify", "notify", "paused_create_ticket", "update_ticket"})


class _PauseGate:
    """Freezes the first call after it is armed until ``release`` is set."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._armed = False
        self.entered = threading.Event()
        self.release = threading.Event()

    def arm(self) -> None:
        with self._lock:
            self._armed = True

    def maybe_pause(self) -> None:
        with self._lock:
            armed, self._armed = self._armed, False
        if armed:
            self.entered.set()
            if not self.release.wait(_TIMEOUT_SECONDS):
                raise TimeoutError("the paused worker was never released")


def _paused(tool: ToolDefinition[Any], gate: _PauseGate) -> ToolDefinition[Any]:
    inner = tool.handler

    def handler(args: Any, ctx: ToolCallContext) -> JsonValue:
        gate.maybe_pause()
        return inner(args, ctx)

    return ToolDefinition(
        name=f"paused_{tool.name}",
        description=tool.description,
        args_model=tool.args_model,
        capabilities=tool.capabilities,
        handler=handler,
        effect=tool.effect,
    )


async def _zombie_workflow(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    if not isinstance(value, dict):
        raise TypeError("input must be an object")
    if value.get("sink") == "notifier":
        await ctx.tool("paused_notify", {"channel": "ops", "message": "deploy started"})
        await ctx.tool("notify", {"channel": "ops", "message": "deploy finished"})
        return "notified"
    created = await ctx.tool("paused_create_ticket", {"title": "deploy", "body": "rollout"})
    if not isinstance(created, dict):
        raise TypeError("create_ticket returned a non-object")
    await ctx.tool("update_ticket", {"ticket_id": str(created["ticket_id"]), "status": "resolved"})
    return "resolved"


@dataclass(frozen=True, slots=True)
class ZombieOutcome:
    sink: str
    fencing_enforced: bool
    zombie_outcome: str
    run_status: str
    stale_rejections: int
    applications_per_step: dict[int, int]
    duplicate_applications: int


def _approve_pending(control: ControlPlane, run_id: str) -> None:
    for request in control.approvals(run_id=run_id):
        if request.status.value == "pending":
            control.decide(
                request.approval_id, principal="bob", approve=True, call_hash=request.call_hash
            )


def run_zombie_scenario(
    workdir: Path, *, sink: SinkName, enforce_fencing: bool, lease_seconds: float = 30.0
) -> ZombieOutcome:
    workdir.mkdir(parents=True, exist_ok=True)
    clock = FakeClock(1_800_000_000.0)
    metrics = create_runtime_metrics()
    journal = Journal(workdir / "journal.db", clock=clock, metrics=metrics)
    sinks = SinkStore(workdir / "sinks.db", clock=clock)
    try:
        gate = _PauseGate()
        tools = build_tool_registry(
            workspace=workdir / "workspace",
            sinks=sinks,
            ticket_fencing=enforce_fencing,
            notifier_fencing=enforce_fencing,
        )
        tools.register(_paused(tools.get("notify"), gate))
        tools.register(_paused(tools.get("create_ticket"), gate))
        workflows = WorkflowRegistry([WorkflowDefinition("zombie", _zombie_workflow, _TOOLS)])
        control = ControlPlane(
            journal, workflows, default_budget=BudgetLimits.of(steps=100, tokens=10**6, usd=10.0)
        )
        run_id = f"zombie-{sink}"
        control.start_run(
            workflow="zombie", input_value={"sink": sink}, principal="alice", run_id=run_id
        )

        def worker(name: str) -> Worker:
            runtime = Runtime(
                journal=journal,
                workflows=workflows,
                tools=tools,
                provider=FakeProvider(echo_policy),
                clock=clock,
                metrics=metrics,
            )
            return Worker(
                runtime, worker_id=name, lease_seconds=lease_seconds, heartbeat_seconds=None
            )

        # 1. The first activation parks the run on the approval of the first write.
        asyncio.run(worker("setup").run_once(run_id=run_id))
        _approve_pending(control, run_id)

        # 2. The zombie executes the approved write and freezes inside the sink call.
        gate.arm()
        result: list[ActivationOutcome | BaseException] = []

        def zombie_main() -> None:
            try:
                outcome = asyncio.run(worker("zombie").run_once(run_id=run_id))
            except BaseException as exc:  # noqa: BLE001 - reported to the caller
                result.append(exc)
            else:
                if outcome is not None:
                    result.append(outcome)

        thread = threading.Thread(target=zombie_main, name="zombie-worker", daemon=True)
        thread.start()
        if not gate.entered.wait(_TIMEOUT_SECONDS):
            raise TimeoutError("the zombie never reached the sink call")

        # 3. Its lease expires; a new owner reclaims the run and finishes it.
        clock.advance(lease_seconds + 1)
        for _ in range(20):
            run = journal.get_run(run_id)
            if run.terminal:
                break
            if run.status is RunStatus.WAITING:
                _approve_pending(control, run_id)
                continue
            asyncio.run(worker("owner").run_once(run_id=run_id))

        # 4. The zombie wakes up and delivers its stale write.
        gate.release.set()
        thread.join(_TIMEOUT_SECONDS)
        zombie = result[0] if result else None
        zombie_outcome = zombie.outcome if isinstance(zombie, ActivationOutcome) else repr(zombie)
        counts = Counter(entry.seq for entry in sinks.ledger(run_id))
        return ZombieOutcome(
            sink=sink,
            fencing_enforced=enforce_fencing,
            zombie_outcome=zombie_outcome,
            run_status=journal.get_run(run_id).status.value,
            stale_rejections=len(sinks.rejections(run_id)),
            applications_per_step=dict(counts),
            duplicate_applications=sum(count - 1 for count in counts.values()),
        )
    finally:
        journal.close()
        sinks.close()
