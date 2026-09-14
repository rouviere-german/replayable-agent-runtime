"""Zombie-worker scenario: a paused worker wakes up after its lease was reclaimed.

A worker executes an approved write and freezes inside the sink call (a stop-the-world pause, or
``docker pause``). Its lease expires, a second worker reclaims the run with a higher fencing token,
re-executes the in-flight step and finishes the run. The frozen worker resumes and delivers its
stale write. Three orderings of that wake-up are covered (``order``):

- ``owner_writes_first``: the new owner has finished the run before the zombie wakes up;
- ``zombie_wakes_before_owner_writes``: the owner has claimed the run and announced its token but
  not yet re-executed the in-flight step, so no sink has seen a write with the new token;
- ``zombie_wakes_right_after_the_claim``: the zombie writes between the claim commit and the
  owner's announcement, the one window sink fencing cannot cover (docs/SEMANTICS.md).

With fencing enforced, the first two orderings reject the stale write (recorded as a rejection)
and the zombie stops with ``lease_lost``; every step is applied exactly once. In the third, the
write lands before any sink knows the new token, so that step is at least once: the notifier
delivers it twice, while the ticket store deduplicates by idempotency key. The control disables
fencing at the sinks: the notifier then delivers the zombie's write a second time in every
ordering, which proves the scenario can fail. Either way the journal itself refuses the zombie's
next append.
"""

from __future__ import annotations

import asyncio
import threading
from collections import Counter
from collections.abc import Callable
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
from replayable_agent_runtime.runtime.engine import ActivationOutcome, Hooks, Runtime
from replayable_agent_runtime.runtime.journal import Journal, RunStatus
from replayable_agent_runtime.runtime.sinks import SinkStore
from replayable_agent_runtime.runtime.telemetry import create_runtime_metrics
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

__all__ = ["ORDERS", "SinkName", "ZombieOrder", "ZombieOutcome", "run_zombie_scenario"]

SinkName = Literal["notifier", "tickets"]
ZombieOrder = Literal[
    "owner_writes_first",
    "zombie_wakes_before_owner_writes",
    "zombie_wakes_right_after_the_claim",
]
ORDERS: tuple[ZombieOrder, ...] = (
    "owner_writes_first",
    "zombie_wakes_before_owner_writes",
    "zombie_wakes_right_after_the_claim",
)
_TIMEOUT_SECONDS = 30.0
_TOOLS = frozenset({"paused_notify", "notify", "paused_create_ticket", "update_ticket"})


class _PauseGate:
    """Runs one queued action per entry into the paused tool: freeze the caller until released,
    or wake the frozen zombie while the caller (the new owner) is about to write."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._actions: list[Callable[[], None]] = []
        self.entered = threading.Event()
        self.release = threading.Event()

    def arm(self, action: Callable[[], None]) -> None:
        with self._lock:
            self._actions.append(action)

    def on_entry(self) -> None:
        with self._lock:
            action = self._actions.pop(0) if self._actions else None
        if action is not None:
            action()

    def freeze(self) -> None:
        self.entered.set()
        if not self.release.wait(_TIMEOUT_SECONDS):
            raise TimeoutError("the paused worker was never released")


class _ClaimWindowHooks(Hooks):
    """Wakes the frozen zombie inside the claim window (after the claim, before the announcement)
    of the first activation that starts after the zombie froze."""

    def __init__(self, gate: _PauseGate, wake: Callable[[], None]) -> None:
        self._gate = gate
        self._wake = wake
        self._fired = False

    def after_claim(self, run_id: str, fencing: int) -> None:
        del run_id, fencing
        if self._gate.entered.is_set() and not self._fired:
            self._fired = True
            self._wake()


def _paused(tool: ToolDefinition[Any], gate: _PauseGate) -> ToolDefinition[Any]:
    inner = tool.handler

    def handler(args: Any, ctx: ToolCallContext) -> JsonValue:
        gate.on_entry()
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
    order: str
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
    workdir: Path,
    *,
    sink: SinkName,
    enforce_fencing: bool,
    lease_seconds: float = 30.0,
    order: ZombieOrder = "owner_writes_first",
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
        thread: threading.Thread | None = None

        def wake_zombie() -> None:
            gate.release.set()
            if thread is not None:
                thread.join(_TIMEOUT_SECONDS)
                if thread.is_alive():
                    raise TimeoutError("the released zombie did not finish")

        hooks = (
            _ClaimWindowHooks(gate, wake_zombie)
            if order == "zombie_wakes_right_after_the_claim"
            else Hooks()
        )

        def worker(name: str) -> Worker:
            runtime = Runtime(
                journal=journal,
                workflows=workflows,
                tools=tools,
                provider=FakeProvider(echo_policy),
                clock=clock,
                metrics=metrics,
                hooks=hooks,
                fences=(sinks,),
            )
            return Worker(
                runtime, worker_id=name, lease_seconds=lease_seconds, heartbeat_seconds=None
            )

        # 1. The first activation parks the run on the approval of the first write.
        asyncio.run(worker("setup").run_once(run_id=run_id))
        _approve_pending(control, run_id)

        # 2. The zombie executes the approved write and freezes inside the sink call.
        gate.arm(gate.freeze)
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

        # 3. Its lease expires; a new owner reclaims the run and finishes it. Depending on the
        #    ordering the zombie wakes up inside the owner's claim window (hooks), right before
        #    the owner's own write (the armed gate), or only after the run is finished (step 4).
        clock.advance(lease_seconds + 1)
        if order == "zombie_wakes_before_owner_writes":
            gate.arm(wake_zombie)
        for _ in range(20):
            run = journal.get_run(run_id)
            if run.terminal:
                break
            if run.status is RunStatus.WAITING:
                _approve_pending(control, run_id)
                continue
            asyncio.run(worker("owner").run_once(run_id=run_id))

        # 4. The zombie wakes up (if it has not already) and delivers its stale write.
        wake_zombie()
        zombie = result[0] if result else None
        zombie_outcome = zombie.outcome if isinstance(zombie, ActivationOutcome) else repr(zombie)
        counts = Counter(entry.seq for entry in sinks.ledger(run_id))
        return ZombieOutcome(
            sink=sink,
            fencing_enforced=enforce_fencing,
            order=order,
            zombie_outcome=zombie_outcome,
            run_status=journal.get_run(run_id).status.value,
            stale_rejections=len(sinks.rejections(run_id)),
            applications_per_step=dict(counts),
            duplicate_applications=sum(count - 1 for count in counts.values()),
        )
    finally:
        journal.close()
        sinks.close()
