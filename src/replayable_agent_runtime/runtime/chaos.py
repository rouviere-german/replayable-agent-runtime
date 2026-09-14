"""Crash-injection harness: random workflow programs, crash plans and recovery drivers.

Used by the property tests, ``bench/eval_suites.py`` and ``bench/chaos_sigkill.py``.

A *program* is a list of operations interpreted by :func:`program_workflow`; later operations take
arguments derived from earlier results, so a replay that returned a different value would change
later fingerprints and be caught. A *crash point* abandons an activation:

- ``after_commit`` at seq N: right after the append containing event N commits;
- ``before_commit`` at seq N: inside that append's transaction, which is rolled back;
- ``after_effect`` at ordinal N: after the N-th external call (tool handler or provider call)
  returns and before its completion is journaled.

Crashes are :class:`SimulatedCrash` (a ``BaseException``) in-process; ``bench/chaos_sigkill.py``
instead pauses a real subprocess at the same points and kills it with SIGKILL
(``TerminateProcess`` on Windows).
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from replayable_agent_runtime.agentic.audit import verify_chain
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest, LLMResponse
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.core.canonical import canonical_json, sha256_hex
from replayable_agent_runtime.runtime.builtin_tools import build_tool_registry
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.engine import Hooks, Runtime
from replayable_agent_runtime.runtime.events import CommandKind, EventRecord, EventType
from replayable_agent_runtime.runtime.journal import Journal, RunRow, RunStatus
from replayable_agent_runtime.runtime.metering import MeteredProvider
from replayable_agent_runtime.runtime.sinks import LedgerEntry, ProviderMeter, SinkStore
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

__all__ = [
    "OP_KINDS",
    "ChaosEnvironment",
    "CrashInjector",
    "CrashPoint",
    "FakeClock",
    "Op",
    "Program",
    "ProgramOutcome",
    "SimulatedCrash",
    "echo_policy",
    "program_definition",
    "program_workflow",
    "random_crash_points",
    "random_program",
]

OpKind = Literal[
    "llm",
    "read",
    "create_ticket",
    "update_ticket",
    "notify",
    "now",
    "uuid",
    "sleep",
    "signal",
    "gate",
    "patch",
]
OP_KINDS: tuple[OpKind, ...] = (
    "llm",
    "read",
    "create_ticket",
    "update_ticket",
    "notify",
    "now",
    "uuid",
    "sleep",
    "signal",
    "gate",
    "patch",
)
CrashKind = Literal["before_commit", "after_commit", "after_effect"]
PROGRAM_TOOLS = frozenset({"kb_lookup", "create_ticket", "update_ticket", "notify", "close_ticket"})
_EFFECT_KINDS = frozenset(
    {CommandKind.LLM.value, CommandKind.TOOL.value, CommandKind.COMPENSATION.value}
)


class SimulatedCrash(BaseException):
    """Abandons an activation mid-flight: no cleanup, no lease release."""


class FakeClock:
    def __init__(self, start: float) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds

    def advance_to(self, value: float) -> None:
        self.value = max(self.value, value)


class Op(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: OpKind
    arg: int = Field(default=0, ge=0, le=99)


class Program(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ops: tuple[Op, ...] = Field(max_length=40)
    compensate: bool = False
    fail_at_end: bool = False


# Values of now()/uuid() seen by every activation of a run, to check they are stable under replay.
_OBSERVATIONS: dict[str, dict[int, set[str]]] = {}


def _observe(run_id: str, index: int, value: str) -> None:
    _OBSERVATIONS.setdefault(run_id, {}).setdefault(index, set()).add(value)


async def _run_op(
    ctx: WorkflowContext, program: Program, index: int, op: Op, tag: str, tickets: list[str]
) -> JsonValue:
    if op.kind == "llm":
        response = await ctx.llm(
            LLMRequest(messages=(ChatMessage.user(f"op {tag}"),), max_tokens=64)
        )
        return response.text
    if op.kind == "read":
        return await ctx.tool("kb_lookup", {"key": f"k{op.arg}"})
    if op.kind == "create_ticket":
        created = await ctx.tool("create_ticket", {"title": f"t {tag}", "body": "chaos"})
        if not isinstance(created, dict):
            raise TypeError("create_ticket returned a non-object")
        ticket_id = str(created["ticket_id"])
        tickets.append(ticket_id)
        if program.compensate:
            ctx.compensate("close_ticket", {"ticket_id": ticket_id})
        return {"ticket_id": ticket_id}
    if op.kind == "update_ticket":
        if not tickets:
            return "no-ticket"
        status = "status_" + chr(ord("a") + op.arg % 26)
        return await ctx.tool("update_ticket", {"ticket_id": tickets[-1], "status": status})
    if op.kind == "notify":
        return await ctx.tool("notify", {"channel": "chaos", "message": f"n {tag}"})
    if op.kind == "now":
        _observe(ctx.run_id, index, repr(await ctx.now()))
        return "now"
    if op.kind == "uuid":
        _observe(ctx.run_id, index, await ctx.uuid())
        return "uuid"
    if op.kind == "sleep":
        await ctx.sleep(op.arg / 10)
        return "slept"
    if op.kind == "signal":
        return await ctx.wait_signal(f"sig{op.arg % 3}")
    if op.kind == "gate":
        return await ctx.await_approval("gate", {"op": index})
    return "patched" if ctx.patched(f"p{op.arg % 4}") else "unpatched"


async def program_workflow(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    """Interpret a :class:`Program`. Arguments depend on a rolling digest of earlier results."""
    if not isinstance(value, dict):
        raise TypeError("program input must be an object")
    program = Program.model_validate(value["program"])
    outputs: list[JsonValue] = []
    tickets: list[str] = []
    digest = ""
    for index, op in enumerate(program.ops):
        tag = f"{index}-{op.arg}-{digest[:8]}"
        output = await _run_op(ctx, program, index, op, tag, tickets)
        outputs.append(output)
        digest = sha256_hex(digest + canonical_json(output))
    if program.fail_at_end:
        raise RuntimeError("program failed on purpose")
    return {"outputs": outputs, "digest": digest}


def program_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="program", fn=program_workflow, tools=PROGRAM_TOOLS, description="chaos interpreter"
    )


def echo_policy(request: LLMRequest, rng: random.Random, turn: int) -> LLMResponse:
    """A stateless fake model: the answer is a function of the request only."""
    del rng, turn
    return LLMResponse(
        model="fake-echo", stop_reason="end_turn", text=f"echo:{request.sha256()[:12]}"
    )


@dataclass(frozen=True, slots=True)
class CrashPoint:
    kind: CrashKind
    at: int


class CrashInjector(Hooks):
    """Fires each crash point once. ``action`` defaults to raising :class:`SimulatedCrash`."""

    def __init__(
        self, points: Sequence[CrashPoint], action: Callable[[CrashPoint], None] | None = None
    ) -> None:
        self.pending = list(points)
        self.fired: list[CrashPoint] = []
        self.effects_seen = 0
        # Crashes that interrupted a step between its external call and its completion commit.
        self.window_seqs: list[int] = []
        self.window_commands: list[tuple[str, int]] = []
        self._action = action

    def _fire(self, point: CrashPoint) -> None:
        self.pending.remove(point)
        self.fired.append(point)
        if self._action is not None:
            self._action(point)
            return
        raise SimulatedCrash(f"{point.kind}@{point.at}")

    def append_hook(self, phase: str, records: Sequence[EventRecord]) -> None:
        for point in list(self.pending):
            if point.kind == phase and any(record.seq == point.at for record in records):
                if phase == "before_commit":
                    for record in records:
                        if (
                            record.type in (EventType.STEP_COMPLETED, EventType.STEP_FAILED)
                            and record.data().get("kind") in _EFFECT_KINDS
                        ):
                            self.window_commands.append((record.run_id, record.command))
                self._fire(point)

    def after_effect(self, kind: str, key: str) -> None:
        ordinal = self.effects_seen
        self.effects_seen += 1
        for point in list(self.pending):
            if point.kind == "after_effect" and point.at == ordinal:
                self.window_seqs.append(int(key.rsplit(":", 1)[1]))
                self._fire(point)


@dataclass(frozen=True, slots=True)
class ProgramOutcome:
    run_id: str
    status: str
    result: JsonValue
    error_type: str | None
    ledger: tuple[LedgerEntry, ...]
    provider_calls: int
    duplicate_provider_seqs: dict[int, int]
    activations: int
    crashes: int
    fired: tuple[CrashPoint, ...]
    window_step_seqs: frozenset[int]
    observations_stable: bool
    journal_ok: bool
    history_length: int

    def sink_ledger(self, sink: str) -> tuple[LedgerEntry, ...]:
        return tuple(entry for entry in self.ledger if entry.sink == sink)

    def equivalent_to(self, other: ProgramOutcome) -> bool:
        """Same status, result, error type and exact external-effect ledger."""
        return (
            self.status == other.status
            and self.result == other.result
            and self.error_type == other.error_type
            and self.ledger == other.ledger
        )

    def at_least_once_equivalent_to(self, other: ProgramOutcome) -> bool:
        """Equal except for notifier re-deliveries of steps interrupted inside their window."""
        if (self.status, self.result, self.error_type) != (
            other.status,
            other.result,
            other.error_type,
        ):
            return False
        if self.sink_ledger("tickets") != other.sink_ledger("tickets"):
            return False
        seen: set[int] = set()
        unique: list[LedgerEntry] = []
        for entry in self.sink_ledger("notifier"):
            if entry.seq in seen:
                if entry.seq not in self.window_step_seqs:
                    return False
                continue
            seen.add(entry.seq)
            unique.append(entry)
        return tuple(unique) == other.sink_ledger("notifier")


@dataclass
class ChaosEnvironment:
    """A journal, sinks and control plane on a fake clock, driven by crashing workers."""

    workdir: Path
    clock_start: float = 1_700_000_000.0
    lease_seconds: float = 30.0
    clock: FakeClock = field(init=False)
    journal: Journal = field(init=False)
    sinks: SinkStore = field(init=False)
    meter: ProviderMeter = field(init=False)
    control: ControlPlane = field(init=False)

    def __post_init__(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.clock = FakeClock(self.clock_start)
        self.journal = Journal(self.workdir / "journal.db", clock=self.clock)
        try:
            self.sinks = SinkStore(self.workdir / "sinks.db", clock=self.clock)
        except Exception:
            # Do not leak open handles on the journal (Windows then refuses to delete it).
            self.journal.close()
            raise
        self.meter = ProviderMeter(self.sinks)
        self.tools = build_tool_registry(workspace=self.workdir / "workspace", sinks=self.sinks)
        self.workflows = WorkflowRegistry([program_definition()])
        self.control = ControlPlane(
            self.journal,
            self.workflows,
            default_budget=BudgetLimits.of(steps=100_000, tokens=10**12, usd=1_000_000.0),
        )

    def runtime(self, hooks: Hooks) -> Runtime:
        provider = MeteredProvider(FakeProvider(echo_policy), self.meter)
        return Runtime(
            journal=self.journal,
            workflows=self.workflows,
            tools=self.tools,
            provider=provider,
            clock=self.clock,
            hooks=hooks,
            fences=(self.sinks,),
        )

    def close(self) -> None:
        self.journal.close()
        self.sinks.close()

    def start(self, program: Program, run_id: str, budget: BudgetLimits | None = None) -> RunRow:
        _OBSERVATIONS.pop(run_id, None)
        run, _ = self.control.start_run(
            workflow="program",
            input_value={"program": program.model_dump(mode="json")},
            principal="chaos-starter",
            run_id=run_id,
            budget=budget,
        )
        return run

    def resolve_wait(self, run: RunRow) -> bool:
        """Play the outside world for a waiting run: fire its timer, signal or approval."""
        waiting = run.waiting_on or {}
        kind = waiting.get("type")
        if kind == "timer":
            self.clock.advance_to(int(waiting["fire_at_us"]) / 1_000_000 + 0.001)
            self.journal.wake_due()
            return True
        if kind == "signal":
            # Numbered from the journal, not process memory, so a recovering process continues
            # the sequence a killed one started (the outside world does not restart with a worker).
            delivered = self.journal.signal_count(run.run_id)
            name = str(waiting["name"])
            self.control.signal(
                run.run_id, name, {"name": name, "delivery": delivered}, principal="chaos-sender"
            )
            return True
        if kind == "approval":
            self.control.decide(
                str(waiting["approval_id"]),
                principal="chaos-approver",
                approve=True,
                call_hash=str(waiting["call_hash"]),
            )
            return True
        return False

    def drive(
        self,
        run_id: str,
        injector: CrashInjector | None = None,
        *,
        max_activations: int = 400,
        stop_after_crash: bool = False,
    ) -> tuple[int, int]:
        """Activate the run until it is terminal or blocked. Returns (activations, crashes).

        ``stop_after_crash`` leaves the run exactly as the crash left it (lease still held).
        """
        hooks = injector or CrashInjector(())
        self.journal.append_hooks[:] = [hooks.append_hook]
        activations = crashes = resolved = 0
        try:
            while activations < max_activations:
                run = self.journal.get_run(run_id)
                if run.terminal or run.status is RunStatus.BLOCKED:
                    break
                if run.status is RunStatus.WAITING:
                    resolved += 1
                    if resolved > 4 * max_activations:
                        raise RuntimeError(f"run {run_id} stays waiting: {run.waiting_on}")
                    if not self.resolve_wait(run):
                        break
                    continue
                worker = Worker(
                    self.runtime(hooks),
                    worker_id=f"chaos-{activations}",
                    lease_seconds=self.lease_seconds,
                    heartbeat_seconds=None,
                )
                activations += 1
                try:
                    outcome = asyncio.run(worker.run_once(run_id=run_id))
                except SimulatedCrash:
                    crashes += 1
                    if stop_after_crash:
                        break
                    self.clock.advance(self.lease_seconds + 1)
                    continue
                if outcome is None:
                    self.clock.advance(self.lease_seconds + 1)
        finally:
            self.journal.append_hooks.clear()
        return activations, crashes

    def outcome(
        self, run_id: str, injector: CrashInjector, activations: int, crashes: int
    ) -> ProgramOutcome:
        run = self.journal.get_run(run_id)
        history = self.journal.load_history(run_id)
        scheduled_command = {
            record.seq: record.command
            for record in history
            if record.type == EventType.STEP_SCHEDULED
        }
        window_commands = {command for rid, command in injector.window_commands if rid == run_id}
        window_commands |= {
            scheduled_command[seq] for seq in injector.window_seqs if seq in scheduled_command
        }
        window_seqs = frozenset(
            seq for seq, command in scheduled_command.items() if command in window_commands
        )
        duplicates = {
            int(key.rsplit(":", 1)[1]): count
            for key, count in self.meter.duplicates(run_id).items()
        }
        observed = _OBSERVATIONS.get(run_id, {})
        error = run.error or {}
        return ProgramOutcome(
            run_id=run_id,
            status=run.status.value,
            result=run.result,
            error_type=error.get("error_type") if run.error else None,
            ledger=tuple(self.sinks.ledger(run_id)),
            provider_calls=self.meter.calls(run_id),
            duplicate_provider_seqs=duplicates,
            activations=activations,
            crashes=crashes,
            fired=tuple(injector.fired),
            window_step_seqs=window_seqs,
            observations_stable=all(len(values) == 1 for values in observed.values()),
            journal_ok=verify_chain(
                history, run_id=run_id, expect_head=(run.head_seq, run.head_hash)
            ).ok,
            history_length=len(history),
        )

    def run(
        self,
        program: Program,
        run_id: str,
        crash_points: Sequence[CrashPoint] = (),
        *,
        budget: BudgetLimits | None = None,
        max_activations: int = 400,
    ) -> ProgramOutcome:
        self.start(program, run_id, budget)
        injector = CrashInjector(crash_points)
        activations, crashes = self.drive(run_id, injector, max_activations=max_activations)
        return self.outcome(run_id, injector, activations, crashes)


def random_program(
    rng: random.Random, *, max_ops: int = 10, kinds: Sequence[OpKind] = OP_KINDS
) -> Program:
    ops = tuple(
        Op(kind=rng.choice(list(kinds)), arg=rng.randrange(0, 100))
        for _ in range(rng.randint(1, max_ops))
    )
    return Program(ops=ops, compensate=rng.random() < 0.3, fail_at_end=rng.random() < 0.15)


def random_crash_points(
    rng: random.Random,
    *,
    horizon: int,
    max_points: int = 3,
    kinds: Sequence[CrashKind] = ("before_commit", "after_commit"),
) -> list[CrashPoint]:
    points: set[CrashPoint] = set()
    for _ in range(rng.randint(1, max_points)):
        kind = rng.choice(list(kinds))
        limit = max(1, horizon)
        points.add(CrashPoint(kind, rng.randrange(0, limit) + (1 if kind != "after_effect" else 0)))
    return sorted(points, key=lambda point: (point.at, point.kind))


def _pause_forever(marker: Path) -> Callable[[CrashPoint], None]:
    def action(point: CrashPoint) -> None:
        marker.write_text(f"{point.kind}@{point.at}\n", encoding="utf-8")
        while True:  # the parent kills this process with SIGKILL / TerminateProcess
            time.sleep(1)

    return action


def drive_main(argv: Sequence[str] | None = None) -> int:
    """Subprocess entry for bench/chaos_sigkill.py: drive a run, pausing at the kill point."""
    parser = argparse.ArgumentParser(prog="rar-chaos-drive")
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--clock-start", type=float, required=True)
    parser.add_argument("--pause-kind", choices=["after_commit", "after_effect"], required=True)
    parser.add_argument("--pause-at", type=int, required=True)
    parser.add_argument("--marker", type=Path, required=True)
    args = parser.parse_args(argv)
    env = ChaosEnvironment(args.workdir, clock_start=args.clock_start)
    injector = CrashInjector(
        [CrashPoint(args.pause_kind, args.pause_at)], action=_pause_forever(args.marker)
    )
    env.drive(args.run_id, injector)
    env.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through a subprocess
    raise SystemExit(drive_main(sys.argv[1:]))
