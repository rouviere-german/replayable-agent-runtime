"""Determinism suite: 22 nondeterministic edits of a workflow, and edits guarded by ``patched``.

A base workflow is run to completion (model calls, a read tool, a durable timer, a signal, a human
gate and an approved write). Each edit is then checked on two paths:

- **strict replay** of the completed history with
  :func:`~replayable_agent_runtime.runtime.engine.replay_check`;
- **the live engine**: a second run of the base workflow is parked in flight (every step recorded,
  the worker killed just before ``RunCompleted`` commits) and activated with the edited code through
  :func:`~replayable_agent_runtime.runtime.engine.activate`.

Every unguarded edit, including edits that raise or return early before the history head, must
raise ``NonDeterminismError`` on replay and leave the in-flight run blocked with nothing appended.
Every edit guarded by ``ctx.patched`` must replay the old history cleanly and finish the in-flight
run.

Workflows are written as step lists so each edit is one visible, reviewable change.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import JsonValue

from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.runtime.chaos import (
    ChaosEnvironment,
    CrashInjector,
    CrashPoint,
    echo_policy,
)
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.engine import Runtime, activate, replay_check
from replayable_agent_runtime.runtime.errors import NonDeterminismError
from replayable_agent_runtime.runtime.journal import RunStatus
from replayable_agent_runtime.runtime.workflows import (
    WorkflowDefinition,
    WorkflowFn,
    WorkflowRegistry,
)

__all__ = [
    "BASE",
    "MUTANTS",
    "PATCHED",
    "MutationResult",
    "Step",
    "evaluate_mutations",
    "scripted",
]

TOOLS = frozenset({"kb_lookup", "create_ticket", "notify"})
WORKFLOW = "mutation_base"


@dataclass(frozen=True, slots=True)
class Step:
    kind: str
    name: str = ""
    args: Mapping[str, JsonValue] = field(default_factory=dict)
    then: tuple[Step, ...] = ()
    otherwise: tuple[Step, ...] = ()


def llm(prompt: str, *, system: str = "", max_tokens: int = 256, role: str = "default") -> Step:
    return Step(
        "llm", role, {"prompt": prompt, "system": system, "max_tokens": max_tokens, "role": role}
    )


def tool(name: str, **args: JsonValue) -> Step:
    return Step("tool", name, args)


NOW = Step("now")
UUID = Step("uuid")


def sleep(seconds: float) -> Step:
    return Step("sleep", "timer", {"seconds": seconds})


def signal(name: str) -> Step:
    return Step("signal", name)


def gate(name: str, **details: JsonValue) -> Step:
    return Step("gate", name, details)


def patched(patch_id: str, then: Sequence[Step], otherwise: Sequence[Step]) -> Step:
    return Step("patched", patch_id, then=tuple(then), otherwise=tuple(otherwise))


def fail(message: str) -> Step:
    """Workflow code raises (for example a validation added in a new version)."""
    return Step("raise", "", {"message": message})


def return_early(**value: JsonValue) -> Step:
    """Workflow code returns an error value instead of continuing."""
    return Step("return", "", value)


class _EarlyReturn(Exception):  # noqa: N818 - control flow inside the scripted workflow only
    def __init__(self, value: dict[str, JsonValue]) -> None:
        super().__init__("early return")
        self.value = value


async def _run_steps(ctx: WorkflowContext, steps: Sequence[Step], outputs: list[JsonValue]) -> None:
    for step in steps:
        if step.kind == "now":
            await ctx.now()
            outputs.append("now")
        elif step.kind == "uuid":
            await ctx.uuid()
            outputs.append("uuid")
        elif step.kind == "llm":
            args = step.args
            response = await ctx.llm(
                LLMRequest(
                    system=str(args["system"]),
                    messages=(ChatMessage.user(str(args["prompt"])),),
                    max_tokens=int(str(args["max_tokens"])),
                    role=str(args["role"]),
                )
            )
            outputs.append(response.text)
        elif step.kind == "tool":
            outputs.append(await ctx.tool(step.name, dict(step.args)))
        elif step.kind == "sleep":
            await ctx.sleep(float(str(step.args["seconds"])))
            outputs.append("slept")
        elif step.kind == "signal":
            outputs.append(await ctx.wait_signal(step.name))
        elif step.kind == "gate":
            outputs.append(await ctx.await_approval(step.name, dict(step.args)))
        elif step.kind == "patched":
            branch = step.then if ctx.patched(step.name) else step.otherwise
            await _run_steps(ctx, branch, outputs)
        elif step.kind == "raise":
            raise ValueError(str(step.args["message"]))
        elif step.kind == "return":
            raise _EarlyReturn(dict(step.args))
        else:
            raise ValueError(f"unknown step kind {step.kind}")


def scripted(steps: Sequence[Step]) -> WorkflowFn:
    async def workflow(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
        del value
        outputs: list[JsonValue] = []
        try:
            await _run_steps(ctx, steps, outputs)
        except _EarlyReturn as early:
            return early.value
        return {"outputs": outputs}

    return workflow


BASE: tuple[Step, ...] = (
    NOW,
    llm("summarise the incident"),
    tool("kb_lookup", key="cache"),
    sleep(5),
    UUID,
    signal("go"),
    gate("deploy", env="prod"),
    tool("create_ticket", title="incident", body="see summary"),
    llm("follow up"),
)


def _replace(index: int, step: Step) -> tuple[Step, ...]:
    return (*BASE[:index], step, *BASE[index + 1 :])


def _remove(index: int) -> tuple[Step, ...]:
    return (*BASE[:index], *BASE[index + 1 :])


def _insert(index: int, step: Step) -> tuple[Step, ...]:
    return (*BASE[:index], step, *BASE[index:])


MUTANTS: tuple[tuple[str, tuple[Step, ...]], ...] = (
    ("prompt_changed", _replace(1, llm("summarize the incident"))),
    ("system_prompt_added", _replace(1, llm("summarise the incident", system="Be terse."))),
    ("max_tokens_changed", _replace(1, llm("summarise the incident", max_tokens=512))),
    ("model_role_changed", _replace(1, llm("summarise the incident", role="planner"))),
    ("tool_argument_changed", _replace(2, tool("kb_lookup", key="caches"))),
    ("tool_renamed", _replace(2, tool("notify", channel="cache", message="lookup"))),
    ("steps_swapped", (BASE[0], BASE[2], BASE[1], *BASE[3:])),
    ("first_step_removed", _remove(0)),
    ("timer_removed", _remove(3)),
    ("last_step_removed", _remove(8)),
    ("step_inserted_first", _insert(0, UUID)),
    ("step_inserted_midway", _insert(4, tool("kb_lookup", key="deploy"))),
    ("step_duplicated", _insert(3, tool("kb_lookup", key="cache"))),
    ("timer_duration_changed", _replace(3, sleep(6))),
    ("timer_replaced_by_clock", _replace(3, NOW)),
    ("uuid_replaced_by_clock", _replace(4, NOW)),
    ("signal_renamed", _replace(5, signal("start"))),
    ("gate_details_changed", _replace(6, gate("deploy", env="staging"))),
    (
        "approved_write_changed",
        _replace(7, tool("create_ticket", title="incident-2", body="see summary")),
    ),
    ("model_call_replaced_by_tool", _replace(8, tool("kb_lookup", key="follow-up"))),
    ("raises_before_the_head", _insert(4, fail("validation added in v2"))),
    ("returns_an_error_value_early", _insert(6, return_early(error="deploy gate skipped"))),
)

PATCHED: tuple[tuple[str, tuple[Step, ...]], ...] = (
    (
        "patched_new_check",
        _insert(7, patched("add-check", [tool("kb_lookup", key="deploy")], [])),
    ),
    (
        "patched_ticket_title",
        _replace(
            7,
            patched(
                "title-v2",
                [tool("create_ticket", title="incident v2", body="see summary")],
                [BASE[7]],
            ),
        ),
    ),
    ("patched_timer_removed", _replace(3, patched("no-timer", [], [BASE[3]]))),
    (
        "patched_extra_model_call",
        (*BASE, patched("extra-summary", [llm("final summary")], [])),
    ),
)


@dataclass(frozen=True, slots=True)
class MutationResult:
    name: str
    expected_nondeterminism: bool
    raised: bool
    detail: str
    in_flight_outcome: str = ""
    in_flight_detail: str | None = None
    in_flight_appended: int = 0

    @property
    def as_expected(self) -> bool:
        if self.raised != self.expected_nondeterminism:
            return False
        if self.expected_nondeterminism:
            return self.in_flight_outcome == "blocked" and self.in_flight_appended == 0
        return self.in_flight_outcome == "completed"


def _park_in_flight(env: ChaosEnvironment, run_id: str, completion_seq: int) -> None:
    """Run the base workflow and kill the worker just before ``RunCompleted`` commits."""
    env.control.start_run(
        workflow=WORKFLOW, input_value=None, principal="chaos-starter", run_id=run_id
    )
    injector = CrashInjector([CrashPoint("before_commit", completion_seq)])
    env.drive(run_id, injector, stop_after_crash=True)
    run = env.journal.get_run(run_id)
    if run.status is not RunStatus.RUNNING or run.head_seq != completion_seq - 1:
        raise RuntimeError(f"could not park {run_id} before its completion")


def evaluate_mutations(workdir: Path) -> list[MutationResult]:
    """Record the base history once; check every edit by strict replay and on an in-flight run."""
    env = ChaosEnvironment(workdir)
    try:
        env.workflows.register(WorkflowDefinition(WORKFLOW, scripted(BASE), TOOLS))
        env.control.start_run(
            workflow=WORKFLOW, input_value=None, principal="chaos-starter", run_id="base-run"
        )
        env.drive("base-run")
        base = env.journal.get_run("base-run")
        if base.status is not RunStatus.COMPLETED:
            raise RuntimeError("the base workflow did not complete")
        completion_seq = base.head_seq
        results: list[MutationResult] = []
        cases = [(name, steps, True) for name, steps in MUTANTS]
        cases += [(name, steps, False) for name, steps in PATCHED]
        in_flight: str | None = None
        for index, (name, steps, expected) in enumerate(cases):
            runtime = Runtime(
                journal=env.journal,
                workflows=WorkflowRegistry([WorkflowDefinition(WORKFLOW, scripted(steps), TOOLS)]),
                tools=env.tools,
                provider=FakeProvider(echo_policy),
                clock=env.clock,
            )
            try:
                outcome = asyncio.run(replay_check(runtime, "base-run"))
            except NonDeterminismError as exc:
                raised, detail = True, str(exc)
            else:
                raised, detail = False, outcome.outcome

            if in_flight is None:
                in_flight = f"in-flight-{index}"
                _park_in_flight(env, in_flight, completion_seq)
            env.clock.advance(env.lease_seconds + 1)
            claim = env.journal.claim("mutation-check", env.lease_seconds, run_id=in_flight)
            if claim is None:
                raise RuntimeError(f"could not claim {in_flight}")
            activation = asyncio.run(activate(runtime, claim))
            after = env.journal.get_run(in_flight)
            appended = after.head_seq - claim.run.head_seq
            if after.status is RunStatus.BLOCKED:
                env.journal.retry_blocked(in_flight)  # reusable: nothing was appended
            else:
                in_flight = None
            results.append(
                MutationResult(
                    name,
                    expected,
                    raised,
                    detail,
                    in_flight_outcome=activation.outcome,
                    in_flight_detail=activation.detail,
                    in_flight_appended=appended,
                )
            )
        return results
    finally:
        env.close()
