"""WorkflowContext: the only door through which workflow code touches the outside world.

Each context method is one *command*. Commands are numbered in the order the workflow issues them.
For command ``k`` the context looks up the events recorded for ``k``:

- recorded and complete: the recorded result is returned (or the recorded failure re-raised)
  without calling anything, after checking the command fingerprint (``NonDeterminismError`` on
  mismatch);
- not recorded (past the history head): the command runs live, and its events are committed to
  the journal before control returns to workflow code;
- partially recorded (``StepScheduled`` without completion: the worker died in between): the step
  is re-executed with the same idempotency key ``run_id:seq``. This is the at-least-once window
  described in docs/SEMANTICS.md.

Live failures are recorded first and then re-raised *from the record*, so workflow code sees the
same exception type live and on replay.
"""

from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from opentelemetry.trace import Span
from pydantic import JsonValue

from replayable_agent_runtime.agentic.approval import ApprovalStatus, SubjectKind, subject_hash
from replayable_agent_runtime.agentic.envelope import wrap_tool_result
from replayable_agent_runtime.agentic.errors import (
    ActivityFailedError,
    AgenticError,
    ApprovalDeniedError,
    ApprovalIntegrityError,
    BudgetExceededError,
    ProviderAuthError,
    ProviderError,
    ProviderRateLimitedError,
    ProviderRefusalError,
    ProviderRequestError,
    ProviderTruncatedError,
    ProviderUnavailableError,
    StaleFencingTokenError,
    ToolArgumentError,
    ToolNotAllowedError,
    ToolPolicyError,
)
from replayable_agent_runtime.agentic.pricing import cost_usd_micros
from replayable_agent_runtime.agentic.providers.base import (
    LLMRequest,
    LLMResponse,
    ToolResultBlock,
    ToolSpec,
    ToolUseBlock,
    acomplete,
)
from replayable_agent_runtime.agentic.tools import EffectClass, ToolCallContext, ValidatedCall
from replayable_agent_runtime.core.canonical import CanonicalJSONError, canonical_json
from replayable_agent_runtime.runtime.budget import RunBudget, fold_budget, limits_from_payload
from replayable_agent_runtime.runtime.db import now_us
from replayable_agent_runtime.runtime.errors import (
    FencedOutError,
    IntegrityHalt,
    NonDeterminismError,
    WorkflowCancelled,
    WorkflowSuspended,
)
from replayable_agent_runtime.runtime.events import (
    CommandKind,
    EventRecord,
    EventType,
    NewEvent,
    command_fingerprint,
)
from replayable_agent_runtime.runtime.journal import Claim, Journal
from replayable_agent_runtime.runtime.metering import CURRENT_STEP
from replayable_agent_runtime.runtime.telemetry import step_span
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition

if TYPE_CHECKING:
    from replayable_agent_runtime.runtime.engine import Runtime

__all__ = ["ActivationState", "ToolCallResult", "WorkflowContext", "rebuild_error"]

_MESSAGE_LIMIT = 500
_REBUILDABLE: dict[str, type[AgenticError]] = {
    "ToolNotAllowedError": ToolNotAllowedError,
    "ToolArgumentError": ToolArgumentError,
    "ToolPolicyError": ToolPolicyError,
    "ActivityFailedError": ActivityFailedError,
}
_PROVIDER_ERRORS: dict[str, type[ProviderError]] = {
    "ProviderError": ProviderError,
    "ProviderAuthError": ProviderAuthError,
    "ProviderRequestError": ProviderRequestError,
    "ProviderTruncatedError": ProviderTruncatedError,
}


def rebuild_error(data: Mapping[str, Any]) -> Exception:
    """Rebuild the exception a recorded ``StepFailed`` / ``CommandRejected`` stands for."""
    error_type = str(data.get("error_type", "ActivityFailedError"))
    message = str(data.get("message", "step failed"))
    details = data.get("details") or {}
    if error_type == "BudgetExceededError":
        return BudgetExceededError(
            str(details.get("dimension", "steps")),
            int(details.get("limit", 0)),
            int(details.get("attempted", 0)),
        )
    if error_type == "ApprovalDeniedError":
        return ApprovalDeniedError(str(data.get("name", "")), details.get("decided_by"))
    if data.get("kind") == CommandKind.LLM.value:
        if error_type == "ProviderRefusalError":
            return ProviderRefusalError(details.get("category"))
        if error_type == "ProviderRateLimitedError":
            return ProviderRateLimitedError(message)
        if error_type == "ProviderUnavailableError":
            return ProviderUnavailableError(message)
        provider_cls = _PROVIDER_ERRORS.get(error_type)
        if provider_cls is not None:
            return provider_cls(message, retryable=bool(data.get("retryable", False)))
        return ActivityFailedError(message, error_type=error_type)
    cls = _REBUILDABLE.get(error_type)
    if cls is ActivityFailedError or cls is None:
        return ActivityFailedError(message, error_type=error_type)
    return cls(message)


def _find(records: list[EventRecord], event_type: EventType) -> EventRecord | None:
    for record in records:
        if record.type == event_type:
            return record
    return None


def _usage_payload(usage: Any) -> dict[str, JsonValue] | None:
    if usage is None:
        return None
    return {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens}


@dataclass(frozen=True, slots=True)
class ToolCallResult:
    """What a model-proposed call produced: the block to send back, and the raw outcome."""

    block: ToolResultBlock
    ok: bool
    output: JsonValue = None


class ActivationState:
    """Per-activation replay state shared by the context and the engine."""

    def __init__(
        self,
        runtime: Runtime,
        claim: Claim,
        history: list[EventRecord],
        definition: WorkflowDefinition,
        *,
        strict: bool = False,
    ) -> None:
        self.runtime = runtime
        self.journal: Journal = runtime.journal
        self.lease = claim.lease
        self.run = claim.run
        self.definition = definition
        self.strict = strict
        self.commands: dict[int, list[EventRecord]] = {}
        for record in history:
            if record.command >= 0:
                self.commands.setdefault(record.command, []).append(record)
        self.recorded_commands = max(self.commands) + 1 if self.commands else 0
        started = history[0].data()
        limits = limits_from_payload(started["budget"])
        if runtime.budget_cap is not None:
            limits = limits.tightest(runtime.budget_cap)
        self.budget: RunBudget = fold_budget(limits, history)
        self.next_command = 0
        self.cancel_requested = claim.run.cancel_requested
        self.cancel_delivered = any(r.type == EventType.CANCEL_REQUESTED for r in history)
        self.compensations: list[tuple[str, dict[str, JsonValue]]] = []
        self.compensating = False
        self._started = time.perf_counter()
        self._head_reached = False
        self.replay_seconds: float | None = None

    def mark_head(self) -> None:
        """Called once replay reaches the first command that is not fully recorded."""
        if self._head_reached:
            return
        self._head_reached = True
        self.replay_seconds = time.perf_counter() - self._started
        self.runtime.metrics.replay_duration_seconds.observe(self.replay_seconds)

    def add(self, command: int, records: list[EventRecord]) -> None:
        self.commands.setdefault(command, []).extend(records)
        for record in records:
            self.budget.apply(record)


class WorkflowContext:
    """Deterministic, journaled access to time, randomness, models, tools, timers and humans."""

    def __init__(self, state: ActivationState) -> None:
        self._state = state

    # ------------------------------------------------------------------ properties

    @property
    def run_id(self) -> str:
        return self._state.lease.run_id

    @property
    def workflow(self) -> str:
        return self._state.definition.name

    @property
    def started_by(self) -> str:
        return self._state.run.started_by

    @property
    def is_replaying(self) -> bool:
        return self._state.next_command < self._state.recorded_commands

    @property
    def budget(self) -> RunBudget:
        return self._state.budget

    def tool_specs(self, names: Collection[str]) -> dict[str, ToolSpec]:
        """Model-facing specs of runtime tools (unknown names fail fast)."""
        registry = self._state.runtime.tools
        return {name: registry.get(name).spec() for name in names}

    # ------------------------------------------------------------------ command plumbing

    def _begin(
        self, kind: CommandKind, name: str, args: JsonValue
    ) -> tuple[int, str, list[EventRecord]]:
        state = self._state
        fingerprint = command_fingerprint(kind, name, args)
        # A patch marker the code no longer checks for (a deprecated patch) is skipped.
        while True:
            command = state.next_command
            records = state.commands.get(command, [])
            if (
                records
                and records[0].type == EventType.PATCH_MARKER
                and kind is not CommandKind.PATCH
            ):
                state.next_command += 1
                continue
            break
        if records and records[0].fingerprint != fingerprint:
            recorded = records[0].data().get("kind", records[0].type)
            raise NonDeterminismError(
                self.run_id,
                command,
                f"workflow issued {kind.value} {name!r} but history recorded {recorded} "
                "with a different fingerprint",
            )
        if not records and state.strict:
            raise NonDeterminismError(
                self.run_id, command, f"history ended but workflow issued {kind.value} {name!r}"
            )
        state.next_command += 1
        mode = "replay" if records else "live"
        state.runtime.metrics.steps_total.labels(kind.value, mode).inc()
        return command, fingerprint, records

    @contextmanager
    def _span(self, command: int, kind: CommandKind, name: str, replay: bool) -> Iterator[Span]:
        with step_span(
            self._state.runtime.tracer,
            run_id=self.run_id,
            command=command,
            kind=kind.value,
            name=name,
            mode="replay" if replay else "live",
        ) as span:
            yield span
            records = self._state.commands.get(command)
            if records:
                span.set_attribute("rar.seq", records[-1].seq)

    def _append(self, command: int, events: list[NewEvent], **kwargs: Any) -> list[EventRecord]:
        records = self._state.journal.append(self._state.lease, events, **kwargs)
        self._state.add(command, records)
        return records

    def _raise_recorded(self, records: list[EventRecord]) -> None:
        """Re-raise a recorded cancellation, rejection or failure for this command."""
        for record in records:
            if record.type == EventType.CANCEL_REQUESTED:
                self._state.cancel_delivered = True
                raise WorkflowCancelled(f"run {self.run_id} was cancelled")
            if record.type in (EventType.COMMAND_REJECTED, EventType.STEP_FAILED):
                raise rebuild_error(record.data())
            if record.type == EventType.APPROVAL_DENIED:
                data = record.data()
                raise ApprovalDeniedError(
                    str(records[0].data().get("name", "")),
                    data.get("decided_by"),
                    data.get("reason"),
                )

    def _deliver_cancel(self, command: int, fingerprint: str) -> None:
        state = self._state
        if state.cancel_requested and not state.cancel_delivered and not state.compensating:
            self._append(
                command,
                [
                    NewEvent(
                        EventType.CANCEL_REQUESTED, {"requested_by": None}, command, fingerprint
                    )
                ],
            )
            state.cancel_delivered = True
            raise WorkflowCancelled(f"run {self.run_id} was cancelled")

    def _reject(
        self,
        command: int,
        fingerprint: str,
        kind: CommandKind,
        name: str,
        error: Exception,
        details: dict[str, JsonValue] | None = None,
    ) -> Exception:
        """Journal a command-time rejection and return the exception rebuilt from the record."""
        record = self._append(
            command,
            [
                NewEvent(
                    EventType.COMMAND_REJECTED,
                    {
                        "kind": kind.value,
                        "name": name,
                        "error_type": type(error).__name__,
                        "message": str(error)[:_MESSAGE_LIMIT],
                        "details": details or {},
                    },
                    command,
                    fingerprint,
                )
            ],
        )[0]
        return rebuild_error(record.data())

    def _charge(self, command: int, fingerprint: str, kind: CommandKind, name: str) -> None:
        try:
            self._state.budget.check_step()
        except BudgetExceededError as exc:
            details: dict[str, JsonValue] = {
                "dimension": exc.dimension,
                "limit": exc.limit,
                "attempted": exc.attempted,
            }
            raise self._reject(command, fingerprint, kind, name, exc, details) from exc

    # ------------------------------------------------------------------ model calls

    async def llm(self, request: LLMRequest) -> LLMResponse:
        """Call the model once; the response (or failure) is journaled and replayed."""
        request_sha256 = request.sha256()
        command, fingerprint, records = self._begin(
            CommandKind.LLM, request.role, {"request_sha256": request_sha256}
        )
        with self._span(command, CommandKind.LLM, request.role, bool(records)):
            completed = _find(records, EventType.STEP_COMPLETED)
            if completed is not None:
                return LLMResponse.model_validate(completed.data()["output"])
            self._raise_recorded(records)
            self._state.mark_head()
            scheduled = _find(records, EventType.STEP_SCHEDULED)
            if scheduled is None:
                self._deliver_cancel(command, fingerprint)
                self._charge(command, fingerprint, CommandKind.LLM, request.role)
                scheduled = self._append(
                    command,
                    [
                        NewEvent(
                            EventType.STEP_SCHEDULED,
                            {
                                "kind": CommandKind.LLM.value,
                                "name": request.role,
                                "request_sha256": request_sha256,
                            },
                            command,
                            fingerprint,
                        )
                    ],
                )[0]
            key = f"{self.run_id}:{scheduled.seq}"
            runtime = self._state.runtime
            token = CURRENT_STEP.set((self.run_id, key))
            try:
                response = await acomplete(runtime.provider, request)
            except ProviderError as exc:
                runtime.hooks.after_effect(CommandKind.LLM.value, key)
                usage = exc.usage
                model = exc.model or "unknown"
                details: dict[str, JsonValue] = {}
                if isinstance(exc, ProviderRefusalError):
                    details["category"] = exc.category
                failure = self._append(
                    command,
                    [
                        NewEvent(
                            EventType.STEP_FAILED,
                            {
                                "kind": CommandKind.LLM.value,
                                "name": request.role,
                                "error_type": type(exc).__name__,
                                "message": str(exc)[:_MESSAGE_LIMIT],
                                "retryable": exc.retryable,
                                "model": exc.model,
                                "usage": _usage_payload(usage),
                                "cost_usd_micros": cost_usd_micros(
                                    model, usage.input_tokens, usage.output_tokens
                                )
                                if usage is not None
                                else 0,
                                "details": details,
                            },
                            command,
                            fingerprint,
                        )
                    ],
                )[0]
                raise rebuild_error(failure.data()) from exc
            finally:
                CURRENT_STEP.reset(token)
            runtime.hooks.after_effect(CommandKind.LLM.value, key)
            self._append(
                command,
                [
                    NewEvent(
                        EventType.STEP_COMPLETED,
                        {
                            "kind": CommandKind.LLM.value,
                            "name": request.role,
                            "output": response.model_dump(mode="json"),
                            "model": response.model,
                            "usage": _usage_payload(response.usage),
                            "cost_usd_micros": cost_usd_micros(
                                response.model,
                                response.usage.input_tokens,
                                response.usage.output_tokens,
                            ),
                        },
                        command,
                        fingerprint,
                    )
                ],
            )
            return response

    # ------------------------------------------------------------------ tools

    async def tool(self, name: str, args: Mapping[str, JsonValue] | None = None) -> JsonValue:
        """Run an allowlisted tool. Write tools block on a durable, hash-bound approval."""
        return await self._tool_command(CommandKind.TOOL, name, dict(args or {}))

    async def tool_call(self, call: ToolUseBlock, *, exposed: Collection[str]) -> ToolCallResult:
        """Run a model-proposed call; policy failures become error results, not exceptions.

        ``exposed`` is the set of tools offered to the model. Output goes back inside an
        untrusted-data envelope. Suspension, cancellation and budget exhaustion still propagate.
        """
        try:
            output = await self._tool_command(
                CommandKind.TOOL, call.name, dict(call.arguments), exposed=exposed
            )
        except (
            ToolNotAllowedError,
            ToolArgumentError,
            ToolPolicyError,
            ApprovalDeniedError,
            ActivityFailedError,
        ) as exc:
            reason = type(exc).__name__
            block = ToolResultBlock(
                tool_use_id=call.id,
                content=f"Call to {call.name} was not executed ({reason}).",
                is_error=True,
            )
            return ToolCallResult(block=block, ok=False)
        envelope = wrap_tool_result(call.name, output)
        block = ToolResultBlock(tool_use_id=call.id, content=envelope.render())
        return ToolCallResult(block=block, ok=True, output=output)

    def _validate(
        self,
        command: int,
        fingerprint: str,
        kind: CommandKind,
        name: str,
        args: dict[str, JsonValue],
        exposed: Collection[str] | None,
    ) -> ValidatedCall:
        definition = self._state.definition
        if exposed is not None and name not in exposed:
            raise self._reject(
                command,
                fingerprint,
                kind,
                name,
                ToolNotAllowedError(f"tool {name} is not offered to the model in this workflow"),
            )
        if name not in definition.tools:
            raise self._reject(
                command,
                fingerprint,
                kind,
                name,
                ToolNotAllowedError(
                    f"tool {name} is not allowlisted for workflow {definition.name}"
                ),
            )
        try:
            validated = self._state.runtime.tools.validate_call(
                ToolUseBlock(id=f"cmd-{command}", name=name, arguments=args)
            )
        except (ToolNotAllowedError, ToolArgumentError) as exc:
            raise self._reject(command, fingerprint, kind, name, exc) from exc
        effect = validated.tool.effect
        if kind is CommandKind.TOOL and effect is EffectClass.COMPENSATE:
            raise self._reject(
                command,
                fingerprint,
                kind,
                name,
                ToolNotAllowedError(f"compensation tool {name} runs only through ctx.compensate"),
            )
        if kind is CommandKind.COMPENSATION and effect is not EffectClass.COMPENSATE:
            raise self._reject(
                command,
                fingerprint,
                kind,
                name,
                ToolNotAllowedError(f"tool {name} is not a compensation tool"),
            )
        return validated

    async def _tool_command(
        self,
        kind: CommandKind,
        name: str,
        args: dict[str, JsonValue],
        *,
        exposed: Collection[str] | None = None,
    ) -> JsonValue:
        try:
            canonical_json(args)
        except CanonicalJSONError as exc:
            raise ToolArgumentError(f"arguments for {name} are not JSON") from exc
        command, fingerprint, records = self._begin(kind, name, args)
        with self._span(command, kind, name, bool(records)):
            completed = _find(records, EventType.STEP_COMPLETED)
            if completed is not None:
                return completed.data().get("output")
            self._raise_recorded(records)
            self._state.mark_head()
            scheduled = _find(records, EventType.STEP_SCHEDULED)
            if scheduled is None:
                if kind is CommandKind.TOOL:
                    self._deliver_cancel(command, fingerprint)
                validated = self._validate(command, fingerprint, kind, name, args, exposed)
                authorized: str | None = None
                if kind is CommandKind.TOOL and validated.tool.needs_approval:
                    await self._approval(
                        command,
                        fingerprint,
                        records,
                        SubjectKind.TOOL,
                        name,
                        validated.canonical_args,
                        validated.call_hash,
                    )
                    authorized = validated.call_hash
                if kind is CommandKind.TOOL:
                    self._charge(command, fingerprint, kind, name)
                scheduled = self._schedule_tool(command, fingerprint, kind, validated)
            else:
                validated = self._revalidate(command, name, args)
                authorized = self._granted_hash(command, records, validated)
            return await self._execute_tool(
                command, fingerprint, kind, validated, scheduled, authorized
            )

    def _revalidate(self, command: int, name: str, args: dict[str, JsonValue]) -> ValidatedCall:
        try:
            return self._state.runtime.tools.validate_call(
                ToolUseBlock(id=f"cmd-{command}", name=name, arguments=args)
            )
        except (ToolNotAllowedError, ToolArgumentError) as exc:
            raise NonDeterminismError(
                self.run_id, command, f"scheduled call to {name} no longer validates"
            ) from exc

    def _granted_hash(
        self, command: int, records: list[EventRecord], validated: ValidatedCall
    ) -> str | None:
        if not validated.tool.needs_approval or validated.tool.effect is EffectClass.COMPENSATE:
            return None
        granted = _find(records, EventType.APPROVAL_GRANTED)
        if granted is None or granted.data().get("call_hash") != validated.call_hash:
            raise IntegrityHalt(
                f"run {self.run_id} command {command}: scheduled write has no matching approval"
            )
        return validated.call_hash

    def _schedule_tool(
        self, command: int, fingerprint: str, kind: CommandKind, validated: ValidatedCall
    ) -> EventRecord:
        lease = self._state.lease
        tool = validated.tool
        event = NewEvent(
            EventType.STEP_SCHEDULED,
            {
                "kind": kind.value,
                "name": tool.name,
                "call_hash": validated.call_hash,
                "effect": tool.effect.value,
            },
            command,
            fingerprint,
        )
        if not tool.has_side_effects:
            return self._append(command, [event])[0]

        def outbox_row(conn: Any, records: Any) -> None:
            record = records[0]
            Journal.outbox_insert(
                conn,
                key=f"{lease.run_id}:{record.seq}",
                run_id=lease.run_id,
                seq=record.seq,
                tool=tool.name,
                call_hash=validated.call_hash,
                effect=tool.effect.value,
                fencing=lease.fencing,
                now=record.recorded_at_us,
            )

        return self._append(command, [event], within=outbox_row)[0]

    async def _execute_tool(
        self,
        command: int,
        fingerprint: str,
        kind: CommandKind,
        validated: ValidatedCall,
        scheduled: EventRecord,
        authorized: str | None,
    ) -> JsonValue:
        state = self._state
        lease = state.lease
        tool = validated.tool
        key = f"{lease.run_id}:{scheduled.seq}"
        context = ToolCallContext(
            run_id=lease.run_id,
            seq=scheduled.seq,
            idempotency_key=key,
            fencing_token=lease.fencing,
        )

        def finish_outbox(status: str) -> Any:
            if not tool.has_side_effects:
                return None

            def within(conn: Any, records: Any) -> None:
                Journal.outbox_finish(
                    conn,
                    key=key,
                    status=status,
                    fencing=lease.fencing,
                    now=records[0].recorded_at_us,
                )

            return within

        def check_size(value: JsonValue) -> None:
            if len(canonical_json(value).encode("utf-8")) > state.runtime.max_output_bytes:
                raise ToolPolicyError(f"output of {tool.name} exceeds the size limit")

        try:
            output = await asyncio.to_thread(validated.run, context, authorized_hash=authorized)
            check_size(output)
        except StaleFencingTokenError as exc:
            state.runtime.metrics.stale_fencing_rejections_total.labels("sink").inc()
            raise FencedOutError(f"sink refused fencing token {lease.fencing}") from exc
        except Exception as exc:
            error_type = (
                type(exc).__name__ if isinstance(exc, ToolPolicyError | ToolArgumentError) else ""
            )
            message = (
                str(exc)[:_MESSAGE_LIMIT]
                if error_type
                else f"tool {tool.name} failed ({type(exc).__name__})"
            )
            failure = self._append(
                command,
                [
                    NewEvent(
                        EventType.STEP_FAILED,
                        {
                            "kind": kind.value,
                            "name": tool.name,
                            "error_type": error_type or "ActivityFailedError",
                            "message": message,
                        },
                        command,
                        fingerprint,
                    )
                ],
                within=finish_outbox("failed"),
            )[0]
            raise rebuild_error(failure.data()) from exc
        state.runtime.hooks.after_effect(kind.value, key)
        self._append(
            command,
            [
                NewEvent(
                    EventType.STEP_COMPLETED,
                    {"kind": kind.value, "name": tool.name, "output": output},
                    command,
                    fingerprint,
                )
            ],
            within=finish_outbox("done"),
        )
        return output

    # ------------------------------------------------------------------ approvals

    async def await_approval(
        self, name: str, details: Mapping[str, JsonValue] | None = None
    ) -> str:
        """Block until a different principal approves this gate; returns the approver."""
        args = dict(details or {})
        call_hash = subject_hash(SubjectKind.GATE, name, args)
        command, fingerprint, records = self._begin(CommandKind.APPROVAL, name, args)
        with self._span(command, CommandKind.APPROVAL, name, bool(records)):
            return await self._approval(
                command, fingerprint, records, SubjectKind.GATE, name, args, call_hash
            )

    async def _approval(
        self,
        command: int,
        fingerprint: str,
        records: list[EventRecord],
        kind: SubjectKind,
        name: str,
        args: dict[str, JsonValue],
        call_hash: str,
    ) -> str:
        state = self._state
        for record in records:
            if record.type == EventType.CANCEL_REQUESTED:
                state.cancel_delivered = True
                raise WorkflowCancelled(f"run {self.run_id} was cancelled")
            if record.type == EventType.APPROVAL_GRANTED:
                data = record.data()
                if data.get("call_hash") != call_hash:
                    raise IntegrityHalt(
                        f"run {self.run_id} command {command}: approval recorded for another call"
                    )
                return str(data["decided_by"])
            if record.type == EventType.APPROVAL_DENIED:
                data = record.data()
                raise ApprovalDeniedError(name, data.get("decided_by"), data.get("reason"))
        state.mark_head()
        requested = _find(records, EventType.APPROVAL_REQUESTED)
        if requested is None:
            self._deliver_cancel(command, fingerprint)
            record = state.journal.request_approval(
                state.lease,
                command=command,
                fingerprint=fingerprint,
                kind=kind,
                name=name,
                args=args,
                call_hash=call_hash,
                requested_by=state.run.started_by,
            )
            state.add(command, [record])
            raise WorkflowSuspended(
                f"waiting for approval of {name}",
                waiting_on={
                    "type": "approval",
                    "approval_id": record.data()["approval_id"],
                    "name": name,
                    "call_hash": call_hash,
                },
            )
        self._deliver_cancel(command, fingerprint)
        requested_data = requested.data()
        approval = state.journal.get_approval(str(requested_data["approval_id"]))
        if approval.status is ApprovalStatus.PENDING:
            raise WorkflowSuspended(
                f"waiting for approval of {name}",
                waiting_on={
                    "type": "approval",
                    "approval_id": approval.approval_id,
                    "name": name,
                    "call_hash": call_hash,
                },
            )
        try:
            approval.verify_integrity(str(requested_data["call_hash"]))
        except ApprovalIntegrityError as exc:
            raise IntegrityHalt(str(exc)) from exc
        if approval.call_hash != call_hash:
            raise IntegrityHalt(f"approval {approval.approval_id} is bound to a different call")
        granted = approval.status is ApprovalStatus.APPROVED
        self._append(
            command,
            [
                NewEvent(
                    EventType.APPROVAL_GRANTED if granted else EventType.APPROVAL_DENIED,
                    {
                        "approval_id": approval.approval_id,
                        "call_hash": approval.call_hash,
                        "decided_by": approval.decided_by or "",
                        "reason": approval.reason,
                    },
                    command,
                    fingerprint,
                )
            ],
        )
        if not granted:
            raise ApprovalDeniedError(name, approval.decided_by, approval.reason)
        return approval.decided_by or ""

    # ------------------------------------------------------------------ time, randomness, waiting

    async def now(self) -> float:
        """Wall-clock seconds, recorded on first execution and replayed afterwards."""
        command, fingerprint, records = self._begin(CommandKind.NOW, "clock", {})
        done = _find(records, EventType.STEP_COMPLETED)
        if done is not None:
            return float(done.data()["output"]) / 1_000_000
        self._raise_recorded(records)
        self._state.mark_head()
        self._deliver_cancel(command, fingerprint)
        value = now_us(self._state.runtime.clock)
        self._append(
            command,
            [
                NewEvent(
                    EventType.STEP_COMPLETED,
                    {"kind": CommandKind.NOW.value, "name": "clock", "output": value},
                    command,
                    fingerprint,
                )
            ],
        )
        return value / 1_000_000

    async def uuid(self) -> str:
        """A random UUID (hex), recorded on first execution and replayed afterwards."""
        command, fingerprint, records = self._begin(CommandKind.UUID, "uuid4", {})
        done = _find(records, EventType.STEP_COMPLETED)
        if done is not None:
            return str(done.data()["output"])
        self._raise_recorded(records)
        self._state.mark_head()
        self._deliver_cancel(command, fingerprint)
        value = uuid.uuid4().hex
        self._append(
            command,
            [
                NewEvent(
                    EventType.STEP_COMPLETED,
                    {"kind": CommandKind.UUID.value, "name": "uuid4", "output": value},
                    command,
                    fingerprint,
                )
            ],
        )
        return value

    async def sleep(self, seconds: float) -> None:
        """A durable timer: the run releases its worker and a scheduler wakes it when due."""
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("sleep duration must be a finite, non-negative number of seconds")
        duration_us = round(seconds * 1_000_000)
        command, fingerprint, records = self._begin(
            CommandKind.SLEEP, "timer", {"duration_us": duration_us}
        )
        if _find(records, EventType.TIMER_FIRED) is not None:
            return
        self._raise_recorded(records)
        self._state.mark_head()
        self._deliver_cancel(command, fingerprint)
        clock = self._state.runtime.clock
        scheduled = _find(records, EventType.TIMER_SCHEDULED)
        if scheduled is None:
            fire_at = now_us(clock) + duration_us
            scheduled = self._append(
                command,
                [
                    NewEvent(
                        EventType.TIMER_SCHEDULED,
                        {"fire_at_us": fire_at, "duration_us": duration_us},
                        command,
                        fingerprint,
                    )
                ],
            )[0]
        fire_at = int(scheduled.data()["fire_at_us"])
        current = now_us(clock)
        if current >= fire_at:
            self._append(
                command,
                [NewEvent(EventType.TIMER_FIRED, {"fired_at_us": current}, command, fingerprint)],
            )
            return
        raise WorkflowSuspended(
            "waiting for timer",
            waiting_on={"type": "timer", "fire_at_us": fire_at},
            wake_at_us=fire_at,
        )

    async def wait_signal(self, name: str) -> JsonValue:
        """Wait for an external signal; its payload is journaled when consumed."""
        command, fingerprint, records = self._begin(CommandKind.SIGNAL, name, {})
        received = _find(records, EventType.SIGNAL_RECEIVED)
        if received is not None:
            return received.data().get("payload")
        self._raise_recorded(records)
        self._state.mark_head()
        self._deliver_cancel(command, fingerprint)
        record = self._state.journal.consume_signal(
            self._state.lease, command=command, fingerprint=fingerprint, name=name
        )
        if record is None:
            raise WorkflowSuspended(
                f"waiting for signal {name}", waiting_on={"type": "signal", "name": name}
            )
        self._state.add(command, [record])
        return record.data().get("payload")

    # ------------------------------------------------------------------ sagas and versioning

    def compensate(self, tool: str, args: Mapping[str, JsonValue]) -> None:
        """Register a compensation; on failure or cancellation they run in reverse order."""
        definition = self._state.definition
        if tool not in definition.tools:
            raise ToolNotAllowedError(
                f"tool {tool} is not allowlisted for workflow {definition.name}"
            )
        if self._state.runtime.tools.get(tool).effect is not EffectClass.COMPENSATE:
            raise ToolNotAllowedError(f"tool {tool} is not a compensation tool")
        values = dict(args)
        canonical_json(values)
        self._state.compensations.append((tool, values))

    async def run_compensations(self) -> int:
        """Run registered compensations newest first; returns how many failed."""
        state = self._state
        state.compensating = True
        failed = 0
        while state.compensations:
            tool, args = state.compensations.pop()
            try:
                await self._tool_command(CommandKind.COMPENSATION, tool, args)
            except (ActivityFailedError, ToolNotAllowedError, ToolArgumentError, ToolPolicyError):
                failed += 1
        return failed

    def patched(self, patch_id: str) -> bool:
        """True for runs that reached this point with the patch; False for older histories.

        New code guards changed behaviour with ``if ctx.patched("id"):``. Past the history head the
        marker is recorded and True is returned; in a replayed history it is True only if the
        marker was recorded at this position.
        """
        state = self._state
        command = state.next_command
        records = state.commands.get(command)
        fingerprint = command_fingerprint(CommandKind.PATCH, patch_id, {})
        if records:
            if records[0].type == EventType.PATCH_MARKER and records[0].fingerprint == fingerprint:
                state.next_command += 1
                return True
            return False
        if state.strict:
            return False
        state.mark_head()
        state.next_command += 1
        self._append(
            command,
            [NewEvent(EventType.PATCH_MARKER, {"patch_id": patch_id}, command, fingerprint)],
        )
        return True

    def ensure_history_consumed(self) -> None:
        """When the workflow finishes, recorded commands it did not re-issue mean it diverged."""
        state = self._state
        for command in range(state.next_command, state.recorded_commands):
            records = state.commands.get(command, [])
            if records and records[0].type != EventType.PATCH_MARKER:
                raise NonDeterminismError(
                    self.run_id,
                    command,
                    "workflow finished but the history records more commands",
                )

    def ensure_only_compensations_remain(self) -> None:
        """Called when workflow code raised or was cancelled, before anything is compensated.

        The only commands an earlier activation can have recorded after the point where the same
        code raised are the compensations it ran for that failure. Any other recorded command means
        the code now raises where it used to continue, so the run diverged: it must be blocked, not
        failed and rolled back.
        """
        state = self._state
        for command in range(state.next_command, state.recorded_commands):
            records = state.commands.get(command, [])
            if not records or records[0].type == EventType.PATCH_MARKER:
                continue
            if records[0].data().get("kind") != CommandKind.COMPENSATION.value:
                raise NonDeterminismError(
                    self.run_id,
                    command,
                    "workflow raised but the history records later commands",
                )
