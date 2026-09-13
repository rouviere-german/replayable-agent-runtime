"""Runtime wiring and one workflow activation (replay to the head, then run live)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

from opentelemetry.trace import NoOpTracer, Tracer
from pydantic import JsonValue

from replayable_agent_runtime.agentic.audit import verify_chain
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.base import LLMProvider
from replayable_agent_runtime.agentic.tools import ToolRegistry
from replayable_agent_runtime.core.canonical import CanonicalJSONError, canonical_json
from replayable_agent_runtime.core.logging import get_logger
from replayable_agent_runtime.core.redaction import redact_text
from replayable_agent_runtime.runtime.context import ActivationState, WorkflowContext
from replayable_agent_runtime.runtime.db import Clock
from replayable_agent_runtime.runtime.errors import (
    FencedOutError,
    HistoryLimitExceededError,
    IntegrityHalt,
    NonDeterminismError,
    WorkflowCancelled,
    WorkflowSuspended,
)
from replayable_agent_runtime.runtime.events import TERMINAL_EVENTS, EventType, NewEvent
from replayable_agent_runtime.runtime.journal import Claim, Journal, Lease, RunStatus
from replayable_agent_runtime.runtime.telemetry import RuntimeMetrics, create_runtime_metrics
from replayable_agent_runtime.runtime.workflows import UnknownWorkflowError, WorkflowRegistry

__all__ = ["ActivationOutcome", "Hooks", "Outcome", "Runtime", "activate", "replay_check"]

Outcome = Literal["completed", "failed", "cancelled", "waiting", "pending", "blocked", "lease_lost"]
_log = get_logger("replayable_agent_runtime.engine")


class Hooks:
    """Test seams. ``after_effect`` runs after an external call returns and before its
    completion is journaled, which is exactly the at-least-once crash window."""

    def after_effect(self, kind: str, key: str) -> None:
        del kind, key


@dataclass
class Runtime:
    journal: Journal
    workflows: WorkflowRegistry
    tools: ToolRegistry
    provider: LLMProvider
    clock: Clock = time.time
    metrics: RuntimeMetrics = field(default_factory=create_runtime_metrics)
    tracer: Tracer = field(default_factory=NoOpTracer)
    hooks: Hooks = field(default_factory=Hooks)
    budget_cap: BudgetLimits | None = None
    max_output_bytes: int = 64_000


@dataclass(frozen=True, slots=True)
class ActivationOutcome:
    run_id: str
    outcome: Outcome
    fencing: int
    detail: str | None = None
    replay_seconds: float | None = None


def _block(runtime: Runtime, claim: Claim, reason: str) -> ActivationOutcome:
    try:
        runtime.journal.block(claim.lease, reason)
    except FencedOutError:
        return ActivationOutcome(claim.lease.run_id, "lease_lost", claim.lease.fencing)
    _log.error("run_blocked", run_id=claim.lease.run_id, reason=reason[:300])
    return ActivationOutcome(claim.lease.run_id, "blocked", claim.lease.fencing, reason)


async def activate(runtime: Runtime, claim: Claim) -> ActivationOutcome:
    """Replay the run's history through its workflow code, then continue live until it waits,
    finishes, is fenced out or must be blocked."""
    lease = claim.lease
    run = claim.run
    with runtime.tracer.start_as_current_span("rar.activation") as span:
        span.set_attribute("rar.run_id", lease.run_id)
        span.set_attribute("rar.fencing", lease.fencing)
        span.set_attribute("rar.reclaimed", claim.reclaimed)
        history = runtime.journal.load_history(lease.run_id)
        verified = verify_chain(
            history, run_id=lease.run_id, expect_head=(run.head_seq, run.head_hash)
        )
        if not verified.ok:
            return _block(
                runtime, claim, f"journal integrity: {verified.error} (seq {verified.seq})"
            )
        if any(record.type in TERMINAL_EVENTS for record in history):
            return _block(runtime, claim, "run has a terminal event but is not closed")
        try:
            definition = runtime.workflows.get(run.workflow)
        except UnknownWorkflowError as exc:
            return _block(runtime, claim, str(exc))
        state = ActivationState(runtime, claim, history, definition)
        ctx = WorkflowContext(state)
        return await _drive(runtime, claim, state, ctx)


async def _drive(
    runtime: Runtime, claim: Claim, state: ActivationState, ctx: WorkflowContext
) -> ActivationOutcome:
    lease = claim.lease
    journal = runtime.journal
    run_id = lease.run_id

    def outcome(kind: Outcome, detail: str | None = None) -> ActivationOutcome:
        return ActivationOutcome(run_id, kind, lease.fencing, detail, state.replay_seconds)

    try:
        try:
            result = await state.definition.fn(ctx, claim.run.input)
        except WorkflowCancelled:
            failed = await ctx.run_compensations()
            state.mark_head()
            journal.finish(
                lease,
                NewEvent(EventType.RUN_CANCELLED, {"compensations_failed": failed}),
                error={"error_type": "WorkflowCancelled", "compensations_failed": failed},
            )
            return outcome("cancelled")
        except Exception as exc:  # noqa: BLE001 - a workflow exception fails the run
            failed = await ctx.run_compensations()
            state.mark_head()
            message = redact_text(str(exc))[:500]
            error: dict[str, JsonValue] = {
                "error_type": type(exc).__name__,
                "message": message,
                "compensations_failed": failed,
            }
            journal.finish(lease, NewEvent(EventType.RUN_FAILED, error), error=error)
            return outcome("failed", type(exc).__name__)
        ctx.ensure_history_consumed()
        state.mark_head()
        try:
            canonical_json(result)
        except CanonicalJSONError:
            invalid: dict[str, JsonValue] = {
                "error_type": "InvalidResult",
                "message": "workflow result is not JSON",
            }
            journal.finish(lease, NewEvent(EventType.RUN_FAILED, invalid), error=invalid)
            return outcome("failed", "InvalidResult")
        journal.finish(lease, NewEvent(EventType.RUN_COMPLETED, {"result": result}), result=result)
        return outcome("completed")
    except WorkflowSuspended as suspended:
        status = journal.suspend(
            lease,
            waiting_on=suspended.waiting_on,
            wake_at_us=suspended.wake_at_us,
            seen_inbox_version=claim.run.inbox_version,
        )
        return outcome("pending" if status is RunStatus.PENDING else "waiting", str(suspended))
    except NonDeterminismError as exc:
        runtime.metrics.nondeterminism_errors_total.inc()
        result_block = _block(runtime, claim, f"nondeterminism: {exc}")
        return outcome(result_block.outcome, result_block.detail)
    except IntegrityHalt as exc:
        result_block = _block(runtime, claim, f"integrity: {exc}")
        return outcome(result_block.outcome, result_block.detail)
    except HistoryLimitExceededError as exc:
        overflow: dict[str, JsonValue] = {
            "error_type": "HistoryLimitExceededError",
            "message": str(exc),
        }
        _log.error("history_limit_exceeded", run_id=run_id, limit=exc.limit)
        try:
            journal.finish(lease, NewEvent(EventType.RUN_FAILED, overflow), error=overflow)
        except FencedOutError:
            return outcome("lease_lost")
        return outcome("failed", "HistoryLimitExceededError")
    except FencedOutError as exc:
        _log.warning("lease_lost", run_id=run_id, fencing=lease.fencing, detail=str(exc)[:200])
        return outcome("lease_lost", str(exc))


async def replay_check(runtime: Runtime, run_id: str) -> ActivationOutcome:
    """Strictly replay a run's recorded history through the current workflow code.

    No command may run live and nothing is written. Returns ``completed`` if the code reproduces
    the history exactly; raises :class:`NonDeterminismError` otherwise.
    """
    journal = runtime.journal
    run = journal.get_run(run_id)
    history = journal.load_history(run_id)
    definition = runtime.workflows.get(run.workflow)
    claim = Claim(
        lease=Lease(run_id=run_id, owner="replay-check", fencing=-1), run=run, reclaimed=False
    )
    state = ActivationState(runtime, claim, history, definition, strict=True)
    ctx = WorkflowContext(state)
    try:
        try:
            await definition.fn(ctx, run.input)
        except (WorkflowCancelled, Exception) as exc:  # noqa: BLE001 - recorded failures replay too
            # The engine compensated after this failure; replay those commands too.
            await ctx.run_compensations()
            ctx.ensure_history_consumed()
            return ActivationOutcome(run_id, "failed", 0, type(exc).__name__)
    except WorkflowSuspended as suspended:
        return ActivationOutcome(run_id, "waiting", 0, str(suspended))
    ctx.ensure_history_consumed()
    return ActivationOutcome(run_id, "completed", 0)
