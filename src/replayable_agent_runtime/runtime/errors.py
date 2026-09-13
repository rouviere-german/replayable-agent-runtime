"""Runtime errors.

Control-flow signals (suspension, cancellation, nondeterminism, lost leases, history overflow)
derive from :class:`RuntimeSignal`, a ``BaseException``, for the same reason
``asyncio.CancelledError`` does: workflow code that catches ``Exception`` must not be able to
swallow them and keep issuing commands.
"""

from __future__ import annotations

from pydantic import JsonValue

from replayable_agent_runtime.agentic.errors import AgenticError, AuditIntegrityError

__all__ = [
    "FencedOutError",
    "HistoryLimitExceededError",
    "IntegrityHalt",
    "JournalError",
    "JournalIntegrityError",
    "NonDeterminismError",
    "RunNotFoundError",
    "RunStateError",
    "RuntimeSignal",
    "WorkflowCancelled",
    "WorkflowSuspended",
]


class RuntimeSignal(BaseException):
    """Base for runtime control flow that user workflow code must not catch."""


class WorkflowSuspended(RuntimeSignal):
    """The workflow must wait (timer, signal or approval); the activation ends here."""

    def __init__(
        self, reason: str, *, waiting_on: dict[str, JsonValue], wake_at_us: int | None = None
    ) -> None:
        super().__init__(reason)
        self.waiting_on = waiting_on
        self.wake_at_us = wake_at_us


class WorkflowCancelled(RuntimeSignal):
    """Cancellation was delivered at a command; compensations run next."""


class NonDeterminismError(RuntimeSignal):
    """Replayed workflow code issued a command that differs from the recorded history."""

    def __init__(self, run_id: str, command: int, message: str) -> None:
        super().__init__(f"run {run_id} command {command}: {message}")
        self.run_id = run_id
        self.command = command


class FencedOutError(RuntimeSignal):
    """This worker's lease was reclaimed; its writes are refused and it must stop."""


class IntegrityHalt(RuntimeSignal):
    """Stored state disagrees with the hash-chained journal (for example an edited approval).

    Nothing is executed; the run is blocked for an operator.
    """


class HistoryLimitExceededError(RuntimeSignal):
    """The run's history would exceed the configured event cap; the run fails loudly."""

    def __init__(self, run_id: str, limit: int) -> None:
        super().__init__(f"run {run_id} exceeded the history limit of {limit} events")
        self.run_id = run_id
        self.limit = limit


class JournalError(AgenticError):
    """Base for journal errors that callers handle."""


class RunNotFoundError(JournalError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} not found")
        self.run_id = run_id


class RunStateError(JournalError):
    """The run is not in a state that allows the operation (for example, already terminal)."""


class JournalIntegrityError(AuditIntegrityError):
    """A run's hash chain or head anchor failed verification; the run is not replayed."""
