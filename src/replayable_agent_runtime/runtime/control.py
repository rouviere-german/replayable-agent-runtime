"""Control-plane operations shared by the HTTP API and the CLI."""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import JsonValue

from replayable_agent_runtime.agentic.approval import ApprovalRequest, ApprovalStatus
from replayable_agent_runtime.agentic.audit import VerifyResult
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.errors import AgenticError
from replayable_agent_runtime.core.canonical import CanonicalJSONError, canonical_json
from replayable_agent_runtime.core.redaction import redact_value
from replayable_agent_runtime.runtime.errors import RunStateError
from replayable_agent_runtime.runtime.journal import Journal, RunRow, RunStatus
from replayable_agent_runtime.runtime.workflows import UnknownWorkflowError, WorkflowRegistry

__all__ = ["ControlPlane", "InputTooLargeError", "approval_view", "event_view"]


class InputTooLargeError(AgenticError):
    """Workflow input or signal payload exceeds the configured size limit."""


def event_view(record: Any, *, redact: bool) -> dict[str, JsonValue]:
    data = record.data()
    return {
        "seq": record.seq,
        "command": record.command,
        "type": record.type,
        "fingerprint": record.fingerprint,
        "recorded_at_us": record.recorded_at_us,
        "payload": redact_value(data) if redact else data,
        "hash": record.hash,
    }


def approval_view(request: ApprovalRequest, *, redact: bool = True) -> dict[str, JsonValue]:
    return {
        "approval_id": request.approval_id,
        "run_id": request.run_id,
        "kind": request.kind.value,
        "name": request.name,
        "args": redact_value(request.args) if redact else request.args,
        "call_hash": request.call_hash,
        "requested_by": request.requested_by,
        "status": request.status.value,
        "decided_by": request.decided_by,
    }


class ControlPlane:
    def __init__(
        self,
        journal: Journal,
        workflows: WorkflowRegistry,
        *,
        default_budget: BudgetLimits,
        max_input_bytes: int = 65_536,
    ) -> None:
        self.journal = journal
        self.workflows = workflows
        self.default_budget = default_budget
        self.max_input_bytes = max_input_bytes

    def _check_size(self, value: JsonValue, what: str) -> str:
        try:
            text = canonical_json(value)
        except CanonicalJSONError as exc:
            raise InputTooLargeError(f"{what} must be JSON") from exc
        if len(text.encode("utf-8")) > self.max_input_bytes:
            raise InputTooLargeError(f"{what} exceeds {self.max_input_bytes} bytes")
        return text

    def start_run(
        self,
        *,
        workflow: str,
        input_value: JsonValue,
        principal: str,
        run_id: str | None = None,
        budget: BudgetLimits | None = None,
    ) -> tuple[RunRow, bool]:
        """Start a run. Returns ``(run, created)``; an identical retry returns the same run."""
        if workflow not in self.workflows:
            raise UnknownWorkflowError(workflow)
        text = self._check_size(input_value, "input")
        limits = self.default_budget if budget is None else budget.tightest(self.default_budget)
        chosen = run_id or uuid.uuid4().hex
        try:
            run = self.journal.create_run(
                run_id=chosen,
                workflow=workflow,
                input_value=input_value,
                started_by=principal,
                budget=limits,
            )
        except RunStateError:
            existing = self.journal.get_run(chosen)
            same = (
                existing.workflow == workflow
                and canonical_json(existing.input) == text
                and existing.started_by == principal
            )
            if not same:
                raise
            return existing, False
        return run, True

    def signal(self, run_id: str, name: str, payload: JsonValue, *, principal: str) -> int:
        self._check_size(payload, "signal payload")
        return self.journal.send_signal(run_id, name, payload, sent_by=principal)

    def cancel(self, run_id: str) -> RunRow:
        return self.journal.request_cancel(run_id)

    def decide(
        self,
        approval_id: str,
        *,
        principal: str,
        approve: bool,
        call_hash: str,
        reason: str | None = None,
    ) -> ApprovalRequest:
        return self.journal.decide_approval(
            approval_id,
            approver=principal,
            approve=approve,
            presented_hash=call_hash,
            reason=reason,
        )

    def get(self, run_id: str) -> RunRow:
        return self.journal.get_run(run_id)

    def runs(self, status: RunStatus | None = None, limit: int = 100) -> list[RunRow]:
        return self.journal.list_runs(status=status, limit=limit)

    def history(self, run_id: str, *, redact: bool = True) -> list[dict[str, JsonValue]]:
        self.journal.get_run(run_id)
        return [event_view(record, redact=redact) for record in self.journal.load_history(run_id)]

    def approvals(
        self, *, run_id: str | None = None, status: ApprovalStatus | None = None
    ) -> list[ApprovalRequest]:
        return self.journal.list_approvals(run_id=run_id, status=status)

    def verify(self, run_id: str, *, expect_head: str | None = None) -> VerifyResult:
        return self.journal.verify_run(run_id, expect_head=expect_head)
