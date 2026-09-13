"""Approval rules: what an approval is bound to and when a decision is valid.

An approval authorizes exactly one subject: a tool call (tool name plus validated, canonical
arguments) or a named gate with details. The subject is identified by its canonical SHA-256
``call_hash``. The durable store lives in the journal database (see
:mod:`replayable_agent_runtime.runtime.journal`); this module holds the storage-independent rules
it enforces:

- the approver must be a different principal from the one who started the run;
- the approver must present the ``call_hash`` they reviewed, and it must equal the pending
  request's hash, so changing any argument invalidates the approval;
- a stored request must still hash to its stored arguments, and must match the hash-chained
  ``ApprovalRequested`` journal event, so an edited approvals row cannot release a different call;
- a request is decided once.
"""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from pydantic import JsonValue

from replayable_agent_runtime.agentic.errors import ApprovalError, ApprovalIntegrityError
from replayable_agent_runtime.agentic.tools import tool_call_hash
from replayable_agent_runtime.core.canonical import canonical_json, sha256_hex

__all__ = [
    "ApprovalRequest",
    "ApprovalStatus",
    "SubjectKind",
    "check_decision",
    "subject_hash",
    "tool_call_hash",
]


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"


class SubjectKind(StrEnum):
    TOOL = "tool"
    GATE = "gate"


def subject_hash(kind: SubjectKind, name: str, args: Mapping[str, JsonValue]) -> str:
    """Canonical hash of what an approval releases."""
    if kind is SubjectKind.TOOL:
        return tool_call_hash(name, args)
    return sha256_hex(canonical_json({"v": 1, "gate": name, "details": dict(args)}))


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    approval_id: str
    run_id: str
    seq: int
    command: int
    kind: SubjectKind
    name: str
    args: dict[str, JsonValue]
    call_hash: str
    requested_by: str
    status: ApprovalStatus
    decided_by: str | None = None
    reason: str | None = None

    def verify_integrity(self, journal_call_hash: str | None = None) -> None:
        """Raise if the stored arguments, hash and journal event disagree."""
        if subject_hash(self.kind, self.name, self.args) != self.call_hash:
            raise ApprovalIntegrityError(
                f"approval {self.approval_id}: stored arguments do not match its call hash"
            )
        if journal_call_hash is not None and not hmac.compare_digest(
            journal_call_hash, self.call_hash
        ):
            raise ApprovalIntegrityError(
                f"approval {self.approval_id}: call hash differs from the journaled request"
            )


def check_decision(
    request: ApprovalRequest,
    *,
    approver: str,
    presented_hash: str,
    journal_call_hash: str,
) -> None:
    """Validate a decision before it is stored. Raises ApprovalError on any violation."""
    if request.status is not ApprovalStatus.PENDING:
        raise ApprovalError(
            f"approval {request.approval_id} is {request.status.value}, not pending"
        )
    request.verify_integrity(journal_call_hash)
    if not hmac.compare_digest(presented_hash.encode(), request.call_hash.encode()):
        raise ApprovalError(
            f"approval {request.approval_id}: presented call hash does not match the pending call"
        )
    if approver == request.requested_by:
        raise ApprovalError("separation of duties: the run starter cannot approve its own run")
