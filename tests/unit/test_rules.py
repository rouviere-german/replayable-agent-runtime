"""Storage-independent rules: approvals, the hash chain and exports, events, registries, errors."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from pydantic import ValidationError

from replayable_agent_runtime.agentic.approval import (
    ApprovalRequest,
    ApprovalStatus,
    SubjectKind,
    check_decision,
    subject_hash,
)
from replayable_agent_runtime.agentic.audit import (
    GENESIS_HASH,
    ChainRecord,
    event_hash,
    export_lines,
    payload_digest,
    verify_chain,
    verify_export,
)
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.errors import (
    ActivityFailedError,
    ApprovalDeniedError,
    ApprovalError,
    ApprovalIntegrityError,
    BudgetExceededError,
    ProviderAuthError,
    ProviderError,
    ProviderRateLimitedError,
    ProviderRefusalError,
    ProviderUnavailableError,
    ToolArgumentError,
    ToolNotAllowedError,
    ToolPolicyError,
)
from replayable_agent_runtime.agentic.tools import Capability, ToolRegistry, tool_call_hash
from replayable_agent_runtime.core.canonical import canonical_json
from replayable_agent_runtime.runtime.budget import RunBudget, fold_budget
from replayable_agent_runtime.runtime.context import rebuild_error
from replayable_agent_runtime.runtime.events import (
    CommandKind,
    EventType,
    command_fingerprint,
    validate_payload,
)
from replayable_agent_runtime.runtime.workflows import (
    UnknownWorkflowError,
    WorkflowDefinition,
    WorkflowRegistry,
)

pytestmark = pytest.mark.unit

TICKET_HASH = tool_call_hash("create_ticket", {"title": "t"})


def pending_request(**overrides: object) -> ApprovalRequest:
    request = ApprovalRequest(
        approval_id="run-1:4",
        run_id="run-1",
        seq=8,
        command=4,
        kind=SubjectKind.TOOL,
        name="create_ticket",
        args={"title": "t"},
        call_hash=TICKET_HASH,
        requested_by="alice",
        status=ApprovalStatus.PENDING,
    )
    return replace(request, **overrides)  # type: ignore[arg-type]


def test_decisions_bind_the_hash_the_approver_saw_and_separate_duties() -> None:
    pending = pending_request()
    check_decision(
        pending, approver="bob", presented_hash=TICKET_HASH, journal_call_hash=TICKET_HASH
    )
    with pytest.raises(ApprovalError, match="separation"):
        check_decision(
            pending, approver="alice", presented_hash=TICKET_HASH, journal_call_hash=TICKET_HASH
        )
    with pytest.raises(ApprovalError, match="does not match"):
        check_decision(
            pending, approver="bob", presented_hash="f" * 64, journal_call_hash=TICKET_HASH
        )
    with pytest.raises(ApprovalError, match="not pending"):
        check_decision(
            pending_request(status=ApprovalStatus.APPROVED),
            approver="bob",
            presented_hash=TICKET_HASH,
            journal_call_hash=TICKET_HASH,
        )
    with pytest.raises(ApprovalIntegrityError, match="stored arguments"):
        pending_request(args={"title": "changed"}).verify_integrity()
    with pytest.raises(ApprovalIntegrityError, match="journaled request"):
        pending.verify_integrity("0" * 64)
    gate = subject_hash(SubjectKind.GATE, "deploy", {"env": "prod"})
    assert gate != subject_hash(SubjectKind.TOOL, "deploy", {"env": "prod"})
    assert gate != subject_hash(SubjectKind.GATE, "deploy", {"env": "staging"})


def _chain(count: int, run_id: str = "r") -> list[ChainRecord]:
    records: list[ChainRecord] = []
    prev = GENESIS_HASH
    for seq in range(count):
        payload = canonical_json({"n": seq, "email": "jane@example.com"})
        digest = payload_digest(payload)
        current = event_hash(
            run_id=run_id,
            seq=seq,
            command=seq,
            type="StepCompleted",
            fingerprint="f" * 64,
            payload_sha256=digest,
            recorded_at_us=seq,
            prev_hash=prev,
        )
        records.append(
            ChainRecord(
                run_id=run_id,
                seq=seq,
                command=seq,
                type="StepCompleted",
                fingerprint="f" * 64,
                payload=payload,
                payload_sha256=digest,
                recorded_at_us=seq,
                prev_hash=prev,
                hash=current,
            )
        )
        prev = current
    return records


def test_chain_verification_reports_the_first_failure() -> None:
    records = _chain(4)
    assert verify_chain(records, run_id="r", expect_head=(3, records[-1].hash)).ok
    assert verify_chain(records, run_id="other").error == "event belongs to another run"
    assert verify_chain([records[0], records[2]], run_id="r").error == "sequence gap or reordering"
    broken = [*records[:2], replace(records[2], prev_hash=GENESIS_HASH), records[3]]
    assert verify_chain(broken, run_id="r").error == "broken hash chain"
    tampered = [records[0], replace(records[1], payload='{"n":9}'), *records[2:]]
    assert verify_chain(tampered, run_id="r").error == "payload modified"
    rehashed = [records[0], replace(records[1], recorded_at_us=99), *records[2:]]
    assert verify_chain(rehashed, run_id="r").error == "event content modified"
    assert verify_chain(records, run_id="r", expect_head=(4, "0" * 64)).error is not None
    assert verify_chain(records, run_id="r").as_dict()["entries"] == 4


def test_exports_are_canonical_and_malformed_ones_are_refused() -> None:
    records = _chain(3)
    raw = "".join(export_lines("r", 2, records[-1].hash, records, redact=False)).encode()
    assert verify_export(raw).ok
    lines = raw.split(b"\n")
    assert verify_export(b"").error == "truncated final line"
    assert verify_export(b"{}\n").error == "header: malformed line"
    header = json.loads(lines[0])
    header["format"] = "other"
    unknown = canonical_json(header).encode() + b"\n"
    assert verify_export(unknown).error == "header: unknown export format"
    spaced = lines[0] + b"\n" + lines[1].replace(b":", b": ", 1) + b"\n"
    assert verify_export(spaced).error == "non-canonical encoding"
    assert verify_export(lines[0] + b"\nnot json\n").error == "malformed line"
    redacted = "".join(export_lines("r", 2, records[-1].hash, records, redact=True))
    assert "jane@example.com" not in redacted
    assert verify_export(redacted.encode()).payloads_verified is False


def test_fingerprints_and_payloads_are_typed() -> None:
    base = command_fingerprint(CommandKind.TOOL, "kb_lookup", {"key": "a"})
    assert base == command_fingerprint(CommandKind.TOOL, "kb_lookup", {"key": "a"})
    assert base != command_fingerprint(CommandKind.COMPENSATION, "kb_lookup", {"key": "a"})
    assert base != command_fingerprint(CommandKind.TOOL, "kb_lookup", {"key": "b"})
    validate_payload(EventType.TIMER_SCHEDULED, {"fire_at_us": 5, "duration_us": 1})
    with pytest.raises(ValidationError):
        validate_payload(EventType.TIMER_SCHEDULED, {"fire_at_us": 5, "unexpected": 1})
    with pytest.raises(ValidationError):
        validate_payload(EventType.STEP_SCHEDULED, {"kind": "teleport", "name": "x"})


def test_workflow_registry_validation() -> None:
    async def noop(ctx: object, value: object) -> None:
        return None

    registry = WorkflowRegistry([WorkflowDefinition("one", noop, frozenset({"missing_tool"}))])
    with pytest.raises(ValueError, match="already registered"):
        registry.register(WorkflowDefinition("one", noop))
    with pytest.raises(ValueError, match="invalid workflow name"):
        WorkflowDefinition("Bad-Name", noop)
    with pytest.raises(UnknownWorkflowError):
        registry.get("two")
    assert registry.names == ("one",)
    with pytest.raises(ValueError, match="unknown tools"):
        registry.check_tools(ToolRegistry({Capability.READ_DATA}))


BUDGET_DETAILS = {"dimension": "steps", "limit": 1, "attempted": 2}


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"error_type": "BudgetExceededError", "details": BUDGET_DETAILS}, BudgetExceededError),
        (
            {"error_type": "ApprovalDeniedError", "name": "x", "details": {"decided_by": "b"}},
            ApprovalDeniedError,
        ),
        ({"error_type": "ToolNotAllowedError", "message": "m"}, ToolNotAllowedError),
        ({"error_type": "ToolArgumentError", "message": "m"}, ToolArgumentError),
        ({"error_type": "ToolPolicyError", "message": "m"}, ToolPolicyError),
        ({"error_type": "Weird", "message": "m"}, ActivityFailedError),
        (
            {"kind": "llm", "error_type": "ProviderRefusalError", "details": {"category": "c"}},
            ProviderRefusalError,
        ),
        (
            {"kind": "llm", "error_type": "ProviderRateLimitedError", "message": "m"},
            ProviderRateLimitedError,
        ),
        (
            {"kind": "llm", "error_type": "ProviderUnavailableError", "message": "m"},
            ProviderUnavailableError,
        ),
        ({"kind": "llm", "error_type": "ProviderAuthError", "message": "m"}, ProviderAuthError),
        ({"kind": "llm", "error_type": "ProviderError", "retryable": True}, ProviderError),
        ({"kind": "llm", "error_type": "KeyError", "message": "m"}, ActivityFailedError),
    ],
)
def test_recorded_failures_rebuild_the_exception_type_workflow_code_saw(
    data: dict[str, object], expected: type[Exception]
) -> None:
    assert type(rebuild_error(data)) is expected


def test_the_budget_fold_charges_each_scheduled_step_once() -> None:
    budget = RunBudget(limits=BudgetLimits.of(steps=1, tokens=100, usd=1.0))
    budget.check_step()
    base = _chain(1)[0]
    scheduled = replace(base, type="StepScheduled", payload='{"kind":"llm","name":"default"}')
    budget.apply(scheduled)
    budget.apply(scheduled)
    assert budget.steps == 1
    with pytest.raises(BudgetExceededError, match="steps"):
        budget.check_step()
    usage_payload = canonical_json(
        {
            "kind": "llm",
            "name": "default",
            "usage": {"input_tokens": 90, "output_tokens": 20},
            "cost_usd_micros": 5,
        }
    )
    usage = replace(base, seq=1, type="StepCompleted", payload=usage_payload)
    folded = fold_budget(BudgetLimits.of(steps=10, tokens=100, usd=1.0), [scheduled, usage])
    assert (folded.steps, folded.state.tokens, folded.usd_micros) == (1, 110, 5)
    with pytest.raises(BudgetExceededError, match="tokens"):
        folded.check_step()
