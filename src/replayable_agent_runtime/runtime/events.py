"""Typed journal events and command fingerprints.

A workflow issues *commands* through its context (``ctx.llm``, ``ctx.tool``, ``ctx.sleep``...).
Commands are numbered 0, 1, 2... in the order the workflow issues them, and every event a command
produces carries that number and the command's *fingerprint*: the SHA-256 of the command kind,
name and canonical arguments. Replay compares the fingerprint of each re-issued command with the
recorded one, so any change in what the workflow asks for is detected as nondeterminism.

Run-level events (``RunStarted``, terminal events) use command ``-1`` and an empty fingerprint.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from replayable_agent_runtime.agentic.audit import ChainRecord
from replayable_agent_runtime.core.canonical import canonical_json, sha256_hex

__all__ = [
    "PAYLOAD_MODELS",
    "RUN_LEVEL",
    "TERMINAL_EVENTS",
    "CommandKind",
    "EventRecord",
    "EventType",
    "NewEvent",
    "command_fingerprint",
    "validate_payload",
]

RUN_LEVEL = -1
EventRecord = ChainRecord


class EventType(StrEnum):
    RUN_STARTED = "RunStarted"
    STEP_SCHEDULED = "StepScheduled"
    STEP_COMPLETED = "StepCompleted"
    STEP_FAILED = "StepFailed"
    COMMAND_REJECTED = "CommandRejected"
    TIMER_SCHEDULED = "TimerScheduled"
    TIMER_FIRED = "TimerFired"
    SIGNAL_RECEIVED = "SignalReceived"
    APPROVAL_REQUESTED = "ApprovalRequested"
    APPROVAL_GRANTED = "ApprovalGranted"
    APPROVAL_DENIED = "ApprovalDenied"
    PATCH_MARKER = "PatchMarker"
    CANCEL_REQUESTED = "CancelRequested"
    RUN_COMPLETED = "RunCompleted"
    RUN_FAILED = "RunFailed"
    RUN_CANCELLED = "RunCancelled"


TERMINAL_EVENTS = frozenset(
    {EventType.RUN_COMPLETED, EventType.RUN_FAILED, EventType.RUN_CANCELLED}
)


class CommandKind(StrEnum):
    LLM = "llm"
    TOOL = "tool"
    COMPENSATION = "compensation"
    NOW = "now"
    UUID = "uuid"
    SLEEP = "sleep"
    SIGNAL = "signal"
    APPROVAL = "approval"
    PATCH = "patch"


def command_fingerprint(kind: CommandKind, name: str, args: JsonValue) -> str:
    """SHA-256 of ``{"v": 1, "kind", "name", "args"}`` in canonical JSON."""
    return sha256_hex(canonical_json({"v": 1, "kind": kind.value, "name": name, "args": args}))


@dataclass(frozen=True, slots=True)
class NewEvent:
    type: EventType
    payload: dict[str, JsonValue]
    command: int = RUN_LEVEL
    fingerprint: str = ""


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _UsagePayload(_Payload):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class RunStartedPayload(_Payload):
    workflow: str
    input: JsonValue
    started_by: str
    budget: dict[str, int]
    max_history_events: int = Field(ge=1)


class StepScheduledPayload(_Payload):
    """The step is charged and about to run. Its idempotency key is ``run_id:seq`` of this event."""

    kind: CommandKind
    name: str
    call_hash: str | None = None
    effect: str | None = None
    request_sha256: str | None = None


class StepCompletedPayload(_Payload):
    kind: CommandKind
    name: str
    output: JsonValue = None
    # An applied side effect whose output exceeded the size limit: its size and SHA-256 instead.
    output_omitted: dict[str, JsonValue] | None = None
    model: str | None = None
    usage: _UsagePayload | None = None
    cost_usd_micros: int = Field(default=0, ge=0)


class StepFailedPayload(_Payload):
    kind: CommandKind
    name: str
    error_type: str
    message: str
    retryable: bool = False
    model: str | None = None
    usage: _UsagePayload | None = None
    cost_usd_micros: int = Field(default=0, ge=0)
    details: dict[str, JsonValue] = Field(default_factory=dict)


class CommandRejectedPayload(_Payload):
    kind: CommandKind
    name: str
    error_type: str
    message: str
    details: dict[str, JsonValue] = Field(default_factory=dict)


class TimerScheduledPayload(_Payload):
    fire_at_us: int
    duration_us: int = Field(ge=0)


class TimerFiredPayload(_Payload):
    fired_at_us: int


class SignalReceivedPayload(_Payload):
    signal_id: int
    name: str
    payload: JsonValue
    sent_by: str


class ApprovalRequestedPayload(_Payload):
    approval_id: str
    subject_kind: str
    name: str
    args: dict[str, JsonValue]
    call_hash: str
    requested_by: str


class ApprovalDecidedPayload(_Payload):
    approval_id: str
    call_hash: str
    decided_by: str
    reason: str | None = None


class PatchMarkerPayload(_Payload):
    patch_id: str


class CancelRequestedPayload(_Payload):
    requested_by: str | None = None


class RunCompletedPayload(_Payload):
    result: JsonValue


class RunFailedPayload(_Payload):
    error_type: str
    message: str
    compensations_failed: int = Field(default=0, ge=0)
    # For history overflow: the command whose events did not fit (strict replay reproduces it).
    command: int | None = Field(default=None, ge=0)


class RunCancelledPayload(_Payload):
    compensations_failed: int = Field(default=0, ge=0)


PAYLOAD_MODELS: dict[EventType, type[_Payload]] = {
    EventType.RUN_STARTED: RunStartedPayload,
    EventType.STEP_SCHEDULED: StepScheduledPayload,
    EventType.STEP_COMPLETED: StepCompletedPayload,
    EventType.STEP_FAILED: StepFailedPayload,
    EventType.COMMAND_REJECTED: CommandRejectedPayload,
    EventType.TIMER_SCHEDULED: TimerScheduledPayload,
    EventType.TIMER_FIRED: TimerFiredPayload,
    EventType.SIGNAL_RECEIVED: SignalReceivedPayload,
    EventType.APPROVAL_REQUESTED: ApprovalRequestedPayload,
    EventType.APPROVAL_GRANTED: ApprovalDecidedPayload,
    EventType.APPROVAL_DENIED: ApprovalDecidedPayload,
    EventType.PATCH_MARKER: PatchMarkerPayload,
    EventType.CANCEL_REQUESTED: CancelRequestedPayload,
    EventType.RUN_COMPLETED: RunCompletedPayload,
    EventType.RUN_FAILED: RunFailedPayload,
    EventType.RUN_CANCELLED: RunCancelledPayload,
}


def validate_payload(event_type: EventType, payload: dict[str, JsonValue]) -> None:
    """Raise ``pydantic.ValidationError`` if ``payload`` does not fit the event type."""
    PAYLOAD_MODELS[event_type].model_validate(payload)
