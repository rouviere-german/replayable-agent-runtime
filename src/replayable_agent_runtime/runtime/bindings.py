"""Columns outside the ``events`` table that must agree with the hash chain.

The chain proves what the journal events say. Some of the same facts are also stored in mutable
columns for querying and locking: the ``runs`` row repeats ``RunStarted``'s workflow, input and
starter, and each ``approvals`` row repeats its ``ApprovalRequested`` event (and, once recorded,
its ``ApprovalGranted`` / ``ApprovalDenied`` event). A verified chain says nothing about those
columns, so an edited ``runs.input_json`` or ``approvals.requested_by`` would otherwise be trusted.

These checks compare the columns with the chained events. The engine runs on the chained values
and blocks a run whose ``runs`` row disagrees; approval decisions check the chained starter and
request; ``Journal.verify_run`` (``rar audit verify``) and the tamper suite report any mismatch.

An approval *decision* that was never journaled (a row flipped to ``approved`` in the database) is
not bound by anything a database writer cannot also rewrite; see docs/THREAT_MODEL.md.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from replayable_agent_runtime.agentic.audit import ChainRecord
from replayable_agent_runtime.core.canonical import CanonicalJSONError, canonical_json

__all__ = [
    "ApprovalColumns",
    "BindingError",
    "RunColumns",
    "approval_binding_error",
    "binding_error",
    "run_binding_error",
]

_RUN_STARTED = "RunStarted"
_REQUESTED = "ApprovalRequested"
_DECIDED = {"ApprovalGranted": "approved", "ApprovalDenied": "denied"}


@dataclass(frozen=True, slots=True)
class RunColumns:
    """The ``runs`` columns that repeat ``RunStarted``. ``input_json`` is the stored text."""

    workflow: str
    input_json: str
    started_by: str


@dataclass(frozen=True, slots=True)
class ApprovalColumns:
    """One ``approvals`` row as stored. ``args_json`` is the stored text."""

    approval_id: str
    run_id: str
    seq: int
    command: int
    subject_kind: str
    name: str
    args_json: str
    call_hash: str
    requested_by: str
    status: str
    decided_by: str | None


@dataclass(frozen=True, slots=True)
class BindingError:
    message: str
    seq: int | None = None


def _same_json(text: str, value: Any) -> bool:
    try:
        return canonical_json(json.loads(text)) == canonical_json(value)
    except (ValueError, CanonicalJSONError):
        return False


def _payload(record: ChainRecord) -> dict[str, Any] | None:
    try:
        return record.data()
    except (ValueError, TypeError):
        return None


def run_binding_error(history: Sequence[ChainRecord], run: RunColumns) -> BindingError | None:
    """Check the ``runs`` row against the chained ``RunStarted`` event (seq 0)."""
    if not history or history[0].type != _RUN_STARTED:
        return BindingError("history does not start with RunStarted", 0)
    data = _payload(history[0])
    if data is None:
        return BindingError("RunStarted payload is malformed", 0)
    if run.workflow != data.get("workflow"):
        return BindingError("runs.workflow differs from RunStarted", 0)
    if run.started_by != data.get("started_by"):
        return BindingError("runs.started_by differs from RunStarted", 0)
    if not _same_json(run.input_json, data.get("input")):
        return BindingError("runs.input_json differs from RunStarted", 0)
    return None


def approval_binding_error(
    history: Sequence[ChainRecord], approvals: Iterable[ApprovalColumns]
) -> BindingError | None:
    """Check every approval row against its chained request and any journaled decision."""
    by_seq = {record.seq: record for record in history}
    rows: dict[str, ApprovalColumns] = {}
    for row in approvals:
        rows[row.approval_id] = row
        prefix = f"approval {row.approval_id}"
        record = by_seq.get(row.seq)
        if record is None or record.type != _REQUESTED or record.command != row.command:
            return BindingError(
                f"{prefix}: no journaled request at the row's seq and command", row.seq
            )
        data = _payload(record)
        if data is None:
            return BindingError(f"{prefix}: journaled request is malformed", row.seq)
        expected = {
            "approval_id": row.approval_id,
            "subject_kind": row.subject_kind,
            "name": row.name,
            "call_hash": row.call_hash,
            "requested_by": row.requested_by,
        }
        for key, value in expected.items():
            if data.get(key) != value:
                return BindingError(f"{prefix}: {key} differs from the journaled request", row.seq)
        if not _same_json(row.args_json, data.get("args")):
            return BindingError(f"{prefix}: args differ from the journaled request", row.seq)
    for record in history:
        if record.type != _REQUESTED and record.type not in _DECIDED:
            continue
        data = _payload(record) or {}
        approval_id = str(data.get("approval_id"))
        found = rows.get(approval_id)
        if found is None:
            return BindingError(
                f"approval {approval_id}: no approval row for the journaled event", record.seq
            )
        if record.type in _DECIDED and (
            found.status != _DECIDED[record.type] or found.decided_by != data.get("decided_by")
        ):
            return BindingError(
                f"approval {approval_id}: decision differs from the journaled decision", record.seq
            )
    return None


def binding_error(
    history: Sequence[ChainRecord], run: RunColumns, approvals: Iterable[ApprovalColumns]
) -> BindingError | None:
    return run_binding_error(history, run) or approval_binding_error(history, approvals)
