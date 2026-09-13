"""Hash chain over journal events, JSONL export and verification.

Every journal event commits to its content and to its predecessor:

``hash = sha256(canonical_json({v, run_id, seq, command, type, fingerprint, payload_sha256,
recorded_at_us, prev_hash}))`` with ``payload_sha256 = sha256(payload text)`` and ``prev_hash`` of
seq 0 equal to 64 zeros. Committing to the payload digest (rather than the payload itself) lets a
redacted export still prove the chain linkage; a full export additionally proves every payload.

Verification detects modified fields, deleted, inserted and reordered events, and re-encoded export
lines. Truncating the tail leaves a valid shorter chain, so the ``runs`` table stores the head
(seq and hash) in the same transaction as every append, exports carry that head, and callers that
distrust the database pass an externally anchored head with ``--expect-head``.

Usage: ``rar audit verify --db var/rar.db`` or ``rar audit verify-export run.jsonl``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from replayable_agent_runtime.core.canonical import canonical_json, sha256_hex
from replayable_agent_runtime.core.redaction import redact_value

__all__ = [
    "EXPORT_FORMAT",
    "GENESIS_HASH",
    "ChainRecord",
    "VerifyResult",
    "event_hash",
    "export_lines",
    "payload_digest",
    "verify_chain",
    "verify_export",
]

GENESIS_HASH = "0" * 64
EXPORT_FORMAT = "rar-journal-export"
_HEX64 = r"^[0-9a-f]{64}$"


def payload_digest(payload: str) -> str:
    return sha256_hex(payload)


def event_hash(
    *,
    run_id: str,
    seq: int,
    command: int,
    type: str,
    fingerprint: str,
    payload_sha256: str,
    recorded_at_us: int,
    prev_hash: str,
) -> str:
    return sha256_hex(
        canonical_json(
            {
                "v": 1,
                "run_id": run_id,
                "seq": seq,
                "command": command,
                "type": type,
                "fingerprint": fingerprint,
                "payload_sha256": payload_sha256,
                "recorded_at_us": recorded_at_us,
                "prev_hash": prev_hash,
            }
        )
    )


@dataclass(frozen=True, slots=True)
class ChainRecord:
    """One journal event as stored. ``payload`` is the canonical JSON text of the payload."""

    run_id: str
    seq: int
    command: int
    type: str
    fingerprint: str
    payload: str
    payload_sha256: str
    recorded_at_us: int
    prev_hash: str
    hash: str

    def data(self) -> dict[str, Any]:
        value = json.loads(self.payload)
        if not isinstance(value, dict):
            raise TypeError("event payload is not a JSON object")
        return value

    def computed_hash(self) -> str:
        return event_hash(
            run_id=self.run_id,
            seq=self.seq,
            command=self.command,
            type=self.type,
            fingerprint=self.fingerprint,
            payload_sha256=self.payload_sha256,
            recorded_at_us=self.recorded_at_us,
            prev_hash=self.prev_hash,
        )


@dataclass(frozen=True, slots=True)
class VerifyResult:
    ok: bool
    entries: int
    head: str
    error: str | None = None
    seq: int | None = None
    payloads_verified: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "entries": self.entries,
            "head": self.head,
            "error": self.error,
            "seq": self.seq,
            "payloads_verified": self.payloads_verified,
        }


def verify_chain(
    records: Iterable[ChainRecord],
    *,
    run_id: str,
    expect_head: tuple[int, str] | None = None,
    redacted_seqs: frozenset[int] = frozenset(),
) -> VerifyResult:
    """Verify one run's chain from seq 0. ``expect_head`` is ``(head_seq, head_hash)``.

    Payload digests are checked for every record except those in ``redacted_seqs``.
    """
    prev = GENESIS_HASH
    count = 0
    for record in records:
        if record.run_id != run_id:
            return VerifyResult(False, count, prev, "event belongs to another run", record.seq)
        if record.seq != count:
            return VerifyResult(False, count, prev, "sequence gap or reordering", record.seq)
        if record.prev_hash != prev:
            return VerifyResult(False, count, prev, "broken hash chain", record.seq)
        if record.seq not in redacted_seqs and payload_digest(record.payload) != (
            record.payload_sha256
        ):
            return VerifyResult(False, count, prev, "payload modified", record.seq)
        if record.computed_hash() != record.hash:
            return VerifyResult(False, count, prev, "event content modified", record.seq)
        prev = record.hash
        count += 1
    verified = not redacted_seqs
    if expect_head is not None and expect_head != (count - 1, prev):
        return VerifyResult(
            False, count, prev, "head mismatch (truncated or rolled back)", None, verified
        )
    return VerifyResult(True, count, prev, None, None, verified)


class _ExportHeader(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    format: str
    v: int
    run_id: str
    head_seq: int = Field(ge=-1)
    head_hash: str = Field(pattern=_HEX64)
    redacted: bool


class _ExportRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str
    seq: int = Field(ge=0)
    command: int = Field(ge=-1)
    type: str
    fingerprint: str
    payload: str
    payload_sha256: str = Field(pattern=_HEX64)
    recorded_at_us: int
    prev_hash: str = Field(pattern=_HEX64)
    hash: str = Field(pattern=_HEX64)


def _redacted_payload(payload: str) -> str:
    return canonical_json(redact_value(json.loads(payload)))


def export_lines(
    run_id: str, head_seq: int, head_hash: str, records: Iterable[ChainRecord], *, redact: bool
) -> Iterator[str]:
    """Yield ``\\n``-terminated canonical JSON lines: a header, then one line per event."""
    header = {
        "format": EXPORT_FORMAT,
        "v": 1,
        "run_id": run_id,
        "head_seq": head_seq,
        "head_hash": head_hash,
        "redacted": redact,
    }
    yield canonical_json(header) + "\n"
    for record in records:
        payload = _redacted_payload(record.payload) if redact else record.payload
        line = {
            "run_id": record.run_id,
            "seq": record.seq,
            "command": record.command,
            "type": record.type,
            "fingerprint": record.fingerprint,
            "payload": payload,
            "payload_sha256": record.payload_sha256,
            "recorded_at_us": record.recorded_at_us,
            "prev_hash": record.prev_hash,
            "hash": record.hash,
        }
        yield canonical_json(line) + "\n"


def _parse_line(line: bytes, model: type[BaseModel]) -> tuple[BaseModel | None, str | None]:
    try:
        text = line.decode("utf-8")
        obj = json.loads(text)
        # Strict: a lax validator would read false or 0.0 as 0, giving one line several encodings.
        parsed = model.model_validate(obj, strict=True)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError):
        return None, "malformed line"
    if canonical_json(obj) != text:
        return None, "non-canonical encoding"
    return parsed, None


def verify_export(raw: bytes, *, expect_head: str | None = None) -> VerifyResult:
    """Verify an export produced by :func:`export_lines`.

    The chain must match the export header's head; ``expect_head`` additionally pins the head
    hash to an externally stored anchor.
    """
    if not raw.endswith(b"\n"):
        return VerifyResult(False, 0, GENESIS_HASH, "truncated final line")
    # Split on "\n" only: bytes.splitlines() also splits on "\r" and other separators.
    lines = raw[:-1].split(b"\n")
    header_obj, error = _parse_line(lines[0], _ExportHeader)
    if header_obj is None:
        return VerifyResult(False, 0, GENESIS_HASH, f"header: {error}")
    header = _ExportHeader.model_validate(header_obj)
    if header.format != EXPORT_FORMAT or header.v != 1:
        return VerifyResult(False, 0, GENESIS_HASH, "header: unknown export format")
    records: list[ChainRecord] = []
    for index, line in enumerate(lines[1:]):
        parsed, error = _parse_line(line, _ExportRecord)
        if parsed is None:
            return VerifyResult(False, index, GENESIS_HASH, error, index)
        item = _ExportRecord.model_validate(parsed)
        records.append(ChainRecord(**item.model_dump()))
    redacted = frozenset(record.seq for record in records) if header.redacted else frozenset()
    result = verify_chain(
        records,
        run_id=header.run_id,
        expect_head=(header.head_seq, header.head_hash),
        redacted_seqs=redacted,
    )
    if result.ok and expect_head is not None and expect_head != result.head:
        return VerifyResult(
            False,
            result.entries,
            result.head,
            "head mismatch with the external anchor",
            None,
            result.payloads_verified,
        )
    return result
