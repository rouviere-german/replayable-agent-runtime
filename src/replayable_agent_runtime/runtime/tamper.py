"""Tamper suite: systematically corrupt a journal and check that verification notices.

Database mutations run on a private copy (made with the SQLite backup API, so committed WAL
content is included), each inside a transaction that is rolled back afterwards. Verification is the
same check ``Journal.verify_run`` performs: the hash chain from seq 0 plus the head anchor in
``runs``. Export mutations flip one byte at a time in a JSONL export.

Out of scope, by design: an attacker who rewrites the whole chain *and* the head anchor. That is
detected only against an externally stored head (``--expect-head``); see docs/THREAT_MODEL.md.
"""

from __future__ import annotations

import sqlite3
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from replayable_agent_runtime.agentic.audit import ChainRecord, verify_chain, verify_export

__all__ = ["TamperReport", "run_db_tamper_suite", "run_export_flip_suite"]

_SELECT_EVENTS = (
    "SELECT run_id, seq, command, type, fingerprint, payload, payload_sha256, recorded_at_us, "
    "prev_hash, hash FROM events WHERE run_id = ? ORDER BY seq"
)
_SELECT_HEAD = "SELECT head_seq, head_hash FROM runs WHERE run_id = ?"
# One static statement per column, so no SQL is ever built from strings.
_UPDATES = {
    "command": "UPDATE events SET command = ? WHERE run_id = ? AND seq = ?",
    "type": "UPDATE events SET type = ? WHERE run_id = ? AND seq = ?",
    "fingerprint": "UPDATE events SET fingerprint = ? WHERE run_id = ? AND seq = ?",
    "payload": "UPDATE events SET payload = ? WHERE run_id = ? AND seq = ?",
    "payload_sha256": "UPDATE events SET payload_sha256 = ? WHERE run_id = ? AND seq = ?",
    "recorded_at_us": "UPDATE events SET recorded_at_us = ? WHERE run_id = ? AND seq = ?",
    "prev_hash": "UPDATE events SET prev_hash = ? WHERE run_id = ? AND seq = ?",
    "hash": "UPDATE events SET hash = ? WHERE run_id = ? AND seq = ?",
}
_COLUMN_INDEX = {
    "command": 2,
    "type": 3,
    "fingerprint": 4,
    "payload": 5,
    "payload_sha256": 6,
    "recorded_at_us": 7,
    "prev_hash": 8,
    "hash": 9,
}


@dataclass
class TamperReport:
    attempts: dict[str, int] = field(default_factory=dict)
    detected: dict[str, int] = field(default_factory=dict)
    undetected: list[str] = field(default_factory=list)

    def record(self, kind: str, label: str, caught: bool) -> None:
        self.attempts[kind] = self.attempts.get(kind, 0) + 1
        if caught:
            self.detected[kind] = self.detected.get(kind, 0) + 1
        else:
            self.undetected.append(f"{kind}:{label}")

    @property
    def total(self) -> int:
        return sum(self.attempts.values())

    @property
    def total_detected(self) -> int:
        return sum(self.detected.values())

    @property
    def detection_rate(self) -> float:
        return self.total_detected / self.total if self.total else 1.0


def _flip_text(value: str) -> str:
    if not value:
        return "0"
    index = len(value) // 2
    return value[:index] + chr(ord(value[index]) ^ 0x01) + value[index + 1 :]


def _caught(conn: sqlite3.Connection, run_id: str) -> bool:
    rows = conn.execute(_SELECT_EVENTS, (run_id,)).fetchall()
    head = conn.execute(_SELECT_HEAD, (run_id,)).fetchone()
    records = [ChainRecord(*row) for row in rows]
    expect = (int(head[0]), str(head[1])) if head is not None else (-1, "")
    return not verify_chain(records, run_id=run_id, expect_head=expect).ok


def _attempt(
    conn: sqlite3.Connection,
    report: TamperReport,
    run_id: str,
    kind: str,
    label: str,
    statements: Iterable[tuple[str, tuple[object, ...]]],
) -> None:
    conn.execute("BEGIN")
    try:
        for sql, params in statements:
            conn.execute(sql, params)
        report.record(kind, label, _caught(conn, run_id))
    finally:
        conn.execute("ROLLBACK")


def run_db_tamper_suite(db_path: Path, run_id: str) -> TamperReport:
    """Modify every column of every event, delete, reorder, truncate and forge-append events."""
    report = TamperReport()
    with tempfile.TemporaryDirectory() as tmp:
        copy_path = Path(tmp) / "tamper.db"
        source = sqlite3.connect(db_path)
        target = sqlite3.connect(copy_path)
        try:
            source.backup(target)
        finally:
            source.close()
            target.close()
        conn = sqlite3.connect(copy_path, isolation_level=None)
        try:
            if _caught(conn, run_id):
                raise ValueError("the journal does not verify before tampering")
            rows = conn.execute(_SELECT_EVENTS, (run_id,)).fetchall()
            for row in rows:
                seq = int(row[1])
                for column, sql in _UPDATES.items():
                    original = row[_COLUMN_INDEX[column]]
                    mutated = original + 1 if isinstance(original, int) else _flip_text(original)
                    _attempt(
                        conn, report, run_id, "modify", f"{seq}.{column}",
                        [(sql, (mutated, run_id, seq))],
                    )  # fmt: skip
                _attempt(
                    conn, report, run_id, "delete", str(seq),
                    [("DELETE FROM events WHERE run_id = ? AND seq = ?", (run_id, seq))],
                )  # fmt: skip
            for seq in range(len(rows) - 1):
                swap = "UPDATE events SET seq = ? WHERE run_id = ? AND seq = ?"
                _attempt(
                    conn, report, run_id, "reorder", f"{seq}<->{seq + 1}",
                    [
                        (swap, (-1, run_id, seq)),
                        (swap, (seq, run_id, seq + 1)),
                        (swap, (seq + 1, run_id, -1)),
                    ],
                )  # fmt: skip
            head_seq = len(rows) - 1
            truncate = "DELETE FROM events WHERE run_id = ? AND seq > ?"
            for dropped in range(1, min(3, head_seq) + 1):
                _attempt(
                    conn, report, run_id, "truncate_tail", str(dropped),
                    [(truncate, (run_id, head_seq - dropped))],
                )  # fmt: skip
            last = rows[-1]
            forged = (
                run_id, head_seq + 1, last[2], last[3], last[4], last[5], last[6], last[7],
                last[9], last[9],
            )  # fmt: skip
            _attempt(
                conn, report, run_id, "forge_append", str(head_seq + 1),
                [("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", forged)],
            )  # fmt: skip
        finally:
            conn.close()
    return report


def run_export_flip_suite(raw: bytes, positions: Iterable[int] | None = None) -> TamperReport:
    """Flip one bit of one byte at each position of an export and verify the result."""
    report = TamperReport()
    if not verify_export(raw).ok:
        raise ValueError("the export does not verify before tampering")
    for position in positions if positions is not None else range(len(raw)):
        flipped = bytearray(raw)
        flipped[position] ^= 0x01
        report.record("export_byte_flip", str(position), not verify_export(bytes(flipped)).ok)
    return report
