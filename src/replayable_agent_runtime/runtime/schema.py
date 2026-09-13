"""SQLAlchemy Core schema of the journal database. Alembic migrations must match it (tested)."""

from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Column, Index, Integer, MetaData, String, Table, Text

__all__ = ["SCHEMA_REVISION", "approvals", "events", "metadata", "outbox", "runs", "signals"]

# Head revision of runtime/migrations/versions; Journal skips Alembic when the DB is already here.
SCHEMA_REVISION = "0001_initial"

metadata = MetaData()

runs = Table(
    "runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("workflow", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("input_json", Text, nullable=False),
    Column("started_by", String(128), nullable=False),
    Column("created_at_us", BigInteger, nullable=False),
    Column("updated_at_us", BigInteger, nullable=False),
    Column("owner", String(128), nullable=True),
    Column("fencing", Integer, nullable=False, server_default="0"),
    Column("lease_expires_at_us", BigInteger, nullable=False, server_default="0"),
    Column("wake_at_us", BigInteger, nullable=True),
    Column("waiting_on", Text, nullable=True),
    Column("cancel_requested", Boolean, nullable=False, server_default="0"),
    Column("inbox_version", Integer, nullable=False, server_default="0"),
    Column("head_seq", Integer, nullable=False, server_default="-1"),
    Column("head_hash", String(64), nullable=False),
    Column("result_json", Text, nullable=True),
    Column("error_json", Text, nullable=True),
    Column("blocked_reason", Text, nullable=True),
    Index("ix_runs_claim", "status", "lease_expires_at_us"),
    Index("ix_runs_wake", "status", "wake_at_us"),
)

events = Table(
    "events",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("seq", Integer, primary_key=True, autoincrement=False),
    Column("command", Integer, nullable=False),
    Column("type", String(32), nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("payload", Text, nullable=False),
    Column("payload_sha256", String(64), nullable=False),
    Column("recorded_at_us", BigInteger, nullable=False),
    Column("prev_hash", String(64), nullable=False),
    Column("hash", String(64), nullable=False),
)

signals = Table(
    "signals",
    metadata,
    Column("signal_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(64), nullable=False),
    Column("name", String(64), nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("sent_by", String(128), nullable=False),
    Column("created_at_us", BigInteger, nullable=False),
    Column("consumed_seq", Integer, nullable=True),
    Index("ix_signals_pending", "run_id", "name", "consumed_seq"),
)

approvals = Table(
    "approvals",
    metadata,
    Column("approval_id", String(96), primary_key=True),
    Column("run_id", String(64), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("command", Integer, nullable=False),
    Column("subject_kind", String(8), nullable=False),
    Column("name", String(64), nullable=False),
    Column("args_json", Text, nullable=False),
    Column("call_hash", String(64), nullable=False),
    Column("requested_by", String(128), nullable=False),
    Column("status", String(16), nullable=False),
    Column("decided_by", String(128), nullable=True),
    Column("decided_at_us", BigInteger, nullable=True),
    Column("reason", Text, nullable=True),
    Column("created_at_us", BigInteger, nullable=False),
    Index("ix_approvals_status", "status", "run_id"),
)

outbox = Table(
    "outbox",
    metadata,
    Column("key", String(96), primary_key=True),
    Column("run_id", String(64), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("tool", String(64), nullable=False),
    Column("call_hash", String(64), nullable=False),
    Column("effect", String(16), nullable=False),
    Column("status", String(16), nullable=False),
    Column("fencing", Integer, nullable=False),
    Column("created_at_us", BigInteger, nullable=False),
    Column("completed_at_us", BigInteger, nullable=True),
    Index("ix_outbox_status", "status", "run_id"),
)
