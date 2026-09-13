"""Initial journal schema: runs, events, signals, approvals, outbox.

Revision ID: 0001_initial
Revises:
Create Date: 2026-09-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("workflow", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("input_json", sa.Text, nullable=False),
        sa.Column("started_by", sa.String(128), nullable=False),
        sa.Column("created_at_us", sa.BigInteger, nullable=False),
        sa.Column("updated_at_us", sa.BigInteger, nullable=False),
        sa.Column("owner", sa.String(128), nullable=True),
        sa.Column("fencing", sa.Integer, nullable=False, server_default="0"),
        sa.Column("lease_expires_at_us", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("wake_at_us", sa.BigInteger, nullable=True),
        sa.Column("waiting_on", sa.Text, nullable=True),
        sa.Column("cancel_requested", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("inbox_version", sa.Integer, nullable=False, server_default="0"),
        sa.Column("head_seq", sa.Integer, nullable=False, server_default="-1"),
        sa.Column("head_hash", sa.String(64), nullable=False),
        sa.Column("result_json", sa.Text, nullable=True),
        sa.Column("error_json", sa.Text, nullable=True),
        sa.Column("blocked_reason", sa.Text, nullable=True),
    )
    op.create_index("ix_runs_claim", "runs", ["status", "lease_expires_at_us"])
    op.create_index("ix_runs_wake", "runs", ["status", "wake_at_us"])
    op.create_table(
        "events",
        sa.Column("run_id", sa.String(64), primary_key=True),
        sa.Column("seq", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("command", sa.Integer, nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("payload", sa.Text, nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False),
        sa.Column("recorded_at_us", sa.BigInteger, nullable=False),
        sa.Column("prev_hash", sa.String(64), nullable=False),
        sa.Column("hash", sa.String(64), nullable=False),
    )
    op.create_table(
        "signals",
        sa.Column("signal_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("payload_json", sa.Text, nullable=False),
        sa.Column("sent_by", sa.String(128), nullable=False),
        sa.Column("created_at_us", sa.BigInteger, nullable=False),
        sa.Column("consumed_seq", sa.Integer, nullable=True),
    )
    op.create_index("ix_signals_pending", "signals", ["run_id", "name", "consumed_seq"])
    op.create_table(
        "approvals",
        sa.Column("approval_id", sa.String(96), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("command", sa.Integer, nullable=False),
        sa.Column("subject_kind", sa.String(8), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("args_json", sa.Text, nullable=False),
        sa.Column("call_hash", sa.String(64), nullable=False),
        sa.Column("requested_by", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("decided_by", sa.String(128), nullable=True),
        sa.Column("decided_at_us", sa.BigInteger, nullable=True),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("created_at_us", sa.BigInteger, nullable=False),
    )
    op.create_index("ix_approvals_status", "approvals", ["status", "run_id"])
    op.create_table(
        "outbox",
        sa.Column("key", sa.String(96), primary_key=True),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("tool", sa.String(64), nullable=False),
        sa.Column("call_hash", sa.String(64), nullable=False),
        sa.Column("effect", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("fencing", sa.Integer, nullable=False),
        sa.Column("created_at_us", sa.BigInteger, nullable=False),
        sa.Column("completed_at_us", sa.BigInteger, nullable=True),
    )
    op.create_index("ix_outbox_status", "outbox", ["status", "run_id"])


def downgrade() -> None:
    for index, table in (
        ("ix_outbox_status", "outbox"),
        ("ix_approvals_status", "approvals"),
        ("ix_signals_pending", "signals"),
        ("ix_runs_wake", "runs"),
        ("ix_runs_claim", "runs"),
    ):
        op.drop_index(index, table_name=table)
    for table in ("outbox", "approvals", "signals", "events", "runs"):
        op.drop_table(table)
