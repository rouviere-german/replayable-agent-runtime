"""Alembic environment. Migrations run programmatically through ``Journal.migrate``."""

from __future__ import annotations

from alembic import context

from replayable_agent_runtime.runtime.schema import metadata

config = context.config


def run_migrations() -> None:
    connection = config.attributes.get("connection")
    if connection is None:
        raise RuntimeError("run migrations through Journal.migrate, which passes a connection")
    context.configure(connection=connection, target_metadata=metadata, render_as_batch=True)
    with context.begin_transaction():
        context.run_migrations()


run_migrations()
