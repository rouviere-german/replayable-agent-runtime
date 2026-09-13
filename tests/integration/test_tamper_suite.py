from __future__ import annotations

import random

import pytest
from pydantic import JsonValue
from sqlalchemy import text

from conftest import Harness
from replayable_agent_runtime.agentic.audit import event_hash, payload_digest, verify_export
from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.db import make_engine
from replayable_agent_runtime.runtime.journal import RunStatus
from replayable_agent_runtime.runtime.tamper import run_db_tamper_suite, run_export_flip_suite

pytestmark = pytest.mark.integration


async def varied(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.now()
    reply = await ctx.llm(LLMRequest(messages=(ChatMessage.user("mail jane@example.com"),)))
    await ctx.tool("kb_lookup", {"key": "cache"})
    payload = await ctx.wait_signal("go")
    ticket = await ctx.tool("create_ticket", {"title": "cache", "body": reply.text})
    return {"signal": payload, "ticket": ticket}


def completed_run(harness: Harness) -> str:
    harness.register("varied", varied, frozenset({"kb_lookup", "create_ticket"}))
    run_id = harness.start("varied")
    worker = harness.worker()
    import asyncio

    asyncio.run(worker.run_until_idle())
    harness.control.signal(
        run_id, "go", {"note": "ship it", "contact": "jane@example.com"}, principal="bob"
    )
    asyncio.run(worker.run_until_idle())
    harness.approve_pending(run_id)
    asyncio.run(worker.run_until_idle())
    assert harness.journal.get_run(run_id).status is RunStatus.COMPLETED
    return run_id


def test_every_modification_deletion_reordering_truncation_and_forgery_is_detected(
    harness: Harness,
) -> None:
    run_id = completed_run(harness)
    events = harness.journal.get_run(run_id).head_seq + 1
    report = run_db_tamper_suite(harness.journal.path, run_id)
    assert report.undetected == []
    assert report.detection_rate == 1.0
    assert report.attempts == {
        "modify": 8 * events,
        "delete": events,
        "reorder": events - 1,
        "truncate_tail": 3,
        "forge_append": 1,
        # Columns outside the events table that the chain binds: the runs row (workflow, input,
        # starter) and every column of the decided approval row.
        "modify_run_row": 3,
        "modify_approval_row": 9,
    }
    assert harness.journal.verify_run(run_id).ok


async def echo_after_sleep(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.sleep(10)
    return {"echo": value, "started_by": ctx.started_by}


_RUN_COLUMN_EDITS = {
    "input_json": "UPDATE runs SET input_json = :v WHERE run_id = :r",
    "workflow": "UPDATE runs SET workflow = :v WHERE run_id = :r",
    "started_by": "UPDATE runs SET started_by = :v WHERE run_id = :r",
}


@pytest.mark.parametrize(
    ("column", "value"),
    [("input_json", '{"amount":1000000}'), ("workflow", "other"), ("started_by", "mallory")],
)
def test_run_columns_outside_the_chain_are_verified_and_never_executed(
    harness: Harness, column: str, value: str
) -> None:
    """Regression: the engine ran on runs.input_json and verify_run ignored the runs row."""
    import asyncio

    harness.register("echo", echo_after_sleep)
    harness.register("other", echo_after_sleep)
    run_id = harness.start("echo", {"amount": 1})
    assert asyncio.run(harness.worker().run_once(run_id=run_id)) is not None
    engine = make_engine(harness.journal.path)
    with engine.begin() as conn:
        conn.execute(text(_RUN_COLUMN_EDITS[column]), {"v": value, "r": run_id})
    engine.dispose()
    result = harness.journal.verify_run(run_id)
    assert not result.ok
    assert result.error is not None
    assert "RunStarted" in result.error
    harness.clock.advance(20)
    harness.journal.wake_due()
    asyncio.run(harness.worker().run_once(run_id=run_id))
    run = harness.journal.get_run(run_id)
    assert run.status is RunStatus.BLOCKED
    assert run.blocked_reason is not None
    assert "integrity" in run.blocked_reason
    assert run.result is None
    assert [r.type for r in harness.journal.load_history(run_id)][-1] == "TimerScheduled"


def test_single_byte_flips_anywhere_in_an_export_are_detected(harness: Harness) -> None:
    run_id = completed_run(harness)
    raw = "".join(harness.journal.export_run(run_id, redact=False)).encode("utf-8")
    header_end = raw.index(b"\n") + 1
    rng = random.Random(7)
    positions = sorted({*range(header_end), *rng.sample(range(header_end, len(raw)), 400)})
    report = run_export_flip_suite(raw, positions)
    assert report.undetected == []
    assert report.attempts["export_byte_flip"] == len(positions)


def test_a_redacted_export_proves_the_chain_but_not_the_payloads(harness: Harness) -> None:
    run_id = completed_run(harness)
    full = "".join(harness.journal.export_run(run_id, redact=False))
    redacted = "".join(harness.journal.export_run(run_id, redact=True))
    assert "jane@example.com" in full
    assert "jane@example.com" not in redacted
    result = verify_export(redacted.encode("utf-8"))
    assert result.ok
    assert result.payloads_verified is False
    assert verify_export(full.encode("utf-8")).payloads_verified is True
    head = harness.journal.get_run(run_id).head_hash
    assert verify_export(full.encode("utf-8"), expect_head=head).ok
    assert not verify_export(full.encode("utf-8"), expect_head="0" * 64).ok
    assert not verify_export(full.encode("utf-8")[:-1]).ok


def test_rewriting_the_whole_chain_and_its_head_needs_an_external_anchor(harness: Harness) -> None:
    run_id = completed_run(harness)
    anchored = harness.journal.get_run(run_id).head_hash
    history = harness.journal.load_history(run_id)
    engine = make_engine(harness.journal.path)
    prev = history[0].prev_hash
    with engine.begin() as conn:
        for record in history:
            payload = record.payload.replace("ship it", "ship NOW")
            digest = payload_digest(payload)
            new_hash = event_hash(
                run_id=run_id,
                seq=record.seq,
                command=record.command,
                type=record.type,
                fingerprint=record.fingerprint,
                payload_sha256=digest,
                recorded_at_us=record.recorded_at_us,
                prev_hash=prev,
            )
            conn.execute(
                text(
                    "UPDATE events SET payload = :p, payload_sha256 = :d, prev_hash = :prev, "
                    "hash = :h WHERE run_id = :r AND seq = :s"
                ),
                {
                    "p": payload,
                    "d": digest,
                    "prev": prev,
                    "h": new_hash,
                    "r": run_id,
                    "s": record.seq,
                },
            )
            prev = new_hash
        conn.execute(
            text("UPDATE runs SET head_hash = :h WHERE run_id = :r"), {"h": prev, "r": run_id}
        )
    engine.dispose()
    assert harness.journal.verify_run(run_id).ok
    anchored_result = harness.journal.verify_run(run_id, expect_head=anchored)
    assert not anchored_result.ok
    assert anchored_result.error == "head mismatch with the external anchor"


def test_events_of_a_deleted_run_are_reported(harness: Harness) -> None:
    run_id = completed_run(harness)
    engine = make_engine(harness.journal.path)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM runs WHERE run_id = :r"), {"r": run_id})
    engine.dispose()
    results = harness.journal.verify_all()
    assert results[run_id].ok is False
    assert results[run_id].error == "events without a run row (run deleted)"
