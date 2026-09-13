from __future__ import annotations

import pytest

from replayable_agent_runtime.agentic.errors import StaleFencingTokenError, ToolPolicyError
from replayable_agent_runtime.runtime.sinks import Notifier, ProviderMeter, SinkStore, TicketStore

pytestmark = pytest.mark.unit


def test_ticket_writes_are_idempotent_by_key_and_return_the_original_result(
    sinks: SinkStore,
) -> None:
    store = TicketStore(sinks)
    first = store.create(run_id="r", key="r:3", token=1, title="t", body="b")
    again = store.create(run_id="r", key="r:3", token=2, title="t", body="b")
    assert first == again == {"ticket_id": "T-1", "status": "open"}
    second = store.create(run_id="r", key="r:5", token=2, title="u", body="b")
    assert second["ticket_id"] == "T-2"
    assert store.update_status(run_id="r", key="r:7", token=2, ticket_id="T-1", status="done") == {
        "ticket_id": "T-1",
        "status": "done",
    }
    assert store.close(run_id="r", key="r:9", token=2, ticket_id="T-2")["status"] == "closed"
    assert [t["status"] for t in store.tickets("r")] == ["done", "closed"]
    assert [(e.op, e.seq) for e in sinks.ledger("r")] == [
        ("create", 3), ("create", 5), ("update", 7), ("close", 9)
    ]  # fmt: skip
    with pytest.raises(ToolPolicyError, match="does not exist"):
        store.update_status(run_id="r", key="r:11", token=2, ticket_id="T-99", status="x")
    with pytest.raises(ToolPolicyError):
        store.close(run_id="other", key="o:1", token=1, ticket_id="T-1")


def test_sinks_refuse_tokens_lower_than_the_highest_seen_and_record_it(sinks: SinkStore) -> None:
    tickets = TicketStore(sinks)
    notifier = Notifier(sinks)
    tickets.create(run_id="r", key="r:1", token=5, title="t", body="b")
    with pytest.raises(StaleFencingTokenError) as info:
        tickets.create(run_id="r", key="r:2", token=4, title="t", body="b")
    assert (info.value.token, info.value.highest) == (4, 5)
    notifier.send(run_id="r", key="r:3", token=7, channel="ops", message="m")
    with pytest.raises(StaleFencingTokenError):
        notifier.send(run_id="r", key="r:4", token=6, channel="ops", message="m")
    # Fences are per sink and per run: another run starts from scratch.
    notifier.send(run_id="other", key="o:1", token=1, channel="ops", message="m")
    rejections = sinks.rejections("r")
    assert [(r["sink"], r["token"], r["highest_token"]) for r in rejections] == [
        ("tickets", 4, 5),
        ("notifier", 6, 7),
    ]
    assert len(sinks.rejections()) == 2


def test_the_notifier_has_no_idempotency_and_fencing_can_be_disabled_for_the_control(
    sinks: SinkStore,
) -> None:
    unfenced = Notifier(sinks, enforce_fencing=False)
    unfenced.send(run_id="r", key="r:1", token=9, channel="ops", message="m")
    unfenced.send(run_id="r", key="r:1", token=1, channel="ops", message="m")
    assert len(unfenced.deliveries("r")) == 2
    assert sinks.rejections() == []


def test_the_provider_meter_counts_calls_per_step(sinks: SinkStore) -> None:
    meter = ProviderMeter(sinks)
    meter.record(run_id="r", key="r:2", request_sha256="a" * 64)
    meter.record(run_id="r", key="r:2", request_sha256="a" * 64)
    meter.record(run_id="r", key="r:4", request_sha256="b" * 64)
    meter.record(run_id="s", key="s:2", request_sha256="c" * 64)
    assert (meter.calls(), meter.calls("r")) == (4, 3)
    assert meter.duplicates("r") == {"r:2": 2}
    assert meter.duplicates() == {"r:2": 2}
