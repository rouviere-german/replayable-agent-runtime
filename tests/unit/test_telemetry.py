from __future__ import annotations

import asyncio

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import JsonValue

from conftest import Harness
from replayable_agent_runtime.agentic.providers.base import ChatMessage, LLMRequest
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.core.tracing import build_tracer_provider
from replayable_agent_runtime.runtime.chaos import echo_policy
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.engine import Runtime
from replayable_agent_runtime.runtime.telemetry import create_runtime_metrics
from replayable_agent_runtime.runtime.worker import Worker

pytestmark = pytest.mark.unit


async def traced(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    await ctx.llm(LLMRequest(messages=(ChatMessage.user("hi"),)))
    await ctx.tool("kb_lookup", {"key": "cache"})
    await ctx.wait_signal("go")
    return "done"


def test_every_step_gets_a_span_tagged_with_its_mode(harness: Harness) -> None:
    exporter = InMemorySpanExporter()
    tracer = build_tracer_provider("test", extra_exporter=exporter).get_tracer("test")
    metrics = create_runtime_metrics()
    harness.register("traced", traced, frozenset({"kb_lookup"}))
    harness.start("traced")

    def worker() -> Worker:
        runtime = Runtime(
            journal=harness.journal,
            workflows=harness.workflows,
            tools=harness.tools,
            provider=FakeProvider(echo_policy),
            clock=harness.clock,
            metrics=metrics,
            tracer=tracer,
        )
        return Worker(runtime, worker_id="traced", heartbeat_seconds=None)

    asyncio.run(worker().run_once())
    live = [span for span in exporter.get_finished_spans() if span.name.startswith("rar.step")]
    assert [(span.name, dict(span.attributes or {})["rar.mode"]) for span in live] == [
        ("rar.step llm", "live"),
        ("rar.step tool", "live"),
    ]

    exporter.clear()
    harness.control.signal("run-1", "go", None, principal="bob")
    asyncio.run(worker().run_once())
    spans = exporter.get_finished_spans()
    replayed = [span for span in spans if span.name.startswith("rar.step")]
    assert [(span.name, dict(span.attributes or {})["rar.mode"]) for span in replayed] == [
        ("rar.step llm", "replay"),
        ("rar.step tool", "replay"),
    ]
    first = dict(replayed[0].attributes or {})
    assert (first["rar.run_id"], first["rar.command"], first["rar.seq"]) == ("run-1", 0, 2)
    activation = next(span for span in spans if span.name == "rar.activation")
    assert dict(activation.attributes or {})["rar.fencing"] == 2

    registry = metrics.registry
    assert registry.get_sample_value("replay_duration_seconds_count") == 2
    assert registry.get_sample_value("activations_total", {"outcome": "completed"}) == 1
    assert registry.get_sample_value("workflow_steps_total", {"kind": "llm", "mode": "replay"}) == 1
    journal_registry = harness.journal_metrics().registry
    appends = journal_registry.get_sample_value("journal_append_latency_seconds_count")
    assert appends is not None
    assert appends >= 6
