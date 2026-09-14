"""Wire journal, sinks, tools, workflows, provider and control plane from settings."""

from __future__ import annotations

import time
from dataclasses import dataclass

from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import CollectorRegistry

from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.anthropic import AnthropicProvider
from replayable_agent_runtime.agentic.providers.base import LLMProvider
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.agentic.providers.replay import ReplayProvider
from replayable_agent_runtime.agentic.tools import ToolRegistry
from replayable_agent_runtime.core.config import Settings
from replayable_agent_runtime.core.tracing import build_tracer_provider
from replayable_agent_runtime.runtime.builtin_tools import (
    FakeServiceTransport,
    build_tool_registry,
    default_services,
)
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.db import Clock
from replayable_agent_runtime.runtime.engine import Runtime
from replayable_agent_runtime.runtime.examples import ResearchPolicy, research_ticket_definition
from replayable_agent_runtime.runtime.journal import Journal
from replayable_agent_runtime.runtime.metering import MeteredProvider
from replayable_agent_runtime.runtime.sinks import ProviderMeter, SinkStore
from replayable_agent_runtime.runtime.telemetry import RuntimeMetrics, create_runtime_metrics
from replayable_agent_runtime.runtime.workflows import WorkflowRegistry

__all__ = ["Services", "build_provider", "build_services", "default_workflows", "settings_budget"]


def default_workflows() -> WorkflowRegistry:
    return WorkflowRegistry([research_ticket_definition()])


def settings_budget(settings: Settings) -> BudgetLimits:
    return BudgetLimits.of(
        steps=settings.budget_max_steps,
        tokens=settings.budget_max_tokens,
        usd=settings.budget_max_usd,
    )


def build_provider(settings: Settings, meter: ProviderMeter) -> LLMProvider:
    """``fake`` (offline, metered), ``replay`` (JSONL cassette) or ``anthropic`` (needs a key)."""
    if settings.provider == "fake":
        return MeteredProvider(FakeProvider(ResearchPolicy()), meter)
    if settings.provider == "replay":
        if settings.cassette_path is None:
            raise ValueError("APP_CASSETTE_PATH is required when APP_PROVIDER=replay")
        return ReplayProvider(settings.cassette_path)
    return AnthropicProvider.from_settings(settings)


@dataclass
class Services:
    settings: Settings
    journal: Journal
    sinks: SinkStore
    meter: ProviderMeter
    tools: ToolRegistry
    workflows: WorkflowRegistry
    runtime: Runtime
    control: ControlPlane
    metrics: RuntimeMetrics

    def close(self) -> None:
        self.journal.close()
        self.sinks.close()


def build_services(
    settings: Settings,
    *,
    registry: CollectorRegistry | None = None,
    tracer_provider: TracerProvider | None = None,
    provider: LLMProvider | None = None,
    workflows: WorkflowRegistry | None = None,
    clock: Clock = time.time,
    service_latency_seconds: float = 0.0,
) -> Services:
    metrics = create_runtime_metrics(registry)
    journal = Journal(
        settings.journal_path,
        synchronous=settings.journal_synchronous,
        max_history_events=settings.max_history_events,
        clock=clock,
        metrics=metrics,
    )
    sinks = SinkStore(settings.sinks_path, synchronous=settings.journal_synchronous, clock=clock)
    meter = ProviderMeter(sinks)
    tools = build_tool_registry(
        workspace=settings.workspace_dir,
        sinks=sinks,
        egress_allowlist=settings.egress_allowlist,
        transport=FakeServiceTransport(default_services(), latency_seconds=service_latency_seconds),
    )
    registry_of_workflows = workflows or default_workflows()
    registry_of_workflows.check_tools(tools)
    tracer = (
        tracer_provider or build_tracer_provider(settings.service_name, settings.tracing_exporter)
    ).get_tracer("replayable_agent_runtime.runtime")
    budget = settings_budget(settings)
    runtime = Runtime(
        journal=journal,
        workflows=registry_of_workflows,
        tools=tools,
        provider=provider or build_provider(settings, meter),
        clock=clock,
        metrics=metrics,
        tracer=tracer,
        budget_cap=budget,
        fences=(sinks,),
    )
    control = ControlPlane(
        journal,
        registry_of_workflows,
        default_budget=budget,
        max_input_bytes=settings.max_input_bytes,
    )
    return Services(
        settings=settings,
        journal=journal,
        sinks=sinks,
        meter=meter,
        tools=tools,
        workflows=registry_of_workflows,
        runtime=runtime,
        control=control,
        metrics=metrics,
    )
