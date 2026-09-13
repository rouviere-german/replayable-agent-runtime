"""Runtime metrics (Prometheus) and per-step OpenTelemetry spans."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from dataclasses import dataclass

from opentelemetry.trace import Span, Tracer
from prometheus_client import CollectorRegistry, Counter, Histogram

__all__ = ["RuntimeMetrics", "create_runtime_metrics", "step_span"]


@dataclass(frozen=True, slots=True)
class RuntimeMetrics:
    registry: CollectorRegistry
    journal_append_latency: Histogram
    replay_duration_seconds: Histogram
    nondeterminism_errors_total: Counter
    lease_reclaims_total: Counter
    stale_fencing_rejections_total: Counter
    activations_total: Counter
    steps_total: Counter


def create_runtime_metrics(registry: CollectorRegistry | None = None) -> RuntimeMetrics:
    reg = registry if registry is not None else CollectorRegistry()
    return RuntimeMetrics(
        registry=reg,
        journal_append_latency=Histogram(
            "journal_append_latency_seconds",
            "Wall time of one journal append transaction (BEGIN IMMEDIATE to COMMIT).",
            registry=reg,
            buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
        ),
        replay_duration_seconds=Histogram(
            "replay_duration_seconds",
            "Time from activation start until replay reaches the recorded history head.",
            registry=reg,
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
        ),
        nondeterminism_errors_total=Counter(
            "nondeterminism_errors_total",
            "Replays whose workflow code diverged from the recorded history.",
            registry=reg,
        ),
        lease_reclaims_total=Counter(
            "lease_reclaims_total",
            "Runs claimed after another worker's lease expired without a clean release.",
            registry=reg,
        ),
        stale_fencing_rejections_total=Counter(
            "stale_fencing_rejections_total",
            "Writes refused because they carried a superseded fencing token.",
            ["where"],
            registry=reg,
        ),
        activations_total=Counter(
            "activations_total",
            "Workflow activations by outcome.",
            ["outcome"],
            registry=reg,
        ),
        steps_total=Counter(
            "workflow_steps_total",
            "Workflow commands by kind and mode (replay or live).",
            ["kind", "mode"],
            registry=reg,
        ),
    )


@contextlib.contextmanager
def step_span(
    tracer: Tracer, *, run_id: str, command: int, kind: str, name: str, mode: str
) -> Iterator[Span]:
    """A span per workflow command, tagged with run id, command, seq (set later) and mode."""
    with tracer.start_as_current_span(f"rar.step {kind}") as span:
        span.set_attribute("rar.run_id", run_id)
        span.set_attribute("rar.command", command)
        span.set_attribute("rar.step.kind", kind)
        span.set_attribute("rar.step.name", name)
        span.set_attribute("rar.mode", mode)
        yield span
