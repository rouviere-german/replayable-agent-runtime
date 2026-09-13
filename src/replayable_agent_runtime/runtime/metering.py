"""Provider call metering keyed by journal step, for recovery accounting.

``ctx.llm`` sets :data:`CURRENT_STEP` to ``(run_id, run_id:seq)`` of the ``StepScheduled`` event
before calling the provider; :class:`MeteredProvider` records one meter row per real provider
call. Counting rows per key shows whether recovery paid for a step twice.
"""

from __future__ import annotations

from contextvars import ContextVar

from replayable_agent_runtime.agentic.providers.base import LLMProvider, LLMRequest, LLMResponse
from replayable_agent_runtime.runtime.sinks import ProviderMeter

__all__ = ["CURRENT_STEP", "MeteredProvider"]

CURRENT_STEP: ContextVar[tuple[str, str] | None] = ContextVar("rar_current_step", default=None)


class MeteredProvider:
    """Wrap a provider and record every call against the journal step it serves."""

    def __init__(self, inner: LLMProvider, meter: ProviderMeter) -> None:
        self._inner = inner
        self._meter = meter
        self.name = f"metered-{inner.name}"

    def complete(self, request: LLMRequest) -> LLMResponse:
        step = CURRENT_STEP.get()
        run_id, key = step if step is not None else ("unscoped", "unscoped:0")
        self._meter.record(run_id=run_id, key=key, request_sha256=request.sha256())
        return self._inner.complete(request)
