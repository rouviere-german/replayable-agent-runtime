"""Step, token and USD budgets persisted in the journal itself.

The limits are part of the hash-chained ``RunStarted`` event and spend is recomputed by folding the
run's own events (``StepScheduled`` for steps, usage and cost on LLM ``StepCompleted`` /
``StepFailed``). Nothing is kept in a side file or in memory across activations, so resuming,
replaying or moving a run to another worker cannot reset spend, and a runtime-side cap can tighten
the recorded limits but never loosen them.

A step is charged when its ``StepScheduled`` event commits, before the call runs: a crash in the
window between execution and completion re-executes an already-charged step instead of charging it
twice. Tokens and cost are known only after a call, so a single call can overshoot; every later
step is then refused. Compensation steps are not charged, so a rollback can always run.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from replayable_agent_runtime.agentic.budget import BudgetLimits, BudgetState
from replayable_agent_runtime.agentic.errors import BudgetExceededError
from replayable_agent_runtime.runtime.events import CommandKind, EventRecord, EventType

__all__ = ["CHARGED_KINDS", "RunBudget", "fold_budget", "limits_from_payload"]

CHARGED_KINDS = frozenset({CommandKind.LLM.value, CommandKind.TOOL.value})


def limits_from_payload(budget: Mapping[str, int]) -> BudgetLimits:
    return BudgetLimits(
        max_steps=int(budget["max_steps"]),
        max_tokens=int(budget["max_tokens"]),
        max_usd_micros=int(budget["max_usd_micros"]),
    )


@dataclass(slots=True)
class RunBudget:
    limits: BudgetLimits
    steps: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    usd_micros: int = 0
    _charged_seqs: set[int] = field(default_factory=set)

    @property
    def state(self) -> BudgetState:
        return BudgetState(
            steps=self.steps,
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            usd_micros=self.usd_micros,
        )

    def check_step(self) -> None:
        """Raise BudgetExceededError if one more step is not allowed."""
        tokens = self.input_tokens + self.output_tokens
        if tokens > self.limits.max_tokens:
            raise BudgetExceededError("tokens", self.limits.max_tokens, tokens)
        if self.usd_micros > self.limits.max_usd_micros:
            raise BudgetExceededError("usd_micros", self.limits.max_usd_micros, self.usd_micros)
        if self.steps + 1 > self.limits.max_steps:
            raise BudgetExceededError("steps", self.limits.max_steps, self.steps + 1)

    def apply(self, record: EventRecord) -> None:
        """Fold one event into the running totals (idempotent per seq)."""
        if record.seq in self._charged_seqs:
            return
        if record.type == EventType.STEP_SCHEDULED:
            if record.data().get("kind") in CHARGED_KINDS:
                self.steps += 1
                self._charged_seqs.add(record.seq)
        elif record.type in (EventType.STEP_COMPLETED, EventType.STEP_FAILED):
            data = record.data()
            usage = data.get("usage")
            if isinstance(usage, dict):
                self.input_tokens += int(usage.get("input_tokens", 0))
                self.output_tokens += int(usage.get("output_tokens", 0))
            self.usd_micros += int(data.get("cost_usd_micros", 0))
            self._charged_seqs.add(record.seq)


def fold_budget(limits: BudgetLimits, records: Iterable[EventRecord]) -> RunBudget:
    budget = RunBudget(limits=limits)
    for record in records:
        budget.apply(record)
    return budget
