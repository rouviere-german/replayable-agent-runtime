"""A durable tool-use agent loop built only from context commands.

Every model turn is a journaled ``ctx.llm`` step and every proposed call a journaled
``ctx.tool_call``, so the loop resumes by replay after a crash without paying for a turn twice.
Tool output goes back to the model inside untrusted-data envelopes; the enforcing controls are the
per-workflow allowlist, the set of tools exposed to the model and approvals on write tools.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

from pydantic import JsonValue

from replayable_agent_runtime.agentic.envelope import UNTRUSTED_DATA_POLICY, UntrustedData
from replayable_agent_runtime.agentic.providers.base import (
    ChatMessage,
    ContentBlock,
    LLMRequest,
    TextBlock,
)
from replayable_agent_runtime.runtime.context import WorkflowContext

__all__ = ["AgentResult", "ToolOutcome", "run_agent"]


@dataclass(frozen=True, slots=True)
class ToolOutcome:
    name: str
    arguments: dict[str, JsonValue]
    ok: bool
    output: JsonValue = None


@dataclass(frozen=True, slots=True)
class AgentResult:
    final_text: str
    turns: int
    tool_outcomes: list[ToolOutcome] = field(default_factory=list)


async def run_agent(
    ctx: WorkflowContext,
    *,
    task: str,
    system: str,
    tools: Sequence[str],
    max_turns: int = 8,
    role: str = "default",
    max_tokens: int = 2048,
    attachments: Sequence[UntrustedData] = (),
) -> AgentResult:
    """Loop model turns and tool calls until the model stops calling tools or ``max_turns``."""
    registry = ctx.tool_specs(tools)
    specs = tuple(registry[name] for name in sorted(tools))
    first: list[ContentBlock] = [TextBlock(text=task)]
    first.extend(TextBlock(text=item.render()) for item in attachments)
    messages: list[ChatMessage] = [ChatMessage(role="user", content=tuple(first))]
    outcomes: list[ToolOutcome] = []
    exposed = frozenset(tools)
    for turn in range(1, max_turns + 1):
        response = await ctx.llm(
            LLMRequest(
                system=f"{system}\n\n{UNTRUSTED_DATA_POLICY}",
                messages=tuple(messages),
                tools=specs,
                max_tokens=max_tokens,
                role=role,
            )
        )
        assistant: list[ContentBlock] = list(response.content)
        if not assistant:
            if response.text:
                assistant.append(TextBlock(text=response.text))
            assistant.extend(response.tool_calls)
        if assistant:
            messages.append(ChatMessage(role="assistant", content=tuple(assistant)))
        if not response.tool_calls:
            return AgentResult(final_text=response.text, turns=turn, tool_outcomes=outcomes)
        results: list[ContentBlock] = []
        for call in response.tool_calls:
            outcome = await ctx.tool_call(call, exposed=exposed)
            outcomes.append(
                ToolOutcome(call.name, dict(call.arguments), outcome.ok, outcome.output)
            )
            results.append(outcome.block)
        messages.append(ChatMessage(role="user", content=tuple(results)))
    return AgentResult(final_text="", turns=max_turns, tool_outcomes=outcomes)
