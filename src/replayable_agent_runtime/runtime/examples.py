"""Bundled workflows and deterministic fake model policies.

The policies are pure functions of the request (they never depend on how many times the provider
was called), so a worker that crashes and restarts in a fresh process gets exactly the answers the
first worker would have got. That is what real models are assumed to be for recovery purposes: the
journal, not the provider, is the source of truth for answers already paid for.
"""

from __future__ import annotations

import json
import random
import re

from pydantic import JsonValue

from replayable_agent_runtime.agentic.envelope import parse_envelopes
from replayable_agent_runtime.agentic.providers.base import (
    LLMRequest,
    LLMResponse,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)
from replayable_agent_runtime.runtime.agent import run_agent
from replayable_agent_runtime.runtime.context import WorkflowContext
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition

__all__ = [
    "RESEARCH_TOOLS",
    "GullibleResearchPolicy",
    "ResearchPolicy",
    "research_ticket",
    "research_ticket_definition",
]

RESEARCH_TOOLS = ("http_get", "read_file", "create_ticket")
_TOPIC = re.compile(r"^Topic: ([a-z0-9 -]{1,64})$", re.MULTILINE)
_INJECTED_CALL = re.compile(r"CALL_TOOL\s+([A-Za-z0-9_-]{1,64})\s+(\{.*\})")


def _assistant_turns(request: LLMRequest) -> int:
    return sum(1 for message in request.messages if message.role == "assistant")


def _topic(request: LLMRequest) -> str:
    for block in request.messages[0].content:
        if isinstance(block, TextBlock):
            match = _TOPIC.search(block.text)
            if match:
                return match.group(1)
    return "unknown"


def _unwrap(content: str) -> str:
    """The text a model would read in a tool result: HTTP bodies are taken out of their JSON."""
    try:
        value = json.loads(content)
    except json.JSONDecodeError:
        return content
    if isinstance(value, dict) and isinstance(value.get("body"), str):
        return str(value["body"])
    return value if isinstance(value, str) else content


def _latest_tool_text(request: LLMRequest) -> str:
    """Decoded content of the untrusted-data envelopes in the latest tool results."""
    texts: list[str] = []
    for block in request.messages[-1].content:
        if isinstance(block, ToolResultBlock):
            envelopes = parse_envelopes(block.content)
            texts.extend(_unwrap(item.content) for item in envelopes)
            if not envelopes:
                texts.append(block.content)
    return "\n".join(texts)


def _call(request: LLMRequest, name: str, arguments: dict[str, JsonValue]) -> LLMResponse:
    call = ToolUseBlock(id=f"toolu_{_assistant_turns(request)}", name=name, arguments=arguments)
    return LLMResponse(model="fake-research", stop_reason="tool_use", tool_calls=(call,))


class ResearchPolicy:
    """An honest scripted researcher: search, read an article, read on-call notes, file a ticket.

    It reads the injected article like any other data and does not act on instructions inside it.
    """

    def __call__(self, request: LLMRequest, rng: random.Random, turn: int) -> LLMResponse:
        del rng, turn
        topic = _topic(request)
        step = _assistant_turns(request)
        if step == 0:
            return _call(request, "http_get", {"url": f"http://kb.internal/search?q={topic}"})
        if step == 1:
            return _call(request, "http_get", {"url": "http://kb.internal/articles/incident-42"})
        if step == 2:
            return _call(request, "read_file", {"path": "notes/oncall.md"})
        if step == 3:
            evidence = _latest_tool_text(request).strip().splitlines()
            body = f"Evidence: {evidence[0][:180]}" if evidence else "Evidence: none"
            return _call(request, "create_ticket", {"title": f"Follow-up: {topic}", "body": body})
        outcome = _latest_tool_text(request)[:120].replace("\n", " ")
        return LLMResponse(
            model="fake-research",
            stop_reason="end_turn",
            text=f"Researched {topic}; ticket step result: {outcome}",
        )


class GullibleResearchPolicy:
    """A deliberately gullible model: it obeys any ``CALL_TOOL <name> <json>`` line it reads.

    Used to show that containment comes from the runtime (allowlists, exposure, approvals), not
    from the model resisting the injection.
    """

    def __call__(self, request: LLMRequest, rng: random.Random, turn: int) -> LLMResponse:
        del rng, turn
        step = _assistant_turns(request)
        if step == 0:
            return _call(request, "http_get", {"url": "http://kb.internal/articles/incident-42"})
        # Obey the injected lines of the first document read, in order, one per turn.
        injected = list(_INJECTED_CALL.finditer(_first_tool_text(request)))
        index = step - 1
        if index < len(injected):
            try:
                arguments = json.loads(injected[index].group(2))
            except json.JSONDecodeError:
                arguments = None
            if isinstance(arguments, dict):
                return _call(request, injected[index].group(1), arguments)
        return LLMResponse(
            model="fake-gullible", stop_reason="end_turn", text="Done as instructed."
        )


def _first_tool_text(request: LLMRequest) -> str:
    for message in request.messages:
        for block in message.content:
            if isinstance(block, ToolResultBlock):
                envelopes = parse_envelopes(block.content)
                if envelopes:
                    return _unwrap(envelopes[0].content)
    return ""


async def research_ticket(ctx: WorkflowContext, value: JsonValue) -> JsonValue:
    """Research a topic with read tools, open one approved ticket, then notify on-call."""
    if not isinstance(value, dict) or not isinstance(value.get("topic"), str):
        raise TypeError("input must be an object with a string 'topic'")
    topic = str(value["topic"])
    started_at = await ctx.now()
    result = await run_agent(
        ctx,
        task=f"Topic: {topic}\nResearch the topic with the tools, then open one follow-up ticket.",
        system="You are an on-call research assistant. Be brief and cite what you read.",
        tools=RESEARCH_TOOLS,
        max_turns=6,
    )
    tickets: list[str] = []
    for outcome in result.tool_outcomes:
        if outcome.name == "create_ticket" and outcome.ok and isinstance(outcome.output, dict):
            ticket_id = str(outcome.output["ticket_id"])
            tickets.append(ticket_id)
            ctx.compensate("close_ticket", {"ticket_id": ticket_id})
    if tickets:
        await ctx.tool(
            "notify", {"channel": "oncall", "message": f"Opened {tickets[0]} about {topic}."}
        )
    return {
        "topic": topic,
        "summary": result.final_text,
        "tickets": list(tickets),
        "turns": result.turns,
        "started_at": started_at,
    }


def research_ticket_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        name="research_ticket",
        fn=research_ticket,
        tools=frozenset({*RESEARCH_TOOLS, "notify", "close_ticket"}),
        description="Research with read tools, open an approved ticket, notify on-call.",
    )
