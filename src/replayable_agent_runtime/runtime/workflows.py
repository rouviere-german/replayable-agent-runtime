"""Workflow definitions and the registry workers resolve run names against."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import JsonValue

from replayable_agent_runtime.agentic.errors import AgenticError
from replayable_agent_runtime.agentic.tools import ToolRegistry

if TYPE_CHECKING:
    from replayable_agent_runtime.runtime.context import WorkflowContext

__all__ = ["UnknownWorkflowError", "WorkflowDefinition", "WorkflowFn", "WorkflowRegistry"]

WorkflowFn = Callable[["WorkflowContext", Any], Awaitable[JsonValue]]
_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class UnknownWorkflowError(AgenticError):
    def __init__(self, name: str) -> None:
        super().__init__(f"workflow {name} is not registered")
        self.name = name


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    """A workflow: an async function of ``(ctx, input)`` plus its tool allowlist.

    Workflow code must be deterministic given its input and the results of context commands: all
    time, randomness, I/O, model calls and waiting go through ``ctx``.
    """

    name: str
    fn: WorkflowFn
    tools: frozenset[str] = frozenset()
    description: str = ""

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError(f"invalid workflow name {self.name!r}")


class WorkflowRegistry:
    def __init__(self, definitions: Iterable[WorkflowDefinition] = ()) -> None:
        self._definitions: dict[str, WorkflowDefinition] = {}
        for definition in definitions:
            self.register(definition)

    def register(self, definition: WorkflowDefinition) -> None:
        if definition.name in self._definitions:
            raise ValueError(f"workflow {definition.name} is already registered")
        self._definitions[definition.name] = definition

    def get(self, name: str) -> WorkflowDefinition:
        definition = self._definitions.get(name)
        if definition is None:
            raise UnknownWorkflowError(name)
        return definition

    def __contains__(self, name: object) -> bool:
        return name in self._definitions

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._definitions))

    def check_tools(self, tools: ToolRegistry) -> None:
        """Fail fast if a workflow allowlists a tool the runtime does not provide."""
        for definition in self._definitions.values():
            missing = sorted(name for name in definition.tools if name not in tools)
            if missing:
                raise ValueError(f"workflow {definition.name} allowlists unknown tools: {missing}")
