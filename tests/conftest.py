"""Shared fixtures. Every test runs offline: non-loopback sockets are blocked."""

from __future__ import annotations

import logging
import os
import socket
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog
from hypothesis import HealthCheck, settings

from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.providers.base import LLMProvider
from replayable_agent_runtime.agentic.providers.fake import FakeProvider
from replayable_agent_runtime.agentic.tools import ToolRegistry
from replayable_agent_runtime.runtime.builtin_tools import build_tool_registry
from replayable_agent_runtime.runtime.chaos import FakeClock, echo_policy
from replayable_agent_runtime.runtime.control import ControlPlane
from replayable_agent_runtime.runtime.engine import Hooks, Runtime
from replayable_agent_runtime.runtime.journal import Journal
from replayable_agent_runtime.runtime.sinks import SinkStore
from replayable_agent_runtime.runtime.telemetry import create_runtime_metrics
from replayable_agent_runtime.runtime.worker import Worker
from replayable_agent_runtime.runtime.workflows import WorkflowDefinition, WorkflowRegistry

settings.register_profile(
    "dev", max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)
settings.register_profile(
    "ci",
    max_examples=200,
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))

REPO_ROOT = Path(__file__).resolve().parents[1]
_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})
GENEROUS_BUDGET = BudgetLimits.of(steps=10_000, tokens=10**9, usd=10_000.0)


@pytest.fixture(autouse=True)
def _offline_and_clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    for name in list(os.environ):
        if name.startswith("APP_"):
            monkeypatch.delenv(name, raising=False)

    real_connect = socket.socket.connect

    def guarded_connect(self: socket.socket, address: Any) -> None:
        host = address[0] if isinstance(address, tuple) else str(address)
        if host not in _LOOPBACK:
            raise RuntimeError(f"tests must not use the network (attempted {host})")
        real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


@pytest.fixture
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    structlog.reset_defaults()
    root.handlers = handlers
    root.setLevel(level)
    logging.getLogger("uvicorn.access").disabled = False


@pytest.fixture
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(1_750_000_000.0)


@pytest.fixture
def journal(tmp_path: Path, clock: FakeClock) -> Iterator[Journal]:
    instance = Journal(tmp_path / "journal.db", clock=clock, metrics=create_runtime_metrics())
    yield instance
    instance.close()


@pytest.fixture
def sinks(tmp_path: Path, clock: FakeClock) -> Iterator[SinkStore]:
    store = SinkStore(tmp_path / "sinks.db", clock=clock)
    yield store
    store.close()


@pytest.fixture
def tools(tmp_path: Path, sinks: SinkStore) -> ToolRegistry:
    return build_tool_registry(workspace=tmp_path / "workspace", sinks=sinks)


class Harness:
    """A runtime around one journal: register workflows, start runs, activate, approve."""

    def __init__(
        self, journal: Journal, sinks: SinkStore, tools: ToolRegistry, clock: FakeClock
    ) -> None:
        self.journal = journal
        self.sinks = sinks
        self.tools = tools
        self.clock = clock
        self.workflows = WorkflowRegistry()
        self.control = ControlPlane(journal, self.workflows, default_budget=GENEROUS_BUDGET)
        self.provider: LLMProvider = FakeProvider(echo_policy)
        self.hooks = Hooks()
        self.budget_cap: BudgetLimits | None = None

    def register(self, name: str, fn: Any, tools: frozenset[str] = frozenset()) -> None:
        self.workflows.register(WorkflowDefinition(name=name, fn=fn, tools=tools))

    def runtime(self) -> Runtime:
        return Runtime(
            journal=self.journal,
            workflows=self.workflows,
            tools=self.tools,
            provider=self.provider,
            clock=self.clock,
            metrics=self.journal_metrics(),
            hooks=self.hooks,
            budget_cap=self.budget_cap,
        )

    def journal_metrics(self) -> Any:
        return self.journal._metrics or create_runtime_metrics()

    def worker(self, worker_id: str = "w1", **kwargs: Any) -> Worker:
        kwargs.setdefault("heartbeat_seconds", None)
        return Worker(self.runtime(), worker_id=worker_id, **kwargs)

    def start(
        self,
        workflow: str,
        value: Any = None,
        *,
        run_id: str = "run-1",
        principal: str = "alice",
        budget: BudgetLimits | None = None,
    ) -> str:
        self.control.start_run(
            workflow=workflow,
            input_value=value,
            principal=principal,
            run_id=run_id,
            budget=budget,
        )
        return run_id

    def approve_pending(self, run_id: str, approver: str = "bob", approve: bool = True) -> int:
        count = 0
        for request in self.control.approvals(run_id=run_id):
            if request.status.value == "pending":
                self.control.decide(
                    request.approval_id,
                    principal=approver,
                    approve=approve,
                    call_hash=request.call_hash,
                )
                count += 1
        return count


@pytest.fixture
def harness(journal: Journal, sinks: SinkStore, tools: ToolRegistry, clock: FakeClock) -> Harness:
    return Harness(journal, sinks, tools, clock)


HarnessFactory = Callable[[], Harness]
