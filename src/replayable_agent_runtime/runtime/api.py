"""FastAPI control plane: start, signal, approve, query, cancel, history and verify runs.

Every route requires a bearer JWT with the listed scope. Handlers are synchronous (FastAPI runs
them in its thread pool) because journal access is blocking SQLite I/O.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, FastAPI, Path, Query, Request
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import CollectorRegistry
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from replayable_agent_runtime.agentic.approval import ApprovalStatus
from replayable_agent_runtime.agentic.budget import BudgetLimits
from replayable_agent_runtime.agentic.errors import ApprovalError
from replayable_agent_runtime.core.app import create_app
from replayable_agent_runtime.core.config import Settings, get_settings
from replayable_agent_runtime.core.errors import AppError
from replayable_agent_runtime.core.security import Principal, require_scope
from replayable_agent_runtime.core.tracing import build_tracer_provider
from replayable_agent_runtime.runtime.control import (
    ControlPlane,
    InputTooLargeError,
    approval_view,
)
from replayable_agent_runtime.runtime.errors import RunNotFoundError, RunStateError
from replayable_agent_runtime.runtime.factory import Services, build_services
from replayable_agent_runtime.runtime.journal import RUN_ID_PATTERN, RunStatus
from replayable_agent_runtime.runtime.worker import Worker, WorkerHealth
from replayable_agent_runtime.runtime.workflows import UnknownWorkflowError

__all__ = ["build_router", "create_control_app"]

_APPROVAL_ID = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}:[0-9]{1,9}$"
_SIGNAL_NAME = r"^[a-z][a-z0-9_-]{0,63}$"

Starter = Annotated[Principal, Depends(require_scope("runs:start"))]
Signaller = Annotated[Principal, Depends(require_scope("runs:signal"))]
Reader = Annotated[Principal, Depends(require_scope("runs:read"))]
Approver = Annotated[Principal, Depends(require_scope("runs:approve"))]
RunIdPath = Annotated[str, Path(pattern=RUN_ID_PATTERN)]


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BudgetRequest(_Body):
    max_steps: int = Field(ge=0, le=100_000)
    max_tokens: int = Field(ge=0, le=100_000_000)
    max_usd: float = Field(ge=0, le=10_000)


class StartRunRequest(_Body):
    workflow: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    input: JsonValue = None
    run_id: str | None = Field(default=None, pattern=RUN_ID_PATTERN)
    budget: BudgetRequest | None = None


class SignalRequest(_Body):
    payload: JsonValue = None


class DecisionRequest(_Body):
    decision: Literal["approve", "deny"]
    call_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str | None = Field(default=None, max_length=500)


def _control(request: Request) -> ControlPlane:
    control: ControlPlane = request.app.state.control
    return control


def _not_found(exc: Exception) -> AppError:
    del exc
    return AppError(404, "not_found", "Run or approval not found.")


def build_router() -> APIRouter:
    router = APIRouter(prefix="/v1")

    @router.post("/runs", status_code=201)
    def start_run(body: StartRunRequest, principal: Starter, request: Request) -> JSONResponse:
        budget = (
            BudgetLimits.of(
                steps=body.budget.max_steps, tokens=body.budget.max_tokens, usd=body.budget.max_usd
            )
            if body.budget is not None
            else None
        )
        try:
            run, created = _control(request).start_run(
                workflow=body.workflow,
                input_value=body.input,
                principal=principal.subject,
                run_id=body.run_id,
                budget=budget,
            )
        except UnknownWorkflowError as exc:
            raise AppError(422, "unknown_workflow", "Workflow is not registered.") from exc
        except InputTooLargeError as exc:
            raise AppError(413, "payload_too_large", "Workflow input is too large.") from exc
        except RunStateError as exc:
            raise AppError(409, "conflict", "A different run already uses this run_id.") from exc
        return JSONResponse(run.summary(), status_code=201 if created else 200)

    @router.get("/runs")
    def list_runs(
        _: Reader,
        request: Request,
        status: RunStatus | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> list[dict[str, JsonValue]]:
        return [run.summary() for run in _control(request).runs(status, limit)]

    @router.get("/runs/{run_id}")
    def get_run(run_id: RunIdPath, _: Reader, request: Request) -> dict[str, JsonValue]:
        try:
            return _control(request).get(run_id).summary()
        except RunNotFoundError as exc:
            raise _not_found(exc) from exc

    @router.get("/runs/{run_id}/history")
    def history(run_id: RunIdPath, _: Reader, request: Request) -> list[dict[str, JsonValue]]:
        try:
            return _control(request).history(run_id, redact=True)
        except RunNotFoundError as exc:
            raise _not_found(exc) from exc

    @router.get("/runs/{run_id}/verify")
    def verify(
        run_id: RunIdPath,
        _: Reader,
        request: Request,
        expect_head: Annotated[str | None, Query(pattern=r"^[0-9a-f]{64}$")] = None,
    ) -> dict[str, object]:
        try:
            return _control(request).verify(run_id, expect_head=expect_head).as_dict()
        except RunNotFoundError as exc:
            raise _not_found(exc) from exc

    @router.post("/runs/{run_id}/signals/{name}", status_code=202)
    def signal(
        run_id: RunIdPath,
        name: Annotated[str, Path(pattern=_SIGNAL_NAME)],
        body: SignalRequest,
        principal: Signaller,
        request: Request,
    ) -> dict[str, Any]:
        try:
            signal_id = _control(request).signal(
                run_id, name, body.payload, principal=principal.subject
            )
        except RunNotFoundError as exc:
            raise _not_found(exc) from exc
        except RunStateError as exc:
            raise AppError(409, "conflict", "The run no longer accepts signals.") from exc
        except InputTooLargeError as exc:
            raise AppError(413, "payload_too_large", "Signal payload is too large.") from exc
        return {"signal_id": signal_id}

    @router.post("/runs/{run_id}/cancel", status_code=202)
    def cancel(run_id: RunIdPath, _: Signaller, request: Request) -> dict[str, JsonValue]:
        try:
            return _control(request).cancel(run_id).summary()
        except RunNotFoundError as exc:
            raise _not_found(exc) from exc
        except RunStateError as exc:
            raise AppError(409, "conflict", "The run cannot be cancelled.") from exc

    @router.get("/approvals")
    def approvals(
        _: Reader,
        request: Request,
        run_id: Annotated[str | None, Query(pattern=RUN_ID_PATTERN)] = None,
        status: ApprovalStatus | None = None,
    ) -> list[dict[str, JsonValue]]:
        return [
            approval_view(item)
            for item in _control(request).approvals(run_id=run_id, status=status)
        ]

    @router.post("/approvals/{approval_id}")
    def decide(
        approval_id: Annotated[str, Path(pattern=_APPROVAL_ID)],
        body: DecisionRequest,
        principal: Approver,
        request: Request,
    ) -> dict[str, JsonValue]:
        control = _control(request)
        try:
            pending = control.journal.get_approval(approval_id)
            run = control.get(pending.run_id)
        except (ApprovalError, RunNotFoundError) as exc:
            raise _not_found(exc) from exc
        if run.started_by == principal.subject:
            raise AppError(
                403, "forbidden", "The principal that started a run cannot approve its steps."
            )
        try:
            decided = control.decide(
                approval_id,
                principal=principal.subject,
                approve=body.decision == "approve",
                call_hash=body.call_hash,
                reason=body.reason,
            )
        except ApprovalError as exc:
            raise AppError(
                409, "conflict", "Approval is not pending or does not match the call hash."
            ) from exc
        return approval_view(decided)

    return router


def create_control_app(
    settings: Settings | None = None,
    *,
    services: Services | None = None,
    registry: CollectorRegistry | None = None,
    tracer_provider: TracerProvider | None = None,
    start_worker: bool | None = None,
) -> FastAPI:
    """The control-plane app, optionally with an embedded worker (scheduler and lease loops)."""
    cfg = settings if settings is not None else get_settings()
    metrics_registry = registry if registry is not None else CollectorRegistry()
    tracing = tracer_provider or build_tracer_provider(cfg.service_name, cfg.tracing_exporter)
    svc = services or build_services(cfg, registry=metrics_registry, tracer_provider=tracing)
    run_worker = cfg.embedded_worker if start_worker is None else start_worker
    health = WorkerHealth(stale_after_seconds=max(10.0, cfg.poll_seconds * 20))
    worker = Worker(
        svc.runtime,
        worker_id=cfg.worker_id,
        lease_seconds=cfg.lease_seconds,
        heartbeat_seconds=min(cfg.heartbeat_seconds, cfg.lease_seconds / 2),
        poll_seconds=cfg.poll_seconds,
        health=health,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        del app
        stop = asyncio.Event()
        task = asyncio.create_task(worker.serve(stop)) if run_worker else None
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                await task

    checks = {"db": svc.journal.ping}
    if run_worker:
        checks["scheduler"] = health.scheduler_ok
        checks["lease_loop"] = health.lease_ok
    return create_app(
        cfg,
        routers=[build_router()],
        readiness_checks=checks,
        registry=svc.metrics.registry,
        tracer_provider=tracing,
        lifespan=lifespan,
        state={"control": svc.control, "services": svc, "worker": worker},
    )
