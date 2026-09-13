"""FastAPI application factory with the baseline hardening applied."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import CollectorRegistry

from replayable_agent_runtime import __version__
from replayable_agent_runtime.core.config import Settings, get_settings
from replayable_agent_runtime.core.errors import install_error_handlers
from replayable_agent_runtime.core.logging import get_logger
from replayable_agent_runtime.core.metrics import create_http_metrics, render_metrics
from replayable_agent_runtime.core.middleware import (
    BodySizeLimitMiddleware,
    RateLimitMiddleware,
    RequestContextMiddleware,
)
from replayable_agent_runtime.core.ratelimit import RateLimiter
from replayable_agent_runtime.core.security import TokenService, extract_bearer, require_scope
from replayable_agent_runtime.core.tracing import build_tracer_provider

__all__ = ["Lifespan", "ReadinessCheck", "create_app"]

ReadinessCheck = Callable[[], bool | Awaitable[bool]]
Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]
_log = get_logger("replayable_agent_runtime.app")


async def _run_check(name: str, check: ReadinessCheck) -> bool:
    try:
        result = check()
        if inspect.isawaitable(result):
            result = await result
        return bool(result)
    except Exception as exc:  # noqa: BLE001 - a failing probe must report not-ready, not crash
        _log.warning("readiness_check_failed", check=name, error_type=type(exc).__name__)
        return False


def create_app(
    settings: Settings | None = None,
    *,
    routers: Sequence[APIRouter] = (),
    readiness_checks: Mapping[str, ReadinessCheck] | None = None,
    registry: CollectorRegistry | None = None,
    tracer_provider: TracerProvider | None = None,
    lifespan: Lifespan | None = None,
    state: Mapping[str, Any] | None = None,
) -> FastAPI:
    """Build the app. All state hangs off ``app.state``; nothing global is mutated."""
    cfg = settings if settings is not None else get_settings()
    checks: dict[str, ReadinessCheck] = dict(readiness_checks or {})
    provider = tracer_provider or build_tracer_provider(cfg.service_name, cfg.tracing_exporter)
    metrics = create_http_metrics(registry)
    tokens = TokenService(
        cfg.jwt_secret,
        issuer=cfg.jwt_issuer,
        audience=cfg.jwt_audience,
        ttl_seconds=cfg.jwt_ttl_seconds,
        leeway_seconds=cfg.jwt_leeway_seconds,
    )
    limiter = RateLimiter(
        cfg.rate_limit_per_second, cfg.rate_limit_burst, max_keys=cfg.rate_limit_max_clients
    )

    app = FastAPI(
        title=cfg.service_name,
        version=__version__,
        docs_url="/docs" if cfg.enable_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if cfg.enable_docs else None,
        lifespan=lifespan,
    )
    app.state.settings = cfg
    app.state.metrics = metrics
    app.state.token_service = tokens
    app.state.rate_limiter = limiter
    app.state.tracer_provider = provider
    for key, value in (state or {}).items():
        setattr(app.state, key, value)
    install_error_handlers(app)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """Liveness: the process is serving requests. Checks no dependencies."""
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        """Readiness: every registered dependency check passes. Reports names, not errors."""
        results = {name: await _run_check(name, check) for name, check in checks.items()}
        ready = all(results.values())
        body: dict[str, Any] = {
            "status": "ready" if ready else "not_ready",
            "checks": {name: "ok" if ok else "fail" for name, ok in results.items()},
        }
        return JSONResponse(body, status_code=200 if ready else 503)

    metrics_deps = [Depends(require_scope("runs:read"))] if cfg.metrics_require_auth else []

    @app.get("/metrics", include_in_schema=False, dependencies=metrics_deps)
    async def metrics_endpoint(request: Request) -> Response:
        payload, content_type = render_metrics(request.app.state.metrics.registry)
        return Response(content=payload, media_type=content_type)

    for router in routers:
        app.include_router(router)

    def identify(headers: Mapping[str, str]) -> str | None:
        principal = tokens.verify(extract_bearer(headers))
        return principal.subject if principal is not None else None

    # add_middleware prepends, so the last one added is the outermost.
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=cfg.max_body_bytes, metrics=metrics)
    app.add_middleware(RateLimitMiddleware, limiter=limiter, identify=identify, metrics=metrics)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cfg.cors_allow_origins),
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
        max_age=600,
    )
    app.add_middleware(
        RequestContextMiddleware,
        tracer=provider.get_tracer("replayable_agent_runtime.http"),
        metrics=metrics,
        docs_enabled=cfg.enable_docs,
    )
    return app
