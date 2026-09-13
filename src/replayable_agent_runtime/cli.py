"""Command line entry point: ``rar`` (or ``python -m replayable_agent_runtime``).

Local commands open the journal database directly, which is equivalent to full control of the
runtime: protect the database file like the journal it is. Separation of duties is still enforced
(``approvals decide --as`` must differ from the run starter).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from replayable_agent_runtime.agentic.approval import ApprovalStatus
from replayable_agent_runtime.agentic.audit import verify_export
from replayable_agent_runtime.agentic.errors import AgenticError
from replayable_agent_runtime.core.config import Settings, get_settings
from replayable_agent_runtime.runtime.errors import NonDeterminismError

__all__ = ["main"]


def _emit(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def _settings(args: argparse.Namespace) -> Settings:
    updates: dict[str, Any] = {}
    for option, field_name in (
        ("journal", "journal_path"),
        ("sinks", "sinks_path"),
        ("workspace", "workspace_dir"),
    ):
        value = getattr(args, option, None)
        if value is not None:
            updates[field_name] = value
    base = get_settings()
    return base.model_copy(update=updates) if updates else base


def _services(args: argparse.Namespace, **kwargs: Any) -> Any:
    from replayable_agent_runtime.runtime.factory import build_services  # noqa: PLC0415

    return build_services(_settings(args), **kwargs)


def _serve(args: argparse.Namespace) -> int:
    import uvicorn  # noqa: PLC0415 - only needed when serving

    from replayable_agent_runtime.core.logging import configure_logging  # noqa: PLC0415
    from replayable_agent_runtime.core.tracing import (  # noqa: PLC0415
        install_global_tracer_provider,
    )
    from replayable_agent_runtime.runtime.api import create_control_app  # noqa: PLC0415

    settings = _settings(args)
    configure_logging(settings.log_level, json_output=settings.log_json)
    app = create_control_app(settings)
    install_global_tracer_provider(app.state.tracer_provider)
    uvicorn.run(
        app,
        host=args.host or settings.host,
        port=args.port or settings.port,
        log_config=None,
        access_log=False,
        server_header=False,
    )
    return 0


def _worker(args: argparse.Namespace) -> int:
    from replayable_agent_runtime.core.logging import configure_logging  # noqa: PLC0415
    from replayable_agent_runtime.runtime.worker import Worker  # noqa: PLC0415

    settings = _settings(args)
    configure_logging(settings.log_level, json_output=settings.log_json, stream=sys.stderr)
    services = _services(args, service_latency_seconds=args.fake_service_latency)
    worker = Worker(
        services.runtime,
        worker_id=args.worker_id or settings.worker_id,
        lease_seconds=settings.lease_seconds,
        heartbeat_seconds=min(settings.heartbeat_seconds, settings.lease_seconds / 2),
        poll_seconds=settings.poll_seconds,
    )
    try:
        if args.until_idle:
            outcomes = asyncio.run(worker.run_until_idle())
            _emit([{"run_id": o.run_id, "outcome": o.outcome} for o in outcomes])
        else:  # pragma: no cover - long-running loop, exercised through the API lifespan
            asyncio.run(worker.serve(asyncio.Event()))
    finally:
        services.close()
    return 0


def _token(args: argparse.Namespace) -> int:
    from replayable_agent_runtime.core.security import TokenService  # noqa: PLC0415

    settings = _settings(args)
    service = TokenService(
        settings.jwt_secret,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        ttl_seconds=settings.jwt_ttl_seconds,
    )
    if not service.configured:
        print(
            "APP_JWT_SECRET is not set (generate one with `rar generate-secret`).", file=sys.stderr
        )
        return 1
    _emit({"token": service.issue(args.sub, args.scope, ttl_seconds=args.ttl)})
    return 0


def _runs(args: argparse.Namespace) -> int:
    services = _services(args)
    control = services.control
    try:
        if args.runs_command == "start":
            run, created = control.start_run(
                workflow=args.workflow,
                input_value=json.loads(args.input),
                principal=args.principal,
                run_id=args.run_id,
            )
            _emit({"created": created, **run.summary()})
        elif args.runs_command == "list":
            _emit([run.summary() for run in control.runs()])
        elif args.runs_command == "show":
            _emit(control.get(args.run_id).summary())
        elif args.runs_command == "history":
            _emit(control.history(args.run_id, redact=not args.no_redact))
        elif args.runs_command == "retry":
            _emit(services.journal.retry_blocked(args.run_id).summary())
        else:
            from replayable_agent_runtime.runtime.engine import replay_check  # noqa: PLC0415

            try:
                outcome = asyncio.run(replay_check(services.runtime, args.run_id))
            except NonDeterminismError as exc:
                _emit({"deterministic": False, "error": str(exc)})
                return 1
            _emit({"deterministic": True, "outcome": outcome.outcome})
    finally:
        services.close()
    return 0


def _signal(args: argparse.Namespace) -> int:
    services = _services(args)
    try:
        signal_id = services.control.signal(
            args.run_id, args.name, json.loads(args.payload), principal=args.principal
        )
        _emit({"signal_id": signal_id})
    finally:
        services.close()
    return 0


def _cancel(args: argparse.Namespace) -> int:
    services = _services(args)
    try:
        _emit(services.control.cancel(args.run_id).summary())
    finally:
        services.close()
    return 0


def _approvals(args: argparse.Namespace) -> int:
    from replayable_agent_runtime.runtime.control import approval_view  # noqa: PLC0415

    services = _services(args)
    try:
        if args.approvals_command == "list":
            status = ApprovalStatus(args.status) if args.status else None
            items = services.control.approvals(run_id=args.run_id, status=status)
            _emit([approval_view(item) for item in items])
        else:
            decided = services.control.decide(
                args.approval_id,
                principal=args.principal,
                approve=args.approve,
                call_hash=args.call_hash,
                reason=args.reason,
            )
            _emit(approval_view(decided))
    finally:
        services.close()
    return 0


def _audit(args: argparse.Namespace) -> int:
    if args.audit_command == "verify-export":
        result = verify_export(args.path.read_bytes(), expect_head=args.expect_head)
        _emit(result.as_dict())
        return 0 if result.ok else 1
    services = _services(args)
    try:
        if args.audit_command == "export":
            lines = services.journal.export_run(args.run_id, redact=args.redact)
            args.out.parent.mkdir(parents=True, exist_ok=True)
            with args.out.open("w", encoding="utf-8", newline="\n") as handle:
                handle.writelines(lines)
            _emit({"exported": str(args.out), "redacted": args.redact})
            return 0
        if args.run_id:
            results = {
                args.run_id: services.journal.verify_run(args.run_id, expect_head=args.expect_head)
            }
        else:
            results = services.journal.verify_all()
        report = {run_id: result.as_dict() for run_id, result in results.items()}
        ok = all(result.ok for result in results.values())
        _emit({"ok": ok, "runs": report})
        return 0 if ok else 1
    finally:
        services.close()


def _demo(args: argparse.Namespace) -> int:
    from replayable_agent_runtime.core.logging import configure_logging  # noqa: PLC0415
    from replayable_agent_runtime.demo import run_demo  # noqa: PLC0415

    configure_logging("WARNING", json_output=True, stream=sys.stderr)
    summary = run_demo(args.workdir)
    _emit(summary)
    return 0 if summary.get("ok") else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rar", description="Replayable agent runtime")
    parser.add_argument("--journal", type=Path, default=None, help="journal database path")
    parser.add_argument("--sinks", type=Path, default=None, help="fake sinks database path")
    parser.add_argument("--workspace", type=Path, default=None, help="file tool workspace")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the control-plane API (and embedded worker)")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)

    worker = sub.add_parser("worker", help="run a worker")
    worker.add_argument("--until-idle", action="store_true")
    worker.add_argument("--worker-id", default=None)
    worker.add_argument("--fake-service-latency", type=float, default=0.0)

    sub.add_parser("generate-secret", help="print a new random APP_JWT_SECRET")
    token = sub.add_parser("token", help="issue a local JWT (needs APP_JWT_SECRET)")
    token.add_argument("--sub", required=True)
    token.add_argument("--scope", nargs="+", required=True)
    token.add_argument("--ttl", type=int, default=None)

    runs = sub.add_parser("runs", help="start and inspect runs")
    runs_sub = runs.add_subparsers(dest="runs_command", required=True)
    start = runs_sub.add_parser("start")
    start.add_argument("--workflow", required=True)
    start.add_argument("--input", default="{}")
    start.add_argument("--as", dest="principal", required=True)
    start.add_argument("--run-id", default=None)
    runs_sub.add_parser("list")
    for name in ("show", "retry", "replay-check"):
        runs_sub.add_parser(name).add_argument("run_id")
    history = runs_sub.add_parser("history")
    history.add_argument("run_id")
    history.add_argument("--no-redact", action="store_true")

    signal = sub.add_parser("signal", help="send a signal to a run")
    signal.add_argument("run_id")
    signal.add_argument("name")
    signal.add_argument("--payload", default="null")
    signal.add_argument("--as", dest="principal", required=True)

    sub.add_parser("cancel", help="request cancellation of a run").add_argument("run_id")

    approvals = sub.add_parser("approvals", help="list and decide approvals")
    approvals_sub = approvals.add_subparsers(dest="approvals_command", required=True)
    listing = approvals_sub.add_parser("list")
    listing.add_argument("--run-id", default=None)
    listing.add_argument("--status", choices=[s.value for s in ApprovalStatus], default=None)
    decide = approvals_sub.add_parser("decide")
    decide.add_argument("approval_id")
    verdict = decide.add_mutually_exclusive_group(required=True)
    verdict.add_argument("--approve", action="store_true")
    verdict.add_argument("--deny", action="store_true")
    decide.add_argument("--call-hash", required=True)
    decide.add_argument("--as", dest="principal", required=True)
    decide.add_argument("--reason", default=None)

    audit = sub.add_parser("audit", help="verify and export the hash-chained journal")
    audit_sub = audit.add_subparsers(dest="audit_command", required=True)
    verify = audit_sub.add_parser("verify")
    verify.add_argument("--run-id", default=None)
    verify.add_argument("--expect-head", default=None)
    export = audit_sub.add_parser("export")
    export.add_argument("run_id")
    export.add_argument("--out", type=Path, required=True)
    export.add_argument("--redact", action="store_true")
    verify_file = audit_sub.add_parser("verify-export")
    verify_file.add_argument("path", type=Path)
    verify_file.add_argument("--expect-head", default=None)

    demo = sub.add_parser("demo", help="run the offline kill-and-resume demo")
    demo.add_argument("--workdir", type=Path, default=Path("var/demo"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    handlers = {
        "serve": _serve,
        "worker": _worker,
        "token": _token,
        "runs": _runs,
        "signal": _signal,
        "cancel": _cancel,
        "approvals": _approvals,
        "audit": _audit,
        "demo": _demo,
    }
    if args.command == "generate-secret":
        from replayable_agent_runtime.core.security import generate_secret  # noqa: PLC0415

        _emit({"jwt_secret": generate_secret()})
        print("Store it only in the environment as APP_JWT_SECRET.", file=sys.stderr)
        return 0
    try:
        return handlers[args.command](args)
    except (AgenticError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
