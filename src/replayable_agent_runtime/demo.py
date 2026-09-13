"""Offline end-to-end demo: kill a worker mid-run with SIGKILL, resume by replay, approve via the
CLI, verify the journal. No API key, no network.

1. Start ``research_ticket`` as ``alice``.
2. Start a worker subprocess whose fake knowledge-base service is slow, and kill it (SIGKILL on
   POSIX, TerminateProcess on Windows) while it is inside the second ``http_get`` call.
3. Restart a worker: it replays the journal (no model call is repeated), re-executes the
   interrupted read, and parks the run on the ``create_ticket`` approval.
4. ``rar approvals decide`` as ``alice`` is refused (separation of duties); as ``bob`` it passes.
   Same for the ``notify`` approval. Workers finish the run.
5. ``rar audit verify`` passes; a one-byte change in an exported copy is detected.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess  # nosec B404 - only starts this package's own worker with a fixed argv
import sys
import time
from pathlib import Path
from typing import Any

from pydantic import JsonValue

from replayable_agent_runtime import cli
from replayable_agent_runtime.agentic.audit import verify_export
from replayable_agent_runtime.core.config import Settings
from replayable_agent_runtime.runtime.events import EventType
from replayable_agent_runtime.runtime.factory import build_services

__all__ = ["run_demo"]

RUN_ID = "demo-research"
_KILL_TIMEOUT_SECONDS = 120.0
_LEASE_SECONDS = 3


def _cli(workdir: Path, *args: str) -> tuple[int, Any]:
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = cli.main([*_paths(workdir), *args])
    text = stdout.getvalue().strip()
    return code, json.loads(text) if text else stderr.getvalue().strip()


def _paths(workdir: Path) -> list[str]:
    return [
        "--journal",
        str(workdir / "rar.db"),
        "--sinks",
        str(workdir / "sinks.db"),
        "--workspace",
        str(workdir / "workspace"),
    ]


def _worker_command(workdir: Path, latency: float) -> list[str]:
    return [
        sys.executable,
        "-m",
        "replayable_agent_runtime",
        *_paths(workdir),
        "worker",
        "--until-idle",
        "--worker-id",
        f"demo-worker-{time.monotonic_ns()}",
        "--fake-service-latency",
        str(latency),
    ]


def _environment() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("APP_")}
    env.pop("ANTHROPIC_API_KEY", None)
    # A short lease so the demo does not wait 30 s for the killed worker's lease to expire.
    env.update(
        {
            "APP_PROVIDER": "fake",
            "APP_LOG_LEVEL": "WARNING",
            "APP_LEASE_SECONDS": str(_LEASE_SECONDS),
            "APP_HEARTBEAT_SECONDS": "1",
        }
    )
    return env


def _run_worker(workdir: Path) -> None:
    subprocess.run(  # noqa: S603  # nosec B603 - fixed argv, our own module, no shell
        _worker_command(workdir, 0.0),
        check=True,
        env=_environment(),
        capture_output=True,
        timeout=300,
    )


def _in_second_http_get(history: list[Any]) -> bool:
    scheduled = [
        r
        for r in history
        if r.type == EventType.STEP_SCHEDULED and r.data().get("name") == "http_get"
    ]
    if len(scheduled) < 2:
        return False
    command = scheduled[1].command
    return not any(
        r.command == command and r.type in (EventType.STEP_COMPLETED, EventType.STEP_FAILED)
        for r in history
    )


def _decide_all(workdir: Path, log: list[dict[str, JsonValue]]) -> None:
    code, pending = _cli(workdir, "approvals", "list", "--run-id", RUN_ID, "--status", "pending")
    if code != 0:
        raise RuntimeError(f"approvals list failed: {pending}")
    for item in pending:
        refused, _ = _cli(
            workdir,
            "approvals", "decide", item["approval_id"], "--approve",
            "--call-hash", item["call_hash"], "--as", "alice",
        )  # fmt: skip
        approved, decided = _cli(
            workdir,
            "approvals", "decide", item["approval_id"], "--approve",
            "--call-hash", item["call_hash"], "--as", "bob",
        )  # fmt: skip
        log.append(
            {
                "tool": item["name"],
                "args": item["args"],
                "call_hash": item["call_hash"],
                "self_approval_refused": refused != 0,
                "approved_by": decided["decided_by"] if approved == 0 else None,
            }
        )


def run_demo(workdir: Path) -> dict[str, JsonValue]:
    if workdir.exists():
        shutil.rmtree(workdir)
    (workdir / "workspace" / "notes").mkdir(parents=True)
    (workdir / "workspace" / "notes" / "oncall.md").write_text(
        "On-call notes: cache tier alarms fired twice this week.\n", encoding="utf-8"
    )
    settings = Settings(
        _env_file=None,
        journal_path=workdir / "rar.db",
        sinks_path=workdir / "sinks.db",
        workspace_dir=workdir / "workspace",
    )
    services = build_services(settings)
    journal = services.journal
    try:
        services.control.start_run(
            workflow="research_ticket",
            input_value={"topic": "cache"},
            principal="alice",
            run_id=RUN_ID,
        )

        # 2. Kill a worker while it is inside the second (slow) knowledge-base read.
        process = subprocess.Popen(  # noqa: S603  # nosec B603 - fixed argv, our own module
            _worker_command(workdir, 1.0),
            env=_environment(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + _KILL_TIMEOUT_SECONDS
        while not _in_second_http_get(journal.load_history(RUN_ID)):
            if process.poll() is not None or time.monotonic() > deadline:
                process.kill()
                raise RuntimeError("worker finished or stalled before the kill point")
            time.sleep(0.02)
        process.kill()
        process.wait(timeout=30)
        at_kill = journal.load_history(RUN_ID)
        calls_at_kill = services.meter.calls(RUN_ID)
        in_flight = journal.list_outbox(run_id=RUN_ID, status="pending")

        # The dead worker never released its lease: no other worker may take the run until it
        # expires. Then a restarted worker reclaims it with a higher fencing token.
        waited_from = time.monotonic()
        while journal.get_run(RUN_ID).lease_expires_at_us > journal.now_us():
            if time.monotonic() - waited_from > 10 * _LEASE_SECONDS:
                raise RuntimeError("the killed worker's lease did not expire")
            time.sleep(0.1)
        lease_wait = round(time.monotonic() - waited_from, 2)

        # 3-4. Restart, approve via the CLI, repeat until the run is finished.
        approvals: list[dict[str, JsonValue]] = []
        restarts = 0
        for _ in range(6):
            _run_worker(workdir)
            restarts += 1
            run = journal.get_run(RUN_ID)
            if run.terminal:
                break
            _decide_all(workdir, approvals)

        run = journal.get_run(RUN_ID)
        # 5. Verify the journal, then tamper with an exported copy.
        verify_code, verified = _cli(workdir, "audit", "verify", "--run-id", RUN_ID)
        export_path = workdir / "export" / f"{RUN_ID}.jsonl"
        _cli(workdir, "audit", "export", RUN_ID, "--out", str(export_path))
        raw = bytearray(export_path.read_bytes())
        index = raw.index(b'"cache')
        raw[index + 1] ^= 0x01
        tampered = verify_export(bytes(raw))

        duplicates = services.meter.duplicates(RUN_ID)
        result = run.result if isinstance(run.result, dict) else {}
        ledger = services.sinks.ledger(RUN_ID)
        approvals_json: list[JsonValue] = list(approvals)
        ticket_writes: list[JsonValue] = [
            json.loads(entry.result) for entry in ledger if entry.sink == "tickets"
        ]
        summary: dict[str, JsonValue] = {
            "run_id": RUN_ID,
            "status": run.status.value,
            "killed_at_head_seq": at_kill[-1].seq,
            "killed_inside": "http_get (second knowledge-base read)",
            "provider_calls_before_kill": calls_at_kill,
            "pending_side_effects_at_kill": len(in_flight),
            "waited_for_dead_lease_seconds": lease_wait,
            "fencing_token_after_recovery": run.fencing,
            "provider_calls_total": services.meter.calls(RUN_ID),
            "duplicate_provider_calls": sum(count - 1 for count in duplicates.values()),
            "worker_restarts": restarts,
            "approvals": approvals_json,
            "ticket_writes": ticket_writes,
            "notifications": len([entry for entry in ledger if entry.sink == "notifier"]),
            "result_tickets": result.get("tickets"),
            "journal_events": run.head_seq + 1,
            "audit_verify_exit_code": verify_code,
            "audit_ok": bool(verified.get("ok")) if isinstance(verified, dict) else False,
            "tampered_export_detected": not tampered.ok,
            "tamper_error": tampered.error,
        }
        summary["ok"] = bool(
            run.status.value == "completed"
            and summary["duplicate_provider_calls"] == 0
            and summary["audit_ok"]
            and summary["tampered_export_detected"]
            and all(bool(item["self_approval_refused"]) for item in approvals)
        )
        return summary
    finally:
        services.close()
