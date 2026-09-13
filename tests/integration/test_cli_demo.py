from __future__ import annotations

import json
import runpy
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from opentelemetry import trace as otel_trace

from replayable_agent_runtime import cli
from replayable_agent_runtime.core import tracing
from replayable_agent_runtime.core.config import get_settings
from replayable_agent_runtime.core.security import TokenService
from replayable_agent_runtime.demo import run_demo

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh_settings() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class Cli:
    def __init__(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        self.paths = [
            "--journal", str(tmp_path / "rar.db"),
            "--sinks", str(tmp_path / "sinks.db"),
            "--workspace", str(tmp_path / "workspace"),
        ]  # fmt: skip
        self._capsys = capsys
        notes = tmp_path / "workspace" / "notes"
        notes.mkdir(parents=True, exist_ok=True)
        (notes / "oncall.md").write_text("Cache alarms fired twice.\n", encoding="utf-8")

    def __call__(self, *args: str) -> tuple[int, Any]:
        self._capsys.readouterr()
        code = cli.main([*self.paths, *args])
        out = self._capsys.readouterr().out.strip()
        return code, json.loads(out) if out else None


@pytest.mark.usefixtures("restore_logging")
def test_the_cli_runs_approves_verifies_and_exports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rar = Cli(tmp_path, capsys)
    code, started = rar(
        "runs", "start", "--workflow", "research_ticket", "--input", '{"topic": "cache"}',
        "--as", "alice", "--run-id", "cli-run",
    )  # fmt: skip
    assert (code, started["created"], started["status"]) == (0, True, "pending")
    for _ in range(3):
        assert rar("worker", "--until-idle")[0] == 0
        code, pending = rar("approvals", "list", "--run-id", "cli-run", "--status", "pending")
        if not pending:
            break
        item = pending[0]
        refused, _ = rar(
            "approvals", "decide", item["approval_id"], "--approve",
            "--call-hash", item["call_hash"], "--as", "alice",
        )  # fmt: skip
        assert refused == 1
        code, decided = rar(
            "approvals", "decide", item["approval_id"], "--approve",
            "--call-hash", item["call_hash"], "--as", "bob",
        )  # fmt: skip
        assert (code, decided["decided_by"]) == (0, "bob")
    assert rar("runs", "show", "cli-run")[1]["status"] == "completed"
    assert [run["run_id"] for run in rar("runs", "list")[1]] == ["cli-run"]
    assert rar("runs", "history", "cli-run")[1][-1]["type"] == "RunCompleted"
    assert rar("runs", "history", "cli-run", "--no-redact")[0] == 0
    assert rar("runs", "replay-check", "cli-run")[1] == {
        "deterministic": True,
        "outcome": "completed",
    }
    assert rar("runs", "retry", "cli-run")[0] == 1
    code, verified = rar("audit", "verify")
    assert (code, verified["ok"]) == (0, True)
    assert rar("audit", "verify", "--run-id", "cli-run", "--expect-head", "0" * 64)[0] == 1
    export = tmp_path / "exports" / "cli-run.jsonl"
    assert rar("audit", "export", "cli-run", "--out", str(export))[0] == 0
    assert rar("audit", "verify-export", str(export))[1]["ok"] is True
    export.write_bytes(export.read_bytes().replace(b"cache", b"cachE", 1))
    assert rar("audit", "verify-export", str(export))[0] == 1
    assert rar("signal", "cli-run", "go", "--as", "bob")[0] == 1
    assert rar("cancel", "cli-run")[0] == 1


@pytest.mark.usefixtures("restore_logging")
def test_signals_cancel_and_bad_input_through_the_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rar = Cli(tmp_path, capsys)
    start = ["runs", "start", "--workflow", "research_ticket", "--as", "alice"]
    assert rar(*start, "--input", '{"topic": "db"}', "--run-id", "r2")[0] == 0
    signalled = rar("signal", "r2", "nudge", "--payload", '{"n": 1}', "--as", "bob")
    assert signalled[1] == {"signal_id": 1}
    assert rar("cancel", "r2")[1]["cancel_requested"] is True
    bad_json_exit, _ = rar(*start, "--input", "{not json")
    assert bad_json_exit == 1
    assert rar("runs", "show", "missing")[0] == 1


def test_tokens_and_secrets(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    rar = Cli(tmp_path, capsys)
    code, secret = rar("generate-secret")
    assert code == 0
    assert rar("token", "--sub", "alice", "--scope", "runs:read")[0] == 1
    monkeypatch.setenv("APP_JWT_SECRET", secret["jwt_secret"])
    get_settings.cache_clear()
    code, issued = rar("token", "--sub", "alice", "--scope", "runs:read", "runs:start")
    assert code == 0
    settings = get_settings()
    principal = TokenService(
        settings.jwt_secret, issuer=settings.jwt_issuer, audience=settings.jwt_audience
    ).verify(issued["token"])
    assert principal is not None
    assert principal.scopes == frozenset({"runs:read", "runs:start"})


@pytest.mark.usefixtures("restore_logging")
def test_cli_serve_configures_uvicorn_safely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import uvicorn

    captured: dict[str, Any] = {}

    def fake_run(app: Any, **kwargs: Any) -> None:
        captured.update(kwargs, app=app)
        app.state.services.close()

    monkeypatch.setattr(uvicorn, "run", fake_run)
    monkeypatch.setattr(tracing, "_installed", [])
    monkeypatch.setattr(otel_trace, "set_tracer_provider", lambda provider: None)
    paths = ["--journal", str(tmp_path / "a.db"), "--sinks", str(tmp_path / "b.db")]
    assert cli.main([*paths, "serve", "--port", "8123"]) == 0
    assert (captured["host"], captured["port"]) == ("127.0.0.1", 8123)
    assert captured["server_header"] is False
    assert captured["access_log"] is False


@pytest.mark.usefixtures("restore_logging")
def test_the_cli_demo_command_reports_failure_through_its_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import replayable_agent_runtime.demo as demo_module

    monkeypatch.setattr(
        demo_module, "run_demo", lambda workdir: {"ok": False, "workdir": str(workdir)}
    )
    assert cli.main(["demo", "--workdir", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out)["ok"] is False


def test_module_entry_point(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["replayable_agent_runtime", "generate-secret"])
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_module("replayable_agent_runtime", run_name="__main__")
    assert exit_info.value.code == 0
    assert "jwt_secret" in capsys.readouterr().out


@pytest.mark.usefixtures("restore_logging")
def test_the_offline_demo_kills_resumes_approves_and_verifies(tmp_path: Path) -> None:
    summary = run_demo(tmp_path / "demo")
    assert summary["ok"] is True
    assert summary["status"] == "completed"
    assert summary["duplicate_provider_calls"] == 0
    assert summary["audit_ok"] is True
    assert summary["tampered_export_detected"] is True
    approvals = cast(list[dict[str, Any]], summary["approvals"])
    assert [item["tool"] for item in approvals] == ["create_ticket", "notify"]
    assert all(item["self_approval_refused"] for item in approvals)
    assert all(item["approved_by"] == "bob" for item in approvals)
    assert summary["waited_for_dead_lease_seconds"] is not None
    assert summary["result_tickets"] == ["T-1"]
