from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

from replayable_agent_runtime.core.config import Settings
from replayable_agent_runtime.core.middleware import SECURITY_HEADERS
from replayable_agent_runtime.core.security import TokenService
from replayable_agent_runtime.runtime.api import create_control_app

pytestmark = pytest.mark.integration

SECRET = "api-test-signing-secret-" + "z" * 24


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "environment": "test",
        "jwt_secret": SecretStr(SECRET),
        "journal_path": tmp_path / "rar.db",
        "sinks_path": tmp_path / "sinks.db",
        "workspace_dir": tmp_path / "workspace",
        "rate_limit_burst": 1_000,
        "rate_limit_per_second": 1_000.0,
    }
    values.update(overrides)
    notes = tmp_path / "workspace" / "notes"
    notes.mkdir(parents=True, exist_ok=True)
    (notes / "oncall.md").write_text("Cache alarms fired twice.\n", encoding="utf-8")
    return Settings(_env_file=None, **values)


def bearer(settings: Settings, subject: str, *scopes: str) -> dict[str, str]:
    service = TokenService(
        settings.jwt_secret, issuer=settings.jwt_issuer, audience=settings.jwt_audience
    )
    return {"Authorization": f"Bearer {service.issue(subject, scopes)}"}


ALL = ("runs:start", "runs:read", "runs:approve", "runs:signal")


class Api:
    def __init__(self, tmp_path: Path, **overrides: Any) -> None:
        self.settings = make_settings(tmp_path, **overrides)
        self.app: FastAPI = create_control_app(self.settings, start_worker=False)

    def work(self) -> None:
        asyncio.run(self.app.state.worker.run_until_idle())

    def close(self) -> None:
        self.app.state.services.close()


@pytest.fixture
def api(tmp_path: Path) -> Iterator[tuple[Api, TestClient]]:
    instance = Api(tmp_path)
    with TestClient(instance.app, raise_server_exceptions=False) as client:
        yield instance, client
    instance.close()


def approve_all(api: Api, client: TestClient, run_id: str, headers: dict[str, str]) -> list[str]:
    names = []
    for item in client.get(
        "/v1/approvals", params={"run_id": run_id, "status": "pending"}, headers=headers
    ).json():
        response = client.post(
            f"/v1/approvals/{item['approval_id']}",
            json={"decision": "approve", "call_hash": item["call_hash"]},
            headers=headers,
        )
        assert response.status_code == 200
        names.append(item["name"])
    return names


def test_the_control_plane_drives_a_run_through_scoped_approvals(
    api: tuple[Api, TestClient],
) -> None:
    server, client = api
    alice = bearer(server.settings, "alice", *ALL)
    reader = bearer(server.settings, "bob", "runs:read")
    bob = bearer(server.settings, "bob", "runs:read", "runs:approve")
    body = {"workflow": "research_ticket", "input": {"topic": "cache"}, "run_id": "api-run"}
    created = client.post("/v1/runs", json=body, headers=alice)
    assert created.status_code == 201
    assert created.json()["status"] == "pending"
    assert client.post("/v1/runs", json=body, headers=alice).status_code == 200
    conflict = client.post("/v1/runs", json={**body, "input": {"topic": "db"}}, headers=alice)
    assert conflict.status_code == 409

    server.work()
    pending = client.get(
        "/v1/approvals", params={"run_id": "api-run", "status": "pending"}, headers=reader
    ).json()
    assert [item["name"] for item in pending] == ["create_ticket"]
    assert pending[0]["args"]["title"] == "Follow-up: cache"
    url = f"/v1/approvals/{pending[0]['approval_id']}"
    decision = {"decision": "approve", "call_hash": pending[0]["call_hash"]}
    starter = client.post(url, json=decision, headers=alice)
    assert (starter.status_code, starter.json()["error"]["code"]) == (403, "forbidden")
    assert client.post(url, json=decision, headers=reader).status_code == 403
    mismatch = client.post(url, json={**decision, "call_hash": "0" * 64}, headers=bob)
    assert mismatch.status_code == 409
    assert client.post(url, json=decision, headers=bob).json()["status"] == "approved"
    server.work()
    assert approve_all(server, client, "api-run", bob) == ["notify"]
    server.work()

    run = client.get("/v1/runs/api-run", headers=reader).json()
    assert run["status"] == "completed"
    assert run["result"]["tickets"] == ["T-1"]
    history = client.get("/v1/runs/api-run/history", headers=reader).json()
    assert history[0]["type"] == "RunStarted"
    assert history[-1]["type"] == "RunCompleted"
    assert client.get("/v1/runs/api-run/verify", headers=reader).json()["ok"] is True
    listed = client.get("/v1/runs", params={"status": "completed"}, headers=reader).json()
    assert [item["run_id"] for item in listed] == ["api-run"]


def test_routes_fail_closed_without_a_valid_token_or_scope(api: tuple[Api, TestClient]) -> None:
    server, client = api
    missing = client.get("/v1/runs")
    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == "Bearer"
    assert client.get("/v1/runs", headers={"Authorization": "Bearer nope"}).status_code == 401
    reader = bearer(server.settings, "bob", "runs:read")
    body = {"workflow": "research_ticket", "input": {"topic": "cache"}}
    forbidden = client.post("/v1/runs", json=body, headers=reader)
    assert (forbidden.status_code, forbidden.json()["error"]["code"]) == (403, "forbidden")


def test_without_a_signing_secret_every_protected_route_is_unauthorized(tmp_path: Path) -> None:
    server = Api(tmp_path, jwt_secret=None)
    other = TokenService(SecretStr(SECRET), issuer="rar-local", audience="rar-control-plane")
    headers = {"Authorization": f"Bearer {other.issue('alice', ALL)}"}
    with TestClient(server.app, raise_server_exceptions=False) as client:
        assert client.get("/v1/runs", headers=headers).status_code == 401
        assert client.get("/healthz").status_code == 200
    server.close()


def test_input_validation_size_limits_and_unknown_resources(tmp_path: Path) -> None:
    server = Api(tmp_path, max_input_bytes=64, max_body_bytes=2_000)
    alice = bearer(server.settings, "alice", *ALL)
    with TestClient(server.app, raise_server_exceptions=False) as client:
        big = {"workflow": "research_ticket", "input": {"topic": "x" * 100}}
        assert client.post("/v1/runs", json=big, headers=alice).status_code == 413
        huge = {"workflow": "research_ticket", "input": {"topic": "x" * 5_000}}
        assert client.post("/v1/runs", json=huge, headers=alice).status_code == 413
        unknown = {"workflow": "no_such_flow", "input": {}}
        assert client.post("/v1/runs", json=unknown, headers=alice).status_code == 422
        bad_id = {"workflow": "research_ticket", "input": {}, "run_id": "../etc"}
        assert client.post("/v1/runs", json=bad_id, headers=alice).status_code == 422
        extra = {"workflow": "research_ticket", "input": {}, "owner": "root"}
        assert client.post("/v1/runs", json=extra, headers=alice).status_code == 422
        for path in ("/v1/runs/missing", "/v1/runs/missing/history", "/v1/runs/missing/verify"):
            assert client.get(path, headers=alice).status_code == 404
        decision = {"decision": "approve", "call_hash": "a" * 64}
        assert (
            client.post("/v1/approvals/missing:1", json=decision, headers=alice).status_code == 404
        )
        assert client.get("/nope").json()["error"]["code"] == "not_found"
    server.close()


def test_signals_and_cancellation_through_the_api(api: tuple[Api, TestClient]) -> None:
    server, client = api
    alice = bearer(server.settings, "alice", *ALL)
    body = {"workflow": "research_ticket", "input": {"topic": "cache"}, "run_id": "cancel-me"}
    client.post("/v1/runs", json=body, headers=alice)
    server.work()
    signal = client.post(
        "/v1/runs/cancel-me/signals/nudge", json={"payload": {"n": 1}}, headers=alice
    )
    assert signal.status_code == 202
    assert client.post("/v1/runs/missing/signals/nudge", json={}, headers=alice).status_code == 404
    assert client.post("/v1/runs/cancel-me/cancel", headers=alice).status_code == 202
    server.work()
    assert client.get("/v1/runs/cancel-me", headers=alice).json()["status"] == "cancelled"
    assert client.post("/v1/runs/cancel-me/cancel", headers=alice).status_code == 409
    assert client.post("/v1/runs/missing/cancel", headers=alice).status_code == 404
    late = client.post("/v1/runs/cancel-me/signals/nudge", json={}, headers=alice)
    assert late.status_code == 409


def test_rate_limits_apply_per_token_subject(tmp_path: Path) -> None:
    server = Api(tmp_path, rate_limit_burst=2, rate_limit_per_second=0.001)
    alice = bearer(server.settings, "alice", "runs:read")
    bob = bearer(server.settings, "bob", "runs:read")
    with TestClient(server.app, raise_server_exceptions=False) as client:
        codes = [client.get("/v1/runs", headers=alice).status_code for _ in range(3)]
        assert codes == [200, 200, 429]
        assert client.get("/v1/runs", headers=bob).status_code == 200
        limited = client.get("/v1/runs", headers=alice)
        assert limited.headers["retry-after"].isdigit()
        forged = [
            client.get("/v1/runs", headers={"Authorization": f"Bearer forged-{i}"}).status_code
            for i in range(3)
        ]
        assert forged == [401, 401, 429]
        assert all(client.get("/healthz").status_code == 200 for _ in range(5))
    server.close()


def test_metrics_require_the_read_scope_and_expose_runtime_metrics(
    api: tuple[Api, TestClient],
) -> None:
    server, client = api
    alice = bearer(server.settings, "alice", *ALL)
    client.post(
        "/v1/runs",
        json={"workflow": "research_ticket", "input": {"topic": "cache"}},
        headers=alice,
    )
    server.work()
    assert client.get("/metrics").status_code == 401
    body = client.get("/metrics", headers=bearer(server.settings, "ops", "runs:read")).text
    for name in (
        "journal_append_latency_seconds_count",
        "replay_duration_seconds_count",
        "nondeterminism_errors_total",
        "lease_reclaims_total",
        "stale_fencing_rejections_total",
        'activations_total{outcome="waiting"}',
    ):
        assert name in body
    health = client.get("/healthz")
    for header, value in SECURITY_HEADERS.items():
        assert health.headers[header] == value


def test_readiness_checks_the_database_scheduler_and_lease_loops(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, poll_seconds=0.05)
    app = create_control_app(settings, start_worker=True)
    with TestClient(app, raise_server_exceptions=False) as client:
        deadline = time.monotonic() + 10
        response = client.get("/readyz")
        while response.status_code != 200 and time.monotonic() < deadline:
            time.sleep(0.05)
            response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json()["checks"] == {"db": "ok", "scheduler": "ok", "lease_loop": "ok"}
    app.state.services.close()
    stopped = create_control_app(make_settings(tmp_path / "other"), start_worker=False)
    with TestClient(stopped, raise_server_exceptions=False) as client:
        assert client.get("/readyz").json()["checks"] == {"db": "ok"}
    stopped.state.services.close()
